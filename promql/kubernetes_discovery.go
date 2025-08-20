// Copyright (C) 2025 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package promql

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/labels"
	informers "k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	discinformers "k8s.io/client-go/informers/discovery/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	disclisters "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

type Worker struct {
	IP   string
	Port int
}

type SegmentWorkerMapping struct {
	SegmentID int64
	Worker    Worker
}

type KubernetesWorkerDiscovery struct {
	// config
	namespace           string
	workerLabelSelector string
	workerPort          int

	// clients/caches
	clientset *kubernetes.Clientset
	factory   informers.SharedInformerFactory
	svcInf    coreinformers.ServiceInformer
	esInf     discinformers.EndpointSliceInformer
	svcList   corelisters.ServiceLister
	esList    disclisters.EndpointSliceLister

	// state
	mu         sync.RWMutex
	workers    []Worker
	running    bool
	cancelFunc context.CancelFunc

	// debounce rebuilds
	debounceMu     sync.Mutex
	debounceTimer  *time.Timer
	debounceDelay  time.Duration
	lastRebuildErr error
}

var _ WorkerDiscovery = (*KubernetesWorkerDiscovery)(nil)

type KubernetesWorkerDiscoveryConfig struct {
	Namespace           string // REQUIRED (or via POD_NAMESPACE)
	WorkerLabelSelector string // REQUIRED, selector applied to Services
	WorkerPort          int    // Fallback when no port found on ES/Service; defaults to 8080
	// If true, include IPv6 endpoints as well.
	AllowIPv6 bool
}

func NewKubernetesWorkerDiscovery(cfg KubernetesWorkerDiscoveryConfig) (*KubernetesWorkerDiscovery, error) {
	ns := cfg.Namespace
	if ns == "" {
		if v := os.Getenv("POD_NAMESPACE"); v != "" {
			ns = v
		} else {
			return nil, fmt.Errorf("namespace is required")
		}
	}
	if cfg.WorkerLabelSelector == "" {
		return nil, fmt.Errorf("WorkerLabelSelector is required")
	}
	if cfg.WorkerPort == 0 {
		cfg.WorkerPort = 8080
	}

	// Prefer in-cluster; fallback to kubeconfig for local dev
	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		if kc, err2 := clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile); err2 == nil {
			k8sConfig = kc
		} else {
			return nil, fmt.Errorf("k8s config error (in-cluster/kubeconfig): %w / %v", err, err2)
		}
	}
	cs, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return nil, fmt.Errorf("create clientset: %w", err)
	}

	factory := informers.NewSharedInformerFactoryWithOptions(
		cs,
		0,
		informers.WithNamespace(ns),
	)

	svcInf := factory.Core().V1().Services()
	esInf := factory.Discovery().V1().EndpointSlices()

	return &KubernetesWorkerDiscovery{
		namespace:           ns,
		workerLabelSelector: cfg.WorkerLabelSelector,
		workerPort:          cfg.WorkerPort,
		clientset:           cs,
		factory:             factory,
		svcInf:              svcInf,
		esInf:               esInf,
		svcList:             svcInf.Lister(),
		esList:              esInf.Lister(),
	}, nil
}

func (k *KubernetesWorkerDiscovery) Start(ctx context.Context) error {
	k.mu.Lock()
	if k.running {
		k.mu.Unlock()
		return fmt.Errorf("worker discovery is already running")
	}
	k.running = true
	k.mu.Unlock()

	slog.Info("Starting worker discovery",
		"namespace", k.namespace,
		"svcSelector", k.workerLabelSelector)

	// Event handlers → debounce → rebuild
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { k.scheduleRebuild() },
		UpdateFunc: func(any, any) { k.scheduleRebuild() },
		DeleteFunc: func(any) { k.scheduleRebuild() },
	}
	if _, err := k.svcInf.Informer().AddEventHandler(handler); err != nil {
		return fmt.Errorf("failed to add event handler to service informer: %w", err)
	}
	if _, err := k.esInf.Informer().AddEventHandler(handler); err != nil {
		return fmt.Errorf("failed to add event handler to endpoint informer: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	k.cancelFunc = cancel

	// Start informers
	go k.factory.Start(runCtx.Done())

	// Wait for caches
	if ok := cache.WaitForCacheSync(
		runCtx.Done(),
		k.svcInf.Informer().HasSynced,
		k.esInf.Informer().HasSynced,
	); !ok {
		if err := k.Stop(); err != nil {
			slog.Error("failed to stop during cache sync failure", "error", err)
		}
		return fmt.Errorf("informer cache sync failed")
	}

	// Initial build
	if err := k.rebuildWorkers(runCtx); err != nil {
		slog.Error("initial worker rebuild failed", "error", err)
	}

	return nil
}

func (k *KubernetesWorkerDiscovery) Stop() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.running {
		return nil
	}
	k.running = false
	if k.cancelFunc != nil {
		k.cancelFunc()
		k.cancelFunc = nil
	}
	// Stop any pending debounce
	k.debounceMu.Lock()
	if k.debounceTimer != nil {
		k.debounceTimer.Stop()
		k.debounceTimer = nil
	}
	k.debounceMu.Unlock()
	return nil
}

func (k *KubernetesWorkerDiscovery) scheduleRebuild() {
	k.debounceMu.Lock()
	defer k.debounceMu.Unlock()
	if k.debounceTimer != nil {
		k.debounceTimer.Stop()
	}
	k.debounceTimer = time.AfterFunc(k.debounceDelay, func() {
		// Use a bounded context for the rebuild
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := k.rebuildWorkers(ctx); err != nil {
			slog.Error("worker rebuild failed", "error", err)
			k.lastRebuildErr = err
		} else {
			k.lastRebuildErr = nil
		}
	})
}

func (k *KubernetesWorkerDiscovery) rebuildWorkers(ctx context.Context) error {
	selector, err := labels.Parse(k.workerLabelSelector)
	if err != nil {
		return fmt.Errorf("invalid label selector %q: %w", k.workerLabelSelector, err)
	}

	svcs, err := k.svcList.Services(k.namespace).List(selector)
	if err != nil {
		return fmt.Errorf("list services (cache): %w", err)
	}

	seen := make(map[string]struct{}, 32)
	out := make([]Worker, 0, 32)

	for _, svc := range svcs {
		esSelector := labels.Set{
			discoveryv1.LabelServiceName: svc.Name,
		}.AsSelector()

		esList, err := k.esList.EndpointSlices(k.namespace).List(esSelector)
		if err != nil {
			slog.Error("list EndpointSlices (cache)", "service", svc.Name, "error", err)
			continue
		}

		defPort := k.workerPort
		if len(svc.Spec.Ports) > 0 && svc.Spec.Ports[0].Port != 0 {
			defPort = int(svc.Spec.Ports[0].Port)
		}

		for _, es := range esList {
			esPort := defPort
			if len(es.Ports) > 0 && es.Ports[0].Port != nil {
				esPort = int(*es.Ports[0].Port)
			}

			for _, ep := range es.Endpoints {
				ready := ep.Conditions.Ready != nil && *ep.Conditions.Ready
				serving := ep.Conditions.Serving == nil || *ep.Conditions.Serving
				terminating := ep.Conditions.Terminating != nil && *ep.Conditions.Terminating
				if !ready || !serving || terminating {
					continue
				}

				for _, addr := range ep.Addresses {
					ip := net.ParseIP(addr)
					if ip == nil || ip.To4() == nil {
						continue
					}
					key := addr + ":" + strconv.Itoa(esPort)
					if _, ok := seen[key]; ok {
						continue
					}
					seen[key] = struct{}{}
					out = append(out, Worker{IP: addr, Port: esPort})
				}
			}
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].IP == out[j].IP {
			return out[i].Port < out[j].Port
		}
		return out[i].IP < out[j].IP
	})

	k.mu.Lock()
	k.workers = out
	k.mu.Unlock()

	slog.Info("Worker snapshot updated", "namespace", k.namespace, "totalWorkers", len(out))
	return nil
}

func (k *KubernetesWorkerDiscovery) GetWorkersForSegments(organizationID uuid.UUID, segmentIDs []int64) ([]SegmentWorkerMapping, error) {
	k.mu.RLock()
	ws := make([]Worker, len(k.workers))
	copy(ws, k.workers)
	k.mu.RUnlock()

	if len(ws) == 0 {
		return nil, fmt.Errorf("no workers available")
	}

	mappings := make([]SegmentWorkerMapping, 0, len(segmentIDs))
	for _, seg := range segmentIDs {
		w := k.assignSegmentToWorker(organizationID, seg, ws)
		mappings = append(mappings, SegmentWorkerMapping{SegmentID: seg, Worker: w})
	}
	return mappings, nil
}

func (k *KubernetesWorkerDiscovery) GetAllWorkers() ([]Worker, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	ws := make([]Worker, len(k.workers))
	copy(ws, k.workers)
	return ws, nil
}

func (k *KubernetesWorkerDiscovery) assignSegmentToWorker(org uuid.UUID, seg int64, ws []Worker) Worker {
	if len(ws) == 0 {
		return Worker{}
	}
	segKey := fmt.Sprintf("%d:%s", seg, org.String())

	var best Worker
	var bestHash uint64
	for i, w := range ws {
		wk := w.IP + ":" + strconv.Itoa(w.Port)
		hv := xxhash.Sum64String(segKey + wk)
		if i == 0 || hv > bestHash {
			best, bestHash = w, hv
		}
	}
	return best
}
