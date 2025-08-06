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

package cmd

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:   "sysinfo",
		Short: "Print Go runtime + cgroup limits & sanity-check alignment",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runSysinfo()
		},
	}

	rootCmd.AddCommand(cmd)
}

func runSysinfo() error {
	// 1) Go runtime basics
	numHost := runtime.NumCPU()
	gomax := runtime.GOMAXPROCS(0)
	gogc := os.Getenv("GOGC")
	if gogc == "" {
		gogc = "<unset>"
	}
	gomem := os.Getenv("GOMEMLIMIT")
	if gomem == "" {
		gomem = "<unset>"
	}

	fmt.Println("=== Go Runtime ===")
	fmt.Printf("  GOOS:           %s\n", runtime.GOOS)
	fmt.Printf("  GOARCH:         %s\n", runtime.GOARCH)
	fmt.Printf("  NumCPU (host):  %d\n", numHost)
	fmt.Printf("  GOMAXPROCS:     %d\n", gomax)
	fmt.Printf("  GOGC:           %s\n", gogc)
	fmt.Printf("  GOMEMLIMIT:     %s\n", gomem)

	// 2) cgroup CPU quota
	quotaCores, rawQuota, err := getCPUQuotaCores()
	fmt.Println("\n=== cgroup CPU quota ===")
	if err != nil {
		fmt.Printf("  error reading quota: %v\n", err)
	} else if quotaCores <= 0 {
		fmt.Printf("  none (unlimited) — raw: %q\n", rawQuota)
	} else {
		fmt.Printf("  %.2f cores  — raw: %q\n", quotaCores, rawQuota)
	}

	// 3) sanity checks
	fmt.Println("\n=== Config sanity check ===")
	// 3a) CPU vs GOMAXPROCS
	if quotaCores > 0 {
		if gomax == numHost {
			fmt.Printf("⚠️  GOMAXPROCS=%d equals host NumCPU=%d; quota not applied\n", gomax, numHost)
		} else if float64(gomax) > quotaCores {
			fmt.Printf("⚠️  GOMAXPROCS=%d exceeds container quota of %.2f cores\n", gomax, quotaCores)
		}
	}

	// 3b) GOGC set?
	if gogc == "<unset>" {
		fmt.Println("⚠️  GOGC is unset; using default=100 (you may want GOGC=50 or lower)")
	}

	// 3c) GOMEMLIMIT set?
	if gomem == "<unset>" {
		fmt.Println("⚠️  GOMEMLIMIT is unset; no heap cap (you may want to set this below your pod memory limit)")
	}

	// 4) Go memory snapshot
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Println("\n=== Go Memory Stats (MiB) ===")
	fmt.Printf("  HeapAlloc:  %6d\n", ms.HeapAlloc/1024/1024)
	fmt.Printf("  HeapSys:    %6d\n", ms.HeapSys/1024/1024)
	fmt.Printf("  StackInuse:%6d\n", ms.StackInuse/1024/1024)
	fmt.Printf("  Sys:        %6d\n", ms.Sys/1024/1024)
	fmt.Printf("  NumGC:      %6d\n", ms.NumGC)

	return nil
}

// getCPUQuotaCores reads cgroup v2 cpu.max or v1 cpu.cfs_* and returns
// the effective quota in cores (e.g. 150000/100000 = 1.5 cores).
// rawQuota is the raw file contents for diagnostic.
func getCPUQuotaCores() (cores float64, rawQuota string, err error) {
	// Try cgroup v2
	const v2path = "/sys/fs/cgroup/cpu.max"
	if data, e := os.ReadFile(v2path); e == nil {
		raw := strings.TrimSpace(string(data))
		rawQuota = raw
		parts := strings.Fields(raw)
		if len(parts) >= 2 {
			if parts[0] == "max" {
				return 0, raw, nil
			}
			q, err1 := strconv.ParseFloat(parts[0], 64)
			p, err2 := strconv.ParseFloat(parts[1], 64)
			if err1 == nil && err2 == nil && p > 0 {
				return q / p, raw, nil
			}
		}
		return 0, rawQuota, fmt.Errorf("unexpected format")
	}

	// Fallback to cgroup v1
	rawQ, errQ := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
	rawP, errP := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
	if errQ == nil && errP == nil {
		sq := strings.TrimSpace(string(rawQ))
		sp := strings.TrimSpace(string(rawP))
		rawQuota = fmt.Sprintf("%s %s", sq, sp)
		if sq != "-1" {
			q, err1 := strconv.ParseFloat(sq, 64)
			p, err2 := strconv.ParseFloat(sp, 64)
			if err1 == nil && err2 == nil && p > 0 {
				return q / p, rawQuota, nil
			}
		}
		return 0, rawQuota, nil
	}

	return 0, "", fmt.Errorf("no cgroup CPU quota files available")
}
