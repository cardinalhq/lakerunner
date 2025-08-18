<p align="center">
  <img src="./assets/lakerunner-chip.png" alt="LakeRunner Logo" height="120">
</p>

<h1 align="center">LakeRunner</h1>
<h3 align="center">Real-Time Telemetry, Straight From Your S3 Bucket</h3>

<p align="center">
<strong>LakeRunner</strong> is an event-driven ingestion engine that turns your S3-compatible object store into a blazing-fast observability backend. It watches for structured telemetry (CSV, Parquet, JSON.gz), transforms it into optimized Apache Parquet, then handles indexing, aggregation, and compaction, all in real time. Paired with our Grafana plugin, your bucket becomes instantly queryable — no black boxes, no vendor lock-in.
</p>

---

## Features

- Automatic ingestion from S3 on object creation
- Event-driven Architecture enabled by S3 Object Notifications. 
- Native support for OpenTelemetry log/metric proto files
- Seamless ingestion for `OTEL Proto`, `CSV` or `.json.gz` formats.
- Works with DataDog, FluentBit, or your custom source!
- Get started locally in <5 mins with the install script
- `docker-compose` support <em> coming soon! </em>

---

## Table of Contents

- [End User Guide](#end-user-guide)
  - [Demo Setup](#demo)
  - [S3 Event Setup](#s3-event-setup)
  - [Next Steps - Get the CLI and Grafana Plugin!](#next-steps)
  - [Configuration](#configuration)
- [License](#license)


## End User Guide

### Demo setup

#### Prerequisites

1. [Docker Desktop](https://docs.docker.com/desktop/), [Rancher Desktop](https://docs.rancherdesktop.io/getting-started/installation/), or equivalent
2. [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation), [minikube](https://minikube.sigs.k8s.io/docs/start/), or equivalent (`brew install kind` or `brew install minikube`)
3. [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) (`brew install kubectl`)
4. [Helm](https://helm.sh/docs/intro/install/) 3.14+ (`brew install helm`)

#### Install LakeRunner Demo (Laptop Version)

1. Create a local cluster using `kind` or `minikube`

```bash copy
kind create cluster
```

2. Ensure that your `kubectl` context is set to the local cluster.

```bash copy
kubectl config use-context kind-kind
```

3. Download and run the LakeRunner install script.

```bash copy
curl -sSL -o install.sh https://raw.githubusercontent.com/cardinalhq/lakerunner-cli/main/scripts/install.sh
chmod +x install.sh
./install.sh
```

4. Follow the on-screen prompts until the installation is complete.

_(We recommend using the default values for all prompts during a local install for the fastest and most seamless experience.)_

##### Explore Data in Grafana

The LakeRunner install script also installs a local [Grafana](https://grafana.com/oss/grafana/), bundled with a preconfigured Cardinal LakeRunner Datasource plugin.

Wait for ~5 minutes for the OpenTelemetry Demo apps to generate some sample telemetry data. Then, access Grafana at `http://localhost:3000` and login with the default credentials:

- Username: `admin`
- Password: `admin`

Navigate to the `Explore` tab, select the `Cardinal` datasource, and try running some queries to explore logs and metrics.

1. [Docker Desktop](https://docs.docker.com/desktop/), [Rancher Desktop](https://docs.rancherdesktop.io/getting-started/installation/), or equivalent
2. [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation), [minikube](https://minikube.sigs.k8s.io/docs/start/), or equivalent (`brew install kind` or `brew install minikube`)
3. [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) (`brew install kubectl`)
4. [Helm](https://helm.sh/docs/intro/install/) 3.14+ (`brew install helm`)


We will discuss configuration options in the next section

> **Note:**  
> Need help? Join the [LakeRunner community on Slack](https://join.slack.com/t/birdhousebycardinalhq/shared_invite/zt-34ds8vt2t-YhGFtoG5NjJX238cMXdLGw) or email us at **support@cardinalhq.io**.  
>  
> **CardinalHQ offers managed LakeRunner deployments.**


### Next Steps

Once you have Lakerunner installed, you are ready to explore the data! You can use the grafana plugin that is bundled in the helm chart, and optionally setup the [Lakerunner CLI](https://github.com/cardinalhq/lakerunner-cli) from either the [releases page](https://github.com/cardinalhq/lakerunner-cli/releases), or with brew


```
brew tap cardinalhq/lakerunner-cli
brew install lakerunner-cli
```

You just need to set the `LAKERUNNER_QUERY_URL` and `LAKERUNNER_API_KEY` urls in the env, and you should be able to explore logs from the CLI!

For a local demo installation with default values, you can run:

```bash copy
export LAKERUNNER_QUERY_URL="http://localhost:7101"
export LAKERUNNER_API_KEY="test-key"
```
