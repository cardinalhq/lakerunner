# TCO: Loki vs. Cardinal Lakerunner

## Executive Summary

Grafana Loki and Cardinal Lakerunner solve different classes of problems—even
when they appear to overlap in log storage. Both Cardinal Lakerunner and Loki
answer immediate operational questions ("What just broke?"). Cardinal Lakerunner
adds long-term durable knowledge and provides a queryable decision system that
answers *"what is happening in my business, systems, and customers over time?"*

This distinction has major cost implications.

While Loki's initial deployment often looks inexpensive, its long-term TCO rises
sharply as data volume, retention requirements, query complexity, and
organizational usage grow. Cardinal Lakerunner, by contrast, is designed to
shift cost from expensive infrastructure to low-cost object storage and
analytics-friendly formats to reduce marginal cost as data scales.

Cardinal Lakerunner integrates with Grafana—teams keep their familiar dashboards
while gaining long-term analytics. Combined with Cardinal's Agent Builder,
observability data can drive automated workflows and cross-dataset insights.

---

## The Core Economic Difference

### Loki: Debugging Infrastructure

* Optimized for short-term, high-churn access
* Only for logs; metrics and traces require additional infrastructure
* Designed around label-based log streams
* Queries are fundamentally forensic and short-term analytical
* Cost grows roughly linearly (or worse) with ingestion volume and retention

### Cardinal Lakerunner: Observability as a Decision System

* Optimized for short-term and long-term forensics and analytics
* Treats logs, metrics, and traces as structured datasets
* Enables trend analysis, behavioral insight, cost attribution, and automation
* Cost grows sub-linearly due to compression, compaction, and object storage
  economics

**Bottom line:** Loki helps engineers resolve incidents.
Cardinal Lakerunner helps organizations make better decisions—with the same
data.

---

## The Hidden Cost: Missed Insight

Loki's real TCO issue isn't just infrastructure—it's opportunity cost.

Because Loki is designed for debugging:

* Queries answer what happened, not why, from log data
* Cross-dataset analysis is limited
* Historical trend analysis is expensive and slow
* Logs rarely influence product, finance, or operations decisions

Cardinal Lakerunner changes the economic role of observability:

**Observability becomes a decision system, not just a debugging tool.**

With Cardinal Lakerunner, teams can:

* Analyze customer behavior from logs
* Correlate cost, performance, and reliability
* Detect long-term trends and anomalies
* Feed observability data into automation and planning

These outcomes compound in value over time—while storage cost stays low.

---

## Cost Drivers Compared

### Storage Costs

| Dimension | Loki | Cardinal Lakerunner |
| - | - | - |
| Primary storage | High cost SSD for speed | Low-cost Object storage (S3, Google Cloud Storage, etc) |
| Storage format | Proprietary | Open: Apache Parquet |
| Compression efficiency | Moderate | Very high |
| Retention cost | High beyond short windows | Low and linear, even at years of retention |

**Key insight:** Loki's storage model is optimized for recent access, not
long-term value. Storing months or years of logs for analysis quickly becomes
expensive.

Cardinal Lakerunner stores data in analytics-optimized formats, allowing
multi-year retention at a fraction of the cost—often orders of magnitude
cheaper than traditional log systems.

---

### Indexing Costs

Loki relies heavily on label indexes:

* High-cardinality labels increase memory, CPU, and operational cost
* Teams are often forced to limit labels, which limits insight
* Accidents where high-cardinality labels are added can quickly increase cost

Cardinal Lakerunner takes a different approach:

* Cardinal Lakerunner loves cardinality
* Minimal ingest-time indexing
* Only a lightweight overview index is maintained outside of object storage
* Full detail lives in S3-compatible storage and is accessed only when needed

**Result:** Cardinal Lakerunner dramatically reduces the amount of expensive
non-S3 infrastructure while still enabling rich, flexible queries later.

---

### Compute Costs

| Aspect | Loki | Cardinal Lakerunner |
| - | - | - |
| Compute Model | Reserved or On-Demand | Spot or Preemptable |
| Storage Model | SSD recommended for query speed | Object Storage with minimal SQL index |
| Query execution | Always online | On-demand |
| Idle cost | High | Automatic on-demand scaling |
| Heavy queries | Query and ingestion scale together | Isolated workloads for fine-grained scaling |

With Loki, every query competes with ingestion and indexing. As usage grows
across teams, this leads to over-provisioning, query throttling, and rising
infrastructure spend.

Cardinal Lakerunner decouples ingestion from analysis: data is processed once,
queries spin up compute only when needed, and the system does not need to be
sized for peak analytical demand 24/7.

---

### Operational Overhead

#### Loki

* Careful label hygiene required
* Scaling challenges with high cardinality
* Continuous tuning as usage grows
* Debugging the debugger becomes a cost center

#### Cardinal Lakerunner

* Ingest once, analyze many times
* Fewer hot paths
* Cloud-native with cost-effective deployment models
* Object storage handles durability and scale
* Predictable cost model based on signal volume

---

## Teams May Start with Loki

Teams often begin with Loki when:

* You only need short-term debugging
* Retention is measured in days or weeks
* Logs are viewed primarily by engineers
* Cost growth is not a major concern (yet)

---

## Cardinal Lakerunner Wins on TCO

Cardinal Lakerunner delivers lower total cost when:

* Logs are used beyond incident response
* You want observability to inform business and operational decisions
* Retention matters (months or years)
* You want predictable, declining cost per GB over time
* You want more than just logs
