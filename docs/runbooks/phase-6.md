# Phase 6 — Kubernetes: k3d, Manifests, Operators

**What you build:** the platform-engineering layer. A local 3-node
Kubernetes cluster (k3d — K8s inside Docker), the core stack redeployed as
real manifests (StatefulSet Postgres, Deployment MinIO + bootstrap Job,
the generator from a locally-built image), and Kafka run the way real
teams run it on K8s: **declared as a custom resource and reconciled by the
Strimzi operator**.

**What you learn:** why data infra on K8s is different (state!), the core
primitives — Deployment vs StatefulSet vs Job, Services & DNS, probes,
resources, PVCs — Kustomize for config generation, Helm for installing
operators, and the operator/CRD pattern that dominates modern data
platforms (Strimzi, Spark Operator, CloudNativePG, Flink Operator…).

---

## Prerequisites

```bash
# macOS: brew install k3d kubectl helm     (Linux: see each project's docs)
k3d version && kubectl version --client && helm version
```

RAM note: run `make down` first — the compose stack and the k8s cluster
don't need to run simultaneously (that's rather the point of this phase).

## Run it

```bash
make k8s-up        # 3-node cluster in Docker (~1 min)
make k8s-core      # build+import generator image, apply k8s/core via kustomize
make k8s-status    # watch until postgres-0, minio, generator are Running
make k8s-kafka     # helm-install Strimzi, declare Kafka + topics
make k8s-status    # watch the operator create shopstream-dual-role-0, entity operator...
```

Then enable clickstream: uncomment `KAFKA_BOOTSTRAP` in
`k8s/core/generator.yaml` (`shopstream-kafka-bootstrap:9092` — the service
Strimzi created) and re-run `make k8s-core`. Note what happens: only the
generator Deployment changes, and it *rolls* — new pod up, old pod down.

Reaching things from the host: Postgres `localhost:30432`, MinIO console
`http://localhost:30901` (NodePorts), anything else via
`kubectl -n shopstream port-forward svc/<name> <port>:<port>`.

## Compose ↔ Kubernetes translation table

| docker-compose concept | Kubernetes equivalent | Where in this repo |
|---|---|---|
| service with a volume | StatefulSet + volumeClaimTemplate | `k8s/core/postgres.yaml` |
| stateless service | Deployment | `minio.yaml`, `generator.yaml` |
| one-shot init service | Job | `minio-init` in `minio.yaml` |
| healthcheck | readiness/liveness probes | postgres, minio |
| service name DNS | Service (ClusterIP) | all |
| ports: mapping | NodePort / port-forward | `*-external` services |
| env / mounted config | Secret / ConfigMap (via Kustomize generators) | `kustomization.yaml` |
| the whole kafka block | one `Kafka` custom resource + operator | `k8s/kafka/kafka.yaml` |

## Things worth doing before moving on

1. **Kill things.** `kubectl -n shopstream delete pod postgres-0` — watch
   it come back with the same name *and the same data* (the PVC survived).
   Delete the generator pod — new pod, new name, no data to care about.
   That's Deployment vs StatefulSet in one minute.
2. **Scale the generator:** `kubectl -n shopstream scale deploy/generator --replicas=3`
   — traffic triples; nothing breaks (each replica is an independent app).
   Now ask: what if you scaled a *consumer* that way? (Consumer groups
   rebalance — that's why streaming jobs scale safely and why the
   generator's event volume just multiplied.)
3. **Watch the operator work.** `kubectl -n shopstream get kafka shopstream -o yaml`
   — read `status:`. Then change `spec.kafka.config` (add
   `log.retention.hours: 24`), apply, and watch Strimzi roll the broker.
   You changed a YAML field; the operator did the operational work.
4. **Declare a topic.** Copy a `KafkaTopic` block, apply it, and find it
   with `kafka-topics --list` from inside the broker pod. Infrastructure
   as data, reconciled continuously.
5. **Starve a pod.** Set the generator's memory limit to `64Mi`, apply,
   and watch it OOMKill-loop (`kubectl get pods -w`). Limits are real.

## Going further (guided exercises, not shipped)

The remaining services all have first-class charts/operators — migrating
them is the same motion repeated, so they're exercises rather than code:

- **Airflow:** `helm install airflow apache-airflow/airflow` — the
  official chart; wire `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` at the
  in-cluster Postgres, mount DAGs via git-sync (the standard pattern).
- **Spark:** the Kubeflow `spark-operator` + a `SparkApplication` CR per
  job in `jobs/streaming/` — replacing our `local[*]` containers with
  driver/executor pods.
- **ClickHouse:** Altinity `clickhouse-operator`; **Debezium/Connect:**
  Strimzi also manages `KafkaConnect` + `KafkaConnector` CRs (yes, the
  connector JSON becomes a CR too).
- **Prometheus/Grafana:** `kube-prometheus-stack` chart — and it starts
  scraping the cluster itself, not just the pipeline.

## In production you would…

- use a managed control plane (EKS/GKE/AKS) + node groups sized per
  workload; keep stateful systems on dedicated node pools or managed
  services entirely;
- manage everything via GitOps (ArgoCD/Flux) — `kubectl apply` from a
  laptop is the local-learning shortcut;
- set requests/limits from measured usage, PodDisruptionBudgets, and
  network policies; secrets from a manager, not `secretGenerator`.

## Graduation

That's the full design implemented: OLTP + events → streaming + CDC →
lakehouse → warehouse → BI, with quality gates, observability, and now
the runtime real platforms use. From here, the best learning is breaking
it and fixing it. The design doc's §9 stretch ideas (Flink, Trino,
Airbyte, OpenLineage, a real cloud warehouse) are all natural next PRs.
