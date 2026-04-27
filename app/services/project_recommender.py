"""
app/services/project_recommender.py — Deterministic portfolio project recommender.

Design contract:
  - Grounded: every recommendation is anchored in verified evidence buckets from
    the candidate profile — never fabricates experience the candidate does not have.
  - Honest: unsupported gaps are named as gaps; adjacent evidence is framed as such.
  - Scoped: always includes a 'minimum shippable slice' version the candidate can
    actually build in a focused weekend sprint.
  - Deterministic: same inputs always produce the same output.
  - No LLM: pure rules, templates, and vocabulary matching.

Public API:
  recommend_project(job_id, conn, profile, extracted, assessment, projects, label)
      -> ProjectRecommendationResult

Two outputs per call:
  1. new_project — a tightly scoped new project concept targeting the primary gap
  2. reposition_existing — how to reframe an existing project (or None if no
     credible candidate is found)
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, field
from typing import Optional

from app.services.scorer import (
    _ALL_VOCAB,
    _EVIDENCE_WEIGHT,
    _build_skill_map,
    _extract_vocab_terms,
    _lookup_skill,
    _normalize,
    _parse_jd_sections,
)
from app.services.project_loader import _is_real_project


# ── Tuning constants ──────────────────────────────────────────────────────────

_CAT_WEIGHT: dict[str, float] = {
    "required":  1.5,
    "preferred": 1.0,
    "domain":    0.5,
}
_ABSENT_EVIDENCE_WEIGHT = 0.3

# Minimum project relevance score to qualify as a reposition candidate
_MIN_REPOSITION_SCORE = 0.5

# Terms that cannot be demonstrated with a portfolio code project
_NON_PROJECT_TERMS = frozenset({
    "agile", "scrum", "git", "rest", "sql", "communication",
    "leadership", "mentoring", "documentation",
})


# ── Display names for normalised terms ───────────────────────────────────────

_GAP_DISPLAY: dict[str, str] = {
    "kafka":           "Apache Kafka",
    "spark":           "Apache Spark",
    "airflow":         "Apache Airflow",
    "kubernetes":      "Kubernetes",
    "k8s":             "Kubernetes",
    "dbt":             "dbt",
    "elasticsearch":   "Elasticsearch",
    "redis":           "Redis",
    "postgresql":      "PostgreSQL",
    "mongodb":         "MongoDB",
    "terraform":       "Terraform",
    "docker":          "Docker",
    "graphql":         "GraphQL",
    "grpc":            "gRPC",
    "fastapi":         "FastAPI",
    "django":          "Django",
    "flask":           "Flask",
    "mlflow":          "MLflow",
    "prometheus":      "Prometheus",
    "grafana":         "Grafana",
    "flink":           "Apache Flink",
    "rabbitmq":        "RabbitMQ",
    "celery":          "Celery",
    "snowflake":       "Snowflake",
    "bigquery":        "BigQuery",
    "redshift":        "Amazon Redshift",
    "dynamodb":        "DynamoDB",
    "sqs":             "Amazon SQS",
    "sns":             "Amazon SNS",
    "rds":             "Amazon RDS",
    "eks":             "Amazon EKS",
    "ecs":             "Amazon ECS",
    "s3":              "Amazon S3",
    "lambda":          "AWS Lambda",
    "aws":             "AWS",
    "gcp":             "Google Cloud",
    "azure":           "Azure",
    "python":          "Python",
    "typescript":      "TypeScript",
    "golang":          "Go",
    "rust":            "Rust",
    "java":            "Java",
    "scala":           "Scala",
    "pytorch":         "PyTorch",
    "tensorflow":      "TensorFlow",
    "scikit-learn":    "scikit-learn",
    "machine learning":"Machine Learning",
    "data engineering":"Data Engineering",
    "microservices":   "Microservices",
    "ci/cd":           "CI/CD",
    "devops":          "DevOps",
    "mlops":           "MLOps",
    "security":        "Security",
}


def _dn(term: str) -> str:
    """Return a human-readable display name for a normalised vocab term."""
    return _GAP_DISPLAY.get(term, term.title() if " " not in term else term.title())


# ── Gap-to-project templates ──────────────────────────────────────────────────
# Each entry maps a normalised tech term to a scoped project concept.
# Keys in title/description/scoped_version/resume_value/notes are substituted:
#   {lang}      — candidate's primary programming language (e.g. Python)
#   {db}        — candidate's primary database (e.g. PostgreSQL)
#   {gap}       — normalised gap term (e.g. kafka)
#   {gap_d}     — display-name gap term (e.g. Apache Kafka)

_GAP_TEMPLATES: dict[str, dict] = {
    "kafka": {
        "business_problem": (
            "Process high-volume event streams reliably without data loss or duplication"
        ),
        "title_template":   "{lang} Event Pipeline with {gap_d}",
        "description":      (
            "A producer/consumer pipeline that generates simulated domain events, routes "
            "them through {gap_d} topics, applies stateful transformations, and writes "
            "aggregated results to {db}. Demonstrates consumer group coordination, offset "
            "management, and exactly-once framing."
        ),
        "stack_additions":  ["kafka"],
        "scoped_version":   (
            "One producer script generating 500 events/min, two consumer groups writing "
            "separate aggregates to {db}. Deployable via docker-compose with a single "
            "`docker compose up`. README includes an offset-reset replay demo."
        ),
        "outcomes":         [
            "Processes 500+ events/min in local simulation; consumer lag ≤ 50 ms under load",
            "Offset reset restores replay of full topic without data loss or duplication",
            "Two independent consumer groups confirmed to maintain separate committed offsets",
        ],
        "resume_value":     (
            "Built a {gap_d} event-streaming pipeline; demonstrated consumer group "
            "coordination, offset management, and at-least-once delivery guarantees."
        ),
        "notes":            (
            "Use docker-compose for {gap_d} + ZooKeeper (or KRaft). Keep the domain simple "
            "(order events or sensor readings). The offset-management mechanics are the "
            "portfolio signal, not the application complexity."
        ),
    },

    "kubernetes": {
        "business_problem": (
            "Deploy and scale services reliably in a containerised production environment"
        ),
        "title_template":   "K8s-Deployed {lang} Service with Helm, HPA, and Health Probes",
        "description":      (
            "A {lang} REST service packaged in Docker, deployed to a local Kind or Minikube "
            "cluster via a Helm chart. Demonstrates liveness/readiness probes, "
            "HorizontalPodAutoscaler, and zero-downtime rolling updates validated under "
            "synthetic load."
        ),
        "stack_additions":  ["kubernetes", "docker"],
        "scoped_version":   (
            "Single FastAPI service with a /health endpoint. Helm chart with HPA "
            "(min=1, max=3, CPU threshold=50%). Rolling update strategy. All resources "
            "fit in a 4-CPU laptop cluster. Deploy-to-demo in under 5 min."
        ),
        "outcomes":         [
            "Zero-downtime rolling update confirmed: k6 load test shows 0 failed requests during deploy",
            "HPA scales from 1 to 3 replicas under synthetic CPU spike; scales back within 60 s",
            "Liveness probe detects injected unhealthy state and restarts pod automatically",
        ],
        "resume_value":     (
            "Deployed a {lang} service to Kubernetes with Helm; demonstrated HPA autoscaling, "
            "rolling updates, and health probe automation."
        ),
        "notes":            (
            "Use Kind (simpler) or Minikube. The application itself can be trivial — "
            "the Helm chart, HPA config, and rolling-update proof are the deliverables. "
            "Include the k6 load-test script in the repo."
        ),
    },

    "spark": {
        "business_problem": (
            "Transform and aggregate datasets at a scale that outgrows single-node Pandas"
        ),
        "title_template":   "PySpark Batch Pipeline with Partitioned {db} Output",
        "description":      (
            "A PySpark pipeline that reads a realistic public dataset (NYC taxi trips or "
            "similar), applies multi-stage transformations and window aggregations, and "
            "writes partitioned Parquet output. Demonstrates query-plan inspection and "
            "partition pruning."
        ),
        "stack_additions":  ["spark"],
        "scoped_version":   (
            "One PySpark script, one test file using PySpark's LocalSparkSession, and a "
            "README benchmarking the same query in Pandas vs. Spark on the same dataset."
        ),
        "outcomes":         [
            "Processes 2 GB NYC taxi dataset in < 60 s on 4 local cores",
            "Partition pruning reduces files scanned by 10× for date-filtered queries",
            "Unit tests for all transformation logic using LocalSparkSession (no cluster needed)",
        ],
        "resume_value":     (
            "Built a PySpark batch pipeline on a 2 GB public dataset; demonstrated "
            "partitioned I/O, window aggregations, and query-plan optimisation."
        ),
        "notes":            (
            "Install PySpark via pip. Use NYC Taxi or Wikipedia pageview data (public, free). "
            "Run in local[4] mode — no cluster required. The benchmarking README is a "
            "strong portfolio artifact."
        ),
    },

    "terraform": {
        "business_problem": (
            "Provision and tear down cloud infrastructure reproducibly with zero manual steps"
        ),
        "title_template":   "AWS Infrastructure-as-Code with Terraform Modules",
        "description":      (
            "A modular Terraform configuration that provisions a VPC, compute layer, and "
            "{db} instance on AWS. Demonstrates remote state (S3 + DynamoDB lock), "
            "parameterised modules, and safe destroy/recreate cycles."
        ),
        "stack_additions":  ["terraform", "aws"],
        "scoped_version":   (
            "Three Terraform modules: vpc/, compute/ (EC2 or ECS), db/ (RDS). "
            "Remote state in S3 with DynamoDB locking. Destroy and recreate verified "
            "to take < 5 min with no orphaned resources."
        ),
        "outcomes":         [
            "Full dev environment provisions in < 3 min; destroys cleanly with 0 orphaned resources",
            "Remote state prevents concurrent apply conflicts; tested with two terminal sessions",
            "`terraform plan` diff reviewed before every apply; plan output included in PR template",
        ],
        "resume_value":     (
            "Authored Terraform modules for AWS VPC, compute, and RDS; demonstrated "
            "remote state management and reproducible environment provisioning."
        ),
        "notes":            (
            "Use a free-tier AWS account. Store secrets in AWS SSM Parameter Store, "
            "not in .tfvars files committed to git. The module structure and state "
            "management are the portfolio signal."
        ),
    },

    "airflow": {
        "business_problem": (
            "Schedule and monitor complex data workflows with dependency tracking and retry logic"
        ),
        "title_template":   "Scheduled ETL DAG with Apache Airflow and {lang}",
        "description":      (
            "An Airflow DAG that extracts data from a public REST API, transforms it with "
            "{lang}, and loads results into {db}. Demonstrates sensors, XCom, configurable "
            "retry logic with exponential backoff, and SLA monitoring."
        ),
        "stack_additions":  ["airflow", "docker"],
        "scoped_version":   (
            "A 4-task DAG: HttpSensor → extract → transform → load. "
            "docker-compose from the official Airflow quickstart. "
            "SLA miss fires a log alert. Retry with backoff tested by injecting a flaky HTTP response."
        ),
        "outcomes":         [
            "Simulated upstream delay triggers sensor timeout and re-queue within configured SLA window",
            "Flaky-API retry with 3× exponential backoff confirmed; task succeeds on third attempt",
            "SLA miss fires a logged alert; DAG run state and duration visible in Airflow UI",
        ],
        "resume_value":     (
            "Built an Airflow DAG with HTTP sensors, exponential-backoff retries, "
            "and SLA monitoring; demonstrated dependency-tracked multi-step ETL."
        ),
        "notes":            (
            "Use the official docker-compose quickstart (CeleryExecutor or LocalExecutor). "
            "Pick a stable public API (OpenMeteo weather is free and reliable). "
            "Keep the DAG to ≤ 5 tasks — clean dependency graph is the deliverable."
        ),
    },

    "graphql": {
        "business_problem": (
            "Expose flexible, client-driven APIs that eliminate over-fetching and under-fetching"
        ),
        "title_template":   "{lang} GraphQL API with DataLoader and Subscriptions",
        "description":      (
            "A GraphQL API built with Strawberry ({lang}) over a {db} backend. "
            "Demonstrates type-safe schema, nested resolvers, DataLoader batching to "
            "eliminate N+1 queries, and a WebSocket subscription for real-time events."
        ),
        "stack_additions":  ["graphql"],
        "scoped_version":   (
            "Three types (e.g. Author, Book, Review), DataLoader for batched author lookups, "
            "and one subscription streaming new review events. "
            "N+1 problem demonstrated without DataLoader, then resolved — both logged."
        ),
        "outcomes":         [
            "N+1 query count drops from O(n) to O(1) DB calls after DataLoader; measured with query log",
            "Subscription streams 3 live events to a WebSocket client with < 10 ms fan-out",
            "Type-safe schema generates introspection docs; tested with pytest-asyncio",
        ],
        "resume_value":     (
            "Designed a GraphQL API with type-safe schema, DataLoader batching, "
            "and WebSocket subscriptions; measured N+1 elimination."
        ),
        "notes":            (
            "Use Strawberry (Python) — cleanest {lang} GraphQL library. "
            "Chinook SQLite is a good relational dataset for this. "
            "DataLoader batching mechanics are the portfolio highlight."
        ),
    },

    "redis": {
        "business_problem": (
            "Reduce database load and enforce rate limits at scale across distributed service instances"
        ),
        "title_template":   "Caching and Rate-Limiting Layer with Redis and {lang}",
        "description":      (
            "A {lang} service using Redis for cache-aside application caching, distributed "
            "token-bucket rate limiting (via a Redis Lua script), and pub/sub for real-time "
            "notifications. Load-tested to demonstrate measurable latency reduction."
        ),
        "stack_additions":  [],
        "scoped_version":   (
            "FastAPI service with three endpoints: one cached (cache-aside), one rate-limited "
            "(100 req/min per client via Lua token-bucket), and one pub/sub subscriber. "
            "k6 load script to benchmark cached vs. uncached p99."
        ),
        "outcomes":         [
            "Cache hit reduces p99 latency from ~40 ms (DB) to ~2 ms (Redis) under k6 load test",
            "Rate limiter enforces 100 req/min; burst of 150 req in 30 s → 50 requests rejected correctly",
            "Pub/sub fan-out delivers event to 3 subscriber instances in < 5 ms",
        ],
        "resume_value":     (
            "Implemented Redis cache-aside caching and distributed rate limiting; "
            "measured p99 latency reduction and confirmed burst protection under load."
        ),
        "notes":            (
            "Run Redis via Docker. Use redis-py. The Lua rate-limiter script is the "
            "technical highlight — include it and explain it in the README. "
            "Keep the application domain simple."
        ),
    },

    "elasticsearch": {
        "business_problem": (
            "Enable fast, relevant full-text search across a large document corpus"
        ),
        "title_template":   "Full-Text Search Service with Elasticsearch and {lang}",
        "description":      (
            "A {lang} service that indexes a realistic document corpus (e.g. Wikipedia "
            "abstracts), exposes a search API, and demonstrates BM25 relevance tuning, "
            "synonym filters, and faceted filtering."
        ),
        "stack_additions":  ["elasticsearch", "docker"],
        "scoped_version":   (
            "Index 100 k Wikipedia abstracts. REST API with query, filter, and highlight "
            "endpoints. A/B comparison of default vs. tuned BM25 field boosts. "
            "Synonym filter with 10 configured mappings, tested with a query set."
        ),
        "outcomes":         [
            "Indexes 100 k documents in < 60 s with bulk API; p99 query < 20 ms",
            "BM25 field-boost tuning improves NDCG@10 by 15% on a 50-query evaluation set",
            "Synonym filter normalises 10 variant spellings; recall confirmed with unit tests",
        ],
        "resume_value":     (
            "Built a full-text search service on Elasticsearch; tuned BM25 relevance, "
            "demonstrated synonym handling and faceted filtering on a 100 k document corpus."
        ),
        "notes":            (
            "Run Elasticsearch via Docker (official image). Use Wikipedia abstracts "
            "(free download). Focus on mapping design and query DSL over application complexity."
        ),
    },

    "grpc": {
        "business_problem": (
            "Build high-performance, strongly-typed inter-service communication with protocol guarantees"
        ),
        "title_template":   "gRPC Service with {lang} and Protocol Buffer Schema Evolution",
        "description":      (
            "A server and client pair communicating over gRPC. Demonstrates unary, "
            "server-streaming, and bidirectional-streaming RPCs; standard health checking; "
            "and protocol buffer schema evolution (adding an optional field without breaking "
            "existing clients)."
        ),
        "stack_additions":  ["grpc"],
        "scoped_version":   (
            "One service with 3 RPC types (unary, server-streaming, bidirectional). "
            "Schema evolution demo: v1 client talks to v2 server. "
            "k6 comparison of gRPC throughput vs. equivalent REST under same latency budget."
        ),
        "outcomes":         [
            "gRPC throughput 2× REST throughput at equivalent p99 latency in k6 benchmark",
            "Bidirectional-streaming RPC handles simulated delayed client messages without deadlock",
            "v1 client communicates correctly with v2 server after adding optional proto field",
        ],
        "resume_value":     (
            "Implemented gRPC server/client with unary and streaming RPCs; "
            "demonstrated proto schema evolution and measured throughput advantage over REST."
        ),
        "notes":            (
            "Use grpcio and grpcio-tools. Include the .proto file and generated stubs in the repo. "
            "Pick a domain model from the JD (e.g. order events or telemetry readings)."
        ),
    },

    "dbt": {
        "business_problem": (
            "Apply software engineering practices (version control, testing, docs) to SQL transforms"
        ),
        "title_template":   "Modular Analytics Pipeline with dbt and {db}",
        "description":      (
            "A dbt project on a public dataset that implements staging → intermediate → mart "
            "layer structure, generic and singular data-quality tests, and an auto-generated "
            "documentation site with lineage graph."
        ),
        "stack_additions":  ["dbt"],
        "scoped_version":   (
            "3 staging models, 2 intermediate models, 1 mart model. "
            "5+ dbt tests catching seeded data-quality issues. "
            "Incremental materialisation on the mart reduces full-refresh time by 80%. "
            "dbt docs site committed to the repo."
        ),
        "outcomes":         [
            "dbt test suite catches 3 data-quality issues deliberately seeded into staging layer",
            "Incremental materialisation reduces subsequent run time by 80% vs. full refresh",
            "Lineage graph in dbt docs shows full dependency chain from source to mart",
        ],
        "resume_value":     (
            "Designed a dbt project with staging/intermediate/mart structure, automated "
            "data-quality tests, and incremental materialisation."
        ),
        "notes":            (
            "Use dbt Core (free) with DuckDB (easiest local setup) or BigQuery sandbox. "
            "NYC 311, IMDB ratings, or any Kaggle tabular dataset works well. "
            "The lineage graph and dbt docs site are strong visual portfolio artifacts."
        ),
    },

    "machine learning": {
        "business_problem": (
            "Turn historical data into actionable predictions that drive product or operational decisions"
        ),
        "title_template":   "End-to-End ML Pipeline with {lang}, scikit-learn, and MLflow",
        "description":      (
            "A machine learning pipeline that trains a classification or regression model "
            "on a public tabular dataset, tracks experiments with MLflow, exposes predictions "
            "via a {lang} REST API, and includes a basic input-distribution drift stub."
        ),
        "stack_additions":  ["mlflow"],
        "scoped_version":   (
            "3 model variants compared in MLflow experiment. Best model served via FastAPI. "
            "Input schema validated with Pydantic; malformed requests rejected. "
            "Drift stub: PSI between training and new-batch distributions computed on each request batch."
        ),
        "outcomes":         [
            "3 model variants tracked in MLflow; best achieves target RMSE/F1 on held-out test set",
            "REST API serves predictions in < 50 ms p99; input validation rejects malformed payloads",
            "Drift detection stub flags distribution shift when PSI exceeds configured threshold",
        ],
        "resume_value":     (
            "Trained and served a classification model via REST API; tracked experiments "
            "with MLflow and added a drift-detection stub."
        ),
        "notes":            (
            "Use any tabular public dataset (UCI, Kaggle). Keep the model simple "
            "(LogisticRegression or RandomForest). The pipeline structure and MLflow "
            "tracking are the portfolio signal, not model accuracy."
        ),
    },

    "microservices": {
        "business_problem": (
            "Decompose a feature into independently deployable services with explicit, tested contracts"
        ),
        "title_template":   "Two-Service {lang} System with Async Event Backbone",
        "description":      (
            "A minimal two-service architecture (e.g. order-service + notification-service) "
            "communicating via REST for synchronous calls and a lightweight queue for async "
            "events. Demonstrates service boundaries, contract testing, and failure isolation."
        ),
        "stack_additions":  ["docker"],
        "scoped_version":   (
            "Exactly two services in Docker Compose. REST for synchronous 'place order'; "
            "async notification via Redis pub/sub or RabbitMQ. "
            "Notification service failure does not fail order placement — tested explicitly."
        ),
        "outcomes":         [
            "Notification service crash does not affect order-service availability (tested with docker stop)",
            "End-to-end happy path: order placed → notification delivered in < 100 ms",
            "Contract test validates shared schema; schema change on one service breaks the test first",
        ],
        "resume_value":     (
            "Built a two-service system with synchronous REST and async event messaging; "
            "demonstrated service isolation and contract testing."
        ),
        "notes":            (
            "Keep it to exactly two services. Docker Compose orchestrates both. "
            "Avoid a full service mesh — boundary thinking and failure isolation are "
            "the portfolio signal, not infrastructure complexity."
        ),
    },

    "ci/cd": {
        "business_problem": (
            "Catch regressions automatically and deploy reliably on every commit without manual steps"
        ),
        "title_template":   "CI/CD Pipeline for a {lang} Service with GitHub Actions",
        "description":      (
            "A GitHub Actions workflow for a {lang} service that runs linting, type-checking, "
            "parallelised unit and integration tests (against a live {db} service container), "
            "builds a Docker image, and deploys to a staging environment on merge to main."
        ),
        "stack_additions":  ["docker"],
        "scoped_version":   (
            "Workflow stages: lint → type-check → test (parallelised, 2 shards) → docker-build → "
            "deploy (GitHub environment with protection rule). "
            "Matrix build across {lang} 3.10 and 3.11. Total runtime < 4 min."
        ),
        "outcomes":         [
            "Full pipeline completes in < 4 min; parallel test shards reduce test stage to 90 s",
            "Matrix build tests two runtime versions; failure in one does not block reporting of the other",
            "Staging smoke test runs automatically on deploy; alerts on HTTP 500 response",
        ],
        "resume_value":     (
            "Designed a CI/CD pipeline with parallelised tests, Docker build, and staged deploy; "
            "demonstrated matrix builds and environment protection rules."
        ),
        "notes":            (
            "GitHub Actions is free for public repos. "
            "Use a service container for the {db} integration tests (no mocking). "
            "The workflow design and parallel shard configuration are the deliverables."
        ),
    },

    "data engineering": {
        "business_problem": (
            "Build reliable, observable data pipelines that keep analytics fresh and trustworthy"
        ),
        "title_template":   "Observable Batch Ingestion Pipeline with {lang} and {db}",
        "description":      (
            "A {lang} batch ingestion pipeline reading from a public REST API, with schema "
            "validation, dead-letter routing for bad records, row-count reconciliation, "
            "and a Prometheus-scraped freshness metric."
        ),
        "stack_additions":  ["airflow", "docker"],
        "scoped_version":   (
            "Single Airflow DAG: extract → validate → load-good / load-DLQ. "
            "Prometheus metric: `pipeline_last_success_seconds`. "
            "Row-count check: source API count vs. {db} row count, logged and alerted on mismatch."
        ),
        "outcomes":         [
            "Bad-schema records routed to dead-letter table; alert fires when DLQ rate > 1%",
            "Freshness metric: data older than 1 h triggers Prometheus alert in demo environment",
            "Row-count reconciliation detects injected data-loss scenario (1 dropped record in 1000)",
        ],
        "resume_value":     (
            "Built a batch ingestion pipeline with schema validation, dead-letter handling, "
            "and freshness monitoring via Prometheus."
        ),
        "notes":            (
            "Use a stable public API (OpenMeteo, CoinGecko, or NASA APOD). "
            "Run Prometheus + Alertmanager via Docker Compose. "
            "Observability artifacts (dashboards, alert config) are the portfolio signal."
        ),
    },

    "devops": {
        "business_problem": (
            "Make a service's reliability and performance observable and alertable without manual inspection"
        ),
        "title_template":   "Observability Stack for a {lang} Service: Prometheus, Grafana, Alertmanager",
        "description":      (
            "A {lang} service instrumented with custom Prometheus metrics (request latency, "
            "error rate, business throughput), visualised in Grafana, with Alertmanager "
            "rules for latency and error-rate SLOs."
        ),
        "stack_additions":  ["prometheus", "grafana", "docker"],
        "scoped_version":   (
            "FastAPI service with 3 custom metrics exposed at /metrics. "
            "Grafana dashboard with p50/p95/p99 latency panels and error-rate panel. "
            "Alert rule: fire when error_rate > 5% for 2 min. Runbook linked from alert annotation."
        ),
        "outcomes":         [
            "p99 latency spike injected artificially triggers alert within 2 min; fires correctly",
            "Custom business metric (e.g. orders/min) visible in Grafana panel",
            "Runbook documented in README and linked from Grafana alert annotation",
        ],
        "resume_value":     (
            "Instrumented a {lang} service with custom Prometheus metrics and Grafana dashboards; "
            "defined SLO-based alert rules and documented a runbook."
        ),
        "notes":            (
            "Use Docker Compose for the observability stack (Prometheus + Grafana + Alertmanager). "
            "A simple FastAPI service with prometheus-fastapi-instrumentator is sufficient. "
            "The Grafana dashboard JSON and alert config are the deliverables."
        ),
    },

    "mlops": {
        "business_problem": (
            "Operationalise ML models so they can be updated, monitored, and rolled back safely"
        ),
        "title_template":   "MLOps Pipeline: Model Registry, Serving, and Drift Detection with {lang}",
        "description":      (
            "An end-to-end MLOps setup: MLflow experiment tracking and model registry, "
            "a {lang} REST serving layer, automated retraining trigger on data drift, "
            "and a model-version promotion/rollback workflow."
        ),
        "stack_additions":  ["mlflow", "docker"],
        "scoped_version":   (
            "MLflow tracking + model registry (local). REST serving layer selecting model version "
            "from registry. Drift detection: PSI on input features triggers a logged retraining event. "
            "Rollback demonstrated by promoting a previous model version."
        ),
        "outcomes":         [
            "Model promoted from Staging to Production via MLflow registry; rollback confirmed",
            "PSI > 0.2 on simulated drifted input triggers retraining job and logs alert",
            "A/B traffic split between model v1 and v2 configurable via environment variable",
        ],
        "resume_value":     (
            "Built an MLOps pipeline with MLflow experiment tracking, versioned model promotion, "
            "drift-triggered retraining, and a REST serving layer."
        ),
        "notes":            (
            "Use MLflow with a simple sklearn model. The operational workflows "
            "(promotion, rollback, drift detection) are the deliverables, not model quality. "
            "Run MLflow tracking server via Docker."
        ),
    },

    "security": {
        "business_problem": (
            "Protect API endpoints and user data against common attack vectors out of the box"
        ),
        "title_template":   "Secure {lang} API: JWT Auth, Rate Limiting, and OWASP Hardening",
        "description":      (
            "A {lang} API demonstrating JWT-based authentication with refresh-token rotation, "
            "role-based access control, distributed rate limiting, input validation, and "
            "mitigations for OWASP API Security Top 10 items."
        ),
        "stack_additions":  [],
        "scoped_version":   (
            "FastAPI service with /auth (login + refresh), /resource (RBAC-protected), "
            "and /admin (role-scoped) endpoints. OWASP ZAP passive scan in CI. "
            "Rate limiter tested with a 150-req burst script."
        ),
        "outcomes":         [
            "OWASP ZAP passive scan reports 0 high-severity findings against the running service",
            "JWT expiry and refresh-token rotation tested with automated pytest suite",
            "SQL injection and oversized payload inputs rejected by middleware; tests confirm it",
        ],
        "resume_value":     (
            "Hardened a {lang} REST API against OWASP API Security Top 10; implemented JWT "
            "authentication with refresh rotation, RBAC, and input validation."
        ),
        "notes":            (
            "Run OWASP ZAP in Docker against your local service (`zap-baseline.py`). "
            "Focus on one concrete control per Top-10 category. "
            "The ZAP scan report and pytest suite are the portfolio artifacts."
        ),
    },

    "sqs": {
        "business_problem": (
            "Decouple service components using managed queues to absorb traffic spikes reliably"
        ),
        "title_template":   "Async Task Queue with Amazon SQS and {lang}",
        "description":      (
            "A {lang} producer that enqueues tasks to SQS and a worker that processes them "
            "with visibility-timeout management, dead-letter queue routing, and exponential "
            "backoff on transient failures."
        ),
        "stack_additions":  ["sqs", "aws"],
        "scoped_version":   (
            "One producer (FastAPI endpoint) and one worker (polling loop). "
            "DLQ configured for messages that fail 3 times. "
            "Localstack used for local development; AWS free-tier SQS for live demo."
        ),
        "outcomes":         [
            "Messages processed reliably; 3 injected transient failures route to DLQ correctly",
            "Visibility timeout prevents duplicate processing during slow worker runs",
            "Worker scales horizontally: 2 parallel workers confirmed to process disjoint message sets",
        ],
        "resume_value":     (
            "Built an SQS-backed async task queue with DLQ, visibility-timeout management, "
            "and exponential-backoff retry."
        ),
        "notes":            (
            "Use LocalStack for local development (free). Switch to real SQS for the live demo. "
            "The DLQ and visibility-timeout mechanics are the portfolio highlights."
        ),
    },

    "dynamodb": {
        "business_problem": (
            "Store and retrieve high-velocity records with single-digit millisecond latency at any scale"
        ),
        "title_template":   "DynamoDB Access-Pattern-Driven Data Model with {lang}",
        "description":      (
            "A {lang} service backed by DynamoDB demonstrating access-pattern-first data modelling: "
            "single-table design, composite sort keys, GSIs for alternative access patterns, "
            "and DynamoDB Streams for event-driven fanout."
        ),
        "stack_additions":  ["dynamodb", "aws"],
        "scoped_version":   (
            "One DynamoDB table (single-table design). 3 access patterns implemented: "
            "get-by-pk, list-by-gsi, and range-query with sort-key prefix. "
            "DynamoDB Streams trigger a Lambda that writes to a secondary read model."
        ),
        "outcomes":         [
            "All 3 access patterns return in < 5 ms p99 under k6 load (100 RPS)",
            "GSI query reduces scanned items by 95% vs. full table scan",
            "DynamoDB Streams fanout delivers events to secondary model within 500 ms",
        ],
        "resume_value":     (
            "Designed a single-table DynamoDB data model for 3 access patterns; demonstrated "
            "GSI query optimisation and Streams-based event fanout."
        ),
        "notes":            (
            "Use LocalStack or DynamoDB Local for development. The data modelling "
            "decisions (access patterns before schema) are the portfolio signal. "
            "Include an access-pattern table in the README."
        ),
    },
}

# Default template for gap terms not in the specific map above
_DEFAULT_GAP_TEMPLATE: dict = {
    "business_problem": (
        "Demonstrate practical, production-like command of {gap_d} in a realistic scenario"
    ),
    "title_template":   "{gap_d} Integration Project with {lang}",
    "description":      (
        "A focused {lang} project that integrates {gap_d} to solve a realistic problem. "
        "Demonstrates the core APIs, error handling patterns, retry logic, and "
        "observability hooks most commonly required in production."
    ),
    "stack_additions":  [],
    "scoped_version":   (
        "A single, well-tested script or service that covers the happy path and at least "
        "two failure modes. Deployable locally with docker-compose or a one-liner install. "
        "Includes a README explaining the design decisions."
    ),
    "outcomes":         [
        "Happy path and two error paths covered by automated tests",
        "Retry with configurable backoff demonstrated and documented",
        "README explains design trade-offs and any surprising API behaviours encountered",
    ],
    "resume_value":     (
        "Built a {gap_d} integration project; demonstrated core APIs, error handling, "
        "and observability in a production-like scenario."
    ),
    "notes":            (
        "Focus on the integration pattern, not application complexity. "
        "A well-tested, well-documented proof-of-concept is more portfolio-valuable "
        "than a feature-complete but untested implementation."
    ),
}


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class ProjectRecommendation:
    """A single project recommendation (new or reposition)."""
    recommendation_type:  str         # 'new_project' | 'reposition_existing'
    title:                str
    why_this_matches:     str
    business_problem:     str
    target_gap_or_signal: str
    stack:                list[str]
    scoped_version:       str
    measurable_outcomes:  list[str]
    resume_value:         str
    implementation_notes: str
    asset_id:             int = 0     # populated after DB write

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RecommendationProvenance:
    """Tracks what evidence drove each recommendation decision."""
    job_id:                   int
    gaps_considered:          list[str]
    adjacent_considered:      list[str]
    direct_considered:        list[str]
    jd_required_skills:       list[str]
    jd_preferred_skills:      list[str]
    used_extraction:          bool
    projects_considered:      int
    primary_gap:              str
    reposition_project_title: str | None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ProjectRecommendationResult:
    """Full output from recommend_project()."""
    job_id:              int
    new_project:         ProjectRecommendation
    reposition_existing: ProjectRecommendation | None
    provenance:          RecommendationProvenance

    def to_dict(self) -> dict:
        return {
            "job_id":              self.job_id,
            "new_project":         self.new_project.to_dict(),
            "reposition_existing": (
                self.reposition_existing.to_dict()
                if self.reposition_existing else None
            ),
            "provenance":          self.provenance.to_dict(),
        }


# ── Public entry point ────────────────────────────────────────────────────────

def recommend_project(
    job_id:     int,
    conn:       sqlite3.Connection,
    profile:    dict,
    extracted:  object | None       = None,
    assessment: object | None       = None,
    projects:   list[dict] | None   = None,
    label:      str                 = "targeted",
) -> ProjectRecommendationResult:
    """
    Generate two portfolio project recommendations for *job_id* and persist them.

    Args:
        job_id:     DB id of the target job.
        conn:       Open SQLite connection (caller owns lifecycle).
        profile:    Candidate profile dict.
        extracted:  Optional ExtractionResult for structured term lists.
        assessment: Optional ScoreBreakdown for pre-computed evidence buckets.
        projects:   Optional list of candidate project dicts (from project_loader).
        label:      Short version label stored in project_recommendations.

    Returns:
        ProjectRecommendationResult with both asset_ids populated.

    Raises:
        ValueError: if *job_id* is not found in the database.
    """
    # ── 1. Load job ────────────────────────────────────────────────────────────
    row = conn.execute(
        "SELECT raw_text, title, company FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    if not row:
        raise ValueError(f"Job id={job_id} not found in database.")
    job_raw_text = row["raw_text"]
    job_title    = row["title"] or "this role"
    job_company  = row["company"] or "this company"

    # ── 2. Resolve term sets ───────────────────────────────────────────────────
    if extracted is not None:
        required_skills  = list(extracted.required_skills)
        preferred_skills = list(extracted.preferred_skills)
        domain_skills    = list(extracted.domain_requirements)
        ats_keywords     = list(extracted.ats_keywords)
        used_extraction  = True
    else:
        sections         = _parse_jd_sections(job_raw_text)
        required_skills  = _extract_vocab_terms(sections.get("must_have") or job_raw_text)
        preferred_skills = _extract_vocab_terms(sections.get("nice_to_have") or "")
        domain_skills    = [
            t for t in _extract_vocab_terms(job_raw_text)
            if _ALL_VOCAB.get(t) == "domain"
        ]
        ats_keywords     = _extract_vocab_terms(job_raw_text)
        used_extraction  = False

    required_set  = set(required_skills)
    preferred_set = set(preferred_skills)
    domain_set    = set(domain_skills)

    # ── 3. Resolve evidence buckets ───────────────────────────────────────────
    skill_map = _build_skill_map(profile, set())

    if assessment is not None:
        gaps_list     = list(assessment.unsupported_gaps)
        adjacent_list = list(assessment.adjacent_evidence)
        direct_list   = list(assessment.direct_evidence)
    else:
        gaps_list     = [t for t in required_skills if not _lookup_skill(t, skill_map)]
        adjacent_list = [
            t for t in required_skills
            if _lookup_skill(t, skill_map) in ("adjacent", "familiar")
        ]
        direct_list   = [
            t for t in required_skills
            if _lookup_skill(t, skill_map) == "direct"
        ]

    # ── 4. Select primary gap ─────────────────────────────────────────────────
    primary_gap = _select_primary_gap(
        gaps_list, adjacent_list, direct_list,
        required_skills, preferred_skills, domain_skills,
    )

    # ── 5. Candidate language + database for template substitution ─────────────
    lang = _get_candidate_lang(profile)
    db   = _get_candidate_db(profile)

    # ── 6. Build new-project recommendation ───────────────────────────────────
    new_rec = _build_new_project_rec(
        primary_gap, lang, db, skill_map,
        required_set, preferred_set, direct_list, gaps_list,
        job_title, job_company,
    )

    # ── 7. Find best reposition candidate ─────────────────────────────────────
    real_projects = [p for p in (projects or []) if _is_real_project(p)]
    best_proj, best_score, best_matched = _find_best_reposition(
        real_projects, required_set, preferred_set, domain_set, skill_map
    )

    reposition_rec: ProjectRecommendation | None = None
    if best_proj is not None and best_score >= _MIN_REPOSITION_SCORE:
        reposition_rec = _build_reposition_rec(
            best_proj, best_matched, primary_gap, lang, db,
            required_set, preferred_set, skill_map,
            job_title, job_company, direct_list, gaps_list,
        )

    # ── 8. Build provenance ────────────────────────────────────────────────────
    provenance = RecommendationProvenance(
        job_id                   = job_id,
        gaps_considered          = gaps_list,
        adjacent_considered      = adjacent_list,
        direct_considered        = direct_list,
        jd_required_skills       = required_skills,
        jd_preferred_skills      = preferred_skills,
        used_extraction          = used_extraction,
        projects_considered      = len(real_projects),
        primary_gap              = primary_gap,
        reposition_project_title = best_proj.get("title") if best_proj else None,
    )

    # ── 9. Persist + return ───────────────────────────────────────────────────
    result = ProjectRecommendationResult(
        job_id              = job_id,
        new_project         = new_rec,
        reposition_existing = reposition_rec,
        provenance          = provenance,
    )

    result.new_project.asset_id = _persist(conn, job_id, result.new_project, label, provenance)
    if result.reposition_existing:
        result.reposition_existing.asset_id = _persist(
            conn, job_id, result.reposition_existing, label, provenance
        )

    return result


# ── Primary gap selection ─────────────────────────────────────────────────────

def _select_primary_gap(
    gaps:          list[str],
    adjacent:      list[str],
    direct:        list[str],
    required:      list[str],
    preferred:     list[str],
    domain:        list[str],
) -> str:
    """
    Select the single most impactful gap to build the new-project recommendation around.

    Priority:
      1. Unsupported gaps with a known template (most impactful to close)
      2. Any unsupported gap (even without a template)
      3. Adjacent-evidence terms (sharpen the signal)
      4. First required skill (worst case: sharpen an existing strength)
      5. First domain signal
      6. Fallback: "python"
    """
    def is_addressable(t: str) -> bool:
        return t not in _NON_PROJECT_TERMS

    # 1. Gaps with a known template
    for t in gaps:
        if is_addressable(t) and t in _GAP_TEMPLATES:
            return t

    # 2. Any gap
    for t in gaps:
        if is_addressable(t):
            return t

    # 3. Adjacent evidence (worth sharpening via a project)
    for t in adjacent:
        if is_addressable(t) and t in _GAP_TEMPLATES:
            return t
    for t in adjacent:
        if is_addressable(t):
            return t

    # 4. First required skill
    for t in required:
        if is_addressable(t):
            return t

    # 5. Domain signal
    for t in domain:
        if is_addressable(t):
            return t

    return "python"


# ── Candidate skill helpers ───────────────────────────────────────────────────

def _get_candidate_lang(profile: dict) -> str:
    """Return the candidate's primary programming language (direct evidence first)."""
    for ev_target in ("direct", "adjacent"):
        for items in profile.get("skills", {}).values():
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name", ""))
                ev   = str(item.get("evidence", ""))
                if ev == ev_target:
                    norm = _normalize(name)
                    if _ALL_VOCAB.get(norm) == "language" and norm not in ("sql", "bash", "r"):
                        return name
    return "Python"


def _get_candidate_db(profile: dict) -> str:
    """Return the candidate's primary database (direct evidence first)."""
    for ev_target in ("direct", "adjacent"):
        for items in profile.get("skills", {}).values():
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name", ""))
                ev   = str(item.get("evidence", ""))
                if ev == ev_target:
                    norm = _normalize(name)
                    if _ALL_VOCAB.get(norm) == "database":
                        return name
    return "PostgreSQL"


def _build_stack(
    lang:         str,
    db:           str,
    primary_gap:  str,
    skill_map:    dict[str, str],
    template_adds: list[str],
    profile:      dict,
) -> list[str]:
    """
    Assemble a realistic, buildable stack for the project.

    Priority:
      - Candidate's direct-evidence language first
      - Candidate's direct-evidence framework (if any)
      - Primary gap technology (display name)
      - Template-specified additions
      - Docker (if candidate has it and it's not already in stack)
    Capped at 7 items. No fabricated skills.
    """
    stack: list[str] = []
    seen:  set[str]  = set()

    def add(display: str, norm: str | None = None) -> None:
        key = (norm or _normalize(display)).lower()
        if key not in seen:
            seen.add(key)
            stack.append(display)

    add(lang)

    # Candidate's direct framework
    for items in profile.get("skills", {}).values():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", ""))
            ev   = str(item.get("evidence", ""))
            norm = _normalize(name)
            if ev == "direct" and _ALL_VOCAB.get(norm) == "framework":
                add(name, norm)
                break
        if len(stack) >= 2:
            break

    # Gap technology (unless it's the same as the candidate's lang)
    if _normalize(primary_gap) != _normalize(lang):
        add(_dn(primary_gap), primary_gap)

    # Template additions
    for t in template_adds:
        if t != primary_gap:
            add(_dn(t), t)

    # Candidate's primary DB
    add(db)

    # Docker if candidate has it
    if _lookup_skill("docker", skill_map):
        add("Docker", "docker")

    return stack[:7]


# ── New-project recommendation builder ───────────────────────────────────────

def _build_new_project_rec(
    primary_gap:  str,
    lang:         str,
    db:           str,
    skill_map:    dict[str, str],
    required_set: set[str],
    preferred_set: set[str],
    direct_list:  list[str],
    gaps_list:    list[str],
    job_title:    str,
    job_company:  str,
) -> ProjectRecommendation:
    """Build a new-project recommendation from the primary gap and templates."""
    tmpl     = _GAP_TEMPLATES.get(primary_gap, _DEFAULT_GAP_TEMPLATE)
    gap_d    = _dn(primary_gap)
    gap_title = gap_d

    subs = {"lang": lang, "db": db, "gap": primary_gap, "gap_d": gap_d, "gap_title": gap_title}

    def _fmt(s: str) -> str:
        try:
            return s.format(**subs)
        except KeyError:
            return s

    title            = _fmt(tmpl["title_template"])
    business_problem = _fmt(tmpl["business_problem"])
    scoped_version   = _fmt(tmpl["scoped_version"])
    outcomes         = [_fmt(o) for o in tmpl["outcomes"]]
    resume_value     = _fmt(tmpl["resume_value"])
    notes            = _fmt(tmpl["notes"])

    stack = _build_stack(lang, db, primary_gap, skill_map, tmpl["stack_additions"], {})

    # Build why_this_matches
    is_gap     = primary_gap in gaps_list
    gap_label  = f"unsupported gap" if is_gap else "adjacent-evidence signal"
    why = (
        f"{gap_d} is a {gap_label} for {job_title}. "
        f"Building a tightly scoped {gap_d} project demonstrates exactly what the role "
        f"needs without fabricating prior experience. "
    )
    if direct_list:
        direct_disp = ", ".join(_dn(t) for t in direct_list[:3])
        why += f"Your direct evidence in {direct_disp} provides the foundation."

    target_gap = (
        f"Unsupported required skill: {gap_d}"
        if is_gap
        else f"Strengthen adjacent evidence: {gap_d}"
    )

    return ProjectRecommendation(
        recommendation_type  = "new_project",
        title                = title,
        why_this_matches     = why,
        business_problem     = business_problem,
        target_gap_or_signal = target_gap,
        stack                = stack,
        scoped_version       = scoped_version,
        measurable_outcomes  = outcomes,
        resume_value         = resume_value,
        implementation_notes = notes,
    )


# ── Reposition candidate search ───────────────────────────────────────────────

def _find_best_reposition(
    projects:     list[dict],
    required_set: set[str],
    preferred_set: set[str],
    domain_set:   set[str],
    skill_map:    dict[str, str],
) -> tuple[dict | None, float, list[str]]:
    """
    Score each candidate project against the job term sets.
    Returns (best_project, score, matched_required_terms).
    """
    best_project: dict | None = None
    best_score    = 0.0
    best_matched: list[str]  = []

    for proj in projects:
        raw_skills = proj.get("skills", [])
        skills = {
            _normalize(s)
            for s in raw_skills
            if isinstance(s, str) and not s.lower().startswith("todo")
        }
        score   = 0.0
        matched = []

        for skill in skills:
            ev   = _lookup_skill(skill, skill_map)
            ev_w = _EVIDENCE_WEIGHT.get(ev, _ABSENT_EVIDENCE_WEIGHT) if ev else _ABSENT_EVIDENCE_WEIGHT
            if skill in required_set:
                score += _CAT_WEIGHT["required"] * ev_w
                matched.append(skill)
            elif skill in preferred_set:
                score += _CAT_WEIGHT["preferred"] * ev_w
                matched.append(skill)
            elif skill in domain_set:
                score += _CAT_WEIGHT["domain"] * ev_w

        if score > best_score:
            best_score   = score
            best_project = proj
            best_matched = matched

    return best_project, best_score, best_matched


# ── Reposition recommendation builder ────────────────────────────────────────

def _build_reposition_rec(
    project:      dict,
    matched:      list[str],
    primary_gap:  str,
    lang:         str,
    db:           str,
    required_set: set[str],
    preferred_set: set[str],
    skill_map:    dict[str, str],
    job_title:    str,
    job_company:  str,
    direct_list:  list[str],
    gaps_list:    list[str],
) -> ProjectRecommendation:
    """Build a 'reposition an existing project' recommendation."""
    proj_title  = project.get("title", "Existing Project")
    proj_skills = [
        _normalize(s) for s in project.get("skills", [])
        if isinstance(s, str) and not s.lower().startswith("todo")
    ]
    gap_d = _dn(primary_gap)

    # Reposition title
    title = f"Reposition: {proj_title} for {job_title}"

    # What the project already demonstrates (only matched skills)
    matched_display = [_dn(t) for t in matched[:4]]
    already_has = ", ".join(matched_display) if matched_display else "relevant skills"

    # What to add: the primary gap if it's not already in the project
    add_gap = primary_gap not in proj_skills

    why = (
        f"Your existing '{proj_title}' already demonstrates {already_has}, "
        f"which overlaps with what {job_title} needs. "
    )
    if add_gap:
        why += (
            f"Adding a {gap_d} component closes the primary gap signal "
            f"while keeping the project grounded in work you have already done."
        )
    else:
        why += (
            f"Reframing the description to emphasise the {job_title}-relevant angle "
            f"and adding concrete metrics makes this project much more readable "
            f"for this role."
        )

    # Business problem — reuse the gap template's
    tmpl = _GAP_TEMPLATES.get(primary_gap, _DEFAULT_GAP_TEMPLATE)
    subs = {"lang": lang, "db": db, "gap": primary_gap, "gap_d": gap_d, "gap_title": gap_d}

    def _fmt(s: str) -> str:
        try:
            return s.format(**subs)
        except KeyError:
            return s

    business_problem = _fmt(tmpl["business_problem"])

    target_gap = (
        f"Close {gap_d} gap by adding it to '{proj_title}'"
        if add_gap
        else f"Sharpen the {gap_d} signal already present in '{proj_title}'"
    )

    # Stack: project's existing skills + gap + lang
    stack: list[str] = []
    seen:  set[str]  = set()

    def add(display: str, norm: str | None = None) -> None:
        key = (norm or _normalize(display)).lower()
        if key not in seen:
            seen.add(key)
            stack.append(display)

    add(lang)
    for s in proj_skills[:4]:
        if s not in (_normalize(lang),):
            add(_dn(s), s)
    if add_gap:
        add(gap_d, primary_gap)

    stack = stack[:7]

    # Scoped version — specific to the reposition
    if add_gap:
        scoped_version = (
            f"Add a {gap_d} component to '{proj_title}': "
            + _fmt(tmpl.get("scoped_version", "A focused integration of {gap_d} into the existing project."))
        )
    else:
        scoped_version = (
            f"Reframe '{proj_title}' for {job_title}: update the README to lead with "
            f"the business problem it solves, add concrete metrics (simulated if needed), "
            f"and add at least one test that validates the {already_has} behaviour."
        )

    # Outcomes — gap-specific + reposition framing
    outcomes = [_fmt(o) for o in tmpl["outcomes"][:2]]
    outcomes.append(
        f"'{proj_title}' README updated with metrics and {job_title}-relevant framing"
    )

    resume_value = (
        f"Extended '{proj_title}' to incorporate {gap_d}; "
        + _fmt(tmpl["resume_value"]).lstrip("Built").lstrip("Designed").strip()
        if add_gap
        else (
            f"Reframed '{proj_title}' to highlight {already_has} "
            f"and measurable outcomes relevant to {job_title}."
        )
    )

    implementation_notes = (
        f"Start from the existing '{proj_title}' repo. "
        + (_fmt(tmpl["notes"]) if add_gap else
           f"Add a METRICS.md documenting simulated or real throughput/latency numbers. "
           f"Revise the README intro to lead with the business problem solved, not the technology used.")
    )

    return ProjectRecommendation(
        recommendation_type  = "reposition_existing",
        title                = title,
        why_this_matches     = why,
        business_problem     = business_problem,
        target_gap_or_signal = target_gap,
        stack                = stack,
        scoped_version       = scoped_version,
        measurable_outcomes  = outcomes,
        resume_value         = resume_value,
        implementation_notes = implementation_notes,
    )


# ── DB persistence ────────────────────────────────────────────────────────────

def _persist(
    conn:       sqlite3.Connection,
    job_id:     int,
    rec:        ProjectRecommendation,
    label:      str,
    provenance: RecommendationProvenance,
) -> int:
    """Write a single recommendation to project_recommendations; return the new row id."""
    cur = conn.execute(
        """INSERT INTO project_recommendations
           (job_id, project_title, rationale, priority,
            recommendation_type, why_this_matches, business_problem,
            target_gap_or_signal, stack_json, scoped_version,
            measurable_outcomes_json, resume_value, implementation_notes,
            label, metadata_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            job_id,
            rec.title,
            rec.why_this_matches,
            1 if rec.recommendation_type == "new_project" else 2,
            rec.recommendation_type,
            rec.why_this_matches,
            rec.business_problem,
            rec.target_gap_or_signal,
            json.dumps(rec.stack),
            rec.scoped_version,
            json.dumps(rec.measurable_outcomes),
            rec.resume_value,
            rec.implementation_notes,
            label,
            json.dumps(provenance.to_dict()),
        ),
    )
    conn.commit()
    return cur.lastrowid


# ── Public loader ─────────────────────────────────────────────────────────────

def load_latest_recommendations(
    conn:   sqlite3.Connection,
    job_id: int,
) -> list[dict]:
    """Load the most recent pair of recommendations for *job_id* from the DB."""
    rows = conn.execute(
        """SELECT * FROM project_recommendations
           WHERE job_id = ?
           ORDER BY id DESC LIMIT 2""",
        (job_id,),
    ).fetchall()
    return [dict(r) for r in rows]
