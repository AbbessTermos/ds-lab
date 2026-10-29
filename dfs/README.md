# Distributed chunk storage (DFS)

Services (docker-compose): 6 storage nodes (FastAPI), gateway = metadata + placement + S3-like API (FastAPI),
PostgreSQL (metadata), Elasticsearch (search). Web UI: http://localhost:8080/ui  (API key: admin-key)

    docker compose up -d --build
    python bench/ring_test.py                      # placement test (add/remove node)
    pytest -v tests                                # API tests
    python bench/gen_small.py data/small           # workload (a) 5000 files 1-50KB
    python bench/gen_large.py data/large 300       # workload (b) 10 files 100-300MB
    python bench/run_bench.py small data/small     # results -> bench/results/
    python bench/run_bench.py large data/large

Config (gateway env): REPLICATION_N=3, WRITE_QUORUM=2, READ_QUORUM=2, EC_K=4, EC_M=2, FAIL_AFTER_SEC=10.
Bucket mode is chosen per bucket: POST /buckets/{name}?mode=rep|ec

## Bonus (task 7)
- Nginx load balancer (port 8080) -> gateway1 + gateway2 (header X-Served-By shows which one answered)
- RabbitMQ (UI http://localhost:15673, guest/guest) queues repair / rebalance / gc jobs, 2 workers (worker.py)
- PostgreSQL primary + streaming replica; failover: docker compose stop postgres, then
  docker compose exec postgres-replica psql -U dfs -d dfs -c "SELECT pg_promote();"
- Prometheus http://localhost:9091, Grafana http://localhost:3001 (dashboard "DFS cluster")
- Kubernetes: kubectl apply -f k8s/  (StatefulSet storage-node with PVCs + liveness/readiness probes)
- Add a node -> rebalancing: docker compose --profile scale up -d node7
