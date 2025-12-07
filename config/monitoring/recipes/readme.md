# dashboard sources

- [system-docker.json](https://grafana.com/grafana/dashboards/893-main/)
- [airflow-dags.json](https://grafana.com/grafana/dashboards/23297-airflow-dags-overview/)
- [k8s.json](https://grafana.com/grafana/dashboards/15661-k8s-dashboard-en-20250125/)

Refreshing prometheus.yml
```bash
docker compose kill -s SIGHUP prometheus
```