# 08. Hit rate и p95/p99

**Требование реализовано для серверных RPC.**

Отправьте четыре GET с заданными исходами и проверьте hit rate и задержки Gateway и Storage Node. Выполняйте шаги измерения подряд, чтобы запросы оставались в минутном окне Prometheus.

## 1. Поднимите кластер с мониторингом

```bash
make build
# HTTP-адреса мониторинга: Prometheus на 9090, Grafana на 3000.
docker compose -p oracache-demo-08 -f compose.cp.yaml -f compose.cp-observability.yaml up -d --build --force-recreate
curl -fsS http://127.0.0.1:9466/routes
```

Дождитесь маршрутов для партиции `"0"`.

## 2. Дождитесь первых выборок Prometheus

```bash
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=up{job=~"cp-api|api-gateway|storage-node"}'
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=min(count_over_time(oracache_cache_rpc_total{operation="get",grpc_code="OK",outcome="hit"}[1m]))'
```

Первый запрос показывает пять целей со значением `1`. Второй должен показать не менее двух выборок. Повторяйте запросы с паузой в несколько секунд, пока Prometheus соберёт данные.

## 3. Отправьте запросы

```bash
# Два HIT из четырёх GET. PUT и DELETE добавляют измерения для своих операций.
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:metrics
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:metrics value --expected-rev=0
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:metrics
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:metrics
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:metrics --rev=999999
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache delete demo:metrics
```

Среди GET ожидаются один `MISS`, два `HIT` и один `VERSION_NOT_FOUND`.

## 4. Посмотрите hit rate

```bash
# Делим скорость HIT на скорость HIT, MISS и VERSION_NOT_FOUND среди успешных GET.
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=sum by (job) (rate(oracache_cache_rpc_total{job=~"api-gateway|storage-node",operation="get",grpc_code="OK",outcome="hit"}[1m])) / sum by (job) (rate(oracache_cache_rpc_total{job=~"api-gateway|storage-node",operation="get",grpc_code="OK",outcome=~"hit|miss|version_not_found"}[1m]))'
```

Ожидаются `api-gateway` и `storage-node` со значением около `0.5`. При пустом результате или `NaN` повторите запрос после очередного сбора.

## 5. Посмотрите p95, p99 и дашборд

```bash
# Перцентили рассчитываются по бакетам гистограммы, отдельно для слоя и операции.
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=histogram_quantile(0.95, sum by (job,operation,le) (rate(oracache_cache_rpc_duration_seconds_bucket[1m])))'
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=histogram_quantile(0.99, sum by (job,operation,le) (rate(oracache_cache_rpc_duration_seconds_bucket[1m])))'
```

Для каждого перцентиля ожидаются шесть значений в секундах. Если минутное окно опустело, повторите шаг с запросами. В Grafana откройте панели «Доля попаданий GET», «Задержка Gateway» и «Задержка Storage Node». [Дашборд Grafana](http://127.0.0.1:3000/d/oracache-real-cluster) · [Описание метрик](../../README.md#мониторинг-реального-кластера)

## 6. Остановите стенд

```bash
docker compose -p oracache-demo-08 -f compose.cp.yaml -f compose.cp-observability.yaml down --volumes
```

## Проверка метрик без Docker

```bash
env GOWORK=off go test -count=1 -v ./internal/cpmetrics -run '^TestCacheRPCOutcomeAndDuration$'
```

Тест проверяет разметку исходов RPC и наблюдения в гистограмме по методам.
