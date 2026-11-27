# 09. Chaos-тесты и консистентность

**По обзору MVP требование реализовано частично.**

Проверьте влияние остановки Control Plane и потери кворума на работающий Gateway. Для смены лидера используйте [сценарий 06](06-topology-routing.md), для паузы региона B — [сценарий 05](05-freshness.md).

## 1. Поднимите стенд и запишите ключ

```bash
make build
docker compose -p oracache-demo-09 -f compose.cp.yaml up -d --build --force-recreate
curl -fsS http://127.0.0.1:9466/routes
```

Когда появятся маршруты партиции `"0"`, выполните PUT.

```bash
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:chaos before --expected-rev=0
```

Ожидается `PUT_STATUS_OK`.

## 2. Остановите Control Plane

```bash
docker compose -p oracache-demo-09 -f compose.cp.yaml stop cp-api
curl -fsS http://127.0.0.1:9466/metrics |
  grep 'oracache_gateway_config_refresh_total{result="rpc_error"}'
```

Повторите запрос к `/metrics` после нескольких опросов CP. Счётчик `rpc_error` растёт.

```bash
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:chaos
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:chaos during-cp-stop
```

Ожидаются `GET_STATUS_HIT` и `PUT_STATUS_OK`. Gateway и Storage Node используют последнюю полученную конфигурацию, пока кворум жив.

```bash
docker compose -p oracache-demo-09 -f compose.cp.yaml start cp-api
curl -fsS http://127.0.0.1:9466/routes
```

Перед следующим шагом дождитесь работающего CP и маршрутов партиции `"0"`.

## 3. Лишите группу кворума

```bash
# Оставляем одну из трёх Storage Node. Этого недостаточно для подтверждения PUT.
docker compose -p oracache-demo-09 -f compose.cp.yaml pause node-2 node-3
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache --timeout=3s put demo:quorum-loss during
```

Ожидается `DeadlineExceeded` или `Unavailable`. Клиент не получил подтверждения записи.

```bash
docker compose -p oracache-demo-09 -f compose.cp.yaml unpause node-2 node-3
curl -fsS http://127.0.0.1:9466/routes
```

Когда восстановится маршрут записи, выполните новый PUT с другим ключом.

```bash
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:quorum-restored after
```

Ожидается `PUT_STATUS_OK`.

## 4. Остановите стенд

Если ноды ещё на паузе, сначала повторите `unpause` из предыдущего шага.

```bash
docker compose -p oracache-demo-09 -f compose.cp.yaml down
```

## Автоматические проверки

Скрипт проверяет смену лидера, разрыв сети только между Gateway и CP и потерю большинства. Для него нужен `jq`. Ответы CLI и метрики сохраняются в отчёте.

```bash
mkdir -p .reports
# pipefail сохраняет ошибку скрипта при записи обоих потоков через tee.
bash -o pipefail -c 'bash integration/e2e/check-observability.sh 2>&1 | tee .reports/demo-09-observability.log'
```

Следующий тест проверяет доставку двух версий нового ключа и удаление старого ключа после паузы B.

```bash
env GOWORK=off go -C integration/e2e test -tags=e2e -count=1 -v -timeout=10m \
  -run '^TestRegionReplication$/^AfterSlavePause$' .
```

Ожидается `PASS` для `AfterSlavePause`.

## Резерв без Docker

```bash
env GOWORK=off go test -count=1 -v -timeout=45s ./internal/node \
  -run '^TestCrossRegionReplicationBacklogSnapshotsAndBothLeaderChanges$'
env GOWORK=off go test -count=1 -v -timeout=45s ./cmd/api-gateway \
  -run '^TestGatewayRoutesRealRaftThroughLeaderAndControlPlaneLoss$'
```

Отказ всего A через региональные Gateway, контролируемая WAN-задержка, потеря событий БД и восстановление после потери памяти остаются за пределами проверки. [Скрипт отказов](../../integration/e2e/check-observability.sh) · [Тест репликации](../../internal/node/replication_cluster_test.go)
