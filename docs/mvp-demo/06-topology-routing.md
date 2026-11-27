# 06. Топология и встроенная балансировка

**По обзору MVP требование реализовано частично.**

Запишите ключ через Gateway, остановите обслужившего запись лидера и повторите операции по тому же клиентскому адресу.

## 1. Поднимите стенд

```bash
make build
docker compose -p oracache-demo-06 -f compose.cp.yaml up -d --build --force-recreate
curl -fsS http://127.0.0.1:9466/routes
```

Дождитесь маршрутов чтения и записи для партиции `"0"`. Поле `revision` обозначает ревизию топологии Control Plane.

## 2. Запишите ключ и определите лидера

```bash
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:routing retained --expected-rev=0
curl -fsS http://127.0.0.1:9466/routes
```

Ожидается `PUT_STATUS_OK` с `rev=1`. Сверьте `servedBy.instance` из PUT и `NodeID` в `write_routes["0"]`.

## 3. Остановите лидера

Выполните только одну из трёх команд — для `NodeID` из предыдущего шага.

```bash
# Если лидер node-1.
docker compose -p oracache-demo-06 -f compose.cp.yaml stop node-1
```

```bash
# Если лидер node-2.
docker compose -p oracache-demo-06 -f compose.cp.yaml stop node-2
```

```bash
# Если лидер node-3.
docker compose -p oracache-demo-06 -f compose.cp.yaml stop node-3
```

```bash
curl -fsS http://127.0.0.1:9466/routes
```

Повторите `curl`, пока не появится новый `NodeID` в обоих маршрутах. `revision` топологии сохраняется.

## 4. Прочитайте и обновите ключ через тот же адрес Gateway

```bash
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:routing
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:routing updated --expected-rev=1
```

GET возвращает `HIT` с `rev=1` и `data=cmV0YWluZWQ=` (`retained`). PUT возвращает `OK` с `rev=2`. В `servedBy.instance` виден новый лидер.

## 5. Остановите стенд

```bash
docker compose -p oracache-demo-06 -f compose.cp.yaml down
```

Ручной прогон покрывает одну партицию в одном регионе. Маршрутизацию при отказе целого региона предстоит проверить отдельно.

## Резерв без Docker

```bash
env GOWORK=off go test -count=1 -v ./internal/gateway -run '^TestRouterKeyRoutingUsesStablePartition$'
env GOWORK=off go test -count=1 -v -timeout=45s ./cmd/api-gateway \
  -run '^TestGatewayRoutesRealRaftThroughLeaderAndControlPlaneLoss$'
```

Первый тест проверяет стабильный выбор партиции. Второй проверяет перевыборы и доступ по последней карте при потере связи с Control Plane. [Маршрутизация](../../internal/gateway/router.go) · [Интеграционный тест](../../cmd/api-gateway/raft_integration_test.go)
