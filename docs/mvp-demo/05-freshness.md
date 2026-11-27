# 05. Модель свежести

**По обзору MVP требование реализовано частично.**

A подтверждает локальную запись во время недоступности B. После возобновления B проверьте совпадение ревизии и байтов. Перед повторным прогоном выполните шаг остановки.

## 1. Поднимите стенд

```bash
make build
docker compose -p oracache-demo-05 -f compose.replication.yaml build cp-api
docker compose -p oracache-demo-05 -f compose.replication.yaml up -d --no-build \
  cp-api api-gateway api-gateway-b a-1 a-2 a-3 b-1 b-2 b-3
curl -fsS http://127.0.0.1:39466/routes
curl -fsS http://127.0.0.1:39066/routes
```

Дождитесь маршрута записи в A и локального маршрута чтения в B для партиции `"0"`.

## 2. Запишите первую версию и прочитайте её в B

```bash
env ORACACHE_ENDPOINT=127.0.0.1:38080 ./bin/oracache put demo:freshness before --expected-rev=0
env ORACACHE_ENDPOINT=127.0.0.1:39080 ./bin/oracache get demo:freshness
```

Повторяйте GET до `HIT` с `rev=1`, `data=YmVmb3Jl` (`before`) и `servedBy.region=region-b`.

## 3. Поставьте B на паузу и обновите ключ в A

```bash
docker compose -p oracache-demo-05 -f compose.replication.yaml pause b-1 b-2 b-3
docker compose -p oracache-demo-05 -f compose.replication.yaml ps
```

Три Storage Node в B должны быть приостановлены.

```bash
env ORACACHE_ENDPOINT=127.0.0.1:38080 ./bin/oracache put demo:freshness after --expected-rev=1
env ORACACHE_ENDPOINT=127.0.0.1:38080 ./bin/oracache get demo:freshness
```

A отвечает `PUT_STATUS_OK`, а затем сразу возвращает `HIT` с `rev=2` и `data=YWZ0ZXI=` (`after`).

## 4. Попробуйте прочитать из B во время паузы

```bash
env ORACACHE_ENDPOINT=127.0.0.1:39080 ./bin/oracache --timeout=3s get demo:freshness
```

Ожидается `DeadlineExceeded` или `Unavailable`. Gateway B продолжает работать, но локальные реплики приостановлены.

## 5. Возобновите B и дождитесь доставки

```bash
docker compose -p oracache-demo-05 -f compose.replication.yaml unpause b-1 b-2 b-3
env ORACACHE_ENDPOINT=127.0.0.1:39080 ./bin/oracache get demo:freshness
```

Повторите GET до `HIT` с `rev=2` и `data=YWZ0ZXI=`. Время сходимости зависит от доступности кворума, связи и очереди. `rev` измеряет порядок изменений в кеше. Для свежести относительно БД нужна версия источника. [ReadIndex](../shardraft.md#чтения-и-ошибки-маршрутизации) · [Очередь репликации](../../internal/replication/state.go)

## 6. Остановите стенд

Если B остался на паузе, сначала выполните `unpause` из предыдущего шага.

```bash
docker compose -p oracache-demo-05 -f compose.replication.yaml down --volumes
```

## Ограничение очереди без Docker

```bash
env GOWORK=off go test -count=1 -v ./internal/node \
  -run '^(TestOutboxCapacityAndOnlySuccessfulChanges|TestReceiverDeduplicationDeletionAndCheckpoint)$'
```

Тесты проверяют `ResourceExhausted` при заполнении очереди и обработку повторной доставки. [Тесты](../../internal/node/replication_state_test.go)
