# 04. Репликация между двумя регионами

**Требование реализовано для базового MVP.**

У двух Raft-групп по три ноды. Gateway A доступен на `38080`, Gateway B — на `39080`. Перед повторным прогоном выполните шаг остановки, чтобы начать с пустого кеша и нового состава групп.

## 1. Соберите CLI и поднимите два региона

```bash
make build
# cp-api собирает общий образ для обоих регионов и Gateway.
docker compose -p oracache-demo-04 -f compose.replication.yaml build cp-api
docker compose -p oracache-demo-04 -f compose.replication.yaml up -d --no-build \
  cp-api api-gateway api-gateway-b a-1 a-2 a-3 b-1 b-2 b-3
docker compose -p oracache-demo-04 -f compose.replication.yaml ps
```

```bash
curl -fsS http://127.0.0.1:39466/routes
curl -fsS http://127.0.0.1:39066/routes
```

Повторите проверки до появления `write_routes["0"]` в A и `read_routes["0"]` с `Region` равным `region-b` в B.

## 2. Запишите две версии в A

```bash
env ORACACHE_ENDPOINT=127.0.0.1:38080 ./bin/oracache put demo:replication first --expected-rev=0
env ORACACHE_ENDPOINT=127.0.0.1:38080 ./bin/oracache put demo:replication second --expected-rev=1
```

Ожидаются два `PUT_STATUS_OK` с `rev=1` и `rev=2`.

## 3. Прочитайте версии в B

```bash
env ORACACHE_ENDPOINT=127.0.0.1:39080 ./bin/oracache get demo:replication
env ORACACHE_ENDPOINT=127.0.0.1:39080 ./bin/oracache get demo:replication --rev=1
env ORACACHE_ENDPOINT=127.0.0.1:39080 ./bin/oracache get demo:replication --rev=2
```

При отставании повторите команды. После доставки все чтения возвращают `GET_STATUS_HIT`, `servedBy.region` равен `region-b`. Для ревизий `1` и `2` ожидаются `data=Zmlyc3Q=` (`first`) и `data=c2Vjb25k` (`second`). Без `--rev` возвращается ревизия `2`.

## 4. Проверьте запрет прямой записи в B

```bash
# 29091 — порт Storage Node b-1. Запрет проверяется до проверки лидерства.
env ORACACHE_ENDPOINT=127.0.0.1:29091 ./bin/oracache put demo:replication illegal
```

Ожидается `FailedPrecondition` с сообщением `client writes require the master region`.

## 5. Удалите ключ в A и проверьте B

```bash
env ORACACHE_ENDPOINT=127.0.0.1:38080 ./bin/oracache delete demo:replication
env ORACACHE_ENDPOINT=127.0.0.1:39080 ./bin/oracache get demo:replication
```

A возвращает `DELETE_STATUS_DELETED`. После доставки удаления B возвращает `GET_STATUS_MISS`. Повторите GET, если он ещё возвращает `HIT`.

## 6. Остановите стенд

```bash
# --volumes удаляет сохранённый состав Raft только у этого демопроекта.
docker compose -p oracache-demo-04 -f compose.replication.yaml down --volumes
```

## Автоматический прогон

```bash
env GOWORK=off go -C integration/e2e test -tags=e2e -count=1 -v -timeout=10m -run '^TestRegionReplication$' .
```

Ожидаются `PASS` для `VersionsAndDeletion` и `AfterSlavePause`. [Docker E2E](../../integration/e2e/replication_test.go)
