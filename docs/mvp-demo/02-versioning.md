# 02. Версионирование и условная запись

**По обзору MVP требование реализовано.**

Вставляйте блоки по очереди из корня репозитория. Команды ниже рассчитаны на пустой стенд, где первые два PUT получают ревизии `1` и `2`. Шаг запуска пересоздаёт контейнеры отдельного проекта `oracache-demo-02` и сбрасывает его кеш в памяти.

## 1. Соберите CLI

```bash
docker version
make build
./bin/oracache --help
```

У Docker должна быть секция `Server`. В справке `oracache` видны `get`, `put` и `delete`.

## 2. Поднимите стенд

```bash
# 28080 — клиентский gRPC-порт, 9466 — HTTP-диагностика Gateway.
docker compose -p oracache-demo-02 -f compose.cp.yaml up -d --build --force-recreate
docker compose -p oracache-demo-02 -f compose.cp.yaml ps
```

Ожидаются `cp-api`, `api-gateway` и три Storage Node со статусом `Up`.

## 3. Проверьте маршруты

```bash
curl -fsS --max-time 3 http://127.0.0.1:9466/routes
```

Дождитесь, чтобы в `read_routes` и `write_routes` у партиции `"0"` появился один и тот же `NodeID`. Если после нескольких попыток карты остаются пустыми, посмотрите логи.

```bash
docker compose -p oracache-demo-02 -f compose.cp.yaml logs --tail=40 api-gateway cp-api node-1 node-2 node-3
```

## 4. Создайте две версии

```bash
# expected-rev=0 разрешает запись только при отсутствии ключа.
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:versions v1 --expected-rev=0
```

Ожидается `PUT_STATUS_OK` с `rev` равным `1`.

```bash
# Теперь обновляем ключ, если его текущая ревизия равна 1.
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:versions v2 --expected-rev=1
```

Ожидается `PUT_STATUS_OK` с `rev` равным `2`.

## 5. Покажите конфликт и историческое чтение

```bash
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:versions outdated --expected-rev=1
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:versions
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:versions --rev=1
```

PUT возвращает `PUT_STATUS_CONFLICT` и текущий `rev` равный `2`. Первый GET возвращает `HIT` с `rev=2` и `data=djI=` (`v2`). Второй возвращает `HIT` с `rev=1`, `latestRev=2` и `data=djE=` (`v1`).

## 6. Удалите версию и проверьте вытеснение

```bash
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache delete demo:versions --rev=1
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:versions --rev=1
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:versions
```

Ожидаются `DELETE_STATUS_DELETED`, `GET_STATUS_VERSION_NOT_FOUND` для первой ревизии и `GET_STATUS_HIT` для последней.

```bash
# Хранятся три последние версии. Три новых PUT вытеснят v2.
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:versions v3
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:versions v4
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:versions v5
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:versions --rev=2
```

Ожидаются три `PUT_STATUS_OK` и затем `GET_STATUS_VERSION_NOT_FOUND`.

```bash
# DELETE без --rev удаляет все версии ключа.
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache delete demo:versions
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:versions
```

Ожидаются `DELETE_STATUS_DELETED` и `GET_STATUS_MISS`.

## 7. Проверьте лимит 10 КиБ

```bash
# head задаёт число байт, tr заполняет их символом x, @- читает stdin.
head -c 10240 /dev/zero | tr '\000' x |
  env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:payload @-
head -c 10241 /dev/zero | tr '\000' x |
  env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:payload @-
```

Первый PUT возвращает `PUT_STATUS_OK`. Второй CLI-вызов выдаёт `payload exceeds 10240 bytes` до обращения к Gateway.

## 8. Остановите стенд

```bash
docker compose -p oracache-demo-02 -f compose.cp.yaml down
```

## Проверить серверные лимиты без Docker

```bash
# -count=1 запускает тесты заново, -run выбирает проверки размера, версий и бюджета.
env GOWORK=off go test -count=1 -v ./internal/store \
  -run '^(TestTooLarge|TestVersionLimit|TestExpectedRev|TestLRUEvictsLeastRecentlyUsedKey|TestSingleKeyKeepsNewestVersionsThatFit)$'
```

Бюджет Storage Node задаётся флагом `--max-bytes`, а число версий — `--max-versions`. [Тесты Store](../../internal/store/store_test.go)
