# 03. Защита от отката старой версии

**По обзору MVP требование реализовано частично.**

Покажите конфликт двух записей на существующий ключ и запоздавшее заполнение после удаления. Команды рассчитаны на чистый кеш в отдельном Compose-проекте.

## 1. Соберите CLI и поднимите стенд

```bash
make build
docker compose -p oracache-demo-03 -f compose.cp.yaml up -d --build --force-recreate
curl -fsS http://127.0.0.1:9466/routes
```

Дождитесь маршрутов чтения и записи партиции `"0"`.

## 2. Проверьте условную запись

```bash
# Оба клиента начинают с ревизии 1. Первый успевает записать newer.
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:cas initial --expected-rev=0
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:cas newer --expected-rev=1
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:cas stale --expected-rev=1
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:cas
```

Первые два PUT подтверждаются с ревизиями `1` и `2`. Третий получает `PUT_STATUS_CONFLICT` с `rev=2`. GET возвращает `newer` (`bmV3ZXI=`).

## 3. Удалите ключ перед запоздавшим заполнением

```bash
# Первый клиент видит MISS, читает старое source-v1 и откладывает PUT.
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:late-fill
# Второй клиент записывает свежие данные. Затем ключ удаляется целиком.
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:late-fill source-v2 --expected-rev=0
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache delete demo:late-fill
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:late-fill
```

Ожидаются `MISS`, `PUT_STATUS_OK`, `DELETE_STATUS_DELETED` и снова `MISS`.

```bash
# Запоздавший клиент завершает заполнение с исходным expected-rev=0.
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:late-fill source-v1 --expected-rev=0
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:late-fill
```

Запись проходит, а GET возвращает старое `source-v1` (`c291cmNlLXYx`) с более высоким кешевым `rev`. Условие `expected_rev=0` проверяет отсутствие ключа. [Store](../../internal/store/store.go)

## 4. Остановите стенд

```bash
docker compose -p oracache-demo-03 -f compose.cp.yaml down
```
