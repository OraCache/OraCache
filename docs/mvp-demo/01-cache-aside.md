# 01. Cache-aside и заполнение после `MISS`

**По обзору MVP требование реализовано частично.**

Запустите стенд из корня репозитория. В демонстрации JSON играет роль результата чтения из источника данных.

## 1. Соберите CLI и запустите Gateway

```bash
make build
# Публикуем адреса Gateway для CLI и HTTP-диагностики.
docker compose -p oracache-demo-01 -f compose.cp.yaml up -d --build --force-recreate
curl -fsS http://127.0.0.1:9466/routes
```

Повторите `curl`, пока в `read_routes` и `write_routes` появится партиция `"0"`.

## 2. Получите промах

```bash
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get profile:101
```

Ожидается `GET_STATUS_MISS`.

## 3. Заполните кеш

```bash
# @- читает JSON из stdin. expected-rev=0 разрешает заполнение отсутствующего ключа.
printf '%s' '{"id":101,"fullName":"Ирина Орлова","city":"Казань"}' |
  env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put profile:101 @- --expected-rev=0
```

Ожидается `PUT_STATUS_OK` с `rev` равным `1` на пустом стенде.

## 4. Прочитайте профиль из кеша

```bash
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get profile:101
```

Ожидается `GET_STATUS_HIT` с тем же `rev`. Для отображения JSON в поле `data` используйте `jq`.

```bash
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get profile:101 |
  jq '.data | @base64d | fromjson'
```

## 5. Остановите стенд

```bash
docker compose -p oracache-demo-01 -f compose.cp.yaml down
```

Автоматический cache-aside-клиент с БД остаётся за пределами этого сценария. [Обзор MVP](mvp-review.md)
