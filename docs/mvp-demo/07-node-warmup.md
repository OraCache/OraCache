# 07. Прогрев перед допуском ноды

**По обзору MVP требование реализовано частично.**

На стенде региона A добавьте `a-4` как learner. Посмотрите её Raft-состояние до и после повышения. Перед повторным прогоном выполните остановку с `--volumes`, чтобы удалить сохранённый состав этого демостенда.

## 1. Поднимите исходную группу и запишите ключ

```bash
make build
docker compose -p oracache-demo-07 -f compose.replication.yaml build cp-api
docker compose -p oracache-demo-07 -f compose.replication.yaml up -d --no-build \
  cp-api api-gateway a-1 a-2 a-3
curl -fsS http://127.0.0.1:39466/routes
```

Когда появится маршрут записи для партиции `"0"`, проверьте исходный состав и создайте контрольную запись.

```bash
env ORACACHE_ENDPOINT=127.0.0.1:38080 ./bin/oracache membership get \
  --group-id=region-a/shard-0/replication-1 --region=region-a --partition-id=0
env ORACACHE_ENDPOINT=127.0.0.1:38080 ./bin/oracache put demo:warmup seeded --expected-rev=0
```

В группе три участника, ответ PUT содержит `rev=1`.

## 2. Запустите пустую ноду

```bash
# Профиль learner включает a-4. Нода уже настроена с новым Raft ID 4 и флагом --join.
docker compose -p oracache-demo-07 -f compose.replication.yaml \
  --profile learner up -d --no-build --no-deps a-4
curl -fsS http://127.0.0.1:38194/status
```

Ожидаются `Running=true`, `Member=false`, `Ready=false`. Повторите `curl`, если процесс ещё запускается.

## 3. Добавьте learner и дождитесь синхронизации

```bash
# Передаём внутренние адреса новой ноды. operation-id остаётся тем же при повторе операции.
env ORACACHE_ENDPOINT=127.0.0.1:38080 ./bin/oracache membership add-learner \
  --group-id=region-a/shard-0/replication-1 --region=region-a --partition-id=0 \
  --operation-id=demo-07-add-a4 --peer-id=4 --node-id=a-4 \
  --address=a-4:28091 --raft-address=http://a-4:28191/raft
curl -fsS http://127.0.0.1:38194/status
```

Ответ membership показывает `learner=true` для `a-4`. В `/status` видны `IsLearner=true` и `Ready=false`. Повторите `curl`, пока `AppliedIndex` достигнет `changeIndex` из ответа `add-learner`.

## 4. Повысьте ноду и прочитайте ключ

```bash
env ORACACHE_ENDPOINT=127.0.0.1:38080 ./bin/oracache membership promote \
  --group-id=region-a/shard-0/replication-1 --region=region-a --partition-id=0 \
  --operation-id=demo-07-promote-a4 --peer-id=4
curl -fsS http://127.0.0.1:38194/status
```

Если learner ещё догоняет журнал, повторите тот же `promote` после проверки `/status`. Ожидаются `voter=true` в membership и `Member=true`, `Ready=true` в `/status`.

```bash
env ORACACHE_ENDPOINT=127.0.0.1:38080 ./bin/oracache get demo:warmup --rev=1
```

Gateway возвращает `HIT` с `rev=1` и `data=c2VlZGVk` (`seeded`).

## 5. Остановите стенд

```bash
docker compose -p oracache-demo-07 -f compose.replication.yaml --profile learner down --volumes
```

## Восстановление снапшота и публикация готовности без Docker

```bash
env GOWORK=off go test -count=1 -v -timeout=35s ./internal/node -run '^TestHTTPMembershipReplacesLostStore$'
env GOWORK=off go test -count=1 -v -timeout=45s ./cmd/api-gateway -run '^TestMembershipThroughGatewayAndCPProjection$'
```

Первый тест проверяет, что пустая реплика восстанавливает значение из снапшота до повышения. Второй проверяет исключение learner из `ReadyReplicaNodeIDs` и добавление туда после повышения. Долю `MISS` при добавлении ноды под нагрузкой предстоит измерить. [Тест снапшота](../../internal/node/membership_test.go) · [Тест проекции CP](../../cmd/api-gateway/membership_integration_test.go)
