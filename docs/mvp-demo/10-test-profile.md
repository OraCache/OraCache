# 10. Тестовый профиль

**По обзору MVP требование реализовано частично.**

Запишите фактический protobuf `Profile` и синтетический UTF-8 JSON с раздельными полями имени. Сверьте байты после чтения обоих значений. Нужны Python 3, `jq`, `base64`, `cmp` и `protoc` со стандартными `.proto` в `/usr/include`.

## 1. Поднимите стенд

```bash
make build
docker compose -p oracache-demo-10 -f compose.cp.yaml up -d --build --force-recreate
curl -fsS http://127.0.0.1:9466/routes
```

Дождитесь маршрутов для партиции `"0"`.

## 2. Создайте protobuf-профиль

```bash
# Сохраняем текстовое представление Profile как вход для protoc.
mkdir -p .reports
cat > .reports/demo-10-profile.textproto <<'PROFILE'
full_name: "Воронова Елена Сергеевна"
age: 34
sex: SEX_FEMALE
registered_at { seconds: 1735689600 }
city: "Казань"
PROFILE
```

Выберите одну из двух команд кодирования. Для системного `protoc` используйте первую.

```bash
# --encode принимает текстовое представление Profile и пишет бинарный protobuf.
protoc -I proto -I /usr/include --encode=gateway.v1.Profile proto/cache/gateway.proto \
  < .reports/demo-10-profile.textproto > .reports/demo-10-profile.pb
```

Если `protoc` установлен через `make generate`, используйте эту команду.

```bash
.tools/protoc/bin/protoc -I proto -I .tools/protoc/include --encode=gateway.v1.Profile proto/cache/gateway.proto \
  < .reports/demo-10-profile.textproto > .reports/demo-10-profile.pb
```

```bash
wc -c .reports/demo-10-profile.pb
```

Ожидается ненулевой размер файла.

## 3. Создайте JSON размером 10 КиБ

```bash
python3 - <<'PYTHON'
import json
from pathlib import Path

profile = {
    "last_name": "Воронова",
    "first_name": "Елена",
    "patronymic": "Сергеевна",
    "age": 34,
    "sex": "SEX_FEMALE",
    "city": "Казань",
    "registered_at": "2025-01-01T00:00:00Z",
    "padding": "",
}


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


# Считаем байты UTF-8. Каждый добавленный символ x занимает один байт.
profile["padding"] = "x" * (10 * 1024 - len(encode(profile)))
payload = encode(profile)
assert len(payload) == 10 * 1024
Path(".reports/demo-10-profile-10k.json").write_bytes(payload)
PYTHON
wc -c .reports/demo-10-profile-10k.json
```

Ожидается `10240` байт.

## 4. Запишите оба файла и прочитайте ответ

```bash
# @путь передаёт файлы целиком, включая двоичные данные.
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:profile:protobuf \
  @.reports/demo-10-profile.pb --expected-rev=0
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache put demo:profile:json \
  @.reports/demo-10-profile-10k.json --expected-rev=0
```

Ожидаются два `PUT_STATUS_OK` с `rev=1` и `rev=2`.

```bash
# Извлекаем data из JSON-ответа и декодируем Base64 в отдельные файлы.
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:profile:protobuf --rev=1 |
  jq -er '.data' | base64 -d > .reports/demo-10-profile.pb.readback
env ORACACHE_ENDPOINT=127.0.0.1:28080 ./bin/oracache get demo:profile:json --rev=2 |
  jq -er '.data' | base64 -d > .reports/demo-10-profile-10k.json.readback
cmp .reports/demo-10-profile.pb .reports/demo-10-profile.pb.readback
cmp .reports/demo-10-profile-10k.json .reports/demo-10-profile-10k.json.readback
```

Для одинаковых файлов `cmp` завершается без вывода.

## 5. Посмотрите поля профиля

```bash
protoc -I proto -I /usr/include --decode=gateway.v1.Profile proto/cache/gateway.proto \
  < .reports/demo-10-profile.pb.readback
jq 'del(.padding)' .reports/demo-10-profile-10k.json.readback
```

При использовании `protoc` из `.tools` декодируйте protobuf этой командой:

```bash
.tools/protoc/bin/protoc -I proto -I .tools/protoc/include --decode=gateway.v1.Profile proto/cache/gateway.proto \
  < .reports/demo-10-profile.pb.readback
```

В protobuf имя хранится в `full_name`. В синтетическом JSON есть отдельные `last_name`, `first_name` и `patronymic`. [Схема Profile](../../proto/cache/gateway.proto) · [Исходные поля](../initial-task.md#структура-данных-для-тестирования)

## 6. Остановите стенд

```bash
docker compose -p oracache-demo-10 -f compose.cp.yaml down
```
