# OraCache

## PoC с Control Plane и Gateway

Подними три `storage-node` в одном регионе и отправь запросы через `api-gateway`. Ноды сами выбирают лидера Raft и сообщают о нём Control Plane (`cp-api`). Клиенту нужен только адрес gateway.

### Запустить стенд

Нужны Go 1.27.3, Make, Bash и `curl`. Docker не нужен. Все команды запускай из корня репозитория; первые три терминала оставь открытыми.

**Терминал 1: собрать бинарники и запустить Control Plane.**

```sh
make build
./bin/cp-api -config configs/cp-raft-local.json -listen 127.0.0.1:28081
```

**Терминал 2 (Bash): запустить три ноды.**

```bash
(
  peers=(
    -peer=1,node-1,127.0.0.1:28091,http://127.0.0.1:28191/raft
    -peer=2,node-2,127.0.0.1:28092,http://127.0.0.1:28192/raft
    -peer=3,node-3,127.0.0.1:28093,http://127.0.0.1:28193/raft
  )
  pids=()
  trap 'kill "${pids[@]}" 2>/dev/null; wait' EXIT
  trap 'exit 0' INT TERM
  for i in 1 2 3; do
    ./bin/storage-node -node-id="node-$i" -peer-id="$i" -cp-addr=127.0.0.1:28081 \
      -heartbeat-interval=1s -grpc-listen="127.0.0.1:2809$i" \
      -raft-listen="127.0.0.1:2819$i" "${peers[@]}" &
    pids+=("$!")
  done
  wait
)
```

**Терминал 3: запустить gateway.** Без `-listen` он отдаёт только диагностику, а не принимает клиентские запросы.

```sh
./bin/api-gateway -region region-a -node-id gw-local -cp-addr 127.0.0.1:28081 \
  -listen 127.0.0.1:28080 -poll-interval 1s
```

### Отправить запросы

В четвёртом терминале посмотри `curl -fsS http://127.0.0.1:9466/routes`. Когда в `read_routes` и `write_routes` появится партиция `0`, можно отправлять запросы. Если карты пока пустые, подожди пару секунд и повтори. Если маршрута всё ещё нет, проверь логи нод, `cp-api` и gateway.

```sh
export ORACACHE_ENDPOINT=127.0.0.1:28080
./bin/oracache put demo:key hello  # PUT_STATUS_OK
./bin/oracache get demo:key        # GET_STATUS_HIT, data: aGVsbG8= (hello в base64)
./bin/oracache delete demo:key     # DELETE_STATUS_DELETED
./bin/oracache get demo:key        # GET_STATUS_MISS
```

### Остановить стенд

Нажми `Ctrl+C` в терминалах с gateway, нодами и Control Plane. Данные Raft и отчёты нод в Control Plane хранятся в памяти. Для нового прогона поднимай всю группу заново; не возвращай одну ноду с пустым состоянием под тем же ID в работающую группу. Стенд слушает только `127.0.0.1` и работает без TLS и аутентификации.

### Проверить смену лидера

Тест сам поднимает стенд на свободных локальных портах, отключает доступ к Control Plane и останавливает лидера. Он проверяет, что через тот же адрес gateway можно прочитать ранее записанное значение и, после восстановления маршрута, снова записать данные. Для `-race` нужны cgo и C-компилятор.

```sh
GOWORK=off go test -race -count=1 -v ./cmd/api-gateway \
  -run '^TestGatewayRoutesRealRaftThroughLeaderAndControlPlaneLoss$'
```

Правила маршрутизации и повторов описаны в [ADR-004](docs/ADR-004-cp-raft-gateway-routing.md).

---

## PoC без Control Plane

Если хочешь проверить только хранилище и Raft, подними три `storage-node` в Docker Compose и отправляй запросы напрямую в ноды.

### Запустить кластер

Нужны запущенный Docker с Compose и **Go 1.27.3** для клиентской команды и тестов. Все команды ниже запускай из корня репозитория.

```sh
docker compose -f compose.storage.yaml up -d --build
docker compose -f compose.storage.yaml ps
```

Должны запуститься `node-1`, `node-2` и `node-3`. Их клиентские адреса: `127.0.0.1:28091`, `127.0.0.1:28092`, `127.0.0.1:28093` соответственно. Порты доступны только с твоего компьютера. Если нода не запустилась, посмотри `docker compose -f compose.storage.yaml logs --tail=50`.

### Отправить запросы

Сначала найди лидера. Номер ноды заранее неизвестен, а запись и чтение по умолчанию работают только через лидера:

```sh
for port in 28091 28092 28093; do
  printf '\nПорт %s\n' "$port"
  ORACACHE_ENDPOINT="127.0.0.1:$port" go run ./cmd/oracache get --timeout=2s demo:key || continue
done
```

Лидер вернёт JSON: `GET_STATUS_MISS`, если ключ ещё не записан, или `GET_STATUS_HIT`, если он уже есть. Ответ `not leader` от реплики — норма. Если так отвечают все три ноды, подожди пару секунд и повтори. Если всё ещё нет JSON, проверь ошибки CLI, `docker compose -f compose.storage.yaml ps` и логи.

Подставь порт лидера вместо `28091` и попробуй запись, чтение и удаление:

```sh
export ORACACHE_ENDPOINT=127.0.0.1:28091
go run ./cmd/oracache put demo:key hello  # PUT_STATUS_OK
go run ./cmd/oracache get demo:key       # GET_STATUS_HIT, data: aGVsbG8= (hello в base64)
go run ./cmd/oracache delete demo:key    # DELETE_STATUS_DELETED
go run ./cmd/oracache get demo:key       # GET_STATUS_MISS
```

Чтобы почитать с реплики, останови весь стенд командой ниже и запусти его заново с `ORACACHE_STORAGE_READ_MODE=local docker compose -f compose.storage.yaml up -d --build`. Попробуй `put demo:key hello` на трёх адресах, пока один не вернёт `PUT_STATUS_OK`: запись по-прежнему принимает только лидер, а `get` в этом режиме его не определяет. Затем выполни `get demo:key` на другом порту. Ожидается `GET_STATUS_HIT`; сразу после записи реплика может ещё вернуть `GET_STATUS_MISS`, тогда повтори чтение.

### Остановить стенд

```sh
docker compose -f compose.storage.yaml down
```

Данные хранятся в памяти и при остановке всего стенда пропадают. Чтобы начать с чистого листа, останови и подними весь стенд заново. Не перезапускай одну остановленную ноду под тем же ID, пока остальные работают.

### Запустить тесты

Для `make`-команд нужен Make, для тестов с `-race` — включённый cgo и C-компилятор.

```sh
make test
go test -race -count=1 -v ./internal/node -run '^TestStandaloneStorageCluster$'
make test-e2e
```

`make test` запускает обычные тесты без Docker. Сквозной тест трёх нод тоже не требует Docker: он сам запускает ноды и проверяет репликацию, чтение и запись после смены лидера. `make test-e2e` проверяет локальный кэш без Raft, а не кластер. Для ручной проверки Docker-стенда пройди сценарий `put → get → delete → get` выше.

---

## Локальный кэш без Raft

Обычный `docker compose up` запускает `cmd/gateway`. Этот сервер хранит данные в собственной памяти и не пересылает запросы в `storage-node`. Примеры и ограничения ниже относятся только к этому серверу.

Локальный Go CLI и синхронный in-memory gRPC сервер по
[`proto/cache/gateway.proto`](proto/cache/gateway.proto). Реализованы все пять
RPC `CacheGateway`: `Get`, `Put`, `Delete`, `BatchGet`, `BatchPut`. CLI предоставляет
`get`, `put`, `delete`; batch доступен через сгенерированный Go клиент.

> **Намеренное упрощение: времени нет, память бесконечна.** Нет TTL,
> продления TTL, hard expiry, ограничения числа версий, фонового удаления,
> репликации, региональной маршрутизации и warmup. Все версии хранятся до
> явного `Delete`. `expires_at` / `hard_expires_at` и `served_by` не заполняются;
> `RoutingHints` игнорируются. Комментарии исходного proto описывают более
> широкую целевую систему, а не гарантии этого локального сервера. Proto не изменён.
>
> Данные и счётчики живут только в памяти процесса и теряются при перезапуске.
> Это тестовый сервер: **plaintext gRPC, без TLS и аутентификации**. По умолчанию
> слушает только localhost; не публикуйте его в недоверенную сеть.

### Запуск локального кэша

С Docker и Compose v2 достаточно:

```sh
docker compose up -d
ORACACHE_ENDPOINT=localhost:50051 go run ./cmd/oracache get key
# {"status":"GET_STATUS_MISS"}
docker compose down
```

При первом запуске Compose собирает `gateway` из Dockerfile; следующие запуски
используют существующий локальный образ `oracache-gateway:local` без pull.
В образе статический бинарник в `scratch`,
непривилегированный UID/GID 65532. Единственный опубликованный порт —
`127.0.0.1:50051`; внутри контейнера gateway слушает `0.0.0.0:50051`.
`ORACACHE_PORT=50052 docker compose up -d` меняет host-порт, `0` выбирает
свободный автоматически (`docker compose port gateway 50051`). Клиенту всегда
нужно явно передать соответствующий `ORACACHE_ENDPOINT`. После изменения кода
используйте `make up` (`docker compose up -d --build`); остановка — `make down`.
Данные теряются при перезапуске контейнера.

Без Docker:

Нужен Go 1.27.3 (локально, в CI и Docker). Сгенерированный код уже в
репозитории: для сборки `protoc` не нужен.

```sh
make build
./bin/gateway                 # 127.0.0.1:50051
# или: ./bin/gateway --listen 127.0.0.1:50052
# порт 0 выбирается ОС, фактический адрес печатается как "listening host:port"
```

Сервер завершает текущие RPC при SIGINT/SIGTERM. У самого кеша нет фоновых
задач, таймеров или goroutine; gRPC использует собственные транспортные goroutine,
а процесс отдельно ожидает сигнал завершения.

В другом терминале:

```sh
export ORACACHE_ENDPOINT=127.0.0.1:50051  # обязательно, значения по умолчанию нет
./bin/oracache get profile:42
# {"status":"GET_STATUS_MISS"}

./bin/oracache put profile:42 'hello' --expected-rev=0
# {"status":"PUT_STATUS_OK","rev":"1"}
./bin/oracache get profile:42
# {"status":"GET_STATUS_HIT","rev":"1","data":"aGVsbG8=","latestRev":"1"}

printf '%s' 'new profile bytes' > profile.bin
./bin/oracache put profile:42 @profile.bin --expected-rev=1
# stdin читается только явно:
printf '%s' 'bytes from stdin' | ./bin/oracache put other:key @-
./bin/oracache get --rev=1 profile:42
./bin/oracache delete --rev=2 profile:42  # удалить только версию 2
./bin/oracache get profile:42            # снова версия 1
./bin/oracache delete profile:42        # удалить все версии
./bin/oracache get profile:42            # MISS
```

### Синтаксис CLI

```text
oracache get    [--rev=N] [--timeout=5s] KEY
oracache put    [--expected-rev=N] [--timeout=5s] KEY VALUE
oracache delete [--rev=N] [--timeout=5s] KEY
```

CLI использует Kong v1.6.1 (Go 1.22). Флаги допустимы до и после позиционных
аргументов. `oracache --help` и `oracache put --help` показывают справку без
endpoint, RPC и чтения stdin. `--timeout` — положительная
длительность одного RPC. `ORACACHE_ENDPOINT` — gRPC target, обычно `host:port`;
соединение всегда без TLS. Нет скрытого localhost fallback.

`put KEY VALUE` требует оба аргумента: обычный VALUE — буквальные байты строки
(включая пробелы в кавычках); `''` — пустой payload. `@path` читает файл без
преобразований, `@-` — stdin, `@@text` записывает буквальное `@text`. Для значения
с начальным дефисом используйте `--` перед позиционными аргументами. `--file`
удалён; пропущенный VALUE — ошибка, а не неявное ожидание stdin.
Кеш не валидирует `Profile`: можно передать protobuf Profile, JSON или любые
бинарные данные. CLI ограничивает чтение 10241 байтом и отклоняет payload
больше **10 КиБ = 10240 байт**, не отправляя RPC. На сервере этот же предел
возвращает доменный `PUT_STATUS_TOO_LARGE`.

stdout содержит один JSON-ответ в формате `protojson`: enum — строкой,
`uint64` — десятичной строкой, `bytes` — **base64**, имена полей — `lowerCamelCase`.
Поля с нулевыми значениями и незаданные сообщения опускаются. Порядок JSON полей
и пробелы не являются частью контракта. stderr — ошибки клиента/транспорта.

Код выхода **0** означает успешный RPC, а не обязательно HIT/OK/DELETED:
MISS, VERSION_NOT_FOUND, CONFLICT, NOT_FOUND тоже возвращаются JSON-ответом и
кодом 0. Проверяйте поле `status`. Ошибки аргументов, файлов, превышение лимита
на стороне CLI, недоступность сервера и ошибки gRPC дают ненулевой код выхода.

### Семантика локального кэша

- KEY — непустая строка. Некорректный ключ даёт gRPC `InvalidArgument`.
- `Put` назначает возрастающие номера **отдельно для каждого ключа**, начиная с 1.
  Счётчик не сбрасывается при удалении одной/всех версий: номер не переиспользуется
  в течение жизни процесса. При исчерпании `uint64` — gRPC `ResourceExhausted`.
- `expected_rev` отсутствует: безусловная запись. Явный `0`: записать только при
  отсутствии живых версий. `N > 0`: записать, только если последняя **сохранившаяся**
  версия равна N. При несовпадении — `PUT_STATUS_CONFLICT` с текущим `rev`
  (0, если живых версий нет). Неудачная запись не расходует номер.
- `Get rev=0` возвращает максимальную существующую версию. После удаления
  последней версии предыдущая становится последней. Явный отсутствующий `rev`
  при наличии других версий даёт `GET_STATUS_VERSION_NOT_FOUND` и `latestRev`.
  Если живых версий ключа нет вообще — `GET_STATUS_MISS`, даже для явного `rev`.
- `Delete rev=0` удаляет все версии, иначе только указанную. Повторное удаление
  возвращает `DELETE_STATUS_NOT_FOUND`, не ошибку gRPC.
- Payload проверяется до `expected_rev`; слишком большой всегда даёт TOO_LARGE.
  Пустой payload допустим. Входные/выходные байты копируются.
- Batch — не больше **1000** элементов, включая пустой batch (допустим).
  Превышение лимита и некорректный ключ дают gRPC `InvalidArgument` для всего
  запроса; ключи проверяются до любых изменений. Ответы идут в порядке запроса.
  `BatchPut` применяет элементы последовательно, включая повторяющиеся ключи;
  доменные ошибки одного элемента не отменяют успешные записи других.
  При исчерпании счётчика возможна частичная запись и `ResourceExhausted` всего RPC.
- Все обращения к состоянию защищены одним mutex, включая batch целиком.
  Условные записи атомарны относительно других RPC. Нет I/O в критической секции.

Go клиент и интерфейс сервера находятся в `gen/gateway/v1` (пакет `gatewayv1`),
модуль — `github.com/OraCache/oracache/gen` (полный import path:
`github.com/OraCache/oracache/gen/gateway/v1`). Лимит входящего gRPC сообщения сервера —
16 МиБ: обычная пачка из 1000 payload по 10 КиБ помещается целиком. Для больших
`BatchGet` Go клиент должен увеличить стандартный receive limit (4 МиБ), например
через `grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16 << 20))`. Этот
транспортный лимит независим от доменных лимитов payload и размера batch.

## Go workspace и модули

В корне закоммичен `go.work` (Go 1.27.3), объединяющий четыре модуля:

| Каталог | Module path | Назначение |
| --- | --- | --- |
| `.` | `github.com/OraCache/oracache` | Control Plane, gateway, хранилище и Raft |
| `gen` | `github.com/OraCache/oracache/gen` | сгенерированные protobuf/gRPC API |
| `cmd/oracache` | `github.com/OraCache/oracache/cmd/oracache` | CLI и Kong |
| `integration/e2e` | `github.com/OraCache/oracache/integration/e2e` | Docker/Compose и testcontainers |

У каждого модуля свои `go.mod`/`go.sum`. Testcontainers не входит в граф
зависимостей gateway, CLI или protobuf; Kong нужен только CLI. Локальные
`replace` связывают модули и без workspace. Из корня можно запускать
`go run ./cmd/oracache --help` и `go run ./cmd/gateway`.
`go test ./...` из корня **не обходит вложенные модули**: используйте `make test`.
Make запускает тесты и vet каждого модуля с `GOWORK=off`, проверяя также его
самостоятельную разрешимость. `make build` использует workspace.
Для обновления зависимостей запускайте `GOWORK=off go mod tidy` отдельно
из каждого из четырёх каталогов. `go.work.sum` — локальный, игнорируемый файл;
не используйте `go work sync` вместо независимого tidy.

## Проверки и генерация

```sh
make build                # в том числе cp-api, storage-node, api-gateway и oracache
make test                 # корневой модуль, protobuf и CLI, -race, без Docker
make test-gateway         # только корневой модуль
make test-protobuf        # компиляция API; сейчас нет test cases
make test-cli             # CLI, включая настоящий localhost gRPC
make vet                  # три обычных модуля, без Docker
make test-e2e             # отдельный модуль, нужен Docker daemon
make generate             # или make gen; генерация в gen/gateway/v1
make docker-build         # только сборка Docker образа
make up                   # пересобрать и запустить Compose
make down                 # остановить Compose
git diff --exit-code -- gen
```

`make test` и `make test-e2e` автоматически устанавливают закреплённый
`gotestsum v1.13.0` (проверен с Go 1.27.3) через `go install ...@v1.13.0`
в `.tools/bin/`, без sudo и без зависимости в production `go.mod`.
Первый запуск требует доступа к Go modules; последующие используют локальный
бинарник и Go-кеши. В консоли видны имена тестов, PASS/FAIL и итог.
Ненулевой код `go test` сохраняется через gotestsum и Make.
JUnit-файлы: `.reports/gateway.xml`, `.reports/protobuf.xml`,
`.reports/cli.xml`, `.reports/e2e.xml`; каталог игнорируется git.
Перед запуском target удаляет свой старый отчёт, а `make test` — все три
обычных отчёта ещё до установки инструмента. Если запуск оборвался до
создания XML, отчёта нет: это не означает «0 тестов успешно».
Не запускайте пересекающиеся test targets одновременно в одном checkout.

GitHub Actions загружает доступные XML как артефакты `junit-unit` и
`junit-e2e`, даже при падении тестов. В **Summary** каждого job
`test-summary/action` показывает все результаты (passed, failed, skipped)
с раскрываемыми деталями из JUnit. Полные логи остаются в шагах job;
отчёт не заменяет exit status тестов, сборки, vet или генерации.
Отдельного HTML/Pages сайта и Python-зависимости нет; workflow требует
только `contents:read`. Если тесты не успели создать XML, смотрите ошибку
исходного шага: отсутствие результатов не означает успешный запуск.

Unit/integration тесты проверяют версии, условные записи, удаление, лимиты,
порядок batch, отсутствие retention/expiry, копирование байтов, конкурентную
запись и CLI с настоящим gRPC транспортом. CLI — ручной инструмент разработчика;
e2e не запускает CLI. Go e2e под тегом `e2e` использует сгенерированный
protobuf/gRPC клиент и `testcontainers-go/modules/compose v0.33.0` (ComposeStack).
Локально он собирает образ из того же `compose.yaml` с уникальным тегом (не
переиспользует старый образ приложения), создаёт отдельный проект, выбирает
свободный host-порт и ждёт listening log + реальный gRPC RPC с таймаутом.
Проверяются полные protobuf ответы: MISS → PUT → HIT с бинарными байтами,
CONFLICT, явный ноль и отсутствие expected_rev, смешанные batch, DELETE → MISS.
Бизнес-матрица версий и счётчика проверяется unit-тестами. Cleanup
зарегистрирован до запуска и ограничен таймаутом. Отсутствие
Docker — ошибка явного e2e запуска, не skip. Обычные тесты Docker не требуют.

Генерация поддерживает Linux x86_64, требует `curl`, `unzip`, `sha256sum`, Go и
доступ к GitHub/Go modules. Makefile использует закреплённые версии:
`protoc 27.2` (архив проверяется SHA-256), `protoc-gen-go v1.34.2`,
`protoc-gen-go-grpc v1.4.0`. Инструменты ставятся только в `.tools/`, системный Go
не изменяется. `make generate` генерирует настоящий protobuf-код и gRPC client/server
интерфейсы из исходного proto. `warmup.proto` намеренно не реализован и не включён
в генерацию этого сервиса. `.tools/` и `bin/` исключены из git. GitHub Actions
проверяет генерацию, race/vet и отдельным job запускает Go e2e с Docker.

### Кеши сборки

Go использует стандартные `GOMODCACHE` и `GOCACHE`; в обоих CI jobs их
сохраняет `actions/setup-go` по четырём `go.sum` и pin инструмента в Makefile.
Docker-кеш независим: зависимости gateway/protobuf находятся в обычном слое,
компиляция использует локальный BuildKit cache mount. В CI Buildx экспортирует
слои через `type=gha,mode=max` (не содержимое mount) и загружает текущий образ
`oracache-gateway:ci` в daemon. E2e использует его через `ORACACHE_GATEWAY_IMAGE`
без повторной сборки; отсутствующий явно заданный образ — ошибка. Локально
переменная не нужна: тест собирает уникальный образ. `-count=1` всегда
выполняет e2e заново, независимо от кеша сборки.

## Исходная документация

- [Задание и целевая архитектура](docs/initial-task.md)
- [Глоссарий](docs/GLOSSARY.md)
