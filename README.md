# OraCache

Подними три ноды хранилища на ноутбуке и отправляй запросы прямо в любую из них. Для этого стенда не нужны Control Plane, gateway или отдельная база данных.

## Запустить кластер

Нужны запущенный Docker с Compose и **Go 1.27.3** для клиентской команды и тестов. Все команды ниже запускай из корня репозитория.

```sh
docker compose -f compose.storage.yaml up -d --build
docker compose -f compose.storage.yaml ps
```

Должны запуститься `node-1`, `node-2` и `node-3`. Их клиентские адреса: `127.0.0.1:28091`, `127.0.0.1:28092`, `127.0.0.1:28093` соответственно. Порты доступны только с твоего компьютера. Если нода не запустилась, посмотри `docker compose -f compose.storage.yaml logs --tail=50`.

## Отправить запросы

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

## Остановить стенд

```sh
docker compose -f compose.storage.yaml down
```

Данные хранятся в памяти и при остановке всего стенда пропадают. Чтобы начать с чистого листа, останови и подними весь стенд заново. Не перезапускай одну остановленную ноду под тем же ID, пока остальные работают.

## Запустить тесты

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

## Control Plane Phase 1 (target code)

Целевой Control Plane реализован отдельно от legacy-кеша и не меняет контракт
`proto/cache/*`:

- `proto/controlplane/v1/controlplane.proto` — `ControlPlaneAPI`
  (`ReportHeartbeat`, `GetConfig`). Assignments регион-скоупные:
  `(region, partition_id)` — отдельная локальная Raft-группа, у master и slave
  независимые лидеры. Конфиг версионирован (`config_revision`/`known_revision`):
  Storage Node получает тело только при новой revision. Gateway получает
  live-view и при прежней revision. В live-view assignment содержит
  `ready/readable/writable` по текущему лидеру: write — только master-регион
  с готовым лидером, read — готовый лидер для strict-чтения.
- `internal/statestore` — граница `Store`; `MemoryStore` — временная Phase 1
  реализация без etcd (TODO и ADR-001). `LiveConfig` строит live-view с TTL.
- `internal/cpapi` — stateless gRPC API: принимает heartbeat, отдаёт snapshot
  и live-view. Liveness считается по TTL. При выборе маршрута учитываются
  только свежие отчёты назначенных реплик о собственном лидерстве. Если
  две реплики заявляют лидерство в одном максимальном term, партиция
  считается неготовой. Placement задаётся статической конфигурацией.
- `internal/cpagent` — встроенный модуль `oracache-node`, не сайдкар:
  heartbeat раз в 5 с, config pull раз в 10 с и немедленный pull, если
  heartbeat вернул новую revision. Ошибки CP логируются и ретраятся; Data
  Plane продолжает работать по last-known-good.
- `internal/gateway` — target Router/Poller: `hash(key) % P` через xxhash64,
  локальный лидер для GET и лидер master-региона для PUT, атомарная подмена
  routing map. `ApplyView` принимает live-view при той же revision и проверяет
  `ready/readable/writable` конкретной партиции; мёртвые и not-ready ноды и
  партиции не маршрутизируются. Legacy `cmd/gateway`/`internal/cache` не
  затронуты.
- `internal/partition` — стабильный xxhash64, fixed partition mapping и
  парсер спецификаций партиций (`all`/`even`/`odd`/`1-9`/`0,2,5-9`).
- `internal/cpmetrics` — реестр метрик CP в формате Prometheus text exposition
  без внешних зависимостей.
- `cmd/cp-api` — plaintext gRPC сервер CP API: `-listen`, `-node-ttl`,
  `-metrics-listen` и `-config`/`-dev-cluster`.
- `cmd/oracache-node` — PoC Storage Node: heartbeat-источник со статической
  топологией внутри шарда; локальные partition-группы отдаёт по HTTP.
- `cmd/api-gateway` — PoC target Gateway: тянет состав кластера из CP и держит
  routing map, который отдаёт по HTTP `/routes`.

```sh
make build

# Вариант 1: синтетический кластер (2 региона × 2 ноды × 128 партиций)
./bin/cp-api -dev-cluster -listen 127.0.0.1:50052 -node-ttl 15s

# Вариант 2: свой bootstrap JSON
./bin/cp-api -listen 127.0.0.1:50052 -config /path/to/cp.json
```

Формат JSON — `revision`, `partition_count`, `regions`, `nodes`, `partitions`;
роль региона — `master`/`slave`. Валидация требует ровно один master-регион,
полный набор assignments для каждого объявленного региона и лидера внутри его
реплик. Пример с уменьшенным числом партиций —
[`configs/cp-dev.json`](configs/cp-dev.json).

### Локальный сценарий control plane

Четыре команды дают полный PoC из задачи #35: ноды шлют heartbeat, gateway
периодически забирает состав и знает, куда маршрутизировать.

```sh
# 1. CP API. node-ttl задаёт TTL liveness; интервал heartbeat нод по
#    умолчанию 5 с, поэтому TTL должен быть больше него.
./bin/cp-api -dev-cluster -listen 127.0.0.1:50052 -node-ttl 15s

# 2. Две ноды: каждая держит все партиции региона и лидирует в своей половине.
#    -status-listen обязателен разным у каждой ноды: дефолт общий.
./bin/oracache-node -node-id eu-1 -region eu -status-listen 127.0.0.1:9465 \
  -partitions all -leader-partitions even -heartbeat-interval 1s
./bin/oracache-node -node-id eu-2 -region eu -status-listen 127.0.0.1:9467 \
  -partitions all -leader-partitions odd  -heartbeat-interval 1s

# 3. Gateway тянет live-view и держит routing map.
./bin/api-gateway -region eu -node-id gw-eu-1 -poll-interval 2s
```

Проверка:

```sh
# Локальные partition-группы ноды (128 групп, роль и готовность).
curl localhost:9465/partitions

# Routing map gateway: 128 read/write маршрутов, лидеры чередуются.
curl localhost:9466/routes

# Метрики CP: heartbeat, config pull, live-ноды по состояниям.
curl localhost:9464/metrics | grep oracache_cp_
```

Отказ ноды: после остановки `eu-1` и истечения TTL маршруты партиций, где она
лидер, исчезают (`read_routes` становится меньше 128), а `config_revision`
**не меняется** — liveness не создаёт новую revision конфигурации.

Спецификации партиций: `all`, `even`, `odd`, `1-9`, `0,2,5-9` и их список.
Флаг `-heartbeat-interval` фиксирует интервал и имеет приоритет над
подсказкой CP API; без него агент следует подсказке (по умолчанию 5 с).

**Рестарт CP API.** Пока сервер собирает первые heartbeat от назначенных
реплик, Gateway с той же ревизией сохраняет последнюю карту. Это окно
закрывается после получения heartbeat от всех назначенных реплик вне drain
или по истечении `-node-ttl` с запуска CP (по умолчанию 15 с). Gateway без карты
или с более старой ревизией сразу получает доступный live-view.

В Phase 1 heartbeat и карта хранятся в памяти. `oracache-node` на стенде
публикует заданные флагами статусы, а `api-gateway` показывает маршруты через
`/routes`. Постоянное хранилище состояния CP и интеграция с реальными Raft
группами относятся к следующим этапам.

Тесты Phase 1 входят в `make test`: валидация конфига, монотонность revision,
контракт CP API, TTL/liveness, выбор лидера среди назначенных реплик, циклы
агента и сохранение маршрута при рестарте CP через loopback gRPC.

## Go workspace и модули

В корне закоммичен `go.work` (Go 1.27.3), объединяющий четыре модуля:

| Каталог | Module path | Назначение |
| --- | --- | --- |
| `.` | `github.com/OraCache/oracache` | gateway и in-memory кеш |
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
make build                # bin/gateway и bin/oracache
make test                 # gateway + protobuf + CLI, -race, без Docker
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
