# OraCache

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

## Быстрый запуск

Нужен Go 1.22+ (локально и в CI проверяется 1.22.2). Сгенерированный код уже в
репозитории: для сборки `protoc` не нужен.

```sh
mkdir -p bin
go build -o bin/oracache ./cmd/oracache
go build -o bin/oracache-server ./cmd/oracache-server
./bin/oracache-server                 # 127.0.0.1:50051
# или: ./bin/oracache-server --listen 127.0.0.1:50052
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

printf '%s' 'hello' | ./bin/oracache put --expected-rev=0 profile:42
# {"status":"PUT_STATUS_OK","rev":"1"}
./bin/oracache get profile:42
# {"status":"GET_STATUS_HIT","rev":"1","data":"aGVsbG8=","latestRev":"1"}

printf '%s' 'new profile bytes' > profile.bin
./bin/oracache put --file profile.bin --expected-rev=1 profile:42
./bin/oracache get --rev=1 profile:42
./bin/oracache delete --rev=2 profile:42  # удалить только версию 2
./bin/oracache get profile:42            # снова версия 1
./bin/oracache delete profile:42        # удалить все версии
./bin/oracache get profile:42            # MISS
```

### Синтаксис CLI

```text
oracache get    [--rev=N] [--timeout=5s] KEY
oracache put    [--file=PATH|-] [--expected-rev=N] [--timeout=5s] KEY
oracache delete [--rev=N] [--timeout=5s] KEY
```

Флаги идут **перед KEY** (стандартный Go `flag`). `--timeout` — положительная
длительность одного RPC. `ORACACHE_ENDPOINT` — gRPC target, обычно `host:port`;
соединение всегда без TLS. Нет скрытого localhost fallback.

`put` читает сырые байты из файла либо stdin (`--file=-`, также по умолчанию).
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

## Семантика сервера

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
модуль — `github.com/OraCache/oracache`. Лимит входящего gRPC сообщения сервера —
16 МиБ: обычная пачка из 1000 payload по 10 КиБ помещается целиком. Для больших
`BatchGet` Go клиент должен увеличить стандартный receive limit (4 МиБ), например
через `grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16 << 20))`. Этот
транспортный лимит независим от доменных лимитов payload и размера batch.

## Проверки и генерация

```sh
go test -race ./...
go vet ./...
bash scripts/smoke.sh
bash scripts/generate.sh
git diff --exit-code -- gen
```

Unit/integration тесты проверяют версии, условные записи, удаление, лимиты,
порядок batch, отсутствие retention/expiry, копирование байтов, конкурентную
запись и CLI с настоящим gRPC транспортом. Smoke сам собирает **оба бинарника**,
запускает сервер на свободном порту localhost, ограниченно ждёт готовность,
проверяет JSON MISS → PUT → HIT с бинарным payload → DELETE → MISS, старую
версию, CONFLICT и сохранение счётчика после удаления. Trap завершает сервер и
удаляет временный каталог. Для smoke нужны bash и Python 3.

Генерация поддерживает Linux x86_64, требует `curl`, `unzip`, `sha256sum`, Go и
доступ к GitHub/Go modules. Скрипт использует закреплённые версии:
`protoc 27.2` (архив проверяется SHA-256), `protoc-gen-go v1.34.2`,
`protoc-gen-go-grpc v1.4.0`. Инструменты ставятся только в `.tools/`, системный Go
не изменяется. Скрипт генерирует настоящий protobuf-код и gRPC client/server
интерфейсы из исходного proto. `warmup.proto` намеренно не реализован и не включён
в генерацию этого сервиса. `.tools/`, `bin/` и временные каталоги smoke исключены
из git. GitHub Actions выполняет те же race/vet/smoke проверки и проверяет
воспроизводимость генерации.

## Исходная документация

- [Задание и целевая архитектура](docs/initial-task.md)
- [Глоссарий](docs/GLOSSARY.md)
