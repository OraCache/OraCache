# Локальное профилирование OraCache на macOS

Нужны Go версии из `go.mod`, Python 3 и свободные loopback-порты 28766, 28780, 28781, 28791–28793, 28891–28893, 28980, 28991–28993. Для варианта с контейнерами нужен Docker Desktop с Compose v2 и поддержкой `dockerfile_inline`. Запускайте команды из корня репозитория, последовательно. Для автономного запуска с уже заполненным Go module cache можно выполнить `export GOPROXY=off GOTOOLCHAIN=local`.

## Один эксперимент

```sh
python3 scripts/profiling/run.py get 500
python3 scripts/profiling/run.py get 10000 --via gateway --phase profile
python3 scripts/profiling/run.py get 10000 --phase trace
python3 scripts/profiling/run.py put 50
python3 scripts/profiling/run.py put 200
python3 scripts/profiling/run.py put 500
python3 scripts/profiling/run.py put 500 --phase profile
python3 scripts/profiling/run.py put 500 --phase trace
```

Каждая команда собирает бинарники, запускает отдельные процессы на loopback, ждёт выбора лидера и завершения прогрева. После измерения скрипт останавливает созданные им процессы. По умолчанию работают одна storage-нода, CP и Gateway. Direct обращается к лидеру. Рабочий набор GET засевается до окна измерений, размер значения 10 240 байт, по умолчанию 15 с, 32 worker, один переиспользуемый gRPC-канал и минимум 100 прогревочных RPC. Клиент ждёт ответ перед следующим запросом worker. Общий ticker может пропускать такты. `actual_RPS` включает ошибки и рассчитан по времени до завершения последнего запроса. Очередь до старта RPC в latency не входит. Перцентили клиента вычислены по отдельным запросам методом nearest rank.

Для PUT обычный рабочий набор составляет 32 ключа. Записи без `expected_rev` разрешены контрактом, Store присваивает глобальную ревизию. Клиент проверяет и gRPC error, и прикладной ответ. Для GET он проверяет версию засеянного значения, размер и все байты ответа. Все ошибки сохраняются в CSV. В summary приведены отдельные распределения успешных и неуспешных вызовов.

## GET по одному и 2048 ключам

Из корня проекта на macOS:

```sh
python3 scripts/profiling/run.py get 10000 --keys 2048 --via direct --seconds 6
scripts/profiling/get_matrix.sh
KEY_COUNTS=2048 PERF_SECONDS=15 scripts/profiling/get_matrix.sh
PERF_SECONDS=30 SEED=71 scripts/profiling/get_matrix.sh
```

Матрица запускает свежую группу на каждую строку. Она сравнивает один ключ и 2048 ключей на 1 и 3 нодах, direct к лидеру, linearizable, 1000/3000/6000/10000 целевых RPS. Значение — 10 240 байт, 32 worker, один клиентский gRPC-канал. По умолчанию измерение длится 6 секунд. В обоих плечах матрицы перед измерением выполняется 2048 прогревочных GET. Для отдельной команды `--warmup N` задаёт нижнюю границу: клиент читает `max(N, keys)` значений с циклическим перебором по индексу. GET без `--keys` использует прежний единственный ключ, PUT без этого флага использует 32 ключа.

1. Клиент последовательно выполняет ровно один подтверждённый PUT для каждого ключа и запоминает возвращённую ревизию. У индекса 0 прежние имя и значение, у остальных имя содержит индекс, а первые восемь байт значения содержат его little-endian представление. При 2048 ключах значения занимают 20 МиБ до накладных расходов.
2. Прогрев читает каждую запись, проверяет HIT, ревизию, размер и байты. Для нескольких ключей в `node-N.after-warmup.json` сохраняется статус каждой ноды, включая `Running`, `LastError` и `SnapshotIndex`. Действуют штатные настройки checkpoint. Ошибка подготовки прерывает запуск до нагрузки, причина записывается в `summary.txt`, CSV содержит только заголовок.
3. После сигнала старта worker отправляют только GET. По умолчанию Go строит одну `math/rand.Rand.Perm(keys)` с `rand.NewSource(seed)` (seed по умолчанию 1). Позиция измеренного запроса по порядку выдачи request ID выбирает элемент этой циклической перестановки. За полный цикл каждый ключ встречается ровно один раз, в неполном цикле количества отличаются максимум на один. Это сбалансированная выборка без возвращения внутри цикла. Порядок фактического прихода параллельных RPC на сервер зависит от планировщика.
4. Таймер, общий ticker, правило nearest rank и знаменатель RPS с drain сохранены. Проверка HIT/версии/размера входит в старый интервал RPC. Дополнительное сравнение всех байтов выполняется после остановки таймера. Оно занимает время worker и может влиять на фактический RPS. Seed PUT и прогрев исключены из измерения, последовательность измеряемых индексов начинается с позиции 0.

**Воспроизведение другой последовательности.** Передайте UTF-8 файл с одним десятичным индексом от 0 до `keys−1` на строку. Первому измеренному GET соответствует первая строка. Файл повторяется циклически, дубли допустимы, seed при replay не участвует. Для точного воспроизведения конечного префикса подготовьте достаточно строк. В каталоге эксперимента остаётся копия `key-sequence.txt`. Если другой генератор использует независимые случайные выборки, Zipf или иное распределение, его фактическую последовательность можно подать этим же способом.

```sh
printf '2\n0\n2\n1\n' > /tmp/oracache-get-order.txt
python3 scripts/profiling/run.py get 1000 --keys 3 --key-sequence /tmp/oracache-get-order.txt
python3 scripts/profiling/run.py get 10000 --nodes 3 --keys 2048 --phase profile --seconds 6
python3 scripts/profiling/run.py get 6000 --keys 2048 --phase trace --seconds 6
go test -race .
python3 scripts/profiling/test_get.py
```

В `summary.txt` указаны алгоритм, seed, число подтверждённых seed PUT, фактическое время и число чтений прогрева, число различных индексов в измерении, RPS всех и успешных вызовов, mean/p50/p95/p99/max успешных RPC, `cache_miss` и `other_errors`. В `latency.csv` к прежним столбцам добавлены `key_index` и `error_kind` (`cache_miss`, `rpc_error`, `invalid_response`). Строки упорядочены по request ID. Параметры запуска, размер значения, Git SHA, ОС и архитектура CPU находятся в `metadata.json`. Клиентская проверка, число соединений и worker, фактический RPS, способ выбора ключа, длительность и топология должны совпадать при сравнении p99 разных генераторов.

### Локальная проверка 25 ноября 2026 года

MacBook Air M5, процессы на одном Mac, direct к лидеру, linearizable, 32 worker, seed 1. В каждом плече матрицы было 2048 прогревочных чтений и 6 секунд измерения. В таблице p99 в миллисекундах. [Все 16 строк с фактическим RPS, средним, перцентилями, покрытием и каталогами исходных CSV](../../.reports/profiling/20261125-132346-get-key-comparison.csv)

| Ноды | Ключи | 1000 RPS | 3000 RPS | 6000 RPS | 10000 RPS |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 0,539 | 0,167 | 0,144 | 0,160 |
| 1 | 2048 | 0,335 | 0,114 | 0,068 | 0,086 |
| 3 | 1 | 1,052 | 0,296 | 0,471 | 1,650 |
| 3 | 2048 | 0,751 | 0,192 | 0,235 | 0,622 |

Из 478 987 измеренных GET все завершились успешно. В каждом многоключевом прогоне CSV содержит все индексы 0–2047. После прогрева каждая нода сообщила `SnapshotIndex=2000` и `Running=true`. В дополнительном контроле клиент заполнил и прогрел 2048 ключей, затем читал только индекс 0 через replay. При 10 000 целевых RPS p99 составил 0,092 мс на одной ноде и 0,509 мс на трёх. [Контроль на одной ноде](../../.reports/profiling/20261125-132613-798836-1n-direct-linearizable-get-10000-baseline/summary.txt) · [Контроль на трёх нодах](../../.reports/profiling/20261125-132621-760761-3n-direct-linearizable-get-10000-baseline/summary.txt)

Переход к 2048 ключам на этом стенде не воспроизвёл p99 общего сервера. Для каждой ячейки итоговой матрицы снято одно короткое окно, порядок плеч фиксирован. Более низкий p99 после заполнения 2048 ключей сам по себе не доказывает ускорения поиска: число подготовительных записей, память Store и создание снимков также различаются. Для повторений можно инвертировать порядок через `KEY_COUNTS="2048 1"`. Здесь базовый SHA — `4adf16b` с локальной инструментацией, на общем сервере — `842cca1`. Для воспроизведения серверного результата нужны его последовательность ключей, фактический RPS, число worker и соединений, размещение процессов и сведения о конкурирующей нагрузке.

## Сравнения и повторения

```sh
python3 scripts/profiling/run.py get 500 --standalone --read local
python3 scripts/profiling/run.py get 500 --standalone --read linearizable
python3 scripts/profiling/run.py get 500 --via direct
python3 scripts/profiling/run.py get 500 --via gateway
python3 scripts/profiling/run.py put 500 --nodes 3
python3 scripts/profiling/run.py put 500 --nodes 3 --phase profile
```

Пара standalone сравнивает режимы чтения при отключённом CP. Пара direct/gateway использует один и тот же стек с CP и строгими чтениями. Встроенная валидация storage запрещает local read вместе с CP. Три ноды получают одинаковый список трёх peer, отдельные адреса и одну партицию. В прогонах с несколькими нодами скрипт собирает профили лидера на момент старта. Для обычного запуска трёх нод в проекте также есть `docker compose -f compose.storage.yaml up --build`, для полного стека — `compose.cp.yaml`.

```sh
# Матрица собирает текущие исходники перед первым прогоном.
scripts/profiling/matrix.sh
# Пример более длинного повторения
PERF_SECONDS=30 scripts/profiling/matrix.sh
```

Матрица делает два независимых baseline-повтора, отдельные profile и trace, проверку трёх нод и отдельное воспроизведение остановки на растущем наборе ключей. Для последнего эксперимента используйте только временный стенд скрипта. `--keys 0` даёт новый ключ на каждый PUT, на дефолтной конфигурации достаточно примерно 5000 записей по 10 КиБ для превышения лимита checkpoint.

```sh
python3 scripts/profiling/run.py put 500 --keys 0 --via gateway --seconds 15
# Профиль роста до фатального checkpoint
python3 scripts/profiling/run.py put 500 --keys 0 --via gateway --phase profile --seconds 8
```

## Docker Compose

```sh
python3 scripts/profiling/run.py get 500 --docker --via gateway
python3 scripts/profiling/run.py get 10000 --docker --via gateway --phase profile
python3 scripts/profiling/run.py put 500 --docker --via gateway --phase trace
```

Скрипт кросс-компилирует статические Linux-бинарники под архитектуру локального Docker Engine в `bin/profiling/linux`. Из них Compose собирает локальный образ `FROM scratch`. У каждого прогона своё имя проекта. Контейнеры используют общее сетевое пространство, что позволяет оставить диагностику на `127.0.0.1` внутри него. На хост публикуются только gRPC и статус маршрутов с привязкой к `127.0.0.1`. Бинарные профили передаются через `docker compose exec -T`. Результат включает логи контейнеров и конфигурацию Docker. Сравнивайте маршруты внутри одного окружения.

Чтобы оставить стенд поднятым после подготовки Linux-бинарников любой командой с `--docker`:

```sh
PROFILE_NODE_ADDR=127.0.0.1:28991 PROFILE_GATEWAY_ADDR=127.0.0.1:28980 \
  docker compose -f compose.profiling.yaml up -d --build
bin/profiling/bench -endpoint 127.0.0.1:28780 -mode get -rps 500 -seconds 15 -csv /tmp/oracache-get.csv
docker compose -f compose.profiling.yaml exec -T cp-api /probe GET \
  http://127.0.0.1:28991/debug/stages
docker compose -f compose.profiling.yaml down
```

У каждого из двух серверных процессов свой `ORACACHE_PPROF_ADDR`. Пустое значение отключает HTTP-диагностику, сбор стадий и повышенную частоту runtime-профилей. Для обычного бинарника задайте, например, `ORACACHE_PPROF_ADDR=127.0.0.1:28991` только в окружении нужной ноды. Compose передаёт значения через `PROFILE_NODE_ADDR` и `PROFILE_GATEWAY_ADDR`. Используйте числовой loopback IPv4 или IPv6.

## Артефакты и разбор

Скрипт печатает уникальный каталог `.reports/profiling/<время>-…`. В каждом каталоге есть `summary.txt`, `latency.csv` и `metadata.json`. Для profile там же находятся CPU, alloc_space, inuse_space после принудительного GC, mutex и block, для trace — бинарный execution trace. Скрипт сохраняет текстовые top-25 flat и cumulative для каждого профиля. Файлы Gateway имеют префикс `gateway`, лидера — `node-N`.

```sh
D=$(ls -dt .reports/profiling/*-direct-linearizable-get-10000-profile | head -1)
go tool pprof -top "$D/node-1.cpu.pprof"
go tool pprof -alloc_space -top "$D/node-1.allocs.pprof"
go tool pprof -inuse_space -top "$D/node-1.heap.pprof"
go tool pprof -top -cum "$D/node-1.mutex.pprof"
go tool pprof -top -cum "$D/node-1.block.pprof"
go tool pprof -http=127.0.0.1:8081 "$D/node-1.cpu.pprof"
python3 scripts/profiling/flame.py "$D/node-1.cpu.pprof"
T=$(ls -dt .reports/profiling/*-direct-linearizable-put-500-trace | head -1)
go tool trace -http=127.0.0.1:8082 "$T/node-1.trace.out"
go tool trace -pprof=sched "$T/node-1.trace.out" > "$T/scheduler.pprof"
go tool pprof -top "$T/scheduler.pprof"
```

Во viewer pprof выберите Flame Graph. `flame.py` сохраняет интерактивный граф рядом с исходным профилем в HTML, без Graphviz. Для сборки и проверки тестов — `go test ./... ./gen/... ./cmd/oracache/...`.

## Границы измерений

Клиент сообщает о завершении прогрева и ждёт разрешения начать нагрузку. В режиме profile скрипт сбрасывает гистограммы перед нагрузкой и параллельно запрашивает временные CPU и delta allocs/mutex/block длительностью `seconds−2` секунд. Полная latency и гистограммы охватывают весь измерительный прогон. Trace снимается отдельно, до 3 с. Heap собирается после нагрузки. В `runtime-before/after.json` доступны дельты циклов GC, общего объёма аллокаций и оценок CPU GC и mark assist. Снимки включают фоновую работу процесса и диагностику.

`*.stages.json` содержит count, среднее и верхние границы бакетов p50/p95/p99 в наносекундах. Восемь бакетов на удвоение, погрешность округления вверх до 12,5% для длинных интервалов. Короткие измерения ограничены разрешением часов. Включённые таймеры, gRPC stats.Handler, профилирование mutex (1 из 5) и block (средний интервал выборки 1 мс заблокированного времени) меняют стоимость вызова. Используйте baseline для абсолютных значений latency.

| Стадия | Начало и конец |
|---|---|
| grpc.receive_decode, decode | От TagRPC при диспетчеризации stream до InPayload после декодирования. Вложенный decode охватывает вызов штатного protobuf-кодека до interceptor. |
| grpc.handler | Вся цепочка unary interceptor с обработчиком, включая проверку размещения для PUT с CP. |
| grpc.respond, encode | От возврата обработчика до stats.End. Вложенный encode — вызов Marshal кодека ответа. |
| grpc.unary | Начало processUnaryRPC (stats.Begin) до stats.End после передачи статуса транспорту. Сетевые очереди, flush сокета и получение ответа клиентом могут продолжаться за этой границей. |
| gateway.route_order, upstream_rpc | Выбор маршрута и построение порядка кандидатов внутри forward. Второй интервал — одна попытка вызова сгенерированного клиента storage вместе с его кодеками. Хеширование ключа, повторные проверки, backoff и обновление preference входят в handler. |
| raft.prepare, handoff | Валидация, proto.Size и внутренний proto.Marshal. От создания request перед отправкой в канал до начала accept в owner-горутине. |
| raft.encode_command, propose, commit_observed | Создание и копирование envelope, синхронный вызов RawNode.Propose, затем от возврата Propose до разбора соответствующей записи CommittedEntries на лидере. Здесь виден момент наблюдения commit приложением. |
| raft.read_barrier | От вызова ReadIndex до проверки readReady и applied ≥ readIndex в completeReads. У local GET этой стадии нет. |
| raft.apply, decode_request, execute | Apply охватывает декодирование payload и применение в state machine после разбора заголовка записи. Для GET decode_request и execute выполняются в accept или completeReads. |
| store.lock, body, clone | До получения Mutex, затем участок под защитой Mutex, включая разблокировку. Clone — вложенное копирование значения. |
| store.lru, retention_evict | Создание/продвижение записи и обновление map при PUT, затем ограничение числа версий, resize и evict. GET использует Peek. |
| raft.checkpoint | Отдельное событие сериализации всего Store внутри processReady после Advance. Count означает число снимков, длительность одного снимка может влиять на несколько соседних RPC. |

Request содержит локальный timestamp передачи. Pending-запрос содержит timestamp ReadIndex или proposal, используется существующая пара peer ID/sequence. Профилирование не добавляет поля в реплицируемую команду. В однооперационных прогонах все измеряемые RPC относятся к выбранному GET или PUT. Не складывайте вложенные интервалы, перцентили разных распределений и секунды ожидания разных горутин.

## Каталог интернет-магазина

Из корня проекта, на свободных портах стенда, выполните любую из команд. По умолчанию каталог содержит 2048 товаров по 10 КиБ. Каждый запуск поднимает свежий стенд. Прогрев делает минимум один GET каждого товара. После него runner проверяет общий leader ID, состояние Running/Ready и достижение всеми репликами commit index лидера. Затем он разрешает клиенту начать отсчёт.

```sh
python3 scripts/profiling/run.py catalog 1000 --seconds 6
python3 scripts/profiling/run.py catalog 6000 --seconds 6 --distribution zipf --zipf-s 1.1
python3 scripts/profiling/run.py catalog 1000 --seconds 6 --nodes 3 --distribution hotspot --hot-traffic 80 --hot-keys 20 --get-percent 98 --miss-rate 1 --sizes 256:60,1024:30,10240:10
python3 scripts/profiling/run.py catalog 1000 --seconds 6 --get-percent 90 --phase profile
python3 scripts/profiling/run.py catalog 1000 --seconds 6 --nodes 3 --get-percent 90 --phase trace
```

- `--distribution uniform` циклически обходит перестановку товаров. Для `hotspot` параметры `--hot-traffic 95 --hot-keys 5` означают, что 95% обращений выбирают первые 5% товаров. Для `zipf` вероятность ранга k пропорциональна k в степени −s, начиная с k=1. Один и тот же закон выбора действует для чтений и обновлений.
- `--get-percent` задаёт процент GET среди всех операций, `--miss-rate` задаёт процент отсутствующих товаров **среди GET**. PUT обновляет существующий товар без условия на ревизию. Содержимое включает ID товара, идентификатор записи, цену и остаток. Чтение проверяет статус, длину, все поля и байты, равенство текущей и последней ревизии и нижнюю границу уже подтверждённых обновлений. После завершения RPC клиент связывает каждое прочитанное обновление с точным товаром, write token и ревизией из ответа PUT. Порядок прихода ACK у разных worker может различаться. Запись с потерянным подтверждением даёт явную ошибку проверки.
- `--sizes` задаёт доли **ключей**, в целых процентах с суммой 100. Размер товара сохраняется при обновлении. Пример выше даёт 1229 × 256 Б, 614 × 1024 Б и 205 × 10240 Б. Контроль `--sizes 65536:100` завершится ненулевым кодом при заполнении с серверным статусом `PUT_STATUS_TOO_LARGE`. Если в смеси есть такая запись, измерение всего сценария отменяется.
- `--seed 1` фиксирует последовательность логических операций, индексов, промахов и раскладку размеров. При разном достигнутом RPS воспроизводится общий префикс. Клиент использует один канал gRPC. Физический порядок параллельных RPC и назначенных ревизий может меняться. Прежний `get --key-sequence <файл>` по-прежнему воспроизводит заданную последовательность индексов.
- В каталоге latency измеряется вокруг вызова сгенерированного клиента. Подготовка значения и проверка ответа находятся за границей таймера. Общий ticker и конечное число worker допускают coordinated omission. При задержках тики могут теряться, а время от планового такта до вызова RPC в latency отсутствует. `unsent_slots` в `summary.txt` показывает дефицит до `target × seconds`. Actual и successful RPS используют реальную длительность вплоть до завершения последних RPC. У исходного GET внутри таймера остаются короткие проверки статуса, длины и ревизии.

В каждом завершённом прогоне `products.csv` описывает заполнение, `latency.csv` содержит пооперационные измерения, `key-frequency.csv` — фактические GET, PUT, HIT, ожидаемые MISS, подтверждённые обновления и ошибки для каждого ключа, включая нулевые частоты. В `catalog.json` раздельно для GET и PUT сохранены число вызовов, ошибки, actual/success RPS, mean/p50/p95/p99/max успешных RPC. Валидный ожидаемый MISS входит в успешные GET. Hit/miss rate рассчитаны среди проверенных ответов GET. Ошибки доступны отдельно. При отказе заполнения `products.csv` описывает **план**, причина отказа остаётся в `summary.txt` и `bench.stderr`.

В режиме catalog/profile после прогрева и до измерения runner дважды запрашивает heap с принудительным GC. Так уже выполненные seed и checkpoint меньше влияют на запаздывающие выборки alloc_space. Эти сборки завершаются до чтения начальных runtime-счётчиков. В смешанном профиле общие стадии gRPC и Store объединяют обе операции. Стадии ReadIndex относятся к чтениям, proposal/commit/apply — к записям. `node-*.after-warmup.json`, `after-load` и `final` позволяют проверить индексы Raft, snapshot и состояние реплик. `--phase profile` сохраняет двоичные pprof и текстовые топы лидера, `--phase trace` — trace лидера. В диагностических фазах native-прогона доступны также CPU time и RSS процессов и число горутин диагностируемого процесса. При отказе ноды остаются доступные логи, ответы status и полученные артефакты.

### Последовательная матрица и тесты

```sh
# Короткая серия по умолчанию, одна нода, 1000 и 10000 целевых RPS
python3 scripts/profiling/catalog_matrix.py
# Однофакторные сравнения на выбранной нагрузке
python3 scripts/profiling/catalog_matrix.py --scenarios uniform hotspot zipf mixed98 --rates 6000
# Исходный контроль, один ключ по 10 КиБ
python3 scripts/profiling/catalog_matrix.py --scenarios legacy-one --rates 10000
# Полный baseline, 3 × 30 с на комбинацию, 1 и 3 ноды, 1000/3000/6000/10000 RPS
python3 scripts/profiling/catalog_matrix.py --full
# Подмножество длительной серии
python3 scripts/profiling/catalog_matrix.py --full --scenarios uniform mixed90 --nodes 3 --rates 6000 10000 --workers 32 --seed 1
go test -race ./... ./gen/... ./cmd/oracache/...
python3 -B scripts/profiling/test_catalog.py
python3 -B scripts/profiling/test_get.py
```

Полная серия включает legacy-one, uniform, hotspot, zipf, mixed98/90/70, miss1/5, sizes и shop. В каждом запуске создаётся отдельный каталог результатов. При неудаче матрица продолжает независимые прогоны, печатает список отказов и завершается с ненулевым статусом. Место на диске потребуется прежде всего для пооперационных CSV. Методика, графики и результаты короткой серии приведены в [отчёте по каталогу](../../docs/realistic-workload-profiling.md).
