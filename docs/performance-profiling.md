# Где OraCache тратит время при GET и PUT

На одной ноде с прогретым значением 10 КиБ работа с данными занимает небольшую часть времени клиентского RPC. При прямом GET на Mac и 500 RPS в одном профильном прогоне среднее время на клиенте составило 246 мкс, серверный unary RPC занял 49 мкс, в нём обработчик — 20,5 мкс, а в обработчике копирование из Store — 3,2 мкс. Через Gateway наибольший измеренный участок его обработчика — ожидание второго RPC. У PUT с постоянно растущим набором ключей есть отдельная, воспроизводимая причина длинных задержек и отказа сервиса — синхронный checkpoint всего Store и ограничение его размера.

## 1. Методика и путь запроса

24 ноября 2026 года. Локальная копия на основе `4adf16be3df80a305f43774bddb3a1f50ef720df` с диагностическими изменениями. Go 1.27.3, grpc-go 1.64.1, protobuf-go 1.34.2, etcd/raft v3.6.0. MacBook Air, Apple M5 (4 производительных и 6 энергоэффективных ядер), 16 ГБ, macOS 26.6 arm64. Клиент на этом же Mac. Нативные процессы используют TCP loopback. Для контрольных контейнерных измерений — Docker Desktop 4.100.1, Engine 29.9.3, Linux/arm64, 10 vCPU, 8 319 438 848 байт памяти у VM. Измерения PostgreSQL в исходных данных и в этом эксперименте отсутствуют.

**Фактический путь на сервере**

1. `cmd/storage-node/main.go → node.New → grpc.NewServer → RegisterCacheGatewayServer(shardraft.GatewayServer(inst))`. Клиентский gRPC работает по HTTP/2. В grpc-go `processUnaryRPC` читает сообщение, generated handler вызывает `dec`, зарегистрированный proto-кодек выполняет Unmarshal до interceptor. При наличии CP обработчик записи также проверяет master-регион и активное размещение по локальной копии конфигурации.
2. **GET**. Адаптер вызывает `Instance.Get → call`, где выполняются валидация, `proto.Size`, внутренний `proto.Marshal`, создание timeout и reply channel. Небуферизованный `requests` передаёт команду в единственную горутину `Run → accept`. Для local read она проверяет готовность и вызывает `decodeRequest → execute`. Для строгого чтения лидер регистрирует pending ID, вызывает `RawNode.ReadIndex`, получает `Ready.ReadStates`, затем `completeReads` проверяет applied index. Далее `StateMachine.Get → Store.Peek → lookup → clone`. Под `sync.Mutex` выполняется поиск версии и копирование 10 КиБ. Порядок LRU при чтении сохраняется.
3. **PUT**. Через тот же канал `accept` передаёт `RawNode.Propose` команду с 18-байтным заголовком и копией payload. `processReady` сохраняет Entries/HardState в MemoryStorage. При нескольких peer исходящие сообщения сериализуются, sendLoop передаёт их по HTTP, входящие ответы обрабатываются в Run через Step. Из `CommittedEntries` извлекаются ID и операция, `decodeRequest` делает повторный protobuf Unmarshal. `execute → StateMachine.Put → Store.Put` проверяет expected revision, назначает глобальную ревизию, копирует значение, меняет LRU, ограничивает версии, пересчитывает размер и при необходимости вытесняет данные. С лимитами по умолчанию хранятся до трёх версий и 256 МиБ по учёту Store.
4. Ответ возвращается вызывающей горутине через reply channel, grpc-go кодирует его и передаёт HTTP/2-транспорту. На клиенте generated stub выходит после получения и декодирования ответа. Checkpoint выполняется в том же owner-цикле после Advance, каждые 1000 applied entries. Ответ на текущую запись уже может быть отправлен, следующие RPC могут ждать входа в цикл.
5. **Gateway**. Точка входа `cmd/api-gateway.serveGateway` регистрирует `gateway.Forwarder`. GET/PUT хеширует ключ через `partition.Of`, использует локальный routing snapshot (`atomic.Pointer`), выбирает кандидатов, получает соединение и вызывает сгенерированный клиент storage. Предпочтения и кэш соединений защищены Mutex. CP обновляет маршруты в фоне. Gateway декодирует и заново кодирует payload на двух RPC. Отдельная команда `cmd/gateway` из обычного `compose.yaml` обслуживает legacy-кэш `internal/cache`; эти измерения относятся к `cmd/api-gateway`.

[Регистрация storage](../internal/node/node.go) · [Generated handlers](../gen/gateway/v1/gateway_grpc.pb.go) · [Вход в Raft](../shardraft/client.go) · [Owner-цикл](../shardraft/instance.go) · [Команда](../shardraft/command.go) · [State machine](../internal/node/statemachine.go) · [Store](../internal/store/store.go) · [Forwarder](../internal/gateway/forwarder.go)

**Нагрузка и единицы.** На каждый прогон создавалась новая группа. Один переиспользуемый gRPC-канал, 32 worker, размер значения 10 240 байт. Один seed PUT и 100 прогревочных вызовов исключены из измерений. GET читает последнюю версию одного ключа. Обычный PUT циклически пишет в 32 ключа без CAS, регистрируются и транспортный статус, и прикладной результат. Растущий набор с отдельным ключом на запись приведён отдельно. Время на клиенте начинается при вызове RPC, у PUT включает подготовку имени ключа. Общий ticker ограничивает частоту, пропущенные при занятости worker такты не накапливаются. Факт RPS — по всем завершившимся вызовам и времени с учётом drain.

Baseline снят на текущей сборке с выключенными диагностическими точками. Отдельного сравнения с бинарником исходного коммита без измерительных точек не проводилось. В таблицах baseline pprof, stats-handler и сбор стадий отключены. Пары чисел через косую черту в таблицах baseline соответствуют двум отдельным 8-секундным прогонам. В таблицах стадий другой формат — **среднее / p50 / p95 / p99 в мкс**. Среднее вычислено из суммы длительностей, перцентили округлены до верхней границы бакета. CPU, allocs, mutex и block взяты в 6-секундном окне около начала 8-секундной нагрузки (HTTP-сбор и старт клиента запускаются независимо). Heap — после нагрузки и GC. Execution trace получен отдельным запуском. Native direct и gateway используют одинаковый набор процессов, а local/linearizable сравниваются напрямую без CP, поскольку `node.New` запрещает local read вместе с CP. Все пути к сырым данным ниже относятся к локальной рабочей копии. [Команды, ограничения таймеров и таблица границ](../scripts/profiling/README.md)

## 2. GET

| Среда · ноды · маршрут · чтение | Цель RPS | Факт RPS | Среднее, мс | p50 | p95 | p99 | Ошибки / попытки |
|---|---:|---:|---:|---:|---:|---:|---|
| Mac · 1 · direct без CP · linearizable | 500 | 498.6 / 499.0 | 0.211 / 0.190 | 0.224 / 0.153 | 0.316 / 0.308 | 0.478 / 0.593 | [0/3989](../.reports/profiling/20261124-175158-963272-1n-standalone-linearizable-get-500-baseline/summary.txt), [0/3992](../.reports/profiling/20261124-175356-748373-1n-standalone-linearizable-get-500-baseline/summary.txt) |
| Mac · 1 · direct без CP · local | 500 | 499.9 / 499.5 | 0.204 / 0.164 | 0.219 / 0.144 | 0.305 / 0.279 | 0.504 / 0.391 | [0/3999](../.reports/profiling/20261124-175208-631450-1n-standalone-local-get-500-baseline/summary.txt), [0/3996](../.reports/profiling/20261124-175406-660181-1n-standalone-local-get-500-baseline/summary.txt) |
| Mac · 1 · direct · linearizable | 500 | 499.9 / 499.5 | 0.218 / 0.186 | 0.229 / 0.161 | 0.318 / 0.306 | 0.513 / 0.428 | [0/3999](../.reports/profiling/20261124-175217-773472-1n-direct-linearizable-get-500-baseline/summary.txt), [0/3996](../.reports/profiling/20261124-175415-789568-1n-direct-linearizable-get-500-baseline/summary.txt) |
| Mac · 1 · gateway · linearizable | 500 | 499.9 / 499.9 | 0.463 / 0.407 | 0.512 / 0.348 | 0.679 / 0.650 | 1.025 / 0.909 | [0/3999](../.reports/profiling/20261124-175227-957426-1n-gateway-linearizable-get-500-baseline/summary.txt), [0/3999](../.reports/profiling/20261124-175425-952566-1n-gateway-linearizable-get-500-baseline/summary.txt) |
| Mac · 1 · direct без CP · linearizable | 10000 | 9987.3 / 9990.2 | 0.050 / 0.053 | 0.044 / 0.045 | 0.096 / 0.110 | 0.163 / 0.180 | [0/79913](../.reports/profiling/20261124-175238-145438-1n-standalone-linearizable-get-10000-baseline/summary.txt), [0/79935](../.reports/profiling/20261124-175435-632249-1n-standalone-linearizable-get-10000-baseline/summary.txt) |
| Mac · 1 · direct без CP · local | 10000 | 9994.3 / 9978.5 | 0.050 / 0.053 | 0.044 / 0.044 | 0.095 / 0.109 | 0.159 / 0.179 | [0/79968](../.reports/profiling/20261124-175248-366734-1n-standalone-local-get-10000-baseline/summary.txt), [0/79842](../.reports/profiling/20261124-175445-089216-1n-standalone-local-get-10000-baseline/summary.txt) |
| Mac · 1 · direct · linearizable | 10000 | 9994.9 / 9988.0 | 0.050 / 0.051 | 0.044 / 0.044 | 0.098 / 0.103 | 0.166 / 0.172 | [0/79973](../.reports/profiling/20261124-175258-073764-1n-direct-linearizable-get-10000-baseline/summary.txt), [0/79918](../.reports/profiling/20261124-175454-567870-1n-direct-linearizable-get-10000-baseline/summary.txt) |
| Mac · 1 · gateway · linearizable | 10000 | 9986.2 / 9978.5 | 0.147 / 0.158 | 0.121 / 0.128 | 0.287 / 0.307 | 0.378 / 0.411 | [0/79904](../.reports/profiling/20261124-175307-800148-1n-gateway-linearizable-get-10000-baseline/summary.txt), [0/79842](../.reports/profiling/20261124-175504-272887-1n-gateway-linearizable-get-10000-baseline/summary.txt) |
| Mac · 3 · direct · linearizable | 500 | 499.9 | 0.489 | 0.456 | 0.789 | 1.172 | [0/3999](../.reports/profiling/20261124-175603-639666-3n-direct-linearizable-get-500-baseline/summary.txt) |
| Docker · 1 · direct · linearizable | 500 | 499.4 | 0.431 | 0.379 | 0.581 | 0.947 | [0/3995](../.reports/profiling/20261124-175752-208842-1n-direct-linearizable-get-500-baseline/summary.txt) |
| Docker · 1 · gateway · linearizable | 500 | 499.4 | 0.557 | 0.539 | 0.750 | 1.000 | [0/3995](../.reports/profiling/20261124-175804-813977-1n-gateway-linearizable-get-500-baseline/summary.txt) |
| Docker · 1 · gateway · linearizable | 10000 | 9678.6 | 1.435 | 1.156 | 3.214 | 4.404 | [0/77452](../.reports/profiling/20261124-175815-468801-1n-gateway-linearizable-get-10000-baseline/summary.txt) |
| Docker · 1 · direct · linearizable | 10000 | 9953.5 | 0.664 | 0.503 | 1.655 | 2.817 | [0/79644](../.reports/profiling/20261124-175837-923061-1n-direct-linearizable-get-10000-baseline/summary.txt) |

Прежнее измерение пользователя при 500 RPS дало mean 0,675 мс, p95 0,855 мс и p99 1,133 мс. В историческом `get.csv` при высокой нагрузке 278 115 успешных GET, mean 5,536 мс, p50 5,371 мс, p95 8,064 мс, p99 9,523 мс. Значение 9257,9 RPS взято из описания прогона. Произведение throughput на среднее время даёт примерно 51 одновременно исполняющийся запрос в стационарном режиме. При 32 синхронных worker такой режим недостижим. Старый CSV не фиксирует число worker, состав группы и конфигурацию хоста. Однозначно приписать разницу с новым прогоном одному компоненту по этим данным невозможно. [Исторический GET CSV](../get.csv)

**Время стадий на storage при прямом строгом чтении**

| Стадия | [500 RPS](../.reports/profiling/20261124-175032-884351-1n-direct-linearizable-get-500-profile/node-1.stages.json) | [10000 RPS](../.reports/profiling/20261124-175045-938729-1n-direct-linearizable-get-10000-profile/node-1.stages.json) |
|---|---:|---:|
| `grpc.decode` | 12.03 / 14.34 / 18.43 / 26.62 | 2.41 / 2.30 / 3.07 / 7.17 |
| `grpc.encode` | 4.16 / 2.81 / 9.21 / 26.62 | 0.89 / 0.48 / 1.41 / 4.09 |
| `grpc.handler` | 20.48 / 20.48 / 40.96 / 73.73 | 5.10 / 3.07 / 6.14 / 45.05 |
| `grpc.receive_decode` | 17.05 / 20.48 / 26.62 / 40.96 | 3.29 / 2.81 / 4.09 / 13.31 |
| `grpc.respond` | 11.94 / 10.24 / 24.57 / 61.44 | 2.01 / 1.28 / 3.33 / 18.43 |
| `grpc.unary` | 49.06 / 53.25 / 90.11 / 163.84 | 10.41 / 7.68 / 15.36 / 73.73 |
| `raft.decode_request` | 0.77 / 0.77 / 1.28 / 2.81 | 0.16 / 0.09 / 0.17 / 0.45 |
| `raft.execute` | 4.87 / 3.84 / 10.24 / 26.62 | 1.10 / 0.64 / 1.66 / 4.61 |
| `raft.handoff` | 4.16 / 4.61 / 6.66 / 13.31 | 1.79 / 0.64 / 1.53 / 6.14 |
| `raft.prepare` | 1.12 / 1.28 / 1.92 / 2.56 | 0.17 / 0.13 / 0.17 / 0.51 |
| `raft.read_barrier` | 1.52 / 1.66 / 2.56 / 4.61 | 0.27 / 0.22 / 0.35 / 0.90 |
| `store.body` | 3.67 / 2.56 / 8.19 / 24.57 | 0.84 / 0.45 / 1.28 / 3.58 |
| `store.clone` | 3.21 / 2.05 / 7.17 / 22.53 | 0.69 / 0.26 / 1.15 / 3.07 |
| `store.lock` | 0.07 / 0.09 / 0.13 / 0.17 | 0.02 / 0.00 / 0.04 / 0.09 |

**Время стадий на Gateway при строгом чтении**

| Стадия | [500 RPS](../.reports/profiling/20261124-175107-557134-1n-gateway-linearizable-get-500-profile/gateway.stages.json) | [10000 RPS](../.reports/profiling/20261124-175118-941436-1n-gateway-linearizable-get-10000-profile/gateway.stages.json) |
|---|---:|---:|
| `gateway.route_order` | 0.76 / 0.90 / 1.41 / 1.79 | 0.22 / 0.17 / 0.38 / 0.64 |
| `gateway.upstream_rpc` | 248.62 / 294.91 / 393.21 / 655.36 | 88.44 / 73.73 / 196.61 / 294.91 |
| `grpc.decode` | 10.57 / 13.31 / 18.43 / 22.53 | 2.87 / 2.30 / 6.14 / 15.36 |
| `grpc.encode` | 4.72 / 3.07 / 8.19 / 28.67 | 1.23 / 0.57 / 2.56 / 11.26 |
| `grpc.handler` | 254.94 / 294.91 / 425.98 / 655.36 | 90.43 / 73.73 / 196.61 / 327.68 |
| `grpc.receive_decode` | 14.67 / 18.43 / 24.57 / 32.77 | 4.19 / 3.07 / 9.21 / 26.62 |
| `grpc.respond` | 11.99 / 10.24 / 22.53 / 57.34 | 3.06 / 1.66 / 6.66 / 36.86 |
| `grpc.unary` | 281.13 / 327.68 / 458.75 / 786.43 | 97.63 / 73.73 / 212.99 / 327.68 |

Для прямого GET при 10 000 целевых RPS на Mac профильный клиент получил 9968 RPS и среднее 58 мкс. Около 10,4 мкс прошло от начала unary RPC на storage до передачи статуса транспорту. Остаток порядка 48 мкс включает клиент, доставку, очереди и планирование до/после этой границы. Разность средних допустима здесь для одинакового набора успешных RPC; разность перцентилей не определяет распределение остатка. Для 500 RPS соответствующий остаток около 197 мкс. В нативных прогонах среднее при более частых запросах оказалось ниже. Частота CPU, размещение на ядрах и пробуждения клиента отдельно не контролировались. Эти данные не задают универсальную кривую зависимости latency от RPS.

В CPU-профиле storage при 10 000 RPS по flat лидируют `syscall.rawsyscalln` (31,87%), `runtime.kevent` (30,22%) и `runtime.pthread_cond_wait` (12,64%). В cumulative есть `runtime.schedule` (47,8%) и цепочка HTTP/2 `loopyWriter.run → bufWriter.Flush → netFD.Write` (flush 23,08%). Это доли CPU-сэмплов процесса. В alloc_space лидируют `proto.MarshalOptions.marshal` (39,52%) и `store.clone` (35,96%). В среднем один обслуженный GET дал около 28 КБ выделений на storage по дельте runtime, за окно прошло 1074 GC-цикла и около 0,024 CPU-с оценки mark assist. Heap после GC в этом запуске показывал порядка 4 МиБ sampled inuse_space, включая runtime и дескрипторы.

В block-профиле видны `selectgo`, ожидание каналов Raft, фоновые циклы CP и ожидание работы в gRPC writer. В mutex-профиле 94,41% delay в `runtime.unlock`. У входа в Store среднее время получения Mutex — 0,024 мкс; p50 округлился до нуля при разрешении часов Mac. При профилировании через Gateway на 10 000 RPS обработчик Gateway в среднем занимал 90,4 мкс, из них 88,4 мкс — вызов storage, а построение очередности маршрутов занимало 0,22 мкс. Вложенные интервалы считаются один раз.

[500 RPS, CPU](../.reports/profiling/20261124-175032-884351-1n-direct-linearizable-get-500-profile/node-1.cpu.flat.txt) · [alloc_space](../.reports/profiling/20261124-175032-884351-1n-direct-linearizable-get-500-profile/node-1.allocs.flat.txt) · [heap](../.reports/profiling/20261124-175032-884351-1n-direct-linearizable-get-500-profile/node-1.heap.flat.txt) · [mutex](../.reports/profiling/20261124-175032-884351-1n-direct-linearizable-get-500-profile/node-1.mutex.cum.txt) · [block](../.reports/profiling/20261124-175032-884351-1n-direct-linearizable-get-500-profile/node-1.block.cum.txt)

[10000 RPS, CPU](../.reports/profiling/20261124-175045-938729-1n-direct-linearizable-get-10000-profile/node-1.cpu.flat.txt) · [alloc_space](../.reports/profiling/20261124-175045-938729-1n-direct-linearizable-get-10000-profile/node-1.allocs.flat.txt) · [heap](../.reports/profiling/20261124-175045-938729-1n-direct-linearizable-get-10000-profile/node-1.heap.flat.txt) · [mutex](../.reports/profiling/20261124-175045-938729-1n-direct-linearizable-get-10000-profile/node-1.mutex.cum.txt) · [block](../.reports/profiling/20261124-175045-938729-1n-direct-linearizable-get-10000-profile/node-1.block.cum.txt)

## 3. PUT

| Среда · ноды · маршрут · чтение | Цель RPS | Факт RPS | Среднее, мс | p50 | p95 | p99 | Ошибки / попытки |
|---|---:|---:|---:|---:|---:|---:|---|
| Mac · 1 · direct | 50 | 49.9 / 49.9 | 0.435 / 0.387 | 0.442 / 0.384 | 0.720 / 0.664 | 0.924 / 0.871 | [0/399](../.reports/profiling/20261124-175317-523337-1n-direct-linearizable-put-50-baseline/summary.txt), [0/399](../.reports/profiling/20261124-175514-548478-1n-direct-linearizable-put-50-baseline/summary.txt) |
| Mac · 1 · direct | 200 | 199.9 / 199.9 | 0.285 / 0.215 | 0.300 / 0.175 | 0.410 / 0.401 | 0.686 / 0.548 | [0/1599](../.reports/profiling/20261124-175327-212180-1n-direct-linearizable-put-200-baseline/summary.txt), [0/1599](../.reports/profiling/20261124-175524-219462-1n-direct-linearizable-put-200-baseline/summary.txt) |
| Mac · 1 · direct | 500 | 498.4 / 498.7 | 0.235 / 0.218 | 0.246 / 0.178 | 0.346 / 0.337 | 0.658 / 0.614 | [0/3987](../.reports/profiling/20261124-175337-413440-1n-direct-linearizable-put-500-baseline/summary.txt), [0/3990](../.reports/profiling/20261124-175533-899126-1n-direct-linearizable-put-500-baseline/summary.txt) |
| Mac · 1 · gateway | 500 | 499.2 / 498.9 | 0.508 / 0.330 | 0.553 / 0.269 | 0.739 / 0.633 | 1.185 / 0.805 | [0/3994](../.reports/profiling/20261124-175347-050273-1n-gateway-linearizable-put-500-baseline/summary.txt), [0/3991](../.reports/profiling/20261124-175543-555680-1n-gateway-linearizable-put-500-baseline/summary.txt) |
| Mac · 3 · direct | 500 | 499.9 | 0.482 | 0.406 | 0.814 | 1.182 | [0/3999](../.reports/profiling/20261124-175623-730730-3n-direct-linearizable-put-500-baseline/summary.txt) |
| Docker · 1 · gateway | 500 | 498.1 | 0.568 | 0.528 | 0.785 | 1.330 | [0/3985](../.reports/profiling/20261124-175827-258600-1n-gateway-linearizable-put-500-baseline/summary.txt) |
| Docker · 1 · direct | 500 | 499.7 | 0.397 | 0.387 | 0.502 | 0.796 | [0/3998](../.reports/profiling/20261124-175849-186760-1n-direct-linearizable-put-500-baseline/summary.txt) |

**Время стадий успешного PUT на storage, 32 ключа**

| Стадия | [500 RPS](../.reports/profiling/20261124-175057-046214-1n-direct-linearizable-put-500-profile/node-1.stages.json) |
|---|---:|
| `grpc.decode` | 13.91 / 16.38 / 28.67 / 40.96 |
| `grpc.encode` | 0.90 / 0.96 / 1.66 / 4.61 |
| `grpc.handler` | 38.56 / 36.86 / 73.73 / 122.88 |
| `grpc.receive_decode` | 21.79 / 24.57 / 40.96 / 61.44 |
| `grpc.respond` | 7.16 / 7.17 / 15.36 / 32.77 |
| `grpc.unary` | 67.14 / 73.73 / 122.88 / 212.99 |
| `raft.apply` | 9.34 / 7.68 / 24.57 / 40.96 |
| `raft.checkpoint` | 4135.93 / 3932.16 / 5242.88 / 5242.88 |
| `raft.commit_observed` | 2.71 / 3.07 / 4.61 / 9.21 |
| `raft.decode_request` | 3.39 / 2.56 / 12.29 / 24.57 |
| `raft.encode_command` | 2.39 / 1.66 / 9.21 / 18.43 |
| `raft.execute` | 5.49 / 4.61 / 15.36 / 26.62 |
| `raft.handoff` | 6.92 / 4.61 / 11.26 / 16.38 |
| `raft.prepare` | 4.23 / 2.81 / 15.36 / 24.57 |
| `raft.propose` | 1.55 / 1.66 / 2.56 / 6.14 |
| `store.body` | 4.65 / 3.58 / 14.34 / 24.57 |
| `store.clone` | 2.42 / 1.15 / 10.24 / 20.48 |
| `store.lock` | 0.06 / 0.04 / 0.09 / 0.13 |
| `store.lru` | 0.21 / 0.17 / 0.45 / 0.51 |
| `store.retention_evict` | 0.25 / 0.26 / 0.48 / 1.28 |

**Время стадий на Gateway**

| Стадия | [500 RPS](../.reports/profiling/20261124-175130-390620-1n-gateway-linearizable-put-500-profile/gateway.stages.json) |
|---|---:|
| `gateway.route_order` | 0.85 / 0.96 / 1.41 / 1.79 |
| `gateway.upstream_rpc` | 284.45 / 327.68 / 458.75 / 786.43 |
| `grpc.decode` | 14.65 / 16.38 / 24.57 / 49.15 |
| `grpc.encode` | 1.57 / 1.66 / 2.81 / 5.12 |
| `grpc.handler` | 291.12 / 327.68 / 458.75 / 786.43 |
| `grpc.receive_decode` | 23.13 / 24.57 / 40.96 / 147.46 |
| `grpc.respond` | 7.77 / 8.19 / 15.36 / 24.57 |
| `grpc.unary` | 321.58 / 393.21 / 491.52 / 917.50 |

При 500 RPS на Mac с включённым профилированием напрямую клиент измерил 249 мкс в среднем. Серверный unary RPC занял 67,1 мкс, handler 38,6 мкс, вход в owner-цикл 6,9 мкс, `Propose` 1,55 мкс, до наблюдения committed entry ещё 2,71 мкс, apply 9,34 мкс. В apply входят повторное декодирование payload и Store. Отдельный checkpoint ограниченного набора ключей занимал в среднем 4,14 мс (четыре события). Делить каждый ответ на фиксированную стоимость checkpoint нельзя.

CPU flat на storage возглавляют `syscall.rawsyscalln` (23,42%), `runtime.kevent` (21,62%) и `runtime.pthread_cond_signal` (19,82%). В alloc_space заметны `impl.consumeBytesNoZero` (25,87%), `MarshalOptions.marshal` (16,67%), `store.clone` (13,59%), gRPC `nopBufferPool.Get` (11,85%) и `shardraft.encodeCommand` (11,41%). По runtime за 3997 PUT и сбор диагностики выделено около 327 МБ и прошло 36 циклов GC. В heap после GC лидируют `store.clone` (34%) и `encodeCommand` (20%) — Store удерживает версии, а Raft хранит команды в памяти между compaction. В block доминирует `selectgo`, в mutex обнаружено около 1,4 мс совокупной задержки за 6 с. Среднее ожидание Mutex Store было 0,058 мкс, тогда как клонирование внутри Store занимало около 2,42 мкс.

[500 RPS, CPU](../.reports/profiling/20261124-175057-046214-1n-direct-linearizable-put-500-profile/node-1.cpu.flat.txt) · [alloc_space](../.reports/profiling/20261124-175057-046214-1n-direct-linearizable-put-500-profile/node-1.allocs.flat.txt) · [heap](../.reports/profiling/20261124-175057-046214-1n-direct-linearizable-put-500-profile/node-1.heap.flat.txt) · [mutex](../.reports/profiling/20261124-175057-046214-1n-direct-linearizable-put-500-profile/node-1.mutex.cum.txt) · [block](../.reports/profiling/20261124-175057-046214-1n-direct-linearizable-put-500-profile/node-1.block.cum.txt)

**Причина прежних ошибок и воспроизведение.** В предоставленном `put.csv` 14 947 измерений. Среди 4893 успешных среднее 1,650 мс и p99 47,519 мс. У 10 054 неуспешных среднее 0,704 мс и p99 0,877 мс. Все ошибки имеют код Unavailable. По исходному CSV 32 ответа содержат `instance stopped`, 7588 — отказ TCP-соединения с узлом, 2434 — отсутствие готовых реплик партиции. В сохранившихся логах Docker всех трёх нод сообщение `checkpoint exceeds PoC transport limit` датировано 23 ноября 2026 года 16:26:57 МСК. CSV записан в 16:27:17 МСК. Ревизионных конфликтов и TOO_LARGE в данных нет. [Исходный PUT CSV](../put.csv)

На свежей одиночной ноде 15 с при 500 RPS с новым ключом на каждую запись и через Gateway повторили цепочку событий. Из 7499 попыток 4897 успешны (mean 0,818 мс, p99 16,366 мс), затем 32 `instance stopped` и 2570 отказов соединения. Лог ноды содержит ту же фатальную ошибку. Тайминги и число успевших ответить RPC зависят от конфигурации и соседних вызовов. [Сводка воспроизведения](../.reports/profiling/20261124-175142-262399-1n-gateway-linearizable-put-500-baseline/summary.txt) · [Лог ноды](../.reports/profiling/20261124-175142-262399-1n-gateway-linearizable-put-500-baseline/node-1.log)

`Store.Checkpoint` синхронно сериализует все ключи и удержанные версии в JSON. Поле `[]byte` кодируется base64. При 5000 разных значений по 10 240 байт только их base64 занимает 68 280 000 байт. В `Instance.processReady` предел сериализованного checkpoint равен 63 МиБ (66 060 288 байт), хотя бюджет Store по умолчанию 256 МиБ. Превышение возвращает ошибку из Run, после чего Node останавливает gRPC. Проверка выполняется также на единственной ноде. [Store.Checkpoint](../internal/store/checkpoint.go) · [processReady](../shardraft/instance.go)

В профильном прогоне на растущем наборе, ограниченном восемью секундами до остановки, были успешны все 3911 PUT. Четыре checkpoint заняли в среднем 39,34 мс против 4,37 мс у 32 ключей по тому же маршруту. У входа в owner-цикл среднее ожидание выросло с 7,80 до 394 мкс, p99 достиг верхней границы бакета 18,87 мс. У клиента p99 вырос с 1,209 до 19,833 мс. В CPU-профиле 15,48% cumulative под Store.Checkpoint, в alloc_space под ним 394,57 МиБ (65,81%). Это связывает хвост задержек с ожиданием сериализации снимка ещё до превышения лимита. [Время стадий](../.reports/profiling/20261124-175733-515236-1n-gateway-linearizable-put-500-profile/node-1.stages.json) · [CPU](../.reports/profiling/20261124-175733-515236-1n-gateway-linearizable-put-500-profile/node-1.cpu.cum.txt) · [Allocations](../.reports/profiling/20261124-175733-515236-1n-gateway-linearizable-put-500-profile/node-1.allocs.cum.txt)

**Docker и три peer.** При 10 000 целевых GET/s в Docker baseline mean составил 0,664 мс напрямую и 1,435 мс через Gateway. В профильном прогоне через Gateway клиент измерил 1,474 мс, весь unary на Gateway — 481 мкс, внутри него вызов storage — 453 мкс, а unary на самой ноде — 54 мкс. Около **0,99 мс** среднего RTT осталось за границами server unary Gateway. Внутри второго RPC около **0,40 мс** осталось за границами unary storage. Сюда входят клиентские кодеки, очереди, планирование, HTTP/2 и путь передачи. Только внешний RPC проходит между macOS и Docker VM через опубликованный порт. Эти интервалы локализуют задержку, но не позволяют назначить её исключительно Docker port forwarding. На storage `ReadIndex` занял в среднем 0,82 мкс, `Store.Peek` под блокировкой — 3,73 мкс, построение порядка маршрутов на Gateway — 0,82 мкс. [Клиент](../.reports/profiling/20261124-175900-345685-1n-gateway-linearizable-get-10000-profile/summary.txt) · [Gateway](../.reports/profiling/20261124-175900-345685-1n-gateway-linearizable-get-10000-profile/gateway.stages.json) · [Storage](../.reports/profiling/20261124-175900-345685-1n-gateway-linearizable-get-10000-profile/node-1.stages.json)

У Docker PUT на стабильном наборе при 500 RPS клиентский mean через Gateway с профилированием — 526 мкс, upstream — 141 мкс, unary на storage — 28,1 мкс, apply — 4,34 мкс. Число peer меняет вывод о Raft. На трёх нативных нодах при прямом PUT интервал от возврата Propose до наблюдения committed entry достиг **272 мкс** при среднем RTT 544 мкс и handler 309 мкс. На одном peer эта же стадия занимала 2,71 мкс. Три ноды размещались на одном Mac, между ними работает HTTP transport. [Docker PUT, Gateway](../.reports/profiling/20261124-175914-044509-1n-gateway-linearizable-put-500-profile/gateway.stages.json) · [Docker PUT, storage](../.reports/profiling/20261124-175914-044509-1n-gateway-linearizable-put-500-profile/node-1.stages.json) · [Три peer, стадии лидера](../.reports/profiling/20261124-175927-547270-3n-direct-linearizable-put-500-profile/node-3.stages.json)

## 4. Flame Graph и execution trace

В Docker также записаны trace обоих серверных процессов во время одного PUT-прогона. Для просмотра каждого файла используйте `go tool trace`. [Gateway trace](../.reports/profiling/20261124-180229-983513-1n-gateway-linearizable-put-500-trace/gateway.trace.out) · [Storage trace](../.reports/profiling/20261124-180229-983513-1n-gateway-linearizable-put-500-trace/node-1.trace.out)

Фактические CPU-стеки доступны как автономные интерактивные flame graph и исходные профили.

| Нагрузка и процесс | Flame Graph | Бинарный профиль |
|---|---|---|
| GET 10 000 RPS, storage | [HTML](../.reports/profiling/20261124-175045-938729-1n-direct-linearizable-get-10000-profile/node-1.cpu.html) | [CPU](../.reports/profiling/20261124-175045-938729-1n-direct-linearizable-get-10000-profile/node-1.cpu.pprof) |
| GET 10 000 RPS, Gateway | [HTML](../.reports/profiling/20261124-175118-941436-1n-gateway-linearizable-get-10000-profile/gateway.cpu.html) | [CPU](../.reports/profiling/20261124-175118-941436-1n-gateway-linearizable-get-10000-profile/gateway.cpu.pprof) |
| PUT 500 RPS, storage | [HTML](../.reports/profiling/20261124-175057-046214-1n-direct-linearizable-put-500-profile/node-1.cpu.html) | [CPU](../.reports/profiling/20261124-175057-046214-1n-direct-linearizable-put-500-profile/node-1.cpu.pprof) |

В отдельном 3-секундном trace при 500 RPS суммарная scheduler latency составила 51,47 мс для GET и 99,52 мс для PUT. Стеки пробуждения из `runtime.selectnbsend` дали соответственно 82,05% и 72,83% этих сумм. В sync-проекции обоих trace видны фоновые ожидания Raft и HTTP/2 controlBuffer. Эти суммы охватывают все горутины процесса, включая сборщик trace. [GET trace](../.reports/profiling/20261124-175553-232637-1n-direct-linearizable-get-500-trace/node-1.trace.out) · [GET scheduler](../.reports/profiling/20261124-175553-232637-1n-direct-linearizable-get-500-trace/trace-sched.txt) · [PUT trace](../.reports/profiling/20261124-175613-360061-1n-direct-linearizable-put-500-trace/node-1.trace.out) · [PUT sync](../.reports/profiling/20261124-175613-360061-1n-direct-linearizable-put-500-trace/trace-sync.txt)

## 5. Узкие места и проверка гипотез

При фильтрации прямых CPU-профилей по стекам с пакетом `google.golang.org/protobuf/proto` для GET 10 000 RPS осталось 10 мс из 3,64 CPU-с, для PUT 500 RPS — 20 мс из 1,11 CPU-с. У короткого CPU-профиля грубое разрешение. Отдельно запущенный GC в этот фильтр не входит. [GET, фильтр protobuf](../.reports/profiling/20261124-175045-938729-1n-direct-linearizable-get-10000-profile/node-1.cpu.protobuf.txt) · [PUT, фильтр protobuf](../.reports/profiling/20261124-175057-046214-1n-direct-linearizable-put-500-profile/node-1.cpu.protobuf.txt)

| Участок | Подтверждённый вклад и источник | Возможное направление проверки |
|---|---|---|
| Второй RPC в `internal/gateway/forwarder.go`, Get/Put | Таймер upstream занимает 248,6 из 254,9 мкс у GET и 284,4 из 291,1 мкс у PUT в обработчике Gateway при 500 RPS. Это ожидание всей попытки к storage. Парные baseline показывают эффект добавления Gateway. | Для клиентов, которым нужна маршрутизация, оценить существующие batch RPC при той же нагрузке по ключам. Для доступных прямых обращений оценивать выигрыш вместе с требованиями маршрутизации и failover. |
| `internal/store/checkpoint.go`, Checkpoint и `shardraft/instance.go`, processReady | Состояние каждой реплики сериализуется целиком в одном owner-цикле. С ростом ключей воспроизведены хвост успешных PUT, превышение 63 МиБ и остановка. | Согласовать бюджеты Store и допустимого снимка, сравнить бинарный или порционный снимок с текущим JSON. Проверить поведение при предельном объёме до оценки оптимизации latency. |
| `internal/store/store.go`, clone и protobuf-кодек gRPC | На GET 10 000 RPS две функции дали 75,48% выделенных байтов. Копия значения занимала 0,69 мкс в среднем в этом прогоне. Копирование предотвращает изменение принадлежащей Store памяти. | Проверить время жизни и владение буферами на контролируемом эксперименте, оценивать bytes/op, GC и клиентский p99 совместно. |
| `shardraft/client.go`, call, `shardraft/command.go`, encodeCommand/decodeRequest | Один и тот же PUT декодируется с сети, кодируется в команду, копируется в envelope и снова декодируется при apply. Доли в alloc_space выше. Для одного peer отдельно измеренные Propose и ожидание committed entry малы по отношению к RTT. | Проверить возможность сократить промежуточные копии с сохранением детерминированной репликации. |
| `shardraft/instance.go`, Run, и Store Mutex | Handoff учитывает очередь перед единственной горутиной. У стабильного PUT его среднее 6,9 мкс, у GET 10 000 RPS — 1,79 мкс. Store.lock на порядки короче. Большие суммы block включают ожидающие фоновые горутины. | Измерять очередь перед Run в присутствии больших checkpoint. Для замены Mutex на RWMutex эти результаты оснований не дают. |

Аллокации и работа GC подтверждаются счётчиками runtime и профилями. Причинной доли GC в хвосте каждого запроса эти данные не устанавливают. Работа protobuf заметна в байтах и таймерах кодека, а вершины CPU на нативном storage принадлежат главным образом системным вызовам и runtime. Клиентская часть gRPC, доставка до начала server unary и завершение передачи ответа после записи в транспорт входят в RTT. Одностороннее время физической сети отдельно не измерялось.

## 6. Вывод

Для устойчивого GET и PUT на одной ноде сначала стоит проверять путь RPC, особенно дополнительный вызов через Gateway. Локальный доступ к map, LRU и получение Store Mutex занимают малую долю даже серверного времени. Для снижения выделений конкретными кандидатами служат копирование значения и промежуточные protobuf-буферы. Для растущего набора PUT первоочередная задача — checkpoint: он способен задерживать следующие запросы и завершить всю группу. В измеренной реализации запись в RAM избавляет от дискового ввода-вывода, но сохраняет сетевой обмен, планирование горутин, кодеки, последовательный owner-цикл и снимки всего состояния.
