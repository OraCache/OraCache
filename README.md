# OraCache

## Демо MVP: два региона

Из корня репозитория, с Docker Compose, `make`, `curl` и `jq`:

```sh
make demo-up
```

`make demo-up` ждёт маршрут записи в A и локальный маршрут чтения в B. После успешного завершения можно отправлять запросы через Gateway. Проверка не ждёт готовности Grafana или завершения репликации в B.

[Grafana](http://127.0.0.1:33000/d/oracache-mvp) показывает трафик и доступность по регионам, роли Raft и длину outbox A → B. Gateway A: `127.0.0.1:38080` (запись и чтение). Gateway B: `127.0.0.1:39080` (чтение).

Prometheus получает список storage-нод из Control Plane отдельно для A и B. [Targets](http://127.0.0.1:39090/targets) показывают, с каких нод он собирает метрики. Если CP недоступен, Prometheus продолжит опрашивать известные ноды, но на таймлайне ролей появится пробел: без CP Grafana не знает состав группы.

Очистка: `make demo-down` удаляет и том с последним известным составом Raft. Данные кэша в памяти; не запускай остановленный voter заново с тем же ID в живой группе.

### Поэтапный показ

Начни с пустого стенда (нужны `curl` и `jq`). `make demo-up-a` дождётся маршрута записи в A, а `make demo-up-b` — маршрута чтения в B. Между этими командами запиши ключ:

```sh
make demo-up-a
export ORACACHE_ENDPOINT=127.0.0.1:38080
go run ./cmd/oracache put demo:geo before-b --expected-rev=0
make demo-up-b
ORACACHE_ENDPOINT=127.0.0.1:39080 go run ./cmd/oracache get demo:geo
```

Если B ещё догоняет, повтори GET. Проверь `HIT`, исходную ревизию и `servedBy.instance=b-*`; очередь должна опустеть.

```sh
make demo-up-learner
GROUP=region-a/shard-0/replication-1
go run ./cmd/oracache membership add-learner --group-id="$GROUP" --region=region-a --operation-id=add-a4 --peer-id=4 --node-id=a-4 --address=a-4:28091 --raft-address=http://a-4:28191/raft
curl -fsS http://127.0.0.1:38194/status
```

`make demo-up-learner` только запускает процесс. В Targets и на таймлайне `a-4` появится после `add-learner`.

Убедись, что `a-4` стала learner и её `AppliedIndex` дошёл до индекса добавления. Найди лидера A на таймлайне Grafana и останови его, например `make demo-stop-node ID=a-3`. Дождись нового лидера, прочитай контрольный ключ и запиши новое значение через тот же Gateway A. Затем замени погибшего voter на `a-4` (ниже `3` — пример ID погибшего лидера):

```sh
go run ./cmd/oracache membership replace --group-id="$GROUP" --region=region-a --operation-id=replace-dead --old-peer-id=3 --learner-id=4
go run ./cmd/oracache membership get --group-id="$GROUP" --region=region-a
```

Остановленный voter остаётся на таймлайне как «Нет scrape», пока он в составе. После `replace` проверь, что `a-4` стала voter, старая нода исчезла из текущих целей, а новая запись снова доходит в B. Автоматический сценарий входит в `make test-e2e`.

## PoC с Control Plane и Gateway

Локальный стенд: три ноды хранилища в одной группе Raft, Control Plane и API Gateway. Клиент обращается к gateway на `127.0.0.1:28080`.

### Запуск

Из корня репозитория, с Docker Compose:

```sh
docker compose -f compose.cp.yaml up -d --build
curl -fsS http://127.0.0.1:9466/routes
```

Стенд готов, когда в `read_routes` и `write_routes` есть маршрут для партиции `0`. Логи: `docker compose -f compose.cp.yaml logs --tail=50`.

### Запросы через gateway

Для CLI нужен Go 1.27.3.

```sh
export ORACACHE_ENDPOINT=127.0.0.1:28080
go run ./cmd/oracache put demo:key hello  # PUT_STATUS_OK
go run ./cmd/oracache get demo:key        # GET_STATUS_HIT
go run ./cmd/oracache delete demo:key     # DELETE_STATUS_DELETED
go run ./cmd/oracache get demo:key        # GET_STATUS_MISS
```

### Управление составом Raft

Административный gRPC-сервис управления составом работает на том же адресе Gateway.
Например, текущий состав группы стенда:

```sh
go run ./cmd/oracache membership get --group-id=region-a/shard-0/poc-1 --region=region-a --partition-id=0
```

Для добавления ноды запустите её с новым `peer-id` и `node-id`, прежним `group-id`
и `--join`. Затем вызовите `membership add-learner` с её клиентским и Raft-адресом;
после синхронизации — `membership promote` либо `membership replace`. Команды
`membership remove` и `membership get` работают через тот же Gateway.
Каждому изменению нужен уникальный `--operation-id`; при неизвестном результате
повторяйте запрос с тем же ID и параметрами.

### Метрики Control Plane и Gateway

Gateway отдаёт `oracache_gateway_partitions_without_route{operation}` на
`/metrics` рядом с `/routes`.

```sh
curl -fsS http://127.0.0.1:9466/metrics
```

После первой карты метрика показывает, для скольких партиций Gateway не
получил маршрут на чтение или запись. Ноль означает полное покрытие карты.
При смене лидера карта может временно потерять маршрут, даже если Raft
позже успешно продолжит работу без смены `config_revision`.

При отдельном запуске `cp-api` метрика
`oracache_cp_heartbeat_age_seconds{region,node_id}` доступна на
`127.0.0.1:9464/metrics` по умолчанию. После истечения TTL она остаётся
видимой, пока пара нода–регион есть в конфигурации. В `compose.cp.yaml`
метрики CP и Storage Node доступны внутри сети Compose.

### Остановка

```sh
docker compose -f compose.cp.yaml down
```

Данные хранятся в памяти. Для чистого запуска пересоздаётся вся группа: перезапуск одной ноды с пустым состоянием под тем же ID не поддерживается.

### Проверить смену лидера

Тест сам поднимает стенд на свободных локальных портах, отключает доступ к Control Plane и останавливает лидера. Он проверяет, что через тот же адрес gateway можно прочитать ранее записанное значение и, после восстановления маршрута, снова записать данные. Для `-race` нужны cgo и C-компилятор.

```sh
GOWORK=off go test -race -count=1 -v ./cmd/api-gateway \
  -run '^TestGatewayRoutesRealRaftThroughLeaderAndControlPlaneLoss$'
```

`make test-e2e` дополнительно запускает стенд в Docker и проверяет чтение и запись через тот же gateway после остановки контейнера лидера. Тест сам поднимает и удаляет свой Compose-проект.

Правила маршрутизации и повторов описаны в [ADR-004](docs/ADR-004-cp-raft-gateway-routing.md).

### Мониторинг реального кластера

Поднимите CP API с MemoryStore, Gateway, три Storage Node одной Raft-группы
и мониторинг в отдельном проекте. При необходимости выберите свободные
локальные порты через переменные окружения.

```sh
export ORACACHE_CP_PORT=28080 ORACACHE_CP_STATUS_PORT=9466
export ORACACHE_PROMETHEUS_PORT=9090 ORACACHE_GRAFANA_PORT=3000
docker compose -p oracache-real -f compose.cp.yaml -f compose.cp-observability.yaml up -d --build
curl -fsS "http://127.0.0.1:${ORACACHE_CP_STATUS_PORT}/routes"
```

Дождитесь маршрута для партиции `0` и выполните команды GET, PUT и DELETE из
раздела «Запросы через gateway». Для графиков на основе `rate` нужен scrape
до запросов и хотя бы ещё один после них. В Compose Gateway публикует
диагностику на loopback, CP API отдаёт `/metrics` на внутреннем порту 9464,
а Storage Node на существующем внутреннем Raft-порту 28191.

- [Дашборд реального кластера](http://127.0.0.1:3000/d/oracache-real-cluster)
- [Цели сбора Prometheus](http://127.0.0.1:9090/targets)

#### Обнаружение Storage Node

Prometheus запрашивает `GET /discovery/storage-nodes?region=region-a` у CP API.
Лидер сообщает через heartbeat полный применённый состав своей Raft-группы.
После `membership add-learner` CP публикует новую ноду как цель сбора.
Для сбора используется схема и `host:port` из `raft_address`, путь всегда
`/metrics`. В конфигурации запуска CP начальные ноды используют поле
`nodes[].raft_address`. Объявленный адрес должен быть доступен из сети
Prometheus. Применённый состав включает learner и outgoing voter в joint
configuration. Нода остаётся целью при потере heartbeat, пока входит хотя бы
в одну отслеживаемую группу.

При первом запуске профиля CP создаёт `/state/membership.json` в томе
`cp-membership` и использует bootstrap-адреса групп, которые ещё не
отчитались. После перезапуска CP восстанавливает применённый состав из
файла. Если состав хотя бы одной группы в запрошенной области неизвестен
или адрес цели некорректен, CP отвечает `503`. Работающий Prometheus
сохраняет предыдущие цели. При одновременном холодном старте Prometheus
получает сохранённые или первоначальные цели с первым успешным ответом.
Если CP восстанавливается без известного состава области, новый Prometheus
ждёт отчёта лидера. `200 []` означает известную область без целей.
Без `--membership-state` CP при каждом запуске ждёт отчётов Raft.

Храните том вместе с жизненным циклом группы. Для новой группы с нуля
используйте новый `group_id` или новый том при полном сбросе стенда.
Один экземпляр CP владеет файлом. Границу доверия, правила проверки
адресов и поведение при отказах описывает [ADR-005](docs/ADR-005-storage-discovery.md).

```sh
docker compose -p oracache-real -f compose.cp.yaml -f compose.cp-observability.yaml \
  exec -T prometheus wget -qO- 'http://cp-api:9464/discovery/storage-nodes?region=region-a'
curl -fsS "http://127.0.0.1:${ORACACHE_PROMETHEUS_PORT:-9090}/api/v1/targets" |
  jq '.data.activeTargets[] | select(.labels.job == "storage-node") | {labels, scrapeUrl, health}'
```

Двухрегиональный профиль опрашивает A и B независимо.

```sh
docker compose -f compose.replication.yaml -f compose.replication-observability.yaml up -d --build
```

Проверка изменения состава поднимает отдельные Docker-проекты со свободными
портами. Она запускает новую ноду, вызывает `membership` через Gateway и
проверяет ответы CP, цели Prometheus и запросы панелей Grafana.

```sh
mkdir -p .reports
integration/e2e/check-storage-discovery.sh > .reports/storage-discovery.log 2>&1
GOWORK=off go test -race -count=1 -v ./cmd/api-gateway \
  -run '^TestMembershipThroughGatewayAndCPProjection$'
```

#### Проверка доступности и отказов

Скрипт повторяет чистый запуск и три отказных сценария на отдельных проектах
со свободными портами. Он требует Docker Compose, Go, `curl` и `jq`, выполняет
RPC с дедлайном 3 с и выводит ответы и значения метрик для отчёта.

```sh
mkdir -p .reports
integration/e2e/check-observability.sh > .reports/observability.log 2>&1
```

#### PromQL

```sh
prom_query() {
  curl -fsSG "http://127.0.0.1:${ORACACHE_PROMETHEUS_PORT:-9090}/api/v1/query" \
    --data-urlencode "query=$1" | jq -c '.data.result[]'
}

prom_query 'up{job=~"cp-api|api-gateway|storage-node"}'
prom_query 'sum by (job,operation) (rate(oracache_cache_rpc_total[1m]))'
prom_query 'sum by (job,operation) (rate(oracache_cache_rpc_total{grpc_code!="OK"}[1m])) / (sum by (job,operation) (rate(oracache_cache_rpc_total[1m])) > 0)'
for q in 0.50 0.95 0.99; do
  prom_query "histogram_quantile($q, sum by (job,operation,le) (rate(oracache_cache_rpc_duration_seconds_bucket[1m])))"
done
prom_query 'sum by (job) (rate(oracache_cache_rpc_total{operation="get",grpc_code="OK",outcome="hit"}[1m])) / (sum by (job) (rate(oracache_cache_rpc_total{operation="get",grpc_code="OK",outcome=~"hit|miss|version_not_found"}[1m])) > 0)'
prom_query 'time() - oracache_gateway_config_last_success_timestamp_seconds'
prom_query 'oracache_raft_commit_index - oracache_raft_applied_index'
prom_query 'oracache_raft_follower_lag_entries'
prom_query 'oracache_raft_replica_state'
```

Доля gRPC-ошибок считается среди обработанных RPC. Доменный `MISS`
получает gRPC `OK` и учитывается в знаменателе доли попаданий. PromQL
возвращает долю от 0 до 1, а Grafana показывает её в процентах. Если
за минуту не было подходящих GET, у графика нет значения. Перцентили
рассчитываются отдельно для каждого слоя. Таймер Gateway включает
маршрутизацию, ожидание Storage Node и повторы. Таймер Storage Node
измеряет собственный обработчик, включая ожидание Raft.

| Сигнал | Источник, единицы и проверка |
| --- | --- |
| `oracache_cache_rpc_total`, `oracache_cache_rpc_duration_seconds` | Число завершённых GET, PUT и DELETE и время обработчика в секундах на серверной границе Gateway и Storage Node. Сверьте `outcome`, `grpc_code` и `_count` после клиентских запросов. |
| `oracache_gateway_forward_attempts_total` | Число запущенных исходящих RPC. `reason` выделяет первый вызов, фактический повтор после NOT_LEADER и повтор чтения после временной ошибки. Batch RPC видны в пересылке. |
| `oracache_cp_rpc_duration_seconds` | Время обработчиков `ReportHeartbeat` и `GetConfig` в секундах. Счётчик гистограммы растёт при heartbeat и опросе. |
| `oracache_gateway_config_refresh_total`, `oracache_gateway_config_last_success_timestamp_seconds`, `oracache_gateway_config_revision` | Результаты опроса и применения карты, Unix-время последнего принятого ответа и ревизия применённой карты. Ответ с прежней ревизией подтверждает связь с CP. Сверьте ревизию с `/routes`. |
| `oracache_raft_{is_leader,is_follower,is_learner,has_leader,ready,term,commit_index,applied_index,snapshot_index,last_index}` | Признаки 0/1, терм и позиции в записях лога из одного `Status()` на scrape. Запись продвигает commit и applied. Конец лога включает незакоммиченные записи. Индекс снапшота до первого чекпоинта равен 0. |
| `oracache_cp_raft_member`, `oracache_cp_raft_membership_fresh` | Участники применённого состава и его свежесть: 1 — подтверждён текущим лидером, 0 — последний известный, -1 — неизвестен. |
| `oracache_raft_stage_duration_seconds`, `oracache_raft_transport_send_errors_total` | Время выполнения команды в apply, ожидания результата ReadIndex до ожидания локального apply, кодирования чекпоинта и восстановления Store, в секундах. ReadIndex учитывает полученные результаты. Счётчик отправок учитывает ошибки вызова транспорта. Проверьте `_count` после GET/PUT, после чекпоинта и при установке снапшота. |
| `process_cpu_seconds_total`, `process_resident_memory_bytes`, `go_memstats_heap_alloc_bytes`, `go_goroutines` | CPU-время процесса, RSS, занятый Go heap и число горутин. `rate(process_cpu_seconds_total[1m])` даёт занятые ядра. CPU и RSS проверены в Linux-контейнерах и могут отсутствовать при запуске на macOS. |
| `oracache_storage_keys`, `oracache_storage_accounted_bytes` | `Store.Stats()` на каждой реплике даёт число ключей и байты, списанные из бюджета Store. Эти байты показаны отдельно от RSS. Сверьте изменение после PUT и DELETE. |

Очередь применения считается по двум индексам одной реплики. Снимок
метрик обновляется перед применением группы записей и после завершения
итерации Raft, поэтому короткий разрыв может пройти между scrape. Для оценки
отставания follower используется commit наблюдаемого лидера той же
пары регион–партиция. Правило требует одного лидера в максимальном
наблюдаемом терме, а разницу ограничивает снизу нулём из-за разновременных
scrape. При неизвестном или неоднозначном лидере серия отсутствует.

В «Ролях реплик по партициям» выберите «Регион матрицы» и, если нужно,
конкретную «Партицию матрицы». `All` показывает все партиции выбранного
региона. Строки таблицы — партиции, столбцы — подтверждённые участники.
`Leader` означает единственного наблюдаемого лидера в максимальном терме.
`Follower` и `Learner` показывают роль живой реплики. `Down` означает
`up=0`, `Unknown` — роль ещё неизвестна. При удалении участник исчезает
из текущего состава, а его собранные ранее метрики доступны в истории
Prometheus до окончания срока хранения. Соседняя панель показывает свежесть
состава для выбранных партиций.

#### Ожидаемые сигналы при отказах

| Сценарий | Что проверить |
| --- | --- |
| Остановлен лидер, две ноды живы | `up` остановленной ноды равен 0. Другой лидер появляется с более высоким термом. GET через прежний адрес Gateway возвращает подтверждённую версию, следующий PUT подтверждается. Ревизия карты сохраняется. |
| Разорвана связь Gateway с CP | `rpc_error` растёт, отметка последнего принятого ответа стоит, все пять scrape-целей остаются UP. GET и PUT доступны по последней карте при живой группе. |
| Приостановлены два follower, CP работает | CP остаётся UP, Gateway получает конфигурацию, клиентский PUT с дедлайном теряет подтверждение. Когда лидерство теряется, серия отставания follower отсутствует. После снятия паузы PUT снова подтверждается. |

`up` описывает доступность scrape. Ready и известный лидер показывают
локальное состояние реплики. Доступность записи проверяется подтверждением
реального PUT с дедлайном. Однорегиональный профиль использует MemoryStore
с чекпоинтом состава в CP. При повторном запуске Storage Node начинает с
пустой памятью. Восстановление прежнего участника Raft требует персистентного
состояния Storage Node (#108).

```sh
docker compose -p oracache-real -f compose.cp.yaml -f compose.cp-observability.yaml down --volumes
```

#### Проверка конфигураций Prometheus

```sh
docker run --rm --entrypoint /bin/promtool -v "$PWD/configs/monitoring:/checks:ro" -w /checks \
  prom/prometheus:v3.5.0 check config prometheus.yml prometheus-real.yml prometheus-replication.yml
docker run --rm --entrypoint /bin/promtool -v "$PWD/configs/monitoring:/checks:ro" -w /checks \
  prom/prometheus:v3.5.0 test rules raft-rules.test.yml
```

### Мониторинг синтетических агентов

Этот стенд запускает два агента с заданными ролями партиций, Prometheus
и Grafana. Агенты имитируют heartbeat для проверки метрик. Для проверки
смены лидера реальной Raft-группы используйте стенд выше. Prometheus опрашивает `/metrics` CP API и Gateway каждые 2 с.
Оба веб-интерфейса доступны только через loopback. Grafana открывает готовый
дашборд в режиме просмотра без входа.

```sh
docker compose -f compose.cp-monitoring.yaml config --quiet
docker compose -f compose.cp-monitoring.yaml up -d --build
docker compose -f compose.cp-monitoring.yaml ps
```

- [Дашборд OraCache · Control Plane и Gateway](http://localhost:3000/d/oracache-cp-monitoring)
- [Цели сбора Prometheus](http://localhost:9090/targets)

В терминале те же метрики можно запросить через API Prometheus. Для вывода
меток и значений нужен `jq`.

```sh
prom_query() {
  curl -fsS -G http://localhost:9090/api/v1/query --data-urlencode "query=$1" |
    jq -c '.data.result[] | {labels: .metric, value: (.value[1] | tonumber)}'
}

prom_query 'up{job=~"cp-api|api-gateway"}'
prom_query 'oracache_cp_heartbeat_age_seconds{job="cp-api"}'
prom_query 'oracache_gateway_partitions_without_route{job="api-gateway"}'
```

1. После первых heartbeat, обновления карты и scrape обе цели показывают
   `up=1`. Возраст heartbeat нод `eu-1` и `eu-2` обычно меньше 1 с.
   Число партиций без маршрута равно 0 и для `operation="get"`, и для
   `operation="put"`.
2. Выполните `docker compose -f compose.cp-monitoring.yaml stop eu-1` и
   повторяйте запросы. Возраст heartbeat `eu-1` растёт. Через примерно 20 с
   (TTL 15 с, poll Gateway 2 с, scrape 2 с) обе серии Gateway показывают 64.
   Обе цели сбора по-прежнему показывают `up=1`.
3. Выполните `docker compose -f compose.cp-monitoring.yaml start eu-1`.
   После нового heartbeat, poll и scrape возраст `eu-1` снова меньше 1 с,
   а число партиций без маршрута возвращается к 0 для GET и PUT.

Панели доступности показывают состояние последнего scrape каждой цели.
Если scrape завершился ошибкой, Prometheus отдаёт `up=0`. Если серия
отсутствует, Grafana показывает отсутствие данных. Ноль на графиках GET
и PUT означает, что в текущей карте есть маршрут для всех партиций.

```sh
docker compose -f compose.cp-monitoring.yaml down
```

---

## PoC без Control Plane

В этом варианте Docker Compose запускает только три `storage-node`. Клиент обращается напрямую к нодам.

Добавление пустой реплики и замена потерявшей состояние ноды без перезапуска
группы описаны в [документации модуля shardraft](docs/shardraft.md#динамический-состав-группы):
схемы вызовов, `--join`, административный HTTP API и ручной пример без Docker.
Тот же Raft-протокол используется и при работе с CP; административный путь
через Gateway описан выше.
Бюджет Store в `storage-node` ограничен 8 МиБ для безопасной передачи snapshot.

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

`make test` запускает обычные тесты без Docker. Сквозной тест трёх нод тоже не требует Docker: он проверяет репликацию, чтение и запись после смены лидера. `make test-e2e` проверяет три стенда: локальный кэш без Raft, PoC с Control Plane и межрегиональную репликацию. Для ручной проверки этого Docker-стенда подходит сценарий `put → get → delete → get` выше.

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
`.reports/cli.xml`, `.reports/e2e.xml`, `.reports/e2e-cp.xml`; каталог игнорируется git.
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

- [Модуль shardraft: устройство и динамический состав Raft-группы](docs/shardraft.md)
- [Задание и целевая архитектура](docs/initial-task.md)
- [Глоссарий](docs/GLOSSARY.md)
