# Проверка статической конфигурации и подсказок о лидере

Статус: исторический отчёт реализации и [выполненных проверок экспериментального варианта](../REQUIREMENTS.md#r-024), E-018. Результаты ниже перенесены из прежней документной ревизии; при редактуре 2026-11-16 тесты и CI заново не запускались и не запрашивались. Уточнение владельца E-017 не меняет результаты запусков, но исключает вывод о согласовании финального CP/discovery. Выбор остаётся в [RFC-004](../rfc/RFC-004-topology.md).

Стенд состоит из трёх узлов одного шарда в одном регионе. Управляющий сервис (CP) сообщает шлюзу (gateway) допустимые адреса, но не выбирает лидера. Проверялось, сможет ли шлюз найти нового лидера после остановки старого и прочитать уже подтверждённую запись.

## Проверенные локальные срезы

- CP: `79e36a61017d7cdb4c4b76586b0d815c96820c5b`.
- Объединённая компонентная база: `641ce8941f0e5587c4a0d2c14c719e0f9f461783`.
- Application: `330c76fae0c1d0a2289a0047934c58b9edb2fc40`.

Выше указан первоначальный локально проверенный срез. Затем код опубликован и разделён для самостоятельного ревью:

- [CP/router #65](https://github.com/OraCache/OraCache/pull/65): `79e36a61017d7cdb4c4b76586b0d815c96820c5b`.
- [Gateway #2](https://github.com/OraCache/petruekhin-vibe/pull/2), base CP: `73ff824f5723ca050d2f17c62b808cb916c3f258`, 6 файлов/+842.
- Объединённая база с gateway: `4d6ca3c063dfd07360ac8d33fff988602d414931`.
- [Application #1](https://github.com/OraCache/petruekhin-vibe/pull/1), base объединённых компонентов с gateway: `8061f787296dfe0e955c266bbea7f88dc37725ae`, 20 файлов/+1796/−8.

Итоговое application-дерево побайтно совпадает с `330c76f`; gateway implementation paths идентичны в его собственном PR, объединённой базе и application. Gateway самостоятельно прошёл root/CLI/gen race-тесты, vet и make build без storage application layer. Объединённая база также независимо прошла root race-тесты и vet. История опубликованных веток сохранена без force-push. Рабочий стенд не обновлялся.

Hosted CI проверен отдельно: [CP test/e2e](https://github.com/OraCache/OraCache/actions/runs/43939829997) и [application test/e2e/poc](https://github.com/OraCache/petruekhin-vibe/actions/runs/43939827488) успешны. Gateway CI запущен отдельным PR; его текущий результат следует смотреть в checks самого PR, а не выводить из результатов application.

## Границы проверки кода

[Контракт статического варианта](static-leader-hints.md) описывает фильтрацию heartbeat, порядок маршрутов и условия повторов.

В более позднем интеграционном срезе `70c07350dc473b171945f40794e21fb4522dcf2e` порядок адресов и политика повторов сверены по [Forwarder](https://github.com/OraCache/petruekhin-vibe/blob/70c07350dc473b171945f40794e21fb4522dcf2e/internal/gateway/forwarder.go). Это проверка кода, не повтор перечисленных ниже запусков.

## Исполненные проверки

- Родительский независимый запуск `GOWORK=off go test -race -count=1 ./...` и `go vet ./...` в root, gen, cmd/oracache — успешно.
- Независимые race-тесты CP/API/router/agent/StateStore — успешно.
- Tagged Docker e2e: `make test-e2e`, TestGateway — успешно. Это legacy gateway e2e, не замена integrated failover.
- Изолированный Compose/CLI smoke дерева `57e80d5` — успешно; итоговый application `330c76f` побайтно совпадает с ним по Git tree. Убит реальный избранный node-3, новым лидером стал node-1. Через прежний gateway прочитана подтверждённая ревизия 2, прошли последующие conditional Put, conflict и Delete.
- Диагностика /heartbeats возвращает конфигурацию и leader_hints.
- Тестовый Compose-проект удалён. Существующий стенд не перезапускался.
- Между component base и application нет изменений общих CP/protobuf/router implementation paths; application diagnostics оформлена отдельно.

## Чего это не доказывает

Нет WAN-приёмки, durable recovery, безопасного rejoin потерявшего RAM peer, mTLS, динамического membership, scheduler, writer fencing, exactly-once или нагрузочных SLO. Подсказка о лидере не удостоверяет кворум и не является разрешением записи.

Проверки [чтения напрямую с узла](single-shard-integration.md) относятся к отдельному standalone-срезу и не входят в этот Compose smoke.
