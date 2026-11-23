# Control Plane статической топологии

## Запуск и проверки

```sh
docker build -f Dockerfile.cp -t oracache-runtime:local .
ORACACHE_CP_IMAGE=oracache-runtime:local go test -tags=integration -count=1 -v ./cmd/cp-api -run '^TestEtcd(HARuntime|ControlPlane)$'
```

Тест выделяет проект Compose, закрытые локальные порты и временные сертификаты. `cp-region-a` и `cp-region-b` читают `/runtime` из общего кластера etcd. Реальная Raft-группа находится в A. Это локальная проверка #46 и часть сценария #82. Приёмка #83 требует запуска на согласованном общем стенде.

На целевой среде разместите три etcd-участника в независимых доменах отказа с устойчивыми томами и контролем RTT. У каждого регионального API должен быть доступ к кворуму etcd. Gateway и агент получают упорядоченный список через `-cp-addr=cp-a.example:50052,cp-b.example:50052`. Используйте один prefix и TTL для всех CP API данного кластера.

- Для связи с CP задайте `ORACACHE_CP_CLUSTER_ID` и `ORACACHE_CP_TLS_{CA,CERT,KEY}`. На сервере укажите DNS/IP в SAN, у клиентов — `spiffe://<cluster>/node/<region>/<node-id>` или `spiffe://<cluster>/gateway/<region>/<id>` с EKU ClientAuth. PKI должна выдавать такие SAN только авторизованному владельцу идентичности.
- Для CP ↔ etcd задайте `ORACACHE_ETCD_TLS_{CA,CERT,KEY}` и HTTPS endpoints. В etcd включите client и peer mTLS. Для production включите etcd auth и сопоставьте CN, роли и namespace. API нужны чтение снимка и чтение/запись heartbeat и receipts, управление leases. Bootstrap, recovery, Controller и backup получают отдельные учётные данные. Ограничьте сетевой доступ к клиентскому и peer-портам etcd.
- Задайте `ORACACHE_CP_SNAPSHOT_FILE` и при необходимости `ORACACHE_CP_SNAPSHOT_MAX_AGE` (по умолчанию 1h). Используйте доступный для записи локальный том с одним владельцем для каждого процесса. Каталог должен существовать.
- Создайте первый snapshot командой `cp-api -store etcd -etcd-endpoints "$ETCDCTL_ENDPOINTS" -etcd-prefix "$CP_PREFIX" -config topology.json -bootstrap-only`. Запускайте рабочие API без параметров bootstrap.
- Диагностика CP `/healthz`, `/readyz`, `/metrics` доступна на отдельном HTTP-порту. `/readyz` проверяет актуальный снимок через кворум. Следите за `oracache_cp_config_ready`, `oracache_cp_statestore_errors_total`, `oracache_gateway_config_snapshot_age_seconds`, покрытием маршрутов и heartbeat age. Диагностические порты оставьте в закрытой сети.

## Обновление и откат

Обновляйте по одному API и проверяйте `/readyz` на каждом шаге. Проверьте heartbeat и совпадение ревизии с ответом второго API. Затем обновите Gateway и агенты с сохранением их томов. При ротации сначала раздайте набор доверенных старых и новых CA, затем новые leaf-сертификаты, затем уберите старые CA.

При откате разверните предыдущие совместимые бинарники под теми же namespace и топологией. Согласуйте профиль TLS до перезапуска клиентов. Откат топологии публикуйте новой ревизией через проверенный путь оператора. Для PoC Storage Node состояние Raft хранится в RAM. Такой процесс нельзя перезапускать в работающую группу как полноценную восстановленную реплику.

## Резервная копия и восстановление

1. Укажите `ETCDCTL_ENDPOINTS`, `ETCDCTL_CACERT`, `ETCDCTL_CERT` и `ETCDCTL_KEY` для администратора. Используйте один endpoint для `etcdctl snapshot save backup.db`. Проверьте backup через `etcdutl snapshot status backup.db -w table`. Сохраните backup, namespace, версию etcd и наблюдавшуюся прикладную ревизию. Ведите максимум опубликованных config revision вне резервируемого etcd.
2. Остановите CP API и Controller и изолируйте восстанавливаемый кластер от действующего. Восстановите на чистые тома тем же backup с `etcdutl snapshot restore backup.db --name <member> --data-dir <new-dir> --initial-cluster <members> --initial-advertise-peer-urls <url> --initial-cluster-token <new-token> --bump-revision <delta> --mark-compacted`. Выберите bump выше прежних наблюдаемых etcd revision. Проверьте имена, права и кворум до подключения клиентов.
3. Сверьте восстановленную топологию с живым статическим Raft. Если безопасный состав и master-роль подтвердить нельзя, сохраните изоляцию. Выберите `NEW_CONFIG_REVISION` выше ревизии backup и всех прикладных ревизий до отказа. Выполните `cp-api -store etcd -etcd-endpoints "$ETCDCTL_ENDPOINTS" -etcd-prefix "$CP_PREFIX" -recover-config-revision "$NEW_CONFIG_REVISION"`. Команда публикует новую ревизию и удаляет восстановленные heartbeat и receipts в одной транзакции.
4. Запустите оба API с `-minimum-config-revision "$NEW_CONFIG_REVISION"`. Проверьте readiness, новые heartbeat, revision Gateway, маршруты и строгие GET/PUT. MVCC bump сам по себе не повышает прикладную ревизию. Для клиентов без checkpoint нижняя граница в API входит в процедуру восстановления.
