# 11. Сдача и воспроизводимость

**По обзору MVP требование реализовано частично.**

Проверьте чистый снимок коммита `5129931`. Все команды выполняются из корня репозитория. Нужны Git, `tar`, `tee`, `make`, Go, C-компилятор для `-race` и Docker Engine.

## 1. Распакуйте снимок

```bash
mkdir -p .reports
# mkdir без -p для снимка защищает существующий прогон от перезаписи.
mkdir .reports/checkout-5129931 &&
  bash -o pipefail -c 'git archive --format=tar 5129931 | tar -xf - -C .reports/checkout-5129931'
```

Ожидаются `Makefile`, исходники и Compose-конфигурации в `.reports/checkout-5129931`. Если каталог уже существует, выберите новое имя каталога во всех командах сценария.

```bash
git show -s --format='Commit %H%nDate %cI%nSubject %s' 5129931 |
  tee .reports/checkout-5129931/run.log
```

В первой строке отчёта будет полный SHA снимка.

## 2. Соберите бинарники

```bash
# -C выполняет make в снимке. pipefail возвращает ошибку make, даже если tee успешен.
bash -o pipefail -c 'make -C .reports/checkout-5129931 build 2>&1 | tee -a .reports/checkout-5129931/run.log'
ls .reports/checkout-5129931/bin
```

Ожидаются `oracache`, `api-gateway`, `cp-api`, `storage-node` и остальные бинарники из `make build`.

## 3. Запустите проверки Go-модулей

```bash
# GOWORK=off запускает каждый модуль отдельно. -race включает детектор гонок.
bash -o pipefail -c 'env GOWORK=off go -C .reports/checkout-5129931 test -race -count=1 -v ./... 2>&1 | tee -a .reports/checkout-5129931/run.log'
bash -o pipefail -c 'env GOWORK=off go -C .reports/checkout-5129931/gen test -race -count=1 -v ./... 2>&1 | tee -a .reports/checkout-5129931/run.log'
bash -o pipefail -c 'env GOWORK=off go -C .reports/checkout-5129931/cmd/oracache test -race -count=1 -v ./... 2>&1 | tee -a .reports/checkout-5129931/run.log'
```

Ожидается `PASS` тестов и `ok` проверенных пакетов. `-count=1` запускает тесты заново, `-v` показывает отдельные проверки.

## 4. Запустите Docker E2E

```bash
# env -u исключает настройки готовых образов из этих вызовов, чтобы E2E собрали образы из снимка.
bash -o pipefail -c 'env -u ORACACHE_GATEWAY_IMAGE -u ORACACHE_CP_IMAGE GOWORK=off go -C .reports/checkout-5129931/integration/e2e test -tags=e2e -count=1 -v -timeout=10m -run "^TestGateway$" . 2>&1 | tee -a .reports/checkout-5129931/run.log'
bash -o pipefail -c 'env -u ORACACHE_GATEWAY_IMAGE -u ORACACHE_CP_IMAGE GOWORK=off go -C .reports/checkout-5129931/integration/e2e test -tags=e2e -count=1 -v -timeout=10m -run "^TestControlPlaneGatewayFailover$" . 2>&1 | tee -a .reports/checkout-5129931/run.log'
bash -o pipefail -c 'env -u ORACACHE_GATEWAY_IMAGE -u ORACACHE_CP_IMAGE GOWORK=off go -C .reports/checkout-5129931/integration/e2e test -tags=e2e -count=1 -v -timeout=10m -run "^TestRegionReplication$" . 2>&1 | tee -a .reports/checkout-5129931/run.log'
```

Ожидаются `PASS` для всех трёх тестов. Тесты удаляют свои Compose-проекты после прогона. [Сборка и тестирование](../../README.md) · [CI](../../.github/workflows/go.yml)

## 5. Посмотрите отчёт

```bash
head -n 3 .reports/checkout-5129931/run.log
tail -n 50 .reports/checkout-5129931/run.log
```

Для защиты приложите отчёт, [схему](../architecture.html), [устройство Raft](../shardraft.md) и один из [сценариев демонстрации](README.md). Отчёт о нагрузке и свежести, публичный снимок и личные вклады команды требуют отдельного подтверждения. [Формат сдачи](../initial-task.md#в-каком-виде-загрузить-результат)
