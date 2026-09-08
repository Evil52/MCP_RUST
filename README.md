# MCP Ozon

## Надёжный локальный запуск

Для macOS сначала скачайте `release.json` и `release-images.json`, которые
workflow `Rust CI` публикует только после прохождения всех обязательных jobs,
multi-platform scanning и provenance attestation, затем установите watchdog:

```bash
sha="$(git rev-parse HEAD)"
release_dir="$(mktemp -d /private/tmp/mcp-ozon-release.XXXXXX)"
gh run download --name "mcp-ozon-release-$sha" --dir "$release_dir"
MCP_RELEASE_EVIDENCE="$release_dir/release.json" \
MCP_RELEASE_IMAGE_LOCK="$release_dir/release-images.json" \
  ./scripts/install-local-runtime-agent.sh
```

LaunchAgent каждые 30 секунд проверяет локальный MCP и Secure MCP Tunnel. Если
Docker-контейнер или `tunnel-client` остановился, он поднимает их штатными
командами `docker start`/`docker restart` и
`tunnel-client runtimes connect`. Для Tunnel проверяется не только локальный
`healthz/readyz`, но и возраст последнего успешного control-plane poll. Poll
старше 90 секунд считается зависшим. Перед перезапуском сторож проверяет прямой
HTTPS-маршрут к `api.openai.com`; при сетевом таймауте или HTTP 403 он не входит
в restart-loop, а завершает проверку с диагностикой VPN/DNS. Между автоматическими
перезапусками действует cooldown 5 минут.

Интерактивный installer проверяет права `600`, не меняет исходные
`.env` и `config/access.json`, атомарно копирует реестр в закрытый
`~/.local/share/mcp-ozon-runtime`, проверяет GitHub provenance attestation,
загружает только закреплённый digest из private GHCR и пересоздаёт контейнер с
`--no-build`. CI evidence связывает tested source SHA с хешем полного image
lock; `org.opencontainers.image.revision` проверяется после pull. Контракт и
настройка pull-only доступа описаны в
[docs/immutable-container-delivery.md](docs/immutable-container-delivery.md).
Фоновый watchdog больше не читает проект, `.env` или Compose из
`Documents`: он только проверяет persistent mount и запускает уже
созданный контейнер. После изменения Rust-кода, `.env` или
`config/access.json` повторно запустите installer. Профиль и runtime API key
читаются из `~/.config/tunnel-client`; секреты в репозиторий не копируются.

## Разработка с автоматическим обновлением

Один раз установите `cargo-watch --locked`, затем запустите
`./scripts/dev-watch.sh`. Host-процесс по умолчанию использует изолированный
`http://127.0.0.1:8789/mcp`, чтобы не конфликтовать с Docker/Tunnel на `8787`,
и автоматически пересобирается после изменения `src/`, `Cargo.toml`,
`Cargo.lock` или `.env`. Другой адрес можно задать через `MCP_DEV_BIND`.

Чтобы изменения Rust-кода автоматически попадали именно в рабочий Docker endpoint
на `8787`, используйте Compose Watch:

```bash
docker compose -f compose.yaml -f compose.dev.yaml watch
```

Магазины, менеджеры и права находятся в `config/access.json` и перечитываются
при каждом MCP-вызове, поэтому их изменения применяются сразу, без пересборки и
без Refresh подключения ChatGPT. Секреты по-прежнему хранятся только в `.env`.
В Git публикуется лишь обезличенный `config/access.example.json`.

Если изменились имя tool, его описание, input/output schema или annotations,
после перезапуска откройте Plugins → OzonOFK → Refresh и начните новый чат.

Read-only MCP-сервер на Rust для Ozon Seller API, Ozon Performance API и Wildberries API. Он
предоставляет ChatGPT инструменты Ozon для аналитики, остатков, цен, заказов, возвратов,
финансов, рейтинга, отзывов, вопросов и рекламной статистики, а также отдельные WB-инструменты
аналитики.

Сервер не содержит методов изменения данных. Все MCP tools помечены `readOnlyHint: true`.

## Настройка

```bash
cp .env.example .env
cp config/access.example.json config/access.json
chmod 600 .env config/access.json
```

Укажите доверенную идентичность экземпляра MCP и заполните ключи нужных магазинов:

```dotenv
MCP_ACTOR_ID=admin
EXAMPLE_OZON_CLIENT_ID=ваш-client-id
EXAMPLE_OZON_API_KEY=ваш-api-key
EXAMPLE_OZON_PERFORMANCE_CLIENT_ID=ваш-performance-client-id
EXAMPLE_OZON_PERFORMANCE_CLIENT_SECRET=ваш-performance-client-secret
EXAMPLE_WB_API_TOKEN=ваш-read-only-wb-token
```

Для `wb_promotion_*` WB-токен должен включать категорию «Продвижение» и уровень
`Read Only`; права записи рекламным инструментам не требуются. Для `wb_search_*`
тому же Personal read-only токену нужна категория «Аналитика», а кабинету — активная
подписка «Джем». Поисковые отчёты WB обновляются раз в час и возвращают агрегированные
средние/медианные позиции без региона и без разделения органики и рекламы — это не
моментальный снимок публичной выдачи.

Файл `.env` исключён из Git. Его отсутствие допустимо при полностью внешнем окружении, но ошибка
чтения или синтаксиса останавливает запуск без вывода потенциально секретной исходной строки.
Seller API key, Performance Client Secret и WB token передаются только от MCP-сервера
соответствующему маркетплейсу и не возвращаются ChatGPT. В
`config/access.json` хранятся только имена переменных окружения
`ozon.client_id_env`, `ozon.api_key_env`, `ozon.performance.client_id_env`,
`ozon.performance.client_secret_env` и `wildberries.api_token_env`, но не значения секретов.
После изменения `.env` или привязки имени credential-переменной пересоздайте контейнер;
изменения обычных правил доступа в реестре по-прежнему подхватываются без пересборки.
Performance Client ID должен быть уникален для одного магазина: совместное использование одной
Performance OAuth-пары разными store запрещено при запуске, чтобы ответ общего рекламного кабинета
не мог обойти store-level ACL.

`MCP_ACTOR_ID` нельзя передавать аргументом MCP-инструмента: он читается только из локального
окружения сервера. В публичном примере `admin` имеет роль `admin` и доступ ко всем настроенным
кабинетам Ozon и WB. Менеджер получает доступ только к закреплённому кабинету. Для менеджеров создавайте
отдельные экземпляры MCP/Tunnel с их `MCP_ACTOR_ID`; общий Tunnel с идентичностью администратора
даст всем его пользователям административный доступ.

Это правило относится только к совместимому режиму `MCP_AUTH_MODE=dev`. В защищённом режиме
`MCP_AUTH_MODE=jwt` сервер не использует `MCP_ACTOR_ID`: он проверяет подпись и claims access token,
а затем сопоставляет только неизменяемый `sub` с обязательным полем `oidc.subject` в
`config/access.json`. `username` и `email` могут храниться только как отображаемые метаданные и
не дают доступ. JWT-runtime не запускается, пока у каждой настроенной OIDC identity нет реального
subject от провайдера. Неизвестный пользователь и любой запрос к `/mcp` без JWT отклоняются до
чтения body, поиска/создания сессии и вызова MCP handler.

`.env.example` и `config/access.example.json` содержат один согласованный обезличенный кабинет.
Реальные кабинеты, Client-Id и API-ключи задаются только в исключённых из Git локальных
`config/access.json` и `.env`.

## Магазины и доступ

Структура доступа в публичном примере выглядит так:

| Marketplace | Selector | Организация | Seller/Client ID | Менеджер |
| --- | --- | --- | ---: | --- |
| Ozon | `example_store` | Example organization | `<replace-with-client-id>` | Store manager |
| Wildberries | `example_wb` | Example Wildberries organization | `<replace-me>` | Store manager |

Для WB токен хранится только в ignored `.env`, а имя переменной — в
`config/access.json → wildberries.api_token_env`. Production-клиент закреплён на официальных
доменах WB и разрешает только явно перечисленные пары `(HTTP-метод, путь, домен, квота)`.
Универсального proxy, пользовательского URL и методов создания, изменения или удаления
данных нет. Для дополнительной защиты токены WB следует выпускать с флагом `Read Only`;
серверный allowlist остаётся обязательным независимо от прав самого токена.

### Где именно enforced read-only

Allowlist проверяется не в слое инструментов, а внутри самих HTTP-клиентов — в
единственном месте, откуда запрос может покинуть процесс:

| Маркетплейс | Источник истины | Точка проверки | Ошибка при отказе |
| --- | --- | --- | --- |
| Ozon Seller | `ozon::READ_ONLY_ENDPOINT_ALLOWLIST` (34 production-пути) | `OzonClient::post` | `endpoint_not_allowed` |
| Ozon Performance | `ozon_performance::READ_ONLY_ENDPOINT_ALLOWLIST` (5 точных пар `method + path` и 2 строгих campaign-ID шаблона) | `PerformanceClient::request` | `endpoint_not_allowed` |
| Wildberries | `wb::READ_ONLY_ENDPOINT_ALLOWLIST` (точные `метод + путь + host + quota`) | `WbClient::request` | `endpoint_not_allowed` |

Поэтому новый вызов к маркетплейсу физически недостижим, пока его путь не добавлен в
allowlist явным коммитом: забытая проверка в слое инструментов больше не открывает
доступ к мутирующему методу. Проверка выполняется до чтения credentials, поэтому
отклонённый путь не может отличить настроенный кабинет от ненастроенного. Все marketplace-клиенты
дополнительно запрещают HTTP-редиректы и ambient proxy, так что ответ upstream или окружение
не могут увести запрос с закреплённого домена.

Текущий production allowlist Ozon Seller дополнительно включает следующие точные read-only
контракты:

- финансы: `POST /v1/finance/accrual/by-day`,
  `POST /v1/finance/accrual/postings`, `POST /v1/finance/accrual/types`,
  `POST /v1/finance/realization/by-day`,
  `POST /v1/finance/cash-flow-statement/list` и
  `POST /v1/finance/mutual-settlement`;
- отправления: stable-списки `POST /v3/posting/fbo/list` и
  `POST /v4/posting/fbs/list`, а также `POST /v4/posting/fbs/unfulfilled/list`,
  `POST /v2/posting/fbo/get`, `POST /v3/posting/fbs/get`,
  `POST /v1/posting/fbo/cancel-reason/list` и
  `POST /v2/posting/fbs/cancel-reason/list`; устаревшие списки
  `/v2/posting/fbo/list` и `/v3/posting/fbs/list` удалены из allowlist;
- склады и товары: `POST /v1/product/info/stocks-by-warehouse/fbo`,
  `POST /v2/product/info/stocks-by-warehouse/fbs`, `POST /v2/warehouse/list`,
  `POST /v3/product/list`, `POST /v3/product/info/list` и
  `POST /v4/product/info/attributes`; `ozon_warehouses` формирует запрос с bounded
  `limit`, а также необязательными `cursor` и `warehouse_ids`;
- отзывы: `POST /v2/review/list`; устаревший `/v1/review/list` удалён из allowlist.

Три finance-accrual метода завершили canary-проверку, включены как обычные stable tools и
больше не требуют preview-флага. Каждый Seller API `POST` выше только читает данные: произвольный
POST или путь по префиксу не разрешается.

Клиент Ozon Performance закреплён на `https://api-performance.ozon.ru` и выпускает наружу
ровно семь форм read-only бизнес-запросов:

- `GET /api/client/campaign`;
- `GET /api/client/statistics/daily/json`;
- `GET /api/client/statistics/expense/json`;
- `GET /api/client/limits/list`;
- `GET /api/client/campaign/{campaignId}/objects`;
- `GET /api/client/campaign/{campaignId}/v2/products`;
- `POST /api/client/statistics/products/sku`.

В двух динамических путях допускается только канонический положительный числовой
`campaignId` и точный суффикс. SKU statistics — ограниченный аналитический POST, а не
изменение кампании.

Внутренний `POST /api/client/token` используется только клиентом для получения OAuth token и
не является MCP tool. HTTP-метод сам по себе не считается доказательством безопасности:
из allowlist намеренно исключены мутирующие GET-пути
`/api/client/campaign/all_sku_promo/activate`,
`/api/client/campaign/all_sku_promo/deactivate` и
`/api/client/campaign/all_sku_promo/set_bid`. Любой неизвестный `method + path` отклоняется
до credentials и сети.

WB Promotion закреплён на `https://advert-api.wildberries.ru` и предоставляет только три
синхронных инструмента чтения:

- `GET /adv/v1/promotion/count` — список и статусы кампаний;
- `GET /api/advert/v2/adverts` — сведения о выбранных кампаниях;
- `GET /adv/v3/fullstats` — показы, клики, расходы и заказы за период.

Даже `GET` не считается автоматически безопасным: мутирующие пути запуска, паузы,
остановки, удаления кампаний и изменения ставок отсутствуют в allowlist и отклоняются
внутри WB-клиента до выбора token и сетевого запроса.

### Ограничения доступа и ресурсов

- Финансовые Ozon Seller tools и все рекламные Ozon Performance tools разрешены только ролям
  `finance` и `admin`; проверка выполняется до выбора credentials и сетевого запроса. Остальные
  роли по-прежнему ограничены закреплёнными кабинетами.
- Ответ маркетплейса маркируется как `untrusted_external_marketplace_data`. Очевидные поля
  телефона, e-mail, адреса, паспорта, покупателя и получателя маскируются до передачи модели.
  Текст отзывов и вопросов считается недоверенными данными, а не инструкциями.
- Неизвестные поля MCP input запрещены, строки/массивы/страницы ограничены схемой и повторной
  runtime-проверкой. Ответы после распаковки ограничены 2 МиБ, поэтому gzip/brotli не обходят
  лимит памяти; крупный период или страницу нужно разделять.
- Ozon имеет общий лимит 32 одновременных исходящих запросов и ограниченные retry/deadline.
  `/v1/analytics/data` дополнительно разделяет один защищённый 65-секундный departure gate по
  Client-Id; одинаковые успешные запросы объединяются и кэшируются на 5 минут. После Analytics 429
  включается адаптивный cooldown от 2 минут до 1 часа (с учётом безопасного `Retry-After`):
  интерактивный вызов завершается сразу, а атомарный сборщик отчёта делает не более двух повторов
  в очереди и не расходует на ожидание больше 10 минут; более длинный cooldown остаётся для
  следующего запуска. Ожидание не занимает сетевые permits. Сборщик ограничен десятью страницами
  (до 9 999 строк), чтобы не превращать дневной запуск в неограниченный backfill.
  Ожидание pacing и retry-backoff не удерживает сетевые permits, поэтому медленный кабинет не
  создаёт ложную перегрузку для остальных кабинетов.
  Ozon Performance дополнительно выдерживает минимум 1 секунду между отправками для одного
  Performance Client ID, допускает не более 2 запросов одновременно на Client ID и 8 глобально,
  кэширует OAuth token и после HTTP 401 выполняет только одно повторение с обновлённым token;
  ожидание OAuth refresh и pacing также не занимает permits исходящих бизнес-запросов.
  WB имеет общий лимит 8, общий deadline 60 секунд и quota, разделяемую alias-кабинетами с
  одинаковым token. Аналитические отчёты и остатки WB отправляются не чаще одного запроса
  за 20 секунд; `wb_orders` и `wb_sales` вместе — не чаще одного запроса в минуту;
  WB Promotion campaign endpoints разделяют интервал 200 мс, а полная рекламная статистика —
  отдельный интервал 20 секунд;
  `wb_ping` — не чаще одного за 10 секунд и только по явному пользовательскому вызову,
  не как мониторинг. Карточки и цены имеют независимые интервалы 600 мс; тарифы коробов,
  паллет и возвратов делят интервал 1 секунду, коэффициенты приёмки — 10 секунд, комиссии —
  60 секунд с быстрым локальным отказом повторного вызова вместо минутного зависания.
  Это намеренно консервативнее отдельных vendor burst-лимитов.
- Начиная с 30 марта 2026 года WB применяет разные лимиты по типу токена. Текущая версия работает
  только как собственная on-premise интеграция продавца с Personal read-only JWT token (`acc=3`).
  При загрузке credentials сервер локально классифицирует claim `acc` и останавливает запуск для
  Base (`1`), Test (`2`), Service (`4`), отсутствующего, строкового или неизвестного значения — до
  первого сетевого запроса и без вывода token/payload в ошибку. Это не проверка JWT-подписи:
  подлинность проверяет WB. Для Service/Base требуется отдельная схема с `X-Client-Secret` и
  проверкой привязки `asid`; текущая конфигурация её намеренно не имитирует.
- HTTP MCP разбирает и исполняет не более 32 non-GET запросов одновременно; 33-й запрос получает
  быстрый HTTP 503, при этом `/livez`, `/readyz` и совместимый `/health` остаются доступными. Ingress-слот освобождается сразу после
  построения ответа. Для result-bearing POST response body действует отдельный лимит 16: слот
  берётся до dispatch и удерживается до EOF или закрытия body. Валидные id-less JSON-RPC
  notifications (включая `notifications/initialized` и `notifications/cancelled`) и валидные
  клиентские responses/errors обходят только result-body лимит, поэтому непрочитанные ответы на
  служебные сообщения не блокируют отмену. Долгоживущие GET/SSE-потоки имеют отдельный лимит 64 и
  поэтому не занимают POST-слоты; при переполнении сервер возвращает HTTP 503 с `Retry-After: 1`.
  Входящий POST body полностью читается не дольше 10 секунд и ограничен 256 КиБ до
  JSON-десериализации; транспорт rmcp повторно применяет тот же лимит как defense in depth.
  До HTTP-разбора действует отдельный лимит 128 принятых TCP-соединений и 10-секундный deadline
  чтения заголовков первого и каждого keep-alive запроса. Внутренний plaintext listener принимает
  только HTTP/1.1: production TLS/HTTP/2 завершается на hardened reverse proxy с собственными
  connection/stream/header/idle limits, а proxy-to-application hop остаётся HTTP/1.1. Это не даёт
  idle HTTP/2 peers бессрочно занять все bounded connection slots приложения.
  Отдельно допускается не более 256 одновременно хранимых сессий. Этот лимит можно уменьшить через
  `MCP_MAX_SESSIONS`; значение должно быть положительным целым числом.
- Browser-запрос с заголовком `Origin` проходит только при точном совпадении origin защищённого
  JWT resource URL. В dev-режиме разрешены только loopback `localhost`, `127.0.0.1` и `::1` на
  любом локальном dev-порту; запросы без `Origin` остаются доступны CLI и server-to-server клиентам.
- Во всех MCP-сессиях суммарно исполняется не более 16 `tools/call`. Переполнение завершается
  быстрым `local_overloaded` до обращения к маркетплейсу; отменённый клиентом вызов освобождает
  локальные HTTP и semaphore-ресурсы, даже если уже доставленный read-only запрос может завершиться
  на стороне маркетплейса.
- После SIGTERM/Ctrl-C HTTP-сервер сразу перестаёт принимать новые соединения, до 55 секунд
  естественно завершает текущие запросы, затем отменяет MCP-сессии, потоки и вызовы и не позже
  65 секунд прекращает оставшиеся connection futures. Compose оставляет 70 секунд, включая
  5-секундный запас до принудительного завершения контейнера.
- Успешный ответ Ozon, Ozon Performance или WB ограничен 2 МиБ после распаковки; крупные периоды и
  страницы нужно разбивать на несколько запросов. Внутренний JSON структурированного MCP-результата
  ограничен 2 МиБ данных и 64 КиБ метаданных. Для совместимости он одновременно передаётся как
  `structuredContent` и текстовый JSON; сериализованный `CallToolResult` ограничен 6 МиБ + 64 КиБ.
- JWKS загружается без proxy/redirect, максимум 1 МиБ, 64 ключа и 16 КиБ на строковое поле;
  конкурентное обновление выполняется singleflight с negative cooldown.

## Запуск для ChatGPT

Рекомендуемый запуск — отдельным Docker-контейнером:

```bash
docker compose up -d --build
docker compose ps
```

В Docker Desktop контейнер отображается как `mcp-ozon-server` в группе
`mcp-ozon`. Он публикует порт только на loopback хоста и запускается без root,
Linux capabilities и права записи в файловую систему контейнера. Контейнер
подключён к отдельной outbound bridge-сети с отключённым inter-container
communication: исходящие запросы к Ozon разрешены, но соседние контейнеры не
получают прямой доступ к MCP listener. Владение Docker daemon остаётся
root-equivalent и не считается границей безопасности.

Production-образ собирается под `musl` в Alpine 3.23 и использует минимальный
Alpine runtime с `ca-certificates`. Бинарник MCP статически слинкован, а `curl`
внутрь образа не устанавливается: контейнерный healthcheck использует BusyBox
`wget`. Builder и runtime закреплены multi-arch digest, сборка проверяется на
`amd64` в CI и локально на `arm64`.

`config/access.json` подключается в контейнер read-only bind mount и перечитывается при каждом
MCP-вызове. Изменения магазинов, aliases, менеджеров и прав применяются без пересборки образа.
Изменения `.env`, включая API-ключи, требуют перезапуска контейнера; сам файл не попадает в
Docker build context и не копируется в образ.

Основной контейнер из `compose.yaml` на порту `8787` явно фиксирует legacy-ключи
`OZON_POSTINGS_VNEXT=false` и `OZON_FINANCE_ACCRUALS_PREVIEW=false`. Оба ключа
принимаются для совместимости, но игнорируются: списки отправлений v3 FBO/v4 FBS и
finance-accrual tools являются stable. Сохраняйте оба значения `false` во всех окружениях.

Изолированный `compose.canary.yaml` на порту `8789` публикует тот же stable read-only
router и также фиксирует оба legacy-ключа в `false`. Canary запускается исключительно вручную,
имеет `restart: "no"` и не должен добавляться в LaunchAgent, автозагрузку, CI или
Compose Watch. Даже ручной canary запускается с ограничениями CPU, памяти, PID и
размера логов, а также с окном graceful shutdown. Рабочий Secure MCP Tunnel при этой проверке
остаётся подключённым к `8787`; не переключайте его на `8789`.

Точный запуск canary того же CI-проверенного SHA без остановки рабочего `8787`
и Tunnel:

```bash
MCP_RELEASE_EVIDENCE="$release_dir/release.json" \
MCP_RELEASE_IMAGE_LOCK="$release_dir/release-images.json" \
  ./scripts/canary-up.sh
curl -fsS http://127.0.0.1:8789/readyz
./scripts/canary-down.sh
```

`canary-up.sh` создаёт изолированный реестр и размещает его временную копию в
защищённом каталоге `/private/tmp` на macOS (или `/tmp` на Linux). Это обходит
ограничение Docker Desktop на bind mount из `~/Documents`; путь передаётся в
Compose через `MCP_CANARY_ACCESS_CONFIG`. `canary-down.sh` останавливает только
canary и удаляет эту временную копию.

Точный откат canary, также без изменения рабочего контейнера и Tunnel:

```bash
docker compose -f compose.canary.yaml down
```

Для локальной разработки без Docker сервер также можно запустить напрямую.
По умолчанию он использует Streamable HTTP:

```bash
cargo run --release
```

Локальные адреса:

```text
MCP:       http://127.0.0.1:8787/mcp
Liveness:  http://127.0.0.1:8787/livez
Readiness: http://127.0.0.1:8787/readyz
Metrics:   http://127.0.0.1:8787/metrics
```

ChatGPT должен видеть MCP через публичный HTTPS endpoint либо Secure MCP Tunnel. Для локального
сервера рекомендуется Secure MCP Tunnel: он позволяет не публиковать ключи и сам MCP в интернете.

В ChatGPT включите Developer mode, создайте Plugin/MCP connection и выберите Tunnel. Если вместо
туннеля используется HTTPS reverse proxy, укажите полный URL, включая `/mcp`.

В настройках OzonOFK установите `Permissions → Always ask`. Ручной выбор приложения в чате
контролируется самим ChatGPT и не передаётся MCP-серверу как проверяемый признак. Поэтому сервер
также работает по принципу fail-closed: при ошибке Ozon API он возвращает терминальную ошибку и
запрещает автоматически обходить её вызовом другого магазина или другого Ozon-инструмента.
Без успешного результата OzonOFK модель не должна утверждать, что получила данные напрямую.

Официальная инструкция OpenAI:
<https://developers.openai.com/plugins/deploy/connect-chatgpt>

## JWT/OIDC для общего deployment

Локальный single-user Tunnel по умолчанию работает с `MCP_AUTH_MODE=dev`. Для общего или
публично доступного MCP используйте `MCP_AUTH_MODE=jwt` и внешний OIDC-провайдер с публичными
HTTPS issuer/JWKS endpoints. Репозиторий не разворачивает собственный identity provider.

Задайте согласованные значения `MCP_JWT_ISSUER`, `MCP_JWT_AUDIENCE`,
`MCP_JWT_REQUIRED_SCOPES`, `MCP_JWT_JWKS_URL` и `MCP_PUBLIC_URL`; пример находится в
`.env.example`. OIDC identity пользователя сопоставляется с полем `oidc` в
`config/access.json` строго по `oidc.subject`; subject нужно получить у настроенного IdP, а не
придумывать вручную. Изменение JWT-relevant registry на subjectless-конфигурацию отклоняется и
переводит readiness в fail-closed состояние.

Без access token доступны только `/livez`, `/readyz`, совместимый `/health`,
безлейбловый `/metrics` и OAuth protected-resource metadata. Каждый запрос
к `/mcp`, включая `initialize`, notifications, `tools/list`, `tools/call`, GET/SSE и DELETE,
должен передавать Bearer token. Сервер принимает только `RS256`, проверяет `iss`, точный `aud`,
обязательные scopes, `exp`/`nbf` и зарегистрированную identity. JWKS загружается напрямую без
ambient proxy и redirect и кэшируется с ограниченным TTL.
Legacy MCP session ID дополнительно привязан к immutable OIDC `sub`, который выполнил
`initialize`; другой аутентифицированный subject получает тот же `404`, что и для неизвестной
сессии, на POST, GET и DELETE. JWT и текущие роли/доступы всё равно проверяются заново на каждом
запросе и в session не кэшируются.

OAuth-клиент для ChatGPT должен использовать Authorization Code + PKCE S256 и точный callback
вида `https://chatgpt.com/connector/oauth/{callback_id}`. Перед production-развёртыванием
проверьте полный OAuth flow с одноразовой тестовой identity, точным resource audience и
обязательным scope `mcp:tools`.

## Локальная проверка

```bash
npx @modelcontextprotocol/inspector@latest
```

Подключите Inspector к `http://127.0.0.1:8787/mcp` и проверьте `tools/list`.

Для stdio-режима, например при локальной отладке или подключении к Claude Desktop:

```bash
MCP_TRANSPORT=stdio cargo run --release
```

## Read-only инструменты

Production router публикует 76 stable tools:

- `marketplace_accounts`
- `list_members`
- `ozon_stores_status`
- `ozon_analytics`
- `ozon_product_stocks`
- `ozon_warehouse_stocks`
- `ozon_fbo_stocks_by_warehouse`
- `ozon_fbs_stocks_by_warehouse`
- `ozon_warehouses`
- `ozon_product_prices`
- `ozon_live_buyer_prices`
- `ozon_stock_turnover`
- `ozon_products`
- `ozon_product_info`
- `ozon_product_attributes`
- `ozon_supply_order_list`
- `ozon_supply_order_get`
- `ozon_fbs_postings`
- `ozon_fbo_postings`
- `ozon_posting_sales_fallback` — полностью пагинирует оба источника и возвращает отдельную
  операционную метрику `non_cancelled_posting_units`; это не Seller Analytics `ordered_units`,
  GMV недоступен, отменённые единицы считаются отдельно
- `ozon_fbs_unfulfilled`
- `ozon_fbo_posting`
- `ozon_fbs_posting`
- `ozon_fbo_cancel_reasons`
- `ozon_fbs_cancel_reasons`
- `ozon_returns`
- `ozon_rfbs_returns`
- `ozon_finance_transactions`
- `ozon_finance_totals`
- `ozon_finance_accrual_postings`
- `ozon_finance_accrual_types`
- `ozon_finance_accrual_by_day`
- `ozon_finance_realization_by_day`
- `ozon_finance_cash_flow`
- `ozon_finance_mutual_settlement`
- `ozon_performance_campaigns`
- `ozon_performance_daily`
- `ozon_performance_expenses`
- `ozon_performance_limits`
- `ozon_performance_campaign_objects`
- `ozon_performance_campaign_products`
- `ozon_performance_sku_statistics`
- `ozon_seller_rating`
- `ozon_seller_rating_history`
- `ozon_reviews`
- `ozon_questions`
- `wb_stores_status`
- `wb_ping`
- `wb_product_cards`
- `wb_product_prices`
- `wb_promotion_campaigns`
- `wb_promotion_campaign_details`
- `wb_promotion_stats`
- `wb_search_product_queries`
- `wb_search_orders_positions`
- `wb_promotion_minimum_bids`
- `wb_promotion_recommended_bids`
- `wb_promotion_search_cluster_bids`
- `wb_sales_funnel`
- `wb_sales_funnel_history`
- `wb_sales_funnel_grouped_history`
- `wb_warehouse_stocks`
- `wb_seller_warehouses`
- `wb_seller_warehouse_stocks`
- `wb_orders`
- `wb_sales`
- `wb_tariff_commissions`
- `wb_tariff_boxes`
- `wb_tariff_pallets`
- `wb_tariff_returns`
- `wb_acceptance_coefficients`

### Остатки Wildberries: FBW и FBS

`wb_warehouse_stocks` получает **текущие FBW-остатки на складах WB**
(аналог FBO). Его страницы `limit/offset` не содержат FBS-остатков.

Для **текущих остатков на складах продавца / FBS**:

1. Разрешите кабинет через `marketplace_accounts` / `wb_stores_status`.
2. Вызовите `wb_seller_warehouses(account)`: это полный список без пагинации.
   Для FBS выбирайте `deliveryType=1`; другие модели доставки сохраняйте
   отдельно. Не скрывайте склады в переходном состоянии без пояснения.
3. Пройдите все cursor-страницы `wb_product_cards`, без фильтра фотографий:
   `with_photo=-1`. Продолжайте с парой `cursor_updated_at/cursor_nm_id` до
   терминальной страницы. Повтор курсора или ошибка блокируют полный итог.
4. Сопоставьте `sizes[].chrtID` с `nmID`, артикулом, размером и баркодами.
   Один размер может иметь несколько баркодов: не размножайте его остаток.
5. Для **каждого** выбранного склада запросите все уникальные `chrtID` пакетами
   до 1000 через `wb_seller_warehouse_stocks(account, warehouse_id, chrt_ids)`.
   Здесь нет `offset`, даты или автоматического обхода остальных складов.
6. Проверяйте `missing_chrt_ids` и `complete_for_requested_ids` каждого пакета.
   Отсутствующая строка означает неизвестный остаток. Настоящий `amount=0`
   сохраняется; отсутствующие строки нулями не дополняются. Дубли, посторонние
   ID и некорректные количества приводят к ошибке. Промежуточную выгрузку не
   называйте полной. Итог отражает период обхода, а не единый момент времени.

Методы используют только `GET /api/v3/warehouses` и
`POST /api/v3/stocks/{warehouseId}` на фиксированном Marketplace API host;
PUT/DELETE и создание складов запрещены. Используется действующий read-only
токен кабинета с доступом к этим методам; при 403 нужны корректные права
чтения, а не подмена результата FBW. На токен действует общий интервал 250 мс
для этих двух чтений; 429 не вызывает скрытый повтор или обходной запрос.
`Retry-After` распространяется на параллельные чтения этого токена (до суток);
без корректного заголовка 429 задаёт паузу 60 секунд. Ответ 409 учитывается
как десять запросов, как требует WB.
Контракт основан на [официальной документации WB](https://dev.wildberries.ru/docs/openapi/work-with-products)
и [объяснении складов продавца и chrtIds](https://dev.wildberries.ru/en/news/101).

`fetched_at` и `observation_kind=current` описывают текущее чтение. Добавление
методов **не восстанавливает остатки за 7 сентября или другую прошлую дату**.
Для этого нужен сохранённый полный снимок именно FBS. Существующий WB-сборщик
сохраняет FBW и не является историей FBS; его автосбор эта доработка не меняет.

Схема публикуется самим MCP через `tools/list`, версия сервера — `0.2.1`,
реестр — 86 инструментов. После выкладки этой версии обновите инструменты
подключения OFK MARKET; если клиент сохранил старую схему, переподключите его
и проверьте наличие обоих `wb_seller_*` методов. Локальный пакет
`ozonofk-suite` содержит навыки, а не копию схемы коннектора: повышение его
версии само по себе новые серверные методы не устанавливает.

Ozon tools являются необязательными чтениями относительно тарифа кабинета. Например,
`ozon_finance_realization_by_day` может требовать Ozon Plus/Pro, а API отзывов — отдельный
платный доступ. Если подписка конкретного магазина не даёт право на данные, tool возвращает
безопасную upstream-ошибку и не подменяет результат данными другого магазина или метода.

## Проверки проекта

```bash
./scripts/local-ci.sh
```

Скрипт проверяет форматирование, все тесты и targets/features, строгий Clippy,
документацию без предупреждений, RustSec, лицензии/источники зависимостей и
ноль непокрытых адресуемых source lines библиотечного ядра по строгому
`cargo llvm-cov --fail-uncovered-lines 0`. Операционные `main.rs` и `src/bin/*`
компилируются через отдельные `--all-targets --all-features` quality-gates, но
честно исключены из метрики core coverage: для coverage отключается служебная
feature `runtime-binaries`, которая управляет только наличием binary targets и
не меняет библиотечное ядро. Нужные локальные инструменты:

```bash
rustup component add clippy rustfmt llvm-tools-preview
cargo install cargo-audit --version 0.22.2 --locked
cargo install cargo-deny --version 0.19.9 --locked
cargo install cargo-llvm-cov --version 0.8.7 --locked
```

## CI и защита Pull Request

GitHub Actions запускает три workflow:

- `Rust CI`: форматирование, тесты, строгий Clippy, rustdoc, Rust 1.98.0,
  ноль непокрытых source lines library core, RustSec/cargo-deny, поиск секретов и ошибок конфигурации,
  сборка и Trivy-сканирование Docker-образа, проверка hardened-запуска;
- `CodeQL`: расширенный статический анализ Rust и самих GitHub Actions;
- `Dependency Review`: блокировка новых уязвимых или запрещённых зависимостей в PR.

Actions закреплены полными commit SHA, runner закреплён на Ubuntu 24.04, базовые
Docker-образы — digest. Dependabot еженедельно предлагает обновления Cargo,
Actions и Docker. Еженедельный запуск `Rust CI` повторно проверяет уже принятую
ветку по актуальным базам уязвимостей.

После публикации репозитория в GitHub включите для `main`/`master` ruleset:

1. Require a pull request, минимум один approval, dismiss stale approvals и
   require conversation resolution.
2. Require branch to be up to date и следующие status checks: `Quality`,
   `Rust 1.98.0`, `Core library source lines`, `Dependency security`, `Hardened container`,
   `Analyze (rust)`, `Analyze (actions)`,
   `Dependency review`.
3. Запретите force-push и удаление защищённой ветки.

В `Settings → Code security` включите Dependency graph, Dependabot alerts,
Dependabot security updates, Secret scanning и Push protection. Для CodeQL
используется уже добавленный advanced-setup workflow — второй раз включать
default setup не нужно. В приватном репозитории CodeQL и Dependency Review
требуют доступной для организации лицензии GitHub Code Security/Advanced
Security; для публичного репозитория они доступны без неё.

В `Settings → Actions → General` оставьте `GITHUB_TOKEN` только read-only по
умолчанию и запретите Actions создавать или одобрять pull request. Для защиты
самих workflow добавьте `.github/CODEOWNERS`, когда будет известен GitHub-логин
владельца или команды ИБ.

## Обновление контекста проекта ChatGPT

Чтобы продолжить работу с актуальным кодом в браузерном проекте ChatGPT,
создайте единый безопасный снимок:

```bash
./scripts/export-chatgpt-context.sh
```

Файл появится в `target/chatgpt/MCP_OZON_PROJECT_CONTEXT.md`. Загрузите его в
Project files вместо предыдущего снимка. Экспорт содержит Rust-код, CI и
документацию, но намеренно не включает `.env`, `.sonar.env`, API-ключи,
Git-историю, IDE-настройки и build-артефакты.

## Отчёты для SonarQube

SonarQube для этого репозитория полностью изолирован в Compose-проекте
`mcp-ozon-sonar`. Он не использует контейнеры, конфигурацию или volumes других
проектов. При первом запуске скрипт создаёт исключённый из Git файл
`.sonar-stack.env` со случайными паролями PostgreSQL и локального администратора,
меняет стандартный пароль `admin` и создаёт отдельный analysis token в
`.sonar.env`. Секреты не выводятся в терминал и имеют права `0600`.

Запустите локальный SonarQube и дождитесь статуса `UP`:

```bash
./scripts/sonar-up.sh
```

Затем создайте отчёты о выполнении тестов, покрытии и Clippy:

```bash
./scripts/sonar-reports.sh
```

Готовые отчёты находятся в `target/sonar`. SonarQube импортирует количество
запущенных тестов из `test-executions.xml` и покрытие кода из `lcov.info`.

После подготовки отчётов запустите сканирование; проект с ключом `mcp-ozon`
создастся автоматически при первом анализе:

```bash
./scripts/sonar-scan.sh
```

Скрипт автоматически поднимает именно Compose из `compose.sonar.yaml`, проверяет
или обновляет локальный analysis token и ожидает Quality Gate. Временный
контейнер сканера удаляется после анализа.

Остановка локального стека без удаления данных:

```bash
docker compose --env-file .sonar-stack.env -f compose.sonar.yaml stop
```
