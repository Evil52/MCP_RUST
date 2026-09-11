# Разовый запуск Nexus через выделенный WB writer

Это локальный operator CLI в существующем `wb-automation`, **не** финансовый
инструмент аналитического MCP. Он ограничен `ofk_region_wb`, названием `Nexus`,
пятью согласованными nm_id и одним переводом **1000 ₽ / type=1**.

Контракт проверен по [документации WB](https://dev.wildberries.ru/openapi/promotion):
`POST /adv/v2/seacat/save-ad`, `PATCH /api/advert/v1/bids`,
`POST /adv/v1/budget/deposit?id=...`, `GET /adv/v0/start?id=...`.
Ставки — в копейках, сумма deposit — в рублях. `type=1` — «Баланс» WB
(взаиморасчёты из продаж); это не `type=0` «Счёт» и не бонусы `type=3`.
Полей cashback в запросе нет. Внешние платежи и автопополнение не реализованы.

## До исполнения

1. Получить точные начальные ставки новой пятёрки. Разные ставки исходной
   кампании не задают однозначного соответствия новым товарам. Не считать
   пример 922 коп. подтверждением пользователя.
2. Проверить сборку, тесты и runtime artifact обычным процессом проекта.
   Наличие кода в checkout не обновляет production и не запускает кампанию.
3. Подготовить приватный JSON-манифест (0600) и устойчивый каталог журнала
   (0700). Один и тот же каталог должен монтироваться во все operator-запуски.
   Не использовать `/tmp` для журнала реальных финансовых операций.
4. Использовать существующие account-bound reader/writer credentials и
   изолированные egress proxies. Writer должен быть Personal production,
   promotion-only read/write, с тем же seller SID, что и reviewed registry.
   Не менять allowlist прокси, не использовать аналитический токен для записи.

Манифест содержит следующие поля; пути примерные, токены сюда не вставляются:

```json
{
  "scope": "fund_and_start",
  "account_id": "ofk_region_wb",
  "campaign_name": "Nexus",
  "source_policy": "/etc/wb-automation/source-policy.json",
  "source_policy_sha256": "SHA256_OF_CANONICAL_RUST_SERIALIZED_POLICY",
  "bids_kopecks": {
    "146312604": 922,
    "207418966": 922,
    "455101276": 922,
    "461126890": 922,
    "529996417": 922
  },
  "budget_rubles": 1000,
  "funding_type": 1,
  "actor_id": "rustam_magasumov",
  "authorization_reference": "REPLACE_WITH_EXACT_USER_AUTHORIZATION",
  "authorized_at": "2026-09-10T12:00:00Z",
  "expires_at": "2026-09-11T12:00:00Z",
  "registry": "/etc/wb-automation/access.json",
  "reader_token": "/etc/wb-automation/reader.token",
  "writer_token": "/etc/wb-automation/writer.token",
  "reader_proxy": "http://ozon-egress:3128",
  "writer_proxy": "http://write-egress:3130",
  "allow_broad_reader": true,
  "journal_directory": "/var/lib/wb-campaign-launch",
  "robot_policy": "/etc/wb-automation/nexus-policy.json"
}
```

Хеш source policy берётся из текущего снимка существующего observer:
`policy_sha256`. Манифест живёт максимум 24 часа. Повторный запуск с другим
манифестом в том же журнале блокируется, а не выдаёт новую попытку пополнения.
Для финансовых команд требуется admin в локальном reviewed access registry.

## Команды

Для нового разрешения **только создать, пользователь пополняет сам** задайте
`scope: "create_only"` и `budget_rubles: 0`. Такой манифест разрешает только
`preflight/create/bids/reconcile`, не запрашивает баланс и запрещает `fund/start`
до любых записей. Это не отключает защитные проверки финансового сценария:
он остаётся отдельным разрешением `fund_and_start` на ровно 1000 ₽.

Внутри штатного изолированного runtime с устойчивым журналом:

```sh
wb-automation campaign-launch preflight /etc/wb-automation/nexus-launch.json
wb-automation campaign-launch create /etc/wb-automation/nexus-launch.json
wb-automation campaign-launch bids /etc/wb-automation/nexus-launch.json
wb-automation campaign-launch fund /etc/wb-automation/nexus-launch.json
wb-automation campaign-launch reconcile /etc/wb-automation/nexus-launch.json
```

`preflight` только читает WB: source campaign, пересечения с незавершёнными
кампаниями, запас WB не менее 20 шт. по каждому SKU, доступный `balance`.
`create` оставляет кампанию в статусе 4/11 и сохраняет подтверждённый ID.
`bids` проверяет минимумы WB и перечитывает точные ставки.
`fund` заново проверяет состояние, требует бюджет 0 ₽, выполняет один
POST и подтверждает `total=1000` ответом WB и отдельным GET бюджета.

Все write-стадии сохраняют `*-attempted.json` через `create_new`, `fsync(file)`
и `fsync(directory)` **до** сетевого запроса. Повтор этапа запрещён даже при
ошибке HTTP, таймауте, падении процесса или частичной записи журнала.
Одновременное исполнение исключает OS file lock. `reconcile` не пишет ни WB,
ни журнал и доступен после истечения авторизации. При неизвестном ID после
create требуется сверка списка кампаний; автоматического повторного create нет.

## Защитный робот и старт

Из `policy-receipt.json` устанавливается отдельная политика **Nexus**; все
торговые защитные параметры копируются из проверенной текущей политики
«Одуванчика»: cap 500 ₽, pause 450 ₽, target ДРР 15%, autotopup off и остальные
пороговые настройки без ослабления. Исходный робот не меняется.

Установить отдельный периодический runner обычным immutable-release процессом
проекта, seed/activate PostgreSQL state обычным guarded workflow. Этот CLI
не устанавливает LaunchAgent, не импортирует/обнуляет состояние робота и не
снимает incident locks. До старта нужны **два** записанных в PostgreSQL цикла
с интервалом 240–420 секунд; последний не старше 90 секунд.

```sh
wb-automation campaign-launch start /etc/wb-automation/nexus-launch.json
wb-automation campaign-launch reconcile /etc/wb-automation/nexus-launch.json
```

Для `start` требуется существующий `WB_AUTOMATION_DATABASE_URL`, точная
установленная политика, campaign advisory lease, отсутствие pending/incident/
protective pause, свежий полный guard snapshot, бюджет 1000 ₽ и нулевой расход.
Если WB не даёт полную стартовую статистику новой кампании, старт блокируется;
не подменять отсутствие данных нулями и не обходить эту проверку.

После HTTP start статус 9, товары и ставки проверяются независимо. Затем
обязательно проверить **следующий** реальный периодический цикл Nexus:
сам факт start/readback не является подтверждением работы робота после старта.

## Границы первого rollout

Реальная кампания/перевод не выполняются тестами. Для production нужны
подтверждённые начальные ставки, отдельный установленный runner и проверенный
runtime artifact. CLI не добавляет широких финансовых полномочий MCP и не
может пополнять существующий «Одуванчик», другие кабинеты или другие кампании.
