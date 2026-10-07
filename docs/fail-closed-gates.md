# Fail-closed gates: симптомы и разблокировка

Система намеренно останавливает работу, когда не может доказать безопасность
следующего шага. Ниже перечислены все такие точки: по какому признаку их
видно, что проверить и как снять блокировку. Ни одна из них не снимается
перезапуском; если перезапуск «помог», причина просто исчезла сама.

| Gate | Признак | Что сделать |
| --- | --- | --- |
| Журнал вызовов MCP недоступен | каждый инструмент отвечает `telemetry_unavailable`; в логе `tool call refused because telemetry could not start` | восстановить PostgreSQL или сессии роли `report_refresh_requester` (см. ниже лимит подключений); данные не правятся |
| Общая квота маркетплейса недоступна | ошибки `SharedQuota(Unavailable)` / `local_overloaded` при `MCP_MARKETPLACE_QUOTA_REQUIRED=true` | проверить доступность БД и роль из `MCP_MARKETPLACE_QUOTA_DATABASE_URL`; локального обхода нет |
| Длинный cooldown вендора | находка `check-runtime-health.sh`: `marketplace quota has N cooldown(s) over 1h`; метрика `mcp_marketplace_quota_capped_cooldowns_total` > 0, если задержка была больше суток; в логе `vendor cooldown exceeds the shared quota ceiling` | выяснить, какой endpoint вернул длинный `Retry-After`. Сохраняемая задержка теперь не больше 24 ч |
| Старые «бесконечные» cooldown | находка `… one without an end`: строки `marketplace_quota.departures` с `next_allowed_at = 'infinity'`, записанные до ограничения в 24 ч | после проверки вендора администратор выполняет `UPDATE marketplace_quota.departures SET next_allowed_at = now() WHERE next_allowed_at = 'infinity'` |
| Неподтверждённая отправка письма | `report-worker` не стартует: `scheduled mail activation refused …`; строка `delivery_batches.status = 'sending'` | проверить Gmail и выполнить `report-worker reconcile-sent <audience> <batch> <attempt> <message-id> --confirm-gmail-sent` либо `reconcile-suppress … --confirm-provider-outcome-unknown`, затем `deliver-one` в режиме `delivery_canary` |
| Нет успешной отправки за 24 ч | тот же отказ запуска без строки `sending` | выполнить канареечный `deliver-one` после устранения причины (OAuth, маршрутизация) |
| Инцидент WB-робота | `IncidentLocked`; `wb_automation.execution_state.incident_class` не пуст | разбор по документам `wb-*-incident-recovery.md`; автоматический сброс не предусмотрен |
| Привязки ключей в реестре изменились | `MCP_ACCESS_CONFIG_RESTART_REQUIRED` | перезапустить MCP, чтобы ключи и реестр перечитались атомарно |
| Недопустимый id в реестре | запуск или перезагрузка реестра падает с `идентификатор actor/кабинета … должен содержать …` | использовать id из `[A-Za-z0-9_-]` (кабинет) и `[A-Za-z0-9._:@-]` (actor), до 128 байт |
| JWKS недоступен дольше TTL (JWT) | HTTP 503 `VerifierUnavailable` | восстановить доступ к IdP; устаревшие ключи намеренно не используются |
| Прерванная миграция | мигратор: `migration ledger mismatch or interrupted migration` | восстановить из резервной копии или вручную завершить миграцию по журналу; повторный запуск без анализа запрещён |
| Лимит подключений роли | в логах PostgreSQL `too many connections for role` | лимиты: `report_refresh_requester` 12, `report_collector` 8, `position_reader` 16; проверить `pg_stat_activity` на зависшие сессии |

## Плановые остановки

`report-worker` после SIGTERM больше не берёт новые письма, но доводит текущую
попытку до записанного результата (до 60 секунд). Поэтому `stop_grace_period`
воркера равен 75 секундам; уменьшать его нельзя, иначе Docker прервёт отправку
и строка останется в `sending`.

## Секреты аналитического сервера

Read-only сервер не использует write-токены WB. При старте он отказывается
работать, если реестр привязывает переменную с `WRITE` в имени, и пишет
предупреждение со списком непустых `*WRITE_TOKEN*` в окружении. Отдельный
env-файл без write-токенов задаётся через `MCP_ENV_FILE`; обнуление токенов
в `compose.yaml` остаётся только запасной мерой.
