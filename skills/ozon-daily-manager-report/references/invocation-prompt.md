# Invocation prompts

## Manual report

```text
Используй $ozon-daily-manager-report и подготовь evidence-locked ежедневный
отчет Ozon за вчера по всем доступным мне кабинетам. Часовой пояс —
Asia/Yekaterinburg. Сравни подтвержденные KPI с предыдущим днем, тем же днем
недели и средним за семь полных предыдущих дней. Соблюдай текущую роль и RBAC,
проверяй status + completeness для каждого кабинета, не заменяй N/D нулем и
выдавай только серверные рекомендации, разрешенные для точного cutoff. Нужен
короткий управленческий отчет и план действий на сегодня, прошедшие QA LOOP.
```

## Scheduled morning companion

Recommended schedule: every day at 08:35 in `Asia/Yekaterinburg`, after the
08:00–08:30 server collection window.

```text
Ежедневный контроль Ozon OFK. Используй $ozon-daily-manager-report и подготовь
утренний отчет за завершенный вчерашний день. Сначала определи текущую роль и
доступные Ozon-кабинеты. Для каждого кабинета проверь collection status и data
completeness. Используй только сопоставимые утренние cutoff, отделяй факты от
гипотез, не подменяй N/D нулем и не выдавай торговые рекомендации без
recommendations_allowed=true и результата серверного manager actions. Если
skill или OzonOFK недоступен, не восстанавливай цифры по памяти: сообщи о
недоступности и сформируй только диагностическую задачу. Ответ — до 500 слов,
с точными датами и финальной строкой QA LOOP.
```

The scheduled companion is not proof of server-side email delivery. Only the
report outbox and provider audit establish that a manager email was sent.

## Explicit current-day refresh

```text
Используй $ozon-daily-manager-report и запроси одно актуальное обновление Ozon
для доступного мне кабинета. Не вызывай прямую live-аналитику и не опрашивай
статус циклом. Если обновление уже завершилось, прочитай опубликованный снимок
через ofk_ozon_sales_analytics и укажи точный snapshot_cutoff_at. Если оно в
очереди или выполняется, верни request ID и состояние; не выдавай предыдущий
снимок за текущий. При ошибке сохрани последний успешный снимок и явно укажи,
что новое обновление не опубликовано.
```
