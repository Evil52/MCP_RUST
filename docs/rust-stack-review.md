# Rust stack review — 2026-09-09

Основа: исходники локального `MCP_OZON`, исходная ревизия `be3b0f2` и изменения
ветки `refactor/rust-stack-boundaries`. Это review кода и build-контрактов,
не аудит работающего production и не подтверждение полноты отчётов.

## Вывод

Rust и текущие основные библиотеки соответствуют задаче: долгоживущие сервисы,
строгие права доступа, ограниченные внешние запросы, воспроизводимые snapshots
и осторожное выполнение Control-команд. Главный долг — размер границ
сопровождения и собственные safety-механизмы, а не отсутствие фреймворка.
Переписывать систему или добавлять пул БД без измерений оснований нет.

Ниже «почему подходит» — инженерное обоснование по реализованным требованиям,
а не утверждение, что именно так была задокументирована история каждого выбора.

## Какие технологии и для чего

| Технология | Что реализовано | Почему подходит / цена выбора |
| --- | --- | --- |
| Rust, Cargo, workspace | Типизированные конфигурации, ошибки, состояния reporting/Control; семь binaries | Проверяемые компилятором границы и владение ресурсами. Не доказывает корректность бизнес-правил; сборка и рефакторинг требуют дисциплины |
| Tokio, tokio-util | Параллельные I/O, semaphore/mutex, deadlines, отмена и shutdown | Один асинхронный runtime для HTTP, фоновых workers и PostgreSQL. Блокирующая работа и удержание guards всё равно требуют review |
| Axum, Hyper, Tower | HTTP ingress, middleware и transport tests | Компонуемые HTTP-границы. Собственные admission/session/shutdown механизмы остаются ответственностью проекта |
| rmcp, локальный fork | MCP tools, схемы, Streamable HTTP и sessions | Готовый протокол плюс необходимые ограничения. Каждый upgrade требует переноса и проверки локальных патчей |
| Serde, serde_json, Schemars | API DTO, JSON и машинные схемы инструментов | Один тип связан с сериализацией и схемой; предметная валидация всё равно нужна отдельно |
| Reqwest, rustls | Ozon/WB HTTP-клиенты, HTTPS, deadlines, безопасные ответы | Общий HTTP/TLS клиент. Read-only endpoint allowlists, pacing и retry policy реализованы проектом, не библиотекой автоматически |
| jsonwebtoken, JWT/JWKS | Проверка идентичности и ключей, привязка доступа к кабинету | Подписанный identity отделён от серверной авторизации; JWKS cache и правила обновления требуют самостоятельных тестов |
| PostgreSQL, tokio-postgres | Согласованные snapshots, least-privilege views/roles, outbox, транзакции и audit | БД обеспечивает ограничения совместно с Rust. Одна сессия сериализует доступ; `NoTls` требует доверенной сетевой границы |
| rust_xlsxwriter | Формирование XLSX-артефактов отчётов | Доставка воспроизводимого файла из серверных фактов без обязательного внешнего табличного сервиса |
| tracing, tracing-subscriber | Структурированные runtime события и диагностика | Диагностика I/O и safety-решений; логи сами по себе не заменяют метрики и end-to-end проверки |
| Docker, Compose, macOS LaunchAgents | Изоляция сервисов, повторяемый запуск и supervision | Соответствует текущему Mac/Docker размещению. Не означает автоматическую отказоустойчивость нескольких хостов |
| GitHub Actions, Clippy, llvm-cov, audit/deny | Проверки Rust, тесты, coverage, зависимости и release gates | Воспроизводимая проверка изменений; результат CI не является доказательством успешного production-сбора |

Версии закреплены в [Cargo.lock](../Cargo.lock); набор прямых зависимостей —
в [Cargo.toml](../Cargo.toml). Read-only Analytics и отдельный Control остаются
разными capability-границами; в этом изменении права и операции не расширяются.

## Риски, исправления и проверка

Приоритеты: P2 — значимый долг сопровождения/проверяемости; P3 — последующий
инфраструктурный шаг. Подтверждённой новой P0/P1-уязвимости этот review не
устанавливает; это не результат полного security-аудита.

| Приоритет | Наблюдение и эффект | Изменение / остаток | Как проверять |
| --- | --- | --- | --- |
| P2 | README заявлял ноль непокрытых строк и отключение binaries в coverage, но CI использует 95.8% строк / 95.5% функций и all-features | README приведён к реально настроенному контракту; gates не ослаблены | Сопоставление README, CI и local-ci; полный coverage run |
| P2 | Крупные production-модули увеличивают объём совместного review | MCP разделён на contracts/validation/normalization и восемь router groups; выделены части WB, reporting read model и executor | Существующие schemas/tools/wire/transaction tests и строгий Clippy |
| P2 | Почти всё в одном crate, изменения имеют широкую compilation boundary | Вынесен независимый `mcp-storage`; остальная декомпозиция поэтапная, ещё не завершена | Workspace tests/doc/coverage, общие lints, Docker-builder contract |
| P2 | Критерий удаления fork в UPSTREAM.md учитывал только typed field, хотя патчей больше | ADR 0003 фиксирует все обязательства и upgrade/removal checklist; проверенных upstream issue/PR links пока нет | Focused transport regressions плюс diff при каждом SDK upgrade |
| P2 | `NoTls` допускает опасную ошибку при переносе БД за доверенный локальный периметр | ADR 0004 запрещает считать remote plaintext поддерживаемой топологией; TLS connector и машинная проверка топологии ещё не реализованы | Отдельный negative-test набор и защищённый transport до remote migration |
| P3 | Возможное ожидание одной PostgreSQL-сессии не было измеримо через текущие HTTP metrics | Добавлены process totals и per-client snapshot: wait, hold, cancellations; пул не добавлен | Cancellation/contended-session tests, `/metrics` без БД; затем наблюдение под реальной нагрузкой |
| P2 | Собственные HTTP/JWKS/retry/supervision механизмы требуют постоянного внимания | Сохранены существующие ограничения и тесты; введён file-growth gate, но он не заменяет review сложности | Новые файлы <= 1,000 строк; явные сокращаемые legacy budgets, без исключения тестов |

## Что изменилось в размерах

Физические строки, включая imports, комментарии и тесты. Код перенесён в
ответственные модули, а не удалён. Суммарный объём проекта не стал меньше.

| Исходный файл | До | После | Примечание |
| --- | ---: | ---: | --- |
| `src/server.rs` | 7,494 | 1,428 | Отдельный ранее существовавший `server/tests.rs` не переносился |
| `src/wb.rs` | 5,945 | 5,181 | Большая часть оставшегося файла — inline tests; production перед ними около 1,560 строк |
| `src/reporting/mcp_read.rs` | 4,750 | 3,399 | Production перед inline tests около 1,675 строк |
| `src/control/automation_executor.rs` | 5,095 | 4,322 | Production перед inline tests около 1,023 строк |

`clippy::too_many_lines` ограничивает функцию, а не общий размер модуля.
Глобальный legacy allow пока сохранён. Новый file-growth gate намеренно
считает и тесты: перенос в `tests` не даёт скрытого исключения из бюджета.

## Граница результата

Это первый проверяемый этап снижения сложности, не завершённый переход на
шесть доменных crates. Не изменены SQL/grants, credentials, marketplace write
policies, SDK-версия и production runtime. DB pool, remote TLS и upstream PRs
требуют отдельных решений и проверок; метрики сами не доказывают bottleneck.

Команды проверок и критерии разделения: [ADR 0002](adr/0002-rust-workspace-boundaries.md).

## Локальная проверка этого изменения

Снимок результатов от 2026-09-09, не постоянная гарантия будущего состояния:

- Полный `cargo test --locked --workspace --all-targets --all-features`
  через `with-position-test-db.sh`: 1,021 passed, 0 failed, 2 ignored,
  28 test targets. Включены PostgreSQL-контракты и probes семи binaries.
- Отдельный полный workspace `cargo llvm-cov` с той же тестовой БД и CI
  exclusions: 95.87% строк, 95.51% функций; оба прежних gate пройдены.
  Запас по функциям небольшой: новые изменения должны добавлять проверки,
  а не снижать порог. Эти проценты по-прежнему включают inline tests.
- Строгий workspace Clippy, rustdoc с `-D warnings`, fmt, ShellCheck,
  file-growth gate и два его unit-теста, shared Rust Docker-builder contract,
  OzonOFK Suite source check, `git diff --check`: успешно.
- `cargo audit --deny warnings`: успешно; `cargo deny check`: успешно,
  с разрешёнными предупреждениями о существующих дублирующихся версиях
  getrandom, untrusted, wasi и windows-sys. Политики не ослаблены.

Оба временных тестовых контейнера PostgreSQL и их volumes удалены скриптом;
отсутствие проверено отдельно. Production не перезапускался. Полные release
Docker images, удалённый GitHub CI и canary этого изменения не запускались.
