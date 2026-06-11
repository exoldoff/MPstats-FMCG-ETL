# AI Index

Короткий индекс проекта для быстрого входа без полного сканирования репозитория. Это основной агентский контекст: отдельный `context.md` больше не ведётся.

## Обязательный порядок чтения

1. `AGENTS.md` — правила работы, коммиты, проверки, ограничения.
2. `docs/AI_INDEX.md` — этот индекс.
3. `README.md` — только если нужен человеческий quick start или общая картина.
4. Релевантные файлы по задаче из разделов ниже.

## Markdown-документы

| Файл | Статус | Когда читать |
| --- | --- | --- |
| `AGENTS.md` | источник правил | всегда перед работой |
| `README.md` | краткий обзор | запуск, структура, состояние проекта |
| `docs/AI_INDEX.md` | индекс для агентов | всегда после `AGENTS.md` |
| `docs/USER_GUIDE.md` | пользовательская инструкция web-app | изменения UI, workflow, расчётов, статусов, справочника, классификатора |
| `filter.md` | справочник MPStats-фильтров | задачи с `filterModel` и CSV-колонкой `Фильтр` |
| `.cursor/agents/mpstats-tasks-handbook.md` | узкий handbook | обновление `TASKS` из CSV-справочника |
| `справочник tasks архив.md` | append-only архив | после обновления `TASKS`, только дописывать недостающие задачи |

Удалённые дубли: `context.md`, `docs/LOCAL_APP.md`, `docs/IMPROVEMENT_PLAN.md`, `classifiers/README.md`. Их актуальный смысл перенесён в `README.md`, этот индекс и `docs/USER_GUIDE.md`.

## Карта проекта

| Зона | Файлы | Когда читать |
| --- | --- | --- |
| Документация | `README.md`, `docs/USER_GUIDE.md` | Общий вход и пользовательские сценарии |
| Web-app | `mpstats_app/`, `web/`, `docs/USER_GUIDE.md` | Единственный пользовательский интерфейс и запуск программы |
| Настройки выгрузки | `pipeline/step1_config.py`, `mpstats_app/api/settings.py`, `mpstats_app/services/project_service.py` | Cookie, текущий проект и настройки web workflow |
| Категории | `Справочник категорий MP STATS.csv`, `filter.md`, `.cursor/agents/mpstats-tasks-handbook.md`, `справочник tasks архив.md` | Добавление или синхронизация TASKS |
| Классификация | `classifiers/rules.csv`, `classifiers/engine.py`, `mpstats_app/services/classifier_rules_service.py` | Правила web-редактора и движок классификатора |
| Сервисы шагов | `pipeline/services/` | Внутренняя бизнес-логика, вызываемая web-app |
| Качество данных | `pipeline/data_quality/`, `pipeline/services/data_quality_service.py`, `mpstats_app/api/quality.py` | Бизнес-проверки MPStats: продажи, ТО, цены, доли, дубли, периоды, согласованность метрик |
| Файловый слой | `pipeline/repositories/` | Чтение/запись CSV/JSON |
| SQL-хранилище | `pipeline/repositories/sql_repository.py`, `pipeline/migrations/`, `mpstats_app/repositories/duckdb_repository.py` | DuckDB-хранилище web-app |
| Web API | `mpstats_app/api/`, `mpstats_app/schemas.py`, `tests/test_web_api.py` | Backend routes, схемы, API-регрессии |
| Рабочие данные | `data/projects/`, `mpstats.duckdb` | Проверка конкретных web-выгрузок и отчётов |

## Текущий поток данных

```text
MPStats API
  -> data/projects/{project}/raw
  -> data/projects/{project}/processed
  -> data/projects/{project}/merged
  -> data/projects/{project}/exports
Справочник категорий MP STATS.csv
  -> план задач marketplace + category + year + month
  -> mpstats.duckdb / cube_registry / mpstats_products
```

## Быстрые команды

Установка Python-зависимостей:

```bash
python3 -m pip install -r requirements.txt
```

Локальная web-app одним ярлыком:

```bash
open "MPStats Local App.command"
```

Windows без Node.js:

```bat
"MPStats Local App.bat"
```

Проверка импортируемых Python-модулей:

```bash
python3 -m compileall pipeline classifiers mpstats_app
```

Узкая проверка web API:

```bash
python3 -m pytest tests/test_web_api.py
```

## Где искать логику по шагам

- Шаг 1: `pipeline/services/export_service.py`.
- Шаг 2: `pipeline/services/enrich_service.py`.
- Шаг 3: `pipeline/services/standardize_service.py`.
- Шаг 4: `pipeline/services/weight_parser_service.py`.
- Шаг 5: `pipeline/services/merge_service.py`.
- Шаг 6: `pipeline/services/classification_service.py` + `classifiers/engine.py`.
- SQL/DuckDB: `mpstats_app/repositories/duckdb_repository.py` + общий helper `pipeline/repositories/sql_repository.py`.
- Smart workflow web-app: `mpstats_app/services/smart_pipeline_service.py`, `mpstats_app/services/category_catalog_service.py`, `mpstats_app/services/classifier_rules_service.py`, `web/src/App.tsx`.

## Что уже есть

- `pyproject.toml` и `requirements.txt` для зависимостей web-app.
- Сервисы шагов 1-6 в `pipeline/services/`.
- Data layer в `pipeline/repositories/` и DuckDB-миграции в `pipeline/migrations/`.
- Локальная FastAPI + React web-app с редактором справочника и классификатора.
- MVP проверки качества данных в `pipeline/data_quality/` и вкладке `Данные` -> `Качество`.
- Regression tests для pipeline services, SQL, web API и парсера веса.

## Чего не хватает

- CI для автоматического запуска тестов.
- Browser/e2e-регрессий frontend.
- Полной настройки порогов качества данных по проектам/категориям через web UI.
- Полной уборки исторических выгрузок, логов и локальных DB-артефактов из tracked-части репозитория.
- Отдельной API-reference для backend; пока ориентируйся на `mpstats_app/api/`, `mpstats_app/schemas.py` и `tests/test_web_api.py`.

## Риски текущей структуры

- Рабочие данные, исторические выгрузки и код лежат рядом; важно не коммитить лишние артефакты.
- Воспроизводимое окружение уже зафиксировано, но локальные рабочие данные всё ещё легко случайно смешать с кодовым diff.
- Пути и названия проектов живут в настройках web-app и локальной DuckDB, поэтому переносимость зависит от аккуратной конфигурации.

## Рекомендуемый принцип изменений

Для новых доработок сначала выноси логику в маленький модуль с тестируемыми функциями, а web routes оставляй тонкими: routes принимают/отдают данные, сервисы выполняют workflow, repositories работают с файлами и DuckDB.

В локальной web-app справочник категорий редактируется через вкладку `Справочник` и сохраняется только в CSV `Справочник категорий MP STATS.csv`; Excel-дубликаты не являются источником. Правила классификатора редактируются через вкладку `Классификатор`, которая пишет `classifiers/rules.csv` без ручного ввода JSON.
