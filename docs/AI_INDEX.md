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
| `docs/USER_GUIDE.md` | пользовательская инструкция | изменения UI, workflow, расчётов, статусов, справочника, классификатора |
| `filter.md` | справочник MPStats-фильтров | задачи с `filterModel` и CSV-колонкой `Фильтр` |
| `.cursor/agents/mpstats-tasks-handbook.md` | узкий handbook | обновление `TASKS` из CSV-справочника |
| `справочник tasks архив.md` | append-only архив | после обновления `TASKS`, только дописывать недостающие задачи |

Удалённые дубли: `context.md`, `docs/LOCAL_APP.md`, `docs/IMPROVEMENT_PLAN.md`, `classifiers/README.md`. Их актуальный смысл перенесён в `README.md`, этот индекс и `docs/USER_GUIDE.md`.

## Карта проекта

| Зона | Файлы | Когда читать |
| --- | --- | --- |
| Notebook UI | `MPstats_export_yearmonnth_v1.3_pipeline.ipynb` | Основной удобный интерфейс пользователя |
| CLI | `pipeline/cli.py`, `pipeline/services/run_service.py` | Автоматизированный запуск без Jupyter |
| Документация | `README.md`, `docs/USER_GUIDE.md` | Общий вход и пользовательские сценарии |
| Настройки выгрузки | `pipeline/step1_export_config.json`, `pipeline/step1_config.py`, `pipeline/step1_gui.py` | Периоды, cookie, TASKS, GUI шага 1 |
| Категории | `Справочник категорий MP STATS.csv`, `filter.md`, `.cursor/agents/mpstats-tasks-handbook.md`, `справочник tasks архив.md` | Добавление или синхронизация TASKS |
| Классификация | `classifiers/rules.csv`, `classifiers/engine.py`, `classifiers/gui.py`, `mpstats_app/services/classifier_rules_service.py` | Правила, GUI и движок классификатора |
| Локальная web-app | `mpstats_app/`, `web/`, `docs/USER_GUIDE.md` | Smart pipeline, manifest, CRUD справочника и классификатора |
| Сервисы шагов | `pipeline/services/` | Основная бизнес-логика программы |
| Файловый слой | `pipeline/repositories/` | Чтение/запись CSV/JSON |
| SQL-хранилище | `pipeline/repositories/sql_repository.py`, `pipeline/services/sql_service.py`, `pipeline/migrations/` | Загрузка/выгрузка данных через DuckDB |
| Web API | `mpstats_app/api/`, `mpstats_app/schemas.py`, `tests/test_web_api.py` | Backend routes, схемы, API-регрессии |
| Рабочие данные | `pipeline/01_*`, `pipeline/02_*`, `pipeline/03_*`, `pipeline/04_*`, `Архив выгрузок/` | Проверка конкретных выгрузок и отчётов |

## Текущий поток данных

```text
MPStats API
  -> pipeline/01_step1_raw/
  -> pipeline/02_step2_enriched/
  -> pipeline/03_step3_standardized/
  -> pipeline/04_step4_parsed/
  -> pipeline/03_<project>_merged.csv
  -> pipeline/03_<project>_merged_classified.csv
```

Smart workflow web-app дополнительно ведёт manifest и куб:

```text
Справочник категорий MP STATS.csv
  -> план задач marketplace + category + year + month
  -> data/projects/{project}/raw|processed|merged
  -> mpstats.duckdb / cube_registry / mpstats_products
```

## Быстрые команды

Установка зависимостей:

```bash
python3 -m pip install -r requirements.txt
```

GUI настроек выгрузки:

```bash
python3 -m pipeline.step1_gui --config "pipeline/step1_export_config.json" --archive "справочник tasks архив.md"
```

GUI правил классификации:

```bash
python3 -m classifiers.gui
```

Локальная web-app одним ярлыком:

```bash
open "MPStats Local App.command"
```

Windows без Node.js:

```bat
"MPStats Local App.bat"
```

Запуск программы без Jupyter:

```bash
python3 -m pipeline.cli run --steps 2-6 --project-name "Мясо 05_18"
```

SQL:

```bash
python3 -m pipeline.cli sql-import --project-name "Мясо 05_18" --table mpstats_products
python3 -m pipeline.cli sql-export --table mpstats_products --output pipeline/sql_export.csv
python3 -m pipeline.cli sql-query --tables
```

Проверка импортируемых Python-модулей:

```bash
python3 -m compileall pipeline classifiers
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
- SQL: `pipeline/services/sql_service.py` + `pipeline/repositories/sql_repository.py`.
- Smart workflow web-app: `mpstats_app/services/smart_pipeline_service.py`, `mpstats_app/services/category_catalog_service.py`, `mpstats_app/services/classifier_rules_service.py`, `web/src/App.tsx`.

## Что уже есть

- `pyproject.toml` и `requirements.txt` для зависимостей.
- CLI `pipeline.cli` и scripts `mpstats-pipeline`, `mpstats-app`.
- Сервисы шагов 1-6 в `pipeline/services/`.
- Data layer в `pipeline/repositories/` и DuckDB-миграции в `pipeline/migrations/`.
- Локальная FastAPI + React web-app с редактором справочника и классификатора.
- Regression tests для pipeline services, SQL, web API и парсера веса.

## Чего не хватает

- CI для автоматического запуска тестов.
- Browser/e2e-регрессий frontend.
- Регулярного отчёта качества данных: аномальные веса, пустые классификации, причины отбрасывания строк.
- Полной уборки исторических выгрузок, логов и локальных DB-артефактов из tracked-части репозитория.
- Отдельной API-reference для backend; пока ориентируйся на `mpstats_app/api/`, `mpstats_app/schemas.py` и `tests/test_web_api.py`.

## Риски текущей структуры

- Ноутбук должен оставаться тонким UI: параметры и вызовы сервисов, без дублирования бизнес-логики.
- Рабочие данные, исторические выгрузки и код лежат рядом; важно не коммитить лишние артефакты.
- Воспроизводимое окружение уже зафиксировано, но локальные рабочие данные всё ещё легко случайно смешать с кодовым diff.
- Пути и названия проектов частично живут в пользовательских настройках и notebook-flow, поэтому переносимость зависит от аккуратной конфигурации.

## Рекомендуемый принцип изменений

Для новых доработок сначала выноси логику в маленький модуль с тестируемыми функциями, а ноутбук оставляй тонким запускателем. GUI должен редактировать конфиг и вызывать сервисы, а не становиться вторым местом бизнес-логики.

В локальной web-app справочник категорий редактируется через вкладку `Справочник` и сохраняется только в CSV `Справочник категорий MP STATS.csv`; Excel-дубликаты не являются источником. Правила классификатора редактируются через вкладку `Классификатор`, которая пишет `classifiers/rules.csv` без ручного ввода JSON.
