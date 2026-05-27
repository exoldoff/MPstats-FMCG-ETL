# MPstats-FMCG-ETL

MPstats-FMCG-ETL — открытый инструмент для команд, которые работают с маркетплейс-данными и хотят превращать сырые выгрузки MPStats в понятный корпоративный датасет.

MPStats даёт выгрузки, но корпформат не спешит:
бизнесу нужен порядок, чтобы каждый отчёт был сшит.
Этот тул закрывает разрыв, где сервис к B2B не готов:
аналитикам и research-отделам меньше рутины и тупиков.

Проект особенно полезен B2B-компаниям, FMCG-брендам, категорийным менеджерам, аналитикам продаж и trade marketing-командам. Он помогает регулярно собирать данные по категориям и маркетплейсам, приводить их к единой структуре, рассчитывать весовые и ценовые метрики, классифицировать товары по бизнес-правилам и готовить данные для отчётов, BI, ассортиментного анализа и управленческих решений.

Идея простая: вместо разрозненных CSV-файлов, ручных склеек и нестабильных Excel-процессов бизнес получает воспроизводимый pipeline с локальным приложением, справочниками, правилами классификации и выгрузкой в удобном виде.

Технически проект умеет выгружать категории из MPStats, нормализовать данные, парсить вес/объём, склеивать файлы и классифицировать товары по настраиваемым правилам.

Сейчас проект можно запускать тремя способами:

- через основной удобный ноутбук `MPstats_export_yearmonnth_v1.3_pipeline.ipynb`;
- через CLI `python3 -m pipeline.cli ...`, если нужен запуск без Jupyter.
- через локальную web-app `MPStats Local App.command` на macOS или `MPStats Local App.bat` на Windows, если нужен умный manifest по категориям и месяцам.

Ноутбук теперь тонкий: в нём остались параметры и ячейки запуска, а бизнес-логика живёт в `pipeline/services/`.

- `pipeline/` — конфигурация и GUI для шага 1;
- `classifiers/` — движок классификации и GUI-редактор правил;
- `pipeline/cli.py` — командный интерфейс для запуска шагов пайплайна;
- `pipeline/services/` — бизнес-логика шагов 1-6;
- `pipeline/repositories/` — файловый слой для CSV/JSON;
- `docs/AI_INDEX.md` — индекс для быстрого входа в проект;
- `docs/USER_GUIDE.md` — подробная пользовательская инструкция.

## Документация

Оставлены только рабочие Markdown-файлы:

| Файл | Для чего нужен |
| --- | --- |
| `AGENTS.md` | правила работы агента, архитектурные ограничения, проверки и формат финального отчёта |
| `README.md` | короткий вход для человека: запуск, структура, текущее состояние |
| `docs/AI_INDEX.md` | быстрый индекс для агента: куда идти по типам задач, что уже есть и чего не хватает |
| `docs/USER_GUIDE.md` | пользовательские сценарии локальной web-app, notebook-flow, файлов и куба |
| `filter.md` | справочник синтаксиса `filterModel` MPStats |
| `.cursor/agents/mpstats-tasks-handbook.md` | узкая инструкция для обновления `TASKS` из CSV-справочника |
| `справочник tasks архив.md` | накопительный архив уже собранных задач `TASKS` |

Убраны отдельные дубли: локальная web-app описана в `docs/USER_GUIDE.md`, классификатор — там же и в `docs/AI_INDEX.md`, план развития заменён актуальным блоком состояния ниже.

## Что делает пайплайн

1. Выгружает CSV из MPStats по маркетплейсам, категориям, периодам и фильтрам.
2. Обогащает сырые файлы метаданными.
3. Приводит колонки к единой схеме.
4. Парсит вес и объём из названий товаров.
5. Склеивает подготовленные CSV в один итоговый файл.
6. Применяет правила классификации из `classifiers/rules.csv`.

## Основные файлы

- `MPstats_export_yearmonnth_v1.3_pipeline.ipynb` — главный пайплайн.
- `pipeline/step1_export_config.json` — настройки выгрузки: период, cookie, задачи.
- `pipeline/step1_gui.py` — GUI для редактирования настроек выгрузки.
- `pipeline/step1_config.py` — загрузка, нормализация и сохранение настроек шага 1.
- `classifiers/rules.csv` — таблица правил классификации.
- `classifiers/engine.py` — применение правил к DataFrame.
- `classifiers/gui.py` — GUI для редактирования правил.
- `Справочник категорий MP STATS.csv` — источник правды по категориям.
- `filter.md` — справочник по `filterModel` для MPStats.
- `справочник tasks архив.md` — накопительный архив задач.
- `Архив выгрузок/` — исторические выгрузки и отчёты.

## Что уже есть

- Воспроизводимые зависимости: `pyproject.toml` и `requirements.txt`.
- CLI: `pipeline.cli` и console script `mpstats-pipeline`.
- Сервисы шагов 1-6 в `pipeline/services/`.
- Файловый и SQL-слой в `pipeline/repositories/`.
- DuckDB-миграции в `pipeline/migrations/`.
- Локальная FastAPI + React web-app: `mpstats_app/` и `web/`.
- Редактор справочника категорий и правил классификатора в web UI.
- Regression tests: `tests/test_pipeline_services.py`, `tests/test_sql_service.py`, `tests/test_web_api.py`, `tests/test_weight_parser_service.py`.

## Чего не хватает

- Автоматического CI, который запускает тесты и проверки перед merge.
- Frontend/e2e-проверок web UI в браузере.
- Регулярного отчёта качества данных: аномальные веса, пустые классификации, причины отбрасывания строк.
- Полной уборки исторических выгрузок, логов и локальных DB-артефактов из tracked-части репозитория.
- Отдельной API-reference для web backend; сейчас источники правды — routes/schemas и `tests/test_web_api.py`.

## Быстрый старт

Открыть GUI настроек выгрузки:

```bash
python3 -m pipeline.step1_gui --config "pipeline/step1_export_config.json" --archive "справочник tasks архив.md"
```

Открыть GUI правил классификации:

```bash
python3 -m classifiers.gui
```

Проверить окружение и рабочие пути:

```bash
python3 -m pipeline.cli doctor --project-name "Мясо 05_18"
```

Запустить локальную обработку уже выгруженных файлов без обращения к MPStats API:

```bash
python3 -m pipeline.cli run --steps 2-6 --project-name "Мясо 05_18"
```

Запустить только склейку:

```bash
python3 -m pipeline.cli merge --project-name "Мясо 05_18"
```

Запустить только классификацию:

```bash
python3 -m pipeline.cli classify --project-name "Мясо 05_18" --rules classifiers/rules.csv
```

Загрузить итоговый файл в локальную SQL-БД DuckDB:

```bash
python3 -m pipeline.cli sql-import --project-name "Мясо 05_18" --table mpstats_products --mode append
```

Выгрузить таблицу или SQL-запрос обратно в CSV:

```bash
python3 -m pipeline.cli sql-export --table mpstats_products --output pipeline/sql_export.csv
python3 -m pipeline.cli sql-export --query "SELECT * FROM mpstats_products WHERE \"Маркетплейс\" = 'Ozon'" --output pipeline/ozon.csv
```

Запуск шага 1 с выгрузкой из MPStats:

```bash
python3 -m pipeline.cli export --config pipeline/step1_export_config.json
```

Для обычной работы открой `MPstats_export_yearmonnth_v1.3_pipeline.ipynb`, поменяй параметры в блоке `0. Настройки и импорты` и запускай нужные шаги сверху вниз. Для шага 1 в конфиге должен быть актуальный cookie.

## Рабочие директории

Ожидаемая структура рабочих артефактов внутри `pipeline/`:

- `01_step1_raw/` — сырые CSV после выгрузки.
- `02_step2_enriched/` — CSV после обогащения.
- `03_step3_standardized/` — CSV после стандартизации.
- `04_step4_parsed/` — CSV после парсинга веса/объёма.
- `logs/` — логи выгрузок.
- `03_<project>_merged.csv` — итоговая склейка.
- `03_<project>_merged_classified.csv` — итог после классификации.

Часть этих директорий может быть не закоммичена или игнорироваться локально, потому что это рабочие данные.

## Зависимости

Проект использует Python 3.10+. Зависимости зафиксированы в `pyproject.toml` и `requirements.txt`:

- `pandas`, `numpy`, `requests`;
- `openpyxl` для записи XLSX;
- `duckdb` для локального SQL-хранилища;
- `fastapi`, `uvicorn` для локальной web-app;
- `tkinter`, обычно поставляется вместе с Python.

Установить зависимости:

```bash
python3 -m pip install -r requirements.txt
```

Для разработки можно использовать `pyproject.toml`.

## Правила классификации

Правила лежат в `classifiers/rules.csv`. Поддерживаются:

- приоритеты правил;
- фильтр по категории;
- операторы `contains`, `not_contains`, `regex`, `equals`, `startswith`;
- режимы `fill_empty` и `overwrite`;
- составные условия через `conditions_json`.

Для обычной работы используй вкладку `Классификатор` в локальной web-app. Детальный пользовательский сценарий описан в `docs/USER_GUIDE.md`; технический вход для агента — в `docs/AI_INDEX.md`.

## Как безопасно дорабатывать

- Сначала прочитай `AGENTS.md`, затем `docs/AI_INDEX.md`.
- Для изменения категорий используй `Справочник категорий MP STATS.csv` как источник правды.
- Для изменения фильтров сверяйся с `filter.md`.
- Для изменения классификации чаще всего достаточно править `classifiers/rules.csv`.
- Для изменений парсинга веса смотри шаг 4 ноутбука.
- Для изменений итоговой склейки смотри шаг 5 ноутбука.

Бизнес-логика живёт в `pipeline/services/`, файловые операции в `pipeline/repositories/`, а notebook и GUI используются как удобные оболочки.

## Локальная web-app

Для нового workflow запускай `MPStats Local App.command` на macOS или `MPStats Local App.bat` на Windows. macOS-ярлык пересобирает frontend через Node.js/npm при изменениях, Windows-ярлык использует готовую папку `web/dist`, создаёт локальную `.venv` и ставит Python-зависимости без админских прав. Внутри есть:

- `Категории` — выбор активных путей для плана загрузки;
- `Справочник` — редактирование CSV-справочника категорий без Excel-дубликатов;
- `Умный план` — задачи `marketplace + category + month`, сверка с локальными файлами, статусы и рекомендованное действие;
- `Классификатор` — полноценный редактор правил без ручного JSON.

Подробнее см. `docs/USER_GUIDE.md`.
