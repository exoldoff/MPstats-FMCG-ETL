# Classifier Performance Audit

Дата аудита: 2026-06-04.

Аудит сделан без оптимизации production-кода классификатора. Добавлены только отдельные profiling/benchmark scripts, которые запускают текущий движок и временно оборачивают его функции для измерений.

## 1. Current classifier flow

Текущий путь классификации:

```text
input processed/merged CSV или XLSX
  -> pipeline.services.classification_service.read_classification_input()
  -> prepare_for_classification()
       - если нет "Название", но есть "SKU" и "Артикул", временно меняет названия колонок
  -> importlib.reload(classifiers.engine)
  -> classifiers.engine.apply_classifiers()
       - load_rules()
       - _validate_and_prepare_rules()
       - for rule in rules.itertuples(index=False)
       - _build_rule_mask()
       - _build_condition_mask()
       - _build_match_mask()
       - out.loc[write_mask, target_column] = set_value
       - fill_unclassified, если передан
  -> postprocess_classified()
       - удаляет служебные колонки веса
       - добавляет "месяц"
       - переименовывает SKU/Название в финальный формат
  -> write_semicolon_csv()
  -> classified CSV
```

Основные файлы:

- `pipeline/services/classification_service.py` - шаг 6, чтение input, вызов движка, postprocess, запись.
- `classifiers/engine.py` - загрузка правил, валидация, построение масок и применение правил.
- `mpstats_app/services/workflow_service.py` - ручная классификация и внешний файл.
- `mpstats_app/services/smart_pipeline_service.py` - smart workflow и reclassify cube.
- `mpstats_app/services/job_service.py` + `pipeline/services/run_service.py` - background pipeline steps.
- `mpstats_app/services/classifier_rules_service.py` - CRUD правил через web UI.
- `tests/test_pipeline_services.py` - текущие regression tests классификации.

## 2. Dataset sizes

Локальный snapshot `data/projects` на момент аудита:

| Metric | Value |
| --- | ---: |
| processed input CSV, без `_classified.csv` | 140 файлов |
| строк во всех processed input CSV | 3,160,138 |
| проект `mpstats` | 317,690 строк |
| проект `тест` | 2,842,448 строк |
| крупнейший input | 238,499 строк |
| крупнейший input path | `data/projects/тест/processed/2025-01/oz/f982d200cc72e084.csv` |

Профилировщик прогнан на:

- mock small: 10,000 строк;
- mock medium: 100,000 строк;
- real small sample: первые 10,000 строк из крупнейшего processed CSV.

Large 500,000 строк добавлен в scripts, но не запускался: для него нужен явный флаг `--include-large`.

## 3. Rules summary

Текущий `classifiers/rules.csv`:

| Metric | Value |
| --- | ---: |
| total rules | 34 |
| active rules | 30 |
| inactive rules | 4 |
| rules with `conditions_json` | 8 |
| duplicate rules | 0 |

Match types:

| match_type | rules |
| --- | ---: |
| regex | 18 |
| contains | 12 |
| equals | 3 |
| otherwise | 1 |

Active target columns:

| target_column | active rules |
| --- | ---: |
| Подкатегория | 17 |
| Вид мяса | 7 |
| Тип | 5 |
| Категория | 1 |

Active match fields:

| match_field | active rules |
| --- | ---: |
| Название | 26 |
| Категория | 2 |
| Подкатегория | 1 |
| empty, for otherwise | 1 |

## 4. Complexity estimate

В runtime нет `df.apply(axis=1)` и нет `iterrows()` по строкам в основном движке. Есть цикл по правилам:

```text
for rule in rules.itertuples(index=False):
    build pandas string masks for the whole dataframe
```

То есть сложность близка к:

```text
N rows x M active rules
```

С учётом `conditions_json` фактическое число string-checks выше, чем `N x M`:

- active rules: 30;
- measured `_build_match_mask` calls на real sample: 42 на один запуск;
- real sample 10,000 строк: `30 x 10,000 = 300,000` active rule checks, measured condition checks: 420,000;
- крупнейший локальный input 238,499 строк: примерно 7.15 млн active rule checks или 10.02 млн condition checks;
- все 140 локальных processed inputs: примерно 94.8 млн active rule checks или 132.7 млн condition checks.

Важная деталь: category filter сейчас применяется после построения текстовой маски. Поэтому правило для `Мясо` всё равно может прогонять regex по строкам другой категории, а потом отбрасывать их category mask.

## 5. Profiling results

Артефакты записаны в `data/classifier_benchmark/`:

- `profile_small.txt/json`
- `profile_medium.txt/json`
- `profile_custom.txt/json`
- `benchmark_mock_small.txt/json`
- `benchmark_mock_medium.txt/json`
- `benchmark_real_10000.txt/json`
- `rules_audit.txt/json`
- `benchmark_summary.txt/json`

Профиль с cProfile:

| Run | Rows | Active rules | Total | Apply rules | String matching | Regex matching | Rows/sec |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| profile small mock | 10,000 | 10 | 0.4385s | 0.3392s | 0.2011s | 0.1566s | 22,804 |
| profile medium mock | 100,000 | 10 | 3.0788s | 2.2138s | 1.5788s | 1.1935s | 32,480 |
| profile real custom | 10,000 | 30 | 0.9534s | 0.8437s | 0.6381s | 0.4443s | 10,489 |

Benchmark без cProfile overhead:

| Run | Rows | Active rules | Total | Apply rules | String matching | Regex matching | Rows/sec |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| benchmark mock small | 10,000 | 10 | 0.2632s | 0.1749s | 0.1298s | 0.1092s | 37,998 |
| benchmark mock medium | 100,000 | 10 | 2.2945s | 1.5756s | 1.2355s | 1.0384s | 43,583 |
| benchmark real 10k | 10,000 | 30 | 0.7031s | 0.6144s | 0.5070s | 0.3843s | 14,223 |

Real 10k phase timings:

| Phase | Time |
| --- | ---: |
| read input CSV | 0.0269s |
| prepare input | 0.0002s |
| module reload | 0.0005s |
| apply rules | 0.6144s |
| postprocess | 0.0080s |
| write output CSV | 0.0531s |
| read rules | 0.0019s |
| prepare rules | 0.0204s |
| string matching total | 0.5070s |

cProfile top for real sample:

- `engine.py:336(apply_classifiers)` - 0.844s cumulative under cProfile;
- `engine.py:232(_build_match_mask)` - 0.638s cumulative;
- pandas `.str.contains` path - 0.589s cumulative;
- `{method 'search' of 're.Pattern' objects}` - 0.456s self time;
- repeated `.str.strip()` path - 0.126s cumulative.

## 6. Top bottlenecks

1. Главный bottleneck - `apply_classifiers()`.

На real benchmark 10k:

```text
apply_rules_seconds / total_seconds = 0.6144 / 0.7031 = 87.4%
```

2. Внутри `apply_classifiers()` главный bottleneck - pandas string matching.

```text
match_seconds / apply_rules_seconds = 0.5070 / 0.6144 = 82.5%
regex_seconds / match_seconds = 0.3843 / 0.5070 = 75.8%
```

3. `contains` тоже идёт через regex engine.

В `classifiers/engine.py` literal contains реализован как:

```python
text.str.contains(re.escape(pattern), case=False, regex=True, na=False)
```

Это сохраняет literal-семантику через `re.escape`, но всё равно запускает regex path. Для `contains` дешевле должен быть `regex=False`.

4. Category filter применяется поздно.

Сейчас сначала строится `_build_rule_mask()` по всему dataframe, затем добавляется `cat_mask`. Для category-specific правил это значит, что regex/contains часто проверяются по строкам, которые всё равно будут отброшены.

5. Правила каждый запуск читаются и валидируются заново.

Для одного файла это немного: на real 10k `read_rules + prepare_rules = 0.0223s`. Но smart/reclassify workflow запускает классификацию по многим task-файлам, поэтому этот overhead повторяется десятки или сотни раз. Дополнительно `classification_service.classify_dataframe()` делает `importlib.reload(classifier_engine)` перед каждым запуском.

Что не найдено:

- нет `df.apply(axis=1)` в runtime классификатора;
- нет `iterrows()` по строкам в runtime классификатора;
- нет fuzzy matching;
- нет построчного логирования;
- нет промежуточной записи файлов внутри apply loop;
- `iterrows()` есть в `ClassifierRulesService._rules_from_frame()`, но это web/UI CRUD правил, не массовая классификация CSV.

## 7. Suspicious rules

Автоматический audit нашёл:

- duplicate rules: 0;
- invalid regex: 0;
- potentially expensive regex: 3;
- broad otherwise fallback: 1;
- never matched on the selected real 10k sample: 27.

Важно: `never_matched_sample` не значит, что правило недостижимо глобально. Real sample взят из одного processed CSV, поэтому правила по другим категориям закономерно могут не матчиться.

Top suspicious:

| Rule row | Priority | Field | Pattern | Reason |
| ---: | ---: | --- | --- | --- |
| 12 | 75 | Название | `^(?=.*(?:свинин\w*|свин\w*))(?!.*(?:говядин\w*|говяж\w*)).*$` | lookahead + full-line `.*` |
| 13 | 80 | Название | `^(?=.*(?:говядин\w*|говяж\w*))(?!.*свин\w*).*$` | lookahead + full-line `.*` |
| 16 | 94 | empty | otherwise | broad fallback, intentionally broad |
| 24 | 135 | Название | `...карбонад\w*.*праздничн\w*.*...` | multiple `.*` inside alternation |

Возможная проблема с priority, которую нужно учитывать при оптимизации: правило 16 fallback для `Мясо -> Подкатегория = Прочее` стоит до более точных мясных правил, но имеет `mode=overwrite`. Движок специально заставляет `otherwise` писать только в пустые target cells, даже если в CSV указан `overwrite`. Затем более точные правила с `overwrite` корректно перезаписывают `Прочее`.

## 8. Correctness risks

Baseline comparison подготовлен и уже прогнан для real small sample:

```text
baseline: existing classified sibling sample
candidate: fresh current classifier output
rows: match
columns: match
diff_counts:
  Категория: 0
  Подкатегория: 0
  Бренд: 0
  Тип: 0
  Вид мяса: 0
```

Главные риски при будущей оптимизации:

- порядок правил: `priority`, затем `row_num`, stable sort;
- `fill_empty` vs `overwrite`;
- особая семантика `otherwise`: всегда только empty target;
- правила могут менять колонку `Категория`, а следующие category filters должны видеть уже изменённый dataframe;
- условия из `conditions_json` могут ссылаться на target columns, заполненные предыдущими правилами;
- `contains` сейчас case-insensitive literal substring через regex path, при замене на `regex=False` нужно сохранить case-insensitive behavior;
- category prefilter нельзя кэшировать навсегда, если ранние правила меняют `Категория`.

## 9. Optimization options

### A. Быстрые безопасные оптимизации

1. Для `match_type == "contains"` использовать `Series.str.contains(pattern, case=False, regex=False, na=False)` вместо `re.escape(pattern), regex=True`.

Почему безопасно: `contains` уже трактуется как literal, а не regex. Это уменьшит regex work для 12 rules и extra conditions.

2. Применять category filter до текстового matching.

Для правила с конкретной категорией сначала построить cheap `cat_mask`; если matching rows = 0, не запускать regex/contains. Если rows > 0, строить текстовую маску только на subset и раскладывать результат обратно в общий индекс.

3. Убрать `importlib.reload(classifier_engine)` из обычного runtime или заменить на явную invalidation по `rules_path.stat().st_mtime_ns`.

4. Кэшировать загруженные и подготовленные rules по `(path, mtime, size)`.

5. Не пересчитывать `out[category_column].fillna("").astype(str).str.strip().str.casefold()` для каждого category-specific правила. Делать per-rule актуально, но с дешёвым cache invalidation, когда правило меняет `Категория`.

6. Не пересчитывать `_is_empty_series(out[target_column])` для target columns, если колонка не менялась с прошлого правила. Это уже менее маленький diff, но всё ещё без изменения семантики.

7. Разделить literal `contains/equals/startswith` и regex rules в rule compiler. Дешёвые literal checks должны идти через literal string APIs.

### B. Нормальное production-решение

1. Ввести rule compiler:

- prepared columns;
- normalized category filters;
- literal contains rules;
- regex rules;
- otherwise rules;
- dependency hints: какие rules читают или пишут target columns.

2. Сделать staged classification:

- exact/equals rules;
- literal contains rules;
- regex fallback;
- otherwise/manual bucket.

Этот вариант можно делать только если compiler сохраняет текущий priority/overwrite behavior.

3. Добавить chunked CSV classification для больших файлов.

Нужно сохранять результат полностью, но читать input chunk-by-chunk. Семантика правил локальна для строки, поэтому chunking возможен. Осторожность нужна с output columns/order.

4. Incremental classification only for new/changed rows.

Для smart pipeline это может дать больше, чем micro-optimization regex, если повторно классифицируются одни и те же processed files.

### C. Переусложнённый вариант

Сейчас не рекомендую:

- ML model;
- embeddings;
- LLM classification;
- external search index;
- новая Aho-Corasick dependency.

A/B должны дать достаточный эффект без изменения бизнес-семантики и без тяжёлых зависимостей.

## Step 1 results: literal `contains` without regex

Изменение:

```text
match_type == "contains":
  before: str.contains(re.escape(pattern), case=False, regex=True, na=False)
  after:  str.contains(pattern, case=False, regex=False, na=False)
```

Корректность:

| Run | Row count | Columns | Категория | Подкатегория | Бренд | Тип | Вид мяса |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| mock small old vs Step 1 | match | match | 0 | 0 | 0 | 0 | n/a |
| mock medium old vs Step 1 | match | match | 0 | 0 | 0 | 0 | n/a |
| real 10k old vs Step 1 | match | match | 0 | 0 | 0 | 0 | 0 |

Single-run benchmark:

| Run | Old total | Step 1 total | Total speedup | Old apply | Step 1 apply | Apply speedup | Old contains | Step 1 contains |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| mock small | 0.2332s | 0.2552s | 0.91x | 0.1589s | 0.1775s | 0.90x | 0.0149s | 0.0098s |
| mock medium | 2.4356s | 2.4153s | 1.01x | 1.6223s | 1.6951s | 0.96x | 0.1478s | 0.1569s |
| real 10k | 0.6771s | 0.7447s | 0.91x | 0.5907s | 0.6534s | 0.90x | 0.1019s | 0.0576s |

Вывод: семантика не изменилась. На real sample literal `contains` стал быстрее по собственной фазе (`0.1019s -> 0.0576s`), но общий single-run total оказался хуже из-за шума и более дорогого regex slice в этом прогоне. Step 1 оставлен как безопасное семантическое упрощение: `contains` больше не запускает regex engine для literal substring.

## 10. Recommended next step

Step 1:

Самая безопасная маленькая оптимизация - заменить `contains` на literal `regex=False` и добавить regression benchmark comparison:

```text
old output vs optimized output
row count, columns, Категория/Подкатегория/Бренд/Тип/Вид мяса diffs = 0
```

Ожидаемый эффект: снизить долю regex engine для `contains` rules и extra conditions. Это маленький diff в `classifiers/engine.py`.

Step 2:

Следующая оптимизация - category prefilter before matching:

```text
if rule.category not in {"", "*"}:
    build category mask first
    if no candidate rows:
        skip expensive text/regex mask
    else:
        run match only on candidate index
```

Ожидаемый эффект: большой выигрыш в smart/reclassify сценариях, где один файл обычно относится к одной категории, а активные правила включают несколько категорий.

После каждого шага обязательно:

- прогнать `python3 -m pytest tests/test_pipeline_services.py -q`;
- прогнать `python3 scripts/benchmark_classifier.py run --all-sizes`;
- сравнить baseline/candidate через `python3 scripts/benchmark_classifier.py compare BASELINE CANDIDATE`;
- проверить real sample diff по ключевым классификационным колонкам.

## 11. What NOT to optimize now

- Не переписывать классификатор на DuckDB как первый шаг.
- Не делать полный CROSS JOIN rows x rules в DuckDB.
- Не добавлять ML/LLM/embeddings/search index.
- Не менять структуру `rules.csv` до замеров простых оптимизаций.
- Не менять event names, output columns и пользовательский classified CSV format.
- Не оптимизировать `ClassifierRulesService._rules_from_frame()` первым: он использует `iterrows()`, но это UI CRUD правил, не массовый CSV runtime.

## DuckDB option

Часть классификации можно перенести в DuckDB, но это не первый кандидат.

Что можно:

- `equals` через joins/rule tables;
- literal contains через `contains/lower`;
- simple regex через `regexp_matches`;
- staging temp tables for candidate rows.

Как избежать полного CROSS JOIN:

- группировать rules по `category`, `match_field`, `match_type`;
- сначала фильтровать строки по category/source/marketplace;
- для equals делать hash join;
- для contains/regex строить candidate subsets по category и только затем применять rules;
- не соединять все строки со всеми rules без предварительного ограничения.

Как сохранить semantics:

- хранить `priority` и `row_num`;
- для каждого `(row_id, target_column)` выбирать применимое правило через window `row_number() over (partition by row_id, target_column order by priority, row_num)`;
- отдельно моделировать `overwrite` и `fill_empty`;
- `otherwise` применять только к target cells, которые остались пустыми;
- учитывать, что более ранние rules могут изменить `Категория` или `Подкатегория`, а следующие conditions могут читать эти значения.

Какой benchmark нужен:

- old pandas classifier vs DuckDB candidate на mock 10k/100k/500k;
- real small sample;
- real largest processed file 238,499 rows;
- baseline comparison по `Категория`, `Подкатегория`, `Бренд`, `Тип`, `Вид мяса`;
- замер memory и output file size.
