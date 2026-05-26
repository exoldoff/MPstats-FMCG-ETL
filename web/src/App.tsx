import {
  AlertTriangle,
  Archive,
  BookOpen,
  CheckCircle2,
  CircleHelp,
  Copy,
  Database,
  Download,
  FileSpreadsheet,
  FolderSync,
  History,
  ListChecks,
  Pause,
  Play,
  Plus,
  RefreshCcw,
  RotateCcw,
  Save,
  Search,
  Settings,
  SkipForward,
  Table2,
  Trash2,
  Upload,
  X
} from "lucide-react";
import type { ReactNode } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import { FilterableTable, type SortDirection } from "./FilterableTable";
import {
  api,
  Category,
  CategorySourceRow,
  ClassificationResponse,
  ClassifierCondition,
  ClassifierRule,
  CubeItem,
  DownloadTask,
  ExportArtifact,
  ExportColumnFilter,
  ExportOptions,
  ExportPayload,
  ExportPreview,
  PipelineRun,
  PipelineSettings,
  ProductSearch,
  ProjectFile,
  QualityProblem,
  QualityProject,
  QualityReport
} from "./api";

type Mode = "historical_backfill" | "monthly_sync";
type Tab = "categories" | "catalog" | "plan" | "files" | "cube" | "export" | "classifier" | "quality";
type FileKindFilter = "all" | "raw" | "processed" | "classified" | "export" | "other";

const defaultPipelineSettings: PipelineSettings = {
  overwrite_raw: false,
  overwrite_processed: false,
  overwrite_db: false,
  max_parallel_downloads: 1,
  retry_count: 1,
  timeout_seconds: 300,
  pause_between_requests: 2,
  max_weight_kg: 40
};

const taskFilters = [
  ["all", "Все"],
  ["errors", "Только ошибки"],
  ["not_downloaded", "Только не скачано"],
  ["not_processed", "Только не обработано"],
  ["not_saved", "Только не сохранено в БД"],
  ["ready", "Только готовые"]
];

const matchTypes = [
  ["contains", "содержит"],
  ["not_contains", "не содержит"],
  ["regex", "regex"],
  ["equals", "равно"],
  ["startswith", "начинается с"],
  ["otherwise", "иначе"]
];

const exportFilterTypes = [
  ["contains", "содержит"],
  ["not_contains", "не содержит"],
  ["equals", "равно"],
  ["startswith", "начинается с"],
  ["gt", ">"],
  ["gte", ">="],
  ["lt", "<"],
  ["lte", "<="]
] as const;

const ruleModes = [
  ["fill_empty", "заполнить пустое"],
  ["overwrite", "перезаписать"]
];

const marketplaceOptions = ["Озон", "WB", "ЯМ"];
const catalogFilterTypes = [
  ["contains", "Содержит"],
  ["notContains", "Исключает"]
] as const;

type ClassifierPreset = "name-to-subcategory" | "sku-to-subcategory" | "name-to-brand" | "otherwise";
type CatalogFilterType = (typeof catalogFilterTypes)[number][0];
type CatalogFilterOperator = "AND" | "OR";
type CatalogFilterCondition = { type: CatalogFilterType; value: string };
type CatalogFilterDraft = { operator: CatalogFilterOperator; conditions: CatalogFilterCondition[] };
type ExportFilterDraft = { id: string; column: string; match_type: string; value: string };

const commonClassifierColumns = ["Название", "SKU", "Артикул", "Бренд", "Категория", "Подкатегория", "Тип", "Вид мяса"];

const statusLabels: Record<string, string> = {
  raw: "raw",
  export: "выгрузка",
  other: "прочее",
  pending: "ожидает",
  downloading: "скачивание",
  downloaded: "скачано",
  processing: "обработка",
  processed: "обработано",
  classifying: "классификация",
  classified: "классифицировано",
  saving_to_db: "сохранение",
  saved_to_db: "в БД",
  failed: "ошибка",
  skipped: "пропуск",
  no_data: "нет данных",
  planned: "план",
  running: "идёт",
  pausing: "пауза...",
  paused: "пауза",
  succeeded: "готово",
  completed_with_errors: "с ошибками"
};

const activeRunStatuses = new Set(["running", "pausing"]);
const passiveCubeActions = new Set(["Предпросмотр БД", "Поиск в БД"]);

function isActiveRunStatus(status: string | null | undefined) {
  return Boolean(status && activeRunStatuses.has(status));
}

const fileKindInfo: Record<Exclude<FileKindFilter, "all">, { label: string; hint: string }> = {
  raw: {
    label: "Raw",
    hint: "Исходные CSV, которые приложение скачало из MPStats. Их держим как резерв, чтобы не скачивать заново при повторной обработке."
  },
  processed: {
    label: "Processed",
    hint: "Файлы после обработки raw: единые колонки, дата, маркетплейс, категория, вес, объём и расчётные показатели."
  },
  classified: {
    label: "Classified",
    hint: "Processed-файлы после правил из вкладки Классификатор. Именно они сохраняются в БД / Куб."
  },
  export: {
    label: "Выгрузки",
    hint: "Готовые XLSX из вкладки Выгрузка. Это файлы для передачи, анализа или ручной работы."
  },
  other: {
    label: "Прочее",
    hint: "Вспомогательные файлы проекта, которые не относятся к основному пути raw -> processed -> classified -> БД."
  }
};

const fileKindFilters: Array<{ value: FileKindFilter; label: string; hint: string }> = [
  {
    value: "all",
    label: "Все",
    hint: "Показать все рабочие файлы проекта, кроме legacy merged-файлов."
  },
  ...(["raw", "processed", "classified", "export", "other"] as const).map((value) => ({
    value,
    label: fileKindInfo[value].label,
    hint: fileKindInfo[value].hint
  }))
];

function monthNow() {
  const now = new Date();
  return { year: now.getFullYear(), month: now.getMonth() + 1 };
}

function errorText(exc: unknown) {
  return exc instanceof Error && exc.message ? exc.message : String(exc);
}

function addLoadError(errors: string[], label: string, reason: unknown) {
  errors.push(`${label}: ${errorText(reason)}`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function emptyCatalogFilter(): CatalogFilterDraft {
  return { operator: "AND", conditions: [] };
}

function parseCatalogFilterText(value: string): CatalogFilterDraft {
  const text = value.trim();
  if (!text) return emptyCatalogFilter();
  try {
    const parsed = JSON.parse(text) as unknown;
    const fromModel = parseCatalogFilterModel(parsed);
    if (fromModel) return fromModel;
  } catch {
    return parseCatalogFilterExpression(text);
  }
  return parseCatalogFilterExpression(text);
}

function parseCatalogFilterModel(model: unknown): CatalogFilterDraft | null {
  if (!isRecord(model)) return null;
  const filter = Object.values(model).find(isRecord);
  if (!filter) return null;
  const operator: CatalogFilterOperator = filter.operator === "OR" ? "OR" : "AND";
  const conditions: CatalogFilterCondition[] = [];
  const first = isRecord(filter.condition1) ? catalogConditionFromModel(filter.condition1) : null;
  const second = isRecord(filter.condition2) ? catalogConditionFromModel(filter.condition2) : null;
  if (first) conditions.push(first);
  if (second) conditions.push(second);
  if (conditions.length === 0) {
    const direct = catalogConditionFromModel(filter);
    if (direct) conditions.push(direct);
  }
  return { operator, conditions: conditions.slice(0, 2) };
}

function catalogConditionFromModel(model: Record<string, unknown>): CatalogFilterCondition | null {
  if (model.filter === undefined || model.filter === null) return null;
  return {
    type: model.type === "notContains" ? "notContains" : "contains",
    value: String(model.filter)
  };
}

function parseCatalogFilterExpression(text: string): CatalogFilterDraft {
  const tokenPattern = /([|&])?\s*(NOT)?\s*(["'])(.*?)\3/gi;
  const matches = [...text.matchAll(tokenPattern)];
  if (matches.length > 0) {
    const conditions = matches
      .map((match) => ({ type: match[2] ? "notContains" : "contains", value: match[4].trim() }) satisfies CatalogFilterCondition)
      .filter((condition) => condition.value)
      .slice(0, 2);
    const operator = matches.some((match) => match[1] === "|") ? "OR" : "AND";
    return { operator, conditions };
  }

  const separator = text.includes("|") ? "|" : text.includes("&") ? "&" : "";
  const parts = separator ? text.split(separator) : [text];
  const conditions = parts
    .map((part) => {
      let cleaned = part.trim().replace(/^['"]|['"]$/g, "");
      const isNegative = /^NOT/i.test(cleaned);
      if (isNegative) cleaned = cleaned.replace(/^NOT/i, "").trim().replace(/^['"]|['"]$/g, "");
      return { type: isNegative ? "notContains" : "contains", value: cleaned } satisfies CatalogFilterCondition;
    })
    .filter((condition) => condition.value)
    .slice(0, 2);
  return { operator: separator === "|" ? "OR" : "AND", conditions };
}

function serializeCatalogFilter(draft: CatalogFilterDraft) {
  const conditions = draft.conditions
    .map((condition) => ({ ...condition, value: condition.value.trim() }))
    .filter((condition) => condition.value)
    .slice(0, 2);
  const separator = draft.operator === "OR" ? "|" : "&";
  return conditions.map(serializeCatalogCondition).join(separator);
}

function serializeCatalogCondition(condition: CatalogFilterCondition) {
  return `${condition.type === "notContains" ? "NOT" : ""}"${condition.value.replace(/"/g, "\\\"")}"`;
}

function monthLabel(year: number, month: number) {
  return `${year}-${String(month).padStart(2, "0")}`;
}

function categoryPeriodLabel(category: Category) {
  if (!category.period_from && !category.period_to) return "весь период";
  return `${category.period_from || "с начала"} - ${category.period_to || "без конца"}`;
}

function categoryFbsLabel(category: Category) {
  return category.fbs ? "FBS" : "";
}

function runTypeLabel(type?: string) {
  return type === "monthly_sync" ? "Ежемесячное обновление" : "Историческая загрузка";
}

export function App() {
  const current = monthNow();
  const [mode, setMode] = useState<Mode>("historical_backfill");
  const [tab, setTab] = useState<Tab>("categories");
  const [projectName, setProjectName] = useState("mpstats");
  const [cookie, setCookie] = useState("");
  const [startYear, setStartYear] = useState(current.year);
  const [startMonth, setStartMonth] = useState(1);
  const [endYear, setEndYear] = useState(current.year);
  const [endMonth, setEndMonth] = useState(current.month);
  const [categories, setCategories] = useState<Category[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [categoryQuery, setCategoryQuery] = useState("");
  const [catalogRows, setCatalogRows] = useState<CategorySourceRow[]>([]);
  const [catalogPath, setCatalogPath] = useState("");
  const [catalogQuery, setCatalogQuery] = useState("");
  const [selectedCatalogId, setSelectedCatalogId] = useState<string | null>(null);
  const [pipelineSettings, setPipelineSettings] = useState<PipelineSettings>(defaultPipelineSettings);
  const [classifierRules, setClassifierRules] = useState<ClassifierRule[]>([]);
  const [rulesPath, setRulesPath] = useState("");
  const [selectedRuleId, setSelectedRuleId] = useState<string | null>(null);
  const [classifierQuery, setClassifierQuery] = useState("");
  const [externalClassifierFile, setExternalClassifierFile] = useState<File | null>(null);
  const [externalClassifierWriteXlsx, setExternalClassifierWriteXlsx] = useState(false);
  const [externalClassifierResult, setExternalClassifierResult] = useState<ClassificationResponse | null>(null);
  const [run, setRun] = useState<PipelineRun | null>(null);
  const [runs, setRuns] = useState<PipelineRun[]>([]);
  const [tasks, setTasks] = useState<DownloadTask[]>([]);
  const [taskFilter, setTaskFilter] = useState("all");
  const [files, setFiles] = useState<ProjectFile[]>([]);
  const [fileKindFilter, setFileKindFilter] = useState<FileKindFilter>("all");
  const [cube, setCube] = useState<CubeItem[]>([]);
  const [qualityProjects, setQualityProjects] = useState<QualityProject[]>([]);
  const [qualityProjectName, setQualityProjectName] = useState("");
  const [qualityReport, setQualityReport] = useState<QualityReport | null>(null);
  const [qualityLoading, setQualityLoading] = useState(false);
  const [qualityError, setQualityError] = useState<string | null>(null);
  const [qualityCopied, setQualityCopied] = useState(false);
  const [productSearch, setProductSearch] = useState("");
  const [products, setProducts] = useState<ProductSearch | null>(null);
  const [productResultTitle, setProductResultTitle] = useState("Предпросмотр БД");
  const [exportOptions, setExportOptions] = useState<ExportOptions | null>(null);
  const [exportCategoryKeys, setExportCategoryKeys] = useState<Set<string>>(new Set());
  const [exportSelectedColumns, setExportSelectedColumns] = useState<Set<string>>(new Set());
  const [exportFilters, setExportFilters] = useState<ExportFilterDraft[]>([]);
  const [exportPeriodFrom, setExportPeriodFrom] = useState("");
  const [exportPeriodTo, setExportPeriodTo] = useState("");
  const [exportOutputDir, setExportOutputDir] = useState("");
  const [exportSplitByCategory, setExportSplitByCategory] = useState(false);
  const [exportExcludedRows, setExportExcludedRows] = useState<Set<string>>(new Set());
  const [exportSortColumn, setExportSortColumn] = useState<string | null>(null);
  const [exportSortDirection, setExportSortDirection] = useState<"asc" | "desc">("asc");
  const [exportPreview, setExportPreview] = useState<ExportPreview | null>(null);
  const [exportArtifacts, setExportArtifacts] = useState<ExportArtifact[]>([]);
  const [exportConfirmLarge, setExportConfirmLarge] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [instructionOpen, setInstructionOpen] = useState(false);
  const previousRunStatusRef = useRef<string | null>(null);

  useEffect(() => {
    void initialLoad();
  }, []);

  useEffect(() => {
    if (!run?.id) return;
    void refreshRun(run.id);
  }, [taskFilter]);

  useEffect(() => {
    if (!run?.id || !isActiveRunStatus(run.status)) return;
    const id = window.setInterval(() => void refreshRun(run.id), 1500);
    return () => window.clearInterval(id);
  }, [run?.id, run?.status, taskFilter]);

  useEffect(() => {
    const previousStatus = previousRunStatusRef.current;
    previousRunStatusRef.current = run?.status ?? null;
    if (!run?.id || tab !== "cube") return;
    if (isActiveRunStatus(previousStatus) && !isActiveRunStatus(run.status)) {
      void refreshFilesAndCube();
      void loadDbPreview().catch((exc) => setError(`Предпросмотр БД: ${errorText(exc)}`));
    }
  }, [run?.id, run?.status, tab]);

  useEffect(() => {
    if (tab === "catalog" && catalogRows.length === 0) {
      void loadCategorySource();
    }
  }, [tab]);

  useEffect(() => {
    if (tab !== "cube") return;
    void refreshFilesAndCube();
    void loadDbPreview().catch((exc) => setError(`Предпросмотр БД: ${errorText(exc)}`));
  }, [tab]);

  useEffect(() => {
    if (tab !== "export") return;
    void loadExportOptions();
  }, [tab, projectName]);

  useEffect(() => {
    if (tab !== "quality") return;
    void loadQualityProjects();
  }, [tab]);

  async function initialLoad() {
    setError(null);
    const loadErrors: string[] = [];
    const [settingsResult, pipelineResult, categoryResult, rulesResult] = await Promise.allSettled([
      api.getWorkflowSettings(),
      api.getPipelineSettings(),
      api.listCategories(),
      api.getClassifierRules()
    ]);

    let loadedProjectName = projectName || "mpstats";
    if (settingsResult.status === "fulfilled") {
      const settings = settingsResult.value;
      setProjectName(settings.project_name || "mpstats");
      loadedProjectName = settings.project_name || "mpstats";
      setCookie(settings.cookie || "");
      if (settings.workflow_mode === "historical_backfill" || settings.workflow_mode === "monthly_sync") {
        setMode(settings.workflow_mode);
      }
      if (settings.start_year) setStartYear(settings.start_year);
      if (settings.start_month) setStartMonth(settings.start_month);
      if (settings.end_year) setEndYear(settings.end_year);
      if (settings.end_month) setEndMonth(settings.end_month);
    } else {
      addLoadError(loadErrors, "Настройки проекта", settingsResult.reason);
    }

    if (pipelineResult.status === "fulfilled") {
      setPipelineSettings({ ...defaultPipelineSettings, ...pipelineResult.value });
    } else {
      addLoadError(loadErrors, "Настройки pipeline", pipelineResult.reason);
    }

    if (categoryResult.status === "fulfilled") {
      setCategories(categoryResult.value.categories);
    } else {
      addLoadError(loadErrors, "Справочник категорий", categoryResult.reason);
    }

    if (rulesResult.status === "fulfilled") {
      setRulesPath(rulesResult.value.path);
      setClassifierRules(rulesResult.value.rules);
      setSelectedRuleId((prev) => prev ?? rulesResult.value.rules[0]?.id ?? null);
    } else {
      addLoadError(loadErrors, "Правила классификатора", rulesResult.reason);
    }

    try {
      const runResponse = await api.listRuns(loadedProjectName);
      setRuns(runResponse.runs);
      if (runResponse.runs[0]) {
        setRun(runResponse.runs[0]);
        const runRefreshError = await refreshRun(runResponse.runs[0].id);
        if (runRefreshError) {
          addLoadError(loadErrors, "Задачи запуска", runRefreshError);
        }
      }
    } catch (exc) {
      addLoadError(loadErrors, "История запусков", exc);
    }

    setError(loadErrors.length ? loadErrors.join(" | ") : null);
  }

  async function refreshRun(runId: string): Promise<string | null> {
    try {
      const [freshRun, taskResponse] = await Promise.all([api.getRun(runId), api.listTasks(runId, taskFilter)]);
      setRun(freshRun);
      setTasks(taskResponse.tasks);
      return null;
    } catch (exc) {
      const message = errorText(exc);
      setError(`Обновление запуска: ${message}`);
      return message;
    }
  }

  async function refreshFilesAndCube() {
    try {
      const [fileResponse, cubeResponse] = await Promise.all([api.listFiles(projectName), api.listCube(projectName)]);
      setFiles(fileResponse.files);
      setCube(cubeResponse.items);
    } catch (exc) {
      setError(`Файлы и куб: ${errorText(exc)}`);
    }
  }

  async function loadDbPreview() {
    const params = new URLSearchParams();
    params.set("project_name", projectName || "mpstats");
    params.set("limit", "100");
    const response = await api.searchProducts(params);
    setProductResultTitle("Предпросмотр БД");
    setProducts(response);
    return response;
  }

  async function loadExportOptions() {
    try {
      const response = await api.getExportOptions(projectName || "mpstats");
      setExportOptions(response);
      setExportPeriodFrom((prev) => prev || response.period_from || "");
      setExportPeriodTo((prev) => prev || response.period_to || "");
      setExportOutputDir((prev) => prev || response.default_output_dir || "");
      setExportCategoryKeys((prev) => {
        const available = new Set(response.categories.map((category) => category.category_key));
        const retained = [...prev].filter((key) => available.has(key));
        return new Set(retained.length ? retained : response.categories.map((category) => category.category_key));
      });
      setExportSelectedColumns((prev) => {
        const available = new Set(response.columns);
        const retained = [...prev].filter((column) => available.has(column));
        return new Set(retained.length ? retained : response.selected_columns);
      });
    } catch (exc) {
      setError(`Настройки выгрузки: ${errorText(exc)}`);
    }
  }

  async function loadQualityProjects() {
    setQualityLoading(true);
    setQualityError(null);
    try {
      const response = await api.listQualityProjects();
      setQualityProjects(response.projects);
      const preferred =
        response.projects.find((project) => project.project_name === qualityProjectName) ??
        response.projects.find((project) => project.project_name === projectName) ??
        response.projects[0] ??
        null;
      if (preferred) {
        setQualityProjectName(preferred.project_name);
      } else {
        setQualityProjectName("");
        setQualityReport(null);
      }
    } catch (exc) {
      setQualityError(errorText(exc));
    } finally {
      setQualityLoading(false);
    }
  }

  async function runQualityReport(targetProject = qualityProjectName) {
    if (!targetProject) {
      setQualityError("Нет проекта для проверки.");
      return null;
    }
    setQualityLoading(true);
    setQualityError(null);
    setQualityCopied(false);
    try {
      const response = await api.getQualityReport(targetProject);
      setQualityReport(response);
      return response;
    } catch (exc) {
      setQualityReport(null);
      setQualityError(errorText(exc));
      return null;
    } finally {
      setQualityLoading(false);
    }
  }

  async function copyQualitySummary() {
    if (!qualityReport) return;
    try {
      await navigator.clipboard.writeText(qualityReport.summary);
      setQualityCopied(true);
      window.setTimeout(() => setQualityCopied(false), 1600);
    } catch (exc) {
      setQualityError(`Не удалось скопировать сводку: ${errorText(exc)}`);
    }
  }

  function selectedExportColumns() {
    const available = exportOptions?.columns ?? [];
    return available.filter((column) => exportSelectedColumns.has(column));
  }

  function buildExportPayload(limit = 100): ExportPayload {
    if (!exportOptions) {
      throw new Error("Настройки выгрузки ещё не загружены.");
    }
    const selectedColumns = selectedExportColumns();
    if (!selectedColumns.length) {
      throw new Error("Выбери хотя бы одну колонку для выгрузки.");
    }
    if (!exportCategoryKeys.size) {
      throw new Error("Выбери хотя бы одну категорию для выгрузки.");
    }
    const filters: ExportColumnFilter[] = exportFilters
      .map((filter) => ({ column: filter.column, value: filter.value.trim(), match_type: filter.match_type }))
      .filter((filter) => filter.value && exportOptions.columns.includes(filter.column));
    return {
      project_name: projectName || "mpstats",
      category_keys: [...exportCategoryKeys],
      period_from: exportPeriodFrom || null,
      period_to: exportPeriodTo || null,
      selected_columns: selectedColumns,
      filters,
      excluded_row_hashes: [...exportExcludedRows],
      sort_column: exportSortColumn,
      sort_direction: exportSortDirection,
      split_by_category: exportSplitByCategory,
      output_dir: exportOutputDir || null,
      confirm_large_export: exportConfirmLarge,
      limit,
      offset: 0
    };
  }

  async function loadExportPreview() {
    const response = await api.previewExport(buildExportPayload(100));
    setExportPreview(response);
    if (response.estimated_files <= 10) setExportConfirmLarge(false);
    return response;
  }

  async function buildExportFiles() {
    const response = await api.buildExport(buildExportPayload(100));
    setExportArtifacts(response.artifacts);
    setExportPreview((prev) => (prev ? { ...prev, total: response.total, estimated_files: response.estimated_files, warnings: response.warnings } : prev));
    return response;
  }

  async function loadCategorySource() {
    try {
      const response = await api.getCategorySource();
      setCatalogPath(response.path);
      setCatalogRows(response.rows);
      setSelectedCatalogId((prev) => prev ?? response.rows[0]?.id ?? null);
    } catch (exc) {
      setError(`Справочник категорий: ${errorText(exc)}`);
    }
  }

  async function saveCategorySource() {
    const response = await api.saveCategorySource(catalogRows);
    setCatalogPath(response.path);
    setCatalogRows(response.rows);
    setSelectedCatalogId(response.rows[0]?.id ?? null);
    const categoryResponse = await api.listCategories();
    setCategories(categoryResponse.categories);
    return response;
  }

  async function runAction<T>(label: string, action: () => Promise<T>, onSuccess?: (value: T) => void) {
    setBusy(label);
    setError(null);
    setMessage(null);
    try {
      const result = await action();
      onSuccess?.(result);
      setMessage(`${label}: готово`);
      if (tab === "files" || tab === "cube") {
        await refreshFilesAndCube();
      }
      if (tab === "cube" && !passiveCubeActions.has(label)) {
        await loadDbPreview();
      }
    } catch (exc) {
      setError(errorText(exc));
    } finally {
      setBusy(null);
    }
  }

  const groupedCategories = useMemo(() => {
    const text = categoryQuery.trim().toLowerCase();
    const filtered = categories.filter((category) => {
      if (!text) return true;
      return `${category.category_name} ${category.marketplace} ${category.path} ${category.period_from ?? ""} ${category.period_to ?? ""}`.toLowerCase().includes(text);
    });
    const groups = new Map<string, Category[]>();
    for (const category of filtered) {
      groups.set(category.category_name, [...(groups.get(category.category_name) ?? []), category]);
    }
    return [...groups.entries()].sort((left, right) => left[0].localeCompare(right[0], "ru"));
  }, [categories, categoryQuery]);

  const selectedCategories = useMemo(() => categories.filter((category) => selected.has(category.category_id)), [categories, selected]);
  const filteredCatalogRows = useMemo(() => {
    const text = catalogQuery.trim().toLowerCase();
    if (!text) return catalogRows;
    return catalogRows.filter((row) => `${row.category_name} ${row.marketplace} ${row.path} ${row.filter_text}`.toLowerCase().includes(text));
  }, [catalogRows, catalogQuery]);
  const selectedCatalogRow = useMemo(() => catalogRows.find((row) => row.id === selectedCatalogId) ?? catalogRows[0] ?? null, [catalogRows, selectedCatalogId]);
  const filteredClassifierRules = useMemo(() => {
    const text = classifierQuery.trim().toLowerCase();
    if (!text) return classifierRules;
    return classifierRules.filter((rule) => {
      const haystack = [
        rule.priority,
        rule.category,
        rule.target_column,
        rule.set_value,
        rule.mode,
        rule.comment,
        ...rule.conditions.flatMap((condition) => [condition.match_field, condition.match_type, condition.pattern])
      ]
        .join(" ")
        .toLowerCase();
      return haystack.includes(text);
    });
  }, [classifierRules, classifierQuery]);
  const selectedRule = useMemo(() => classifierRules.find((rule) => rule.id === selectedRuleId) ?? classifierRules[0] ?? null, [classifierRules, selectedRuleId]);
  const filteredFiles = useMemo(() => {
    if (fileKindFilter === "all") return files;
    return files.filter((file) => file.kind === fileKindFilter);
  }, [files, fileKindFilter]);
  const monthsCount = monthCount(startYear, startMonth, endYear, endMonth);

  function toggleCategory(id: string) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function toggleExportCategory(id: string) {
    setExportCategoryKeys((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function toggleAllExportCategories() {
    const all = exportOptions?.categories.map((category) => category.category_key) ?? [];
    setExportCategoryKeys((prev) => (prev.size === all.length ? new Set() : new Set(all)));
  }

  function toggleExportColumn(column: string) {
    setExportSelectedColumns((prev) => {
      const next = new Set(prev);
      if (next.has(column)) next.delete(column);
      else next.add(column);
      return next;
    });
  }

  function toggleAllExportColumns() {
    const all = exportOptions?.columns ?? [];
    setExportSelectedColumns((prev) => (prev.size === all.length ? new Set() : new Set(all)));
  }

  function toggleExportSort(column: string, direction?: SortDirection) {
    setExportSortColumn(column);
    setExportSortDirection((current) => direction ?? (exportSortColumn === column && current === "asc" ? "desc" : "asc"));
  }

  function addExportFilter() {
    const firstColumn = exportOptions?.columns[0] ?? "";
    setExportFilters((prev) => [...prev, { id: `filter-${Date.now()}-${prev.length}`, column: firstColumn, match_type: "contains", value: "" }]);
  }

  function updateExportFilter(id: string, patch: Partial<ExportFilterDraft>) {
    setExportFilters((prev) => prev.map((filter) => (filter.id === id ? { ...filter, ...patch } : filter)));
  }

  function deleteExportFilter(id: string) {
    setExportFilters((prev) => prev.filter((filter) => filter.id !== id));
  }

  function excludeExportRow(rowHash: string) {
    if (!rowHash) return;
    setExportExcludedRows((prev) => new Set([...prev, rowHash]));
    setExportPreview((prev) =>
      prev
        ? {
            ...prev,
            rows: prev.rows.filter((row) => String(row["__row_hash"] ?? "") !== rowHash),
            total: Math.max(0, prev.total - 1)
          }
        : prev
    );
  }

  function selectGroup(items: Category[]) {
    setSelected((prev) => {
      const next = new Set(prev);
      const allSelected = items.every((item) => next.has(item.category_id));
      for (const item of items) {
        if (allSelected) next.delete(item.category_id);
        else next.add(item.category_id);
      }
      return next;
    });
  }

  function setPipelineValue<K extends keyof PipelineSettings>(key: K, value: PipelineSettings[K]) {
    setPipelineSettings((prev) => ({ ...prev, [key]: value }));
  }

  function updateCatalogRow(id: string, patch: Partial<CategorySourceRow>) {
    setCatalogRows((prev) => prev.map((row) => (row.id === id ? { ...row, ...patch } : row)));
  }

  function addCatalogRow() {
    const id = `new-category-${Date.now()}`;
    const next: CategorySourceRow = {
      id,
      active: true,
      category_name: "",
      marketplace: "WB",
      fbs: false,
      period_from: "",
      period_to: "",
      comment: "",
      path: "",
      filter_text: "",
      path2: "",
      filter2_text: "",
      actualization: ""
    };
    setCatalogRows((prev) => [next, ...prev]);
    setSelectedCatalogId(id);
  }

  function deleteCatalogRow(id: string) {
    setCatalogRows((prev) => prev.filter((row) => row.id !== id));
    setSelectedCatalogId((prev) => (prev === id ? null : prev));
  }

  function updateRule(id: string, patch: Partial<ClassifierRule>) {
    setClassifierRules((prev) => prev.map((rule) => (rule.id === id ? { ...rule, ...patch } : rule)));
  }

  function addRule() {
    const id = `new-rule-${Date.now()}`;
    const next: ClassifierRule = {
      id,
      active: true,
      priority: nextPriority(classifierRules),
      category: "*",
      target_column: "",
      set_value: "",
      mode: "fill_empty",
      comment: "",
      conditions: [emptyCondition()]
    };
    setClassifierRules((prev) => [...prev, next]);
    setSelectedRuleId(id);
  }

  function addRuleFromPreset(preset: ClassifierPreset) {
    const id = `preset-rule-${Date.now()}`;
    const base: ClassifierRule = {
      id,
      active: true,
      priority: nextPriority(classifierRules),
      category: "*",
      target_column: "Подкатегория",
      set_value: "",
      mode: "fill_empty",
      comment: "",
      conditions: [emptyCondition()]
    };
    const next =
      preset === "sku-to-subcategory"
        ? { ...base, conditions: [{ join_with_prev: "and", match_field: "SKU", match_type: "equals", pattern: "" }] }
        : preset === "name-to-brand"
          ? { ...base, target_column: "Бренд", conditions: [{ join_with_prev: "and", match_field: "Название", match_type: "contains", pattern: "" }] }
          : preset === "otherwise"
            ? { ...base, target_column: "Подкатегория", set_value: "Прочее", conditions: [{ join_with_prev: "and", match_field: "", match_type: "otherwise", pattern: "" }] }
            : { ...base, conditions: [{ join_with_prev: "and", match_field: "Название", match_type: "contains", pattern: "" }] };
    setClassifierRules((prev) => [...prev, next]);
    setSelectedRuleId(id);
  }

  function duplicateRule(rule: ClassifierRule) {
    const id = `copy-rule-${Date.now()}`;
    setClassifierRules((prev) => [...prev, { ...rule, id, priority: nextPriority(prev), conditions: rule.conditions.map((condition) => ({ ...condition })) }]);
    setSelectedRuleId(id);
  }

  function deleteRule(id: string) {
    setClassifierRules((prev) => prev.filter((rule) => rule.id !== id));
    setSelectedRuleId((prev) => (prev === id ? null : prev));
  }

  async function classifyExternalFile() {
    if (!externalClassifierFile) {
      throw new Error("Выбери CSV или XLSX файл для классификации.");
    }
    return api.classifyExternalFile(projectName, externalClassifierFile, externalClassifierWriteXlsx);
  }

  function updateCondition(ruleId: string, index: number, patch: Partial<ClassifierCondition>) {
    setClassifierRules((prev) =>
      prev.map((rule) => {
        if (rule.id !== ruleId) return rule;
        return {
          ...rule,
          conditions: rule.conditions.map((condition, conditionIndex) =>
            conditionIndex === index ? { ...condition, ...patch, join_with_prev: index === 0 ? "and" : patch.join_with_prev ?? condition.join_with_prev } : condition
          )
        };
      })
    );
  }

  function addCondition(ruleId: string) {
    setClassifierRules((prev) => prev.map((rule) => (rule.id === ruleId ? { ...rule, conditions: [...rule.conditions, emptyCondition("and")] } : rule)));
  }

  function deleteCondition(ruleId: string, index: number) {
    setClassifierRules((prev) =>
      prev.map((rule) => {
        if (rule.id !== ruleId || rule.conditions.length <= 1) return rule;
        const conditions = rule.conditions.filter((_, conditionIndex) => conditionIndex !== index);
        if (conditions[0]) conditions[0] = { ...conditions[0], join_with_prev: "and" };
        return { ...rule, conditions };
      })
    );
  }

  async function createPlan() {
    await saveCurrentWorkflowSettings();
    const created =
      mode === "monthly_sync"
        ? await api.monthlySync({ project_name: projectName, settings: pipelineSettings, start_immediately: false, wait: false })
        : await api.createPlan({
            project_name: projectName,
            run_type: "historical_backfill",
            category_ids: [...selected],
            start_year: startYear,
            start_month: startMonth,
            end_year: endYear,
            end_month: endMonth,
            settings: pipelineSettings
          });
    setRun(created);
    setTab("plan");
    await refreshRun(created.id);
    const runResponse = await api.listRuns(projectName);
    setRuns(runResponse.runs);
  }

  async function syncNewMonth() {
    await saveCurrentWorkflowSettings();
    const created = await api.monthlySync({ project_name: projectName, settings: pipelineSettings, start_immediately: true, wait: false });
    setRun(created);
    setTab("plan");
    await refreshRun(created.id);
  }

  function saveCurrentWorkflowSettings() {
    return api.saveWorkflowSettings({
      cookie,
      project_name: projectName,
      workflow_mode: mode,
      start_year: startYear,
      start_month: startMonth,
      end_year: endYear,
      end_month: endMonth
    });
  }

  return (
    <div className="workflow-shell">
      <header className="app-header">
        <div className="brand">
          <div className="brand-mark">MP</div>
          <div>
            <h1>MPStats Workflow</h1>
            <p>Идемпотентный план по категориям, маркетплейсам и месяцам.</p>
          </div>
        </div>
        <div className="mode-switch" aria-label="Режим запуска">
          <button className={mode === "historical_backfill" ? "active" : ""} onClick={() => setMode("historical_backfill")}>
            <History size={17} />
            Историческая загрузка
          </button>
          <button className={mode === "monthly_sync" ? "active" : ""} onClick={() => setMode("monthly_sync")}>
            <RefreshCcw size={17} />
            Ежемесячное обновление
          </button>
        </div>
        <button className="help-button" onClick={() => setInstructionOpen(true)}>
          <BookOpen size={17} />
          Инструкция
        </button>
      </header>

      {error ? <Notice tone="error" text={error} onClose={() => setError(null)} /> : null}
      {message ? <Notice tone="success" text={message} onClose={() => setMessage(null)} /> : null}

      <main className="workflow-grid">
        <aside className="left-rail">
          <section className="panel">
            <SectionTitle icon={<Settings />} title="Проект и доступ" hint="Project name разделяет файлы, manifest и записи БД. Cookie нужен только для скачивания из MPStats." />
            <div className="form-grid">
              <label>
                <FieldLabel text="Название проекта" hint="Имя рабочего набора. По нему приложение разделяет планы, файлы и записи в БД; лучше писать коротко и понятно, например «Сахар»." />
                <input value={projectName} onChange={(event) => setProjectName(event.target.value)} />
              </label>
              <label>
                <FieldLabel text="MPStats cookie" hint="Cookie текущей авторизованной сессии MPStats. Нужен только для скачивания; если устарел, задачи будут падать с ошибкой доступа." />
                <textarea className="cookie-input" value={cookie} onChange={(event) => setCookie(event.target.value)} placeholder="Вставь cookie из MPStats" />
              </label>
              {mode === "historical_backfill" ? (
                <div className="period-grid">
                  <label>
                    <FieldLabel text="С года" hint="Первый год периода исторической загрузки. Используется при создании плана задач." />
                    <input type="number" value={startYear} onChange={(event) => setStartYear(Number(event.target.value))} />
                  </label>
                  <label>
                    <FieldLabel text="С месяца" hint="Первый месяц периода, число от 1 до 12. Например, 1 — январь." />
                    <input type="number" min={1} max={12} value={startMonth} onChange={(event) => setStartMonth(Number(event.target.value))} />
                  </label>
                  <label>
                    <FieldLabel text="По год" hint="Последний год периода исторической загрузки. Конечный месяц включается в план." />
                    <input type="number" value={endYear} onChange={(event) => setEndYear(Number(event.target.value))} />
                  </label>
                  <label>
                    <FieldLabel text="По месяц" hint="Последний месяц периода, число от 1 до 12. Период считается включительно." />
                    <input type="number" min={1} max={12} value={endMonth} onChange={(event) => setEndMonth(Number(event.target.value))} />
                  </label>
                </div>
              ) : (
                <div className="sync-note">
                  <strong>Новый месяц определяется автоматически.</strong>
                  <span>Берём последний сохранённый месяц и создаём задачи на следующий.</span>
                </div>
              )}
            </div>
            <button
              className="primary-button"
              title="Сохраняет project name, cookie, период, режим и настройки pipeline в локальную DuckDB."
              onClick={() =>
                void runAction("Сохранение настроек", async () => {
                  await saveCurrentWorkflowSettings();
                  return api.savePipelineSettings(pipelineSettings);
                })
              }
            >
              <Save size={17} />
              Сохранить настройки
            </button>
          </section>

          <section className="panel">
            <SectionTitle icon={<ListChecks />} title="Настройки pipeline" hint="Эти параметры управляют повторными запусками: что можно пересобирать, сколько ждать MPStats и какие файлы считать готовыми." />
            <div className="settings-grid">
              <Toggle label="Перескачивать raw" hint="Если включено, приложение заново скачает исходные CSV из MPStats даже когда raw-файл уже есть. Полезно, если отчёт в MPStats изменился." checked={pipelineSettings.overwrite_raw} onChange={(value) => setPipelineValue("overwrite_raw", value)} />
              <Toggle label="Пересобирать processed" hint="Если включено, приложение заново обработает raw-файлы: приведёт колонки, пересчитает вес, объём и классификацию. Скачивание при этом не обязательно повторяется." checked={pipelineSettings.overwrite_processed} onChange={(value) => setPipelineValue("overwrite_processed", value)} />
              <Toggle label="Перезаписывать БД" hint="Если включено, сохранённый срез для той же связки проект + месяц + маркетплейс + категория будет заменён. Используй осторожно, когда нужно обновить уже загруженные данные." checked={pipelineSettings.overwrite_db} onChange={(value) => setPipelineValue("overwrite_db", value)} />
              <label>
                <FieldLabel text="Количество повторов" hint="Сколько раз повторить задачу после временной ошибки скачивания или подготовки отчёта MPStats. 0 — не повторять." />
                <input type="number" min={0} value={pipelineSettings.retry_count} onChange={(event) => setPipelineValue("retry_count", Number(event.target.value))} />
              </label>
              <label>
                <FieldLabel text="Таймаут, сек" hint="Сколько секунд ждать, пока MPStats подготовит отчёт или ответит на запрос. Если отчёты большие, лучше увеличить." />
                <input type="number" min={30} value={pipelineSettings.timeout_seconds} onChange={(event) => setPipelineValue("timeout_seconds", Number(event.target.value))} />
              </label>
              <label>
                <FieldLabel text="Пауза между запросами" hint="Пауза между обращениями к MPStats в секундах. Помогает не упираться в ограничения сервиса и снижает риск временных ошибок." />
                <input type="number" min={0} value={pipelineSettings.pause_between_requests} onChange={(event) => setPipelineValue("pause_between_requests", Number(event.target.value))} />
              </label>
              <label>
                <FieldLabel text="Параллельные скачивания" hint="Сколько задач скачивания можно запускать одновременно. Безопасный режим — 1; увеличивай только если MPStats стабильно отвечает." />
                <input type="number" min={1} max={8} value={pipelineSettings.max_parallel_downloads} onChange={(event) => setPipelineValue("max_parallel_downloads", Number(event.target.value))} />
              </label>
              <label>
                <FieldLabel text="Максимальный вес, кг" hint="Порог проверки парсинга веса из названия товара. Значения выше порога считаются подозрительными и помечаются как аномалии." />
                <input type="number" min={1} value={pipelineSettings.max_weight_kg} onChange={(event) => setPipelineValue("max_weight_kg", Number(event.target.value))} />
              </label>
            </div>
          </section>

          <section className="panel">
            <SectionTitle icon={<FileSpreadsheet />} title="Правила и справочник" hint="Редактирование вынесено в центральные вкладки, чтобы не работать с сырым CSV или JSON." />
            <div className="mini-actions">
              <button className="ghost-button" title="Открывает полноценный редактор правил классификатора." onClick={() => setTab("classifier")}>
                <FileSpreadsheet size={17} />
                Правила
              </button>
              <button className="ghost-button" title="Открывает CSV-справочник категорий и путей." onClick={() => { setTab("catalog"); void loadCategorySource(); }}>
                <FolderSync size={17} />
                Справочник
              </button>
            </div>
          </section>
        </aside>

        <section className="center-stage">
          <nav className="tabs">
            <button className={tab === "categories" ? "active" : ""} title="Выбор активных путей для исторической загрузки." onClick={() => setTab("categories")}>Категории</button>
            <button className={tab === "catalog" ? "active" : ""} title="Редактор CSV-справочника категорий." onClick={() => { setTab("catalog"); void loadCategorySource(); }}>Справочник</button>
            <button className={tab === "plan" ? "active" : ""} onClick={() => setTab("plan")}>План загрузки</button>
            <button className={tab === "files" ? "active" : ""} onClick={() => { setTab("files"); void refreshFilesAndCube(); }}>Файлы</button>
            <button className={tab === "cube" ? "active" : ""} onClick={() => setTab("cube")}>БД / Куб</button>
            <button className={tab === "export" ? "active" : ""} title="Подготовить XLSX из сохранённого куба." onClick={() => setTab("export")}>Выгрузка</button>
            <button className={tab === "quality" ? "active" : ""} title="Проверить итоговые CSV перед работой с отчётом." onClick={() => setTab("quality")}>Качество данных</button>
            <button className={tab === "classifier" ? "active" : ""} title="Правила классификатора без ручного JSON." onClick={() => setTab("classifier")}>Классификатор</button>
          </nav>

          {tab === "categories" ? (
            <section className="panel stage-panel">
              <SectionTitle icon={<ListChecks />} title="Категории" meta={`${selected.size} путей выбрано`} hint="Здесь выбираются активные пути из справочника для создания плана загрузки." />
              <div className="toolbar">
                <label className="search-field">
                  <Search size={17} />
                  <input value={categoryQuery} onChange={(event) => setCategoryQuery(event.target.value)} placeholder="Категория, marketplace или путь" />
                </label>
                <button className="ghost-button" onClick={() => void runAction("Синхронизация справочника", () => api.syncCategories(), () => void initialLoad())}>
                  <FolderSync size={17} />
                  Обновить справочник
                </button>
              </div>
              <div className="category-list">
                {groupedCategories.map(([group, items]) => (
                  <div className="category-group" key={group}>
                    <button className="group-button" onClick={() => selectGroup(items)}>
                      <span>{group}</span>
                      <small>{items.filter((item) => selected.has(item.category_id)).length}/{items.length}</small>
                    </button>
                    <div className="category-items">
                      {items.map((category) => (
                        <label className="category-row" key={category.category_id}>
                          <input type="checkbox" checked={selected.has(category.category_id)} onChange={() => toggleCategory(category.category_id)} />
                          <span>
                            <strong>{category.marketplace}</strong>
                            <em>{category.path}</em>
                            {categoryFbsLabel(category) ? <small>{categoryFbsLabel(category)}</small> : null}
                            <small>{categoryPeriodLabel(category)}</small>
                          </span>
                        </label>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </section>
          ) : null}

          {tab === "catalog" ? (
            <CategorySourceEditor
              rows={filteredCatalogRows}
              selectedRow={selectedCatalogRow}
              sourcePath={catalogPath}
              query={catalogQuery}
              onQueryChange={setCatalogQuery}
              onSelect={setSelectedCatalogId}
              onAdd={addCatalogRow}
              onDelete={deleteCatalogRow}
              onReload={loadCategorySource}
              onSave={() => void runAction("Сохранение справочника", saveCategorySource)}
              onChange={updateCatalogRow}
            />
          ) : null}

          {tab === "plan" ? (
            <section className="panel stage-panel">
              <SectionTitle icon={<Table2 />} title="План загрузки" meta={run ? `${run.total_tasks || tasks.length} задач` : "план не создан"} />
              <div className="toolbar wrap">
                {taskFilters.map(([value, label]) => (
                  <button key={value} className={`chip-button ${taskFilter === value ? "active" : ""}`} onClick={() => setTaskFilter(value)}>
                    {label}
                  </button>
                ))}
              </div>
              {tasks.length ? <TaskTable tasks={tasks} onRetry={(taskId) => void runAction("Повтор задачи", () => api.retryTask(taskId), (fresh) => { setRun(fresh); void refreshRun(fresh.id); })} /> : <Empty text="Создай план, и здесь появятся задачи category × marketplace × month." />}
            </section>
          ) : null}

          {tab === "files" ? (
            <section className="panel stage-panel">
              <SectionTitle
                icon={<Archive />}
                title="Файлы"
                meta={`${filteredFiles.length}/${files.length} файлов`}
                hint="Рабочий путь web-app: raw -> processed -> classified -> БД / Куб -> XLSX выгрузка. Legacy merged-файлы здесь скрыты, куб теперь хранится в БД."
              />
              <div className="toolbar wrap file-kind-toolbar">
                {fileKindFilters.map((filter) => (
                  <button
                    key={filter.value}
                    className={fileKindFilter === filter.value ? "chip-button active" : "chip-button"}
                    type="button"
                    data-tooltip={filter.hint}
                    title={filter.hint}
                    onClick={() => setFileKindFilter(filter.value)}
                  >
                    {filter.label}
                    <CircleHelp className="chip-help" size={14} aria-hidden="true" />
                  </button>
                ))}
              </div>
              {filteredFiles.length ? <FilesTable files={filteredFiles} /> : <Empty text={files.length ? "В выбранном типе файлов пока нет." : "Файлы появятся после скачивания и обработки."} />}
            </section>
          ) : null}

          {tab === "cube" ? (
            <section className="panel stage-panel">
              <SectionTitle icon={<Database />} title="БД / Куб" meta={`${cube.length} срезов`} />
              <div className="toolbar">
                <label className="search-field">
                  <Search size={17} />
                  <input value={productSearch} onChange={(event) => setProductSearch(event.target.value)} placeholder="Поиск SKU, название, бренд" />
                </label>
                <button
                  className="ghost-button"
                  title="Показать первые 100 строк из актуальной таблицы товаров."
                  onClick={() => void runAction("Предпросмотр БД", loadDbPreview)}
                >
                  <Database size={17} />
                  Предпросмотр
                </button>
                <button
                  className="ghost-button"
                  onClick={() => {
                    const params = new URLSearchParams();
                    const query = productSearch.trim();
                    params.set("project_name", projectName || "mpstats");
                    if (query) params.set("query", query);
                    params.set("limit", "100");
                    void runAction("Поиск в БД", () => api.searchProducts(params), (result) => {
                      setProductResultTitle(query ? "Поиск в БД" : "Предпросмотр БД");
                      setProducts(result);
                    });
                  }}
                >
                  <Search size={17} />
                  Найти
                </button>
              </div>
              {cube.length ? <CubeTable items={cube} /> : <Empty text="После сохранения задач здесь появится registry куба." />}
              {products ? (
                <div className="db-results">
                  <h3>{productResultTitle}: {products.total} записей</h3>
                  <SimpleTable columns={visibleProductColumns(products.columns)} rows={products.rows} />
                </div>
              ) : null}
            </section>
          ) : null}

          {tab === "export" ? (
            <ExportWorkspace
              options={exportOptions}
              selectedCategoryKeys={exportCategoryKeys}
              selectedColumns={exportSelectedColumns}
              filters={exportFilters}
              periodFrom={exportPeriodFrom}
              periodTo={exportPeriodTo}
              outputDir={exportOutputDir}
              splitByCategory={exportSplitByCategory}
              excludedCount={exportExcludedRows.size}
              sortColumn={exportSortColumn}
              sortDirection={exportSortDirection}
              preview={exportPreview}
              artifacts={exportArtifacts}
              confirmLarge={exportConfirmLarge}
              busy={Boolean(busy)}
              onReloadOptions={loadExportOptions}
              onToggleCategory={toggleExportCategory}
              onToggleAllCategories={toggleAllExportCategories}
              onToggleColumn={toggleExportColumn}
              onToggleAllColumns={toggleAllExportColumns}
              onAddFilter={addExportFilter}
              onFilterChange={updateExportFilter}
              onDeleteFilter={deleteExportFilter}
              onPeriodFromChange={setExportPeriodFrom}
              onPeriodToChange={setExportPeriodTo}
              onOutputDirChange={setExportOutputDir}
              onSplitByCategoryChange={setExportSplitByCategory}
              onConfirmLargeChange={setExportConfirmLarge}
              onSort={toggleExportSort}
              onClearSort={() => {
                setExportSortColumn(null);
                setExportSortDirection("asc");
              }}
              onExcludeRow={excludeExportRow}
              onClearExcluded={() => setExportExcludedRows(new Set())}
              onPreview={() => void runAction("Предпросмотр выгрузки", loadExportPreview)}
              onBuild={() => void runAction("Выгрузка XLSX", buildExportFiles)}
            />
          ) : null}

          {tab === "quality" ? (
            <DataQualityWorkspace
              projects={qualityProjects}
              selectedProject={qualityProjectName}
              report={qualityReport}
              loading={qualityLoading}
              error={qualityError}
              copied={qualityCopied}
              onProjectChange={(value) => {
                setQualityProjectName(value);
                setQualityReport(null);
                setQualityError(null);
              }}
              onReloadProjects={loadQualityProjects}
              onRun={() => void runQualityReport()}
              onCopySummary={() => void copyQualitySummary()}
            />
          ) : null}

          {tab === "classifier" ? (
            <ClassifierRulesEditor
              rules={filteredClassifierRules}
              totalRules={classifierRules.length}
              selectedRule={selectedRule}
              rulesPath={rulesPath}
              query={classifierQuery}
              onQueryChange={setClassifierQuery}
              onSelect={setSelectedRuleId}
              onAdd={addRule}
              onAddPreset={addRuleFromPreset}
              onDuplicate={duplicateRule}
              onDelete={deleteRule}
              onChange={updateRule}
              onConditionChange={updateCondition}
              onAddCondition={addCondition}
              onDeleteCondition={deleteCondition}
              onSave={() => void runAction("Сохранение правил классификатора", () => api.saveClassifierRules(classifierRules), (response) => {
                setRulesPath(response.path);
                setClassifierRules(response.rules);
                setSelectedRuleId(response.rules[0]?.id ?? null);
              })}
              externalFile={externalClassifierFile}
              externalWriteXlsx={externalClassifierWriteXlsx}
              externalResult={externalClassifierResult}
              busy={Boolean(busy)}
              onExternalFileChange={(file) => {
                setExternalClassifierFile(file);
                setExternalClassifierResult(null);
              }}
              onExternalWriteXlsxChange={setExternalClassifierWriteXlsx}
              onExternalClassify={() => void runAction("Классификация внешнего файла", classifyExternalFile, setExternalClassifierResult)}
            />
          ) : null}
        </section>

        <aside className="right-rail">
          <section className="panel flow-panel">
            <SectionTitle icon={<Play />} title="Текущий запуск" />
            <RunSummary run={run} selectedCount={selectedCategories.length} monthsCount={monthsCount} mode={mode} />
            <div className="action-stack">
              <button className="action-button" disabled={Boolean(busy) || (mode === "historical_backfill" && !selected.size)} onClick={() => void runAction("Создание плана", createPlan)}>
                <ListChecks size={20} />
                <span><strong>Создать план</strong><small>Проверить manifest и файлы</small></span>
              </button>
              <button className="action-button" disabled={Boolean(busy) || !run?.id} onClick={() => run?.id && void runAction("Запуск", () => api.startRun(run.id), (fresh) => { setRun(fresh); void refreshRun(fresh.id); })}>
                <Play size={20} />
                <span><strong>Запустить</strong><small>Ожидающие задачи и ошибки</small></span>
              </button>
              <button className="action-button" disabled={Boolean(busy) || !run?.id} onClick={() => run?.id && void runAction("Пауза", () => api.pauseRun(run.id), setRun)}>
                <Pause size={20} />
                <span><strong>Пауза</strong><small>Остановится между стадиями</small></span>
              </button>
              <button className="action-button" disabled={Boolean(busy) || !run?.id} onClick={() => run?.id && void runAction("Продолжение", () => api.resumeRun(run.id), (fresh) => { setRun(fresh); void refreshRun(fresh.id); })}>
                <SkipForward size={20} />
                <span><strong>Продолжить</strong><small>Незавершённый запуск</small></span>
              </button>
              <button className="action-button" disabled={Boolean(busy) || !run?.id} onClick={() => run?.id && void runAction("Повтор ошибок", () => api.retryErrors(run.id), (fresh) => { setRun(fresh); void refreshRun(fresh.id); })}>
                <RotateCcw size={20} />
                <span><strong>Повторить ошибки</strong><small>Только задачи с ошибкой</small></span>
              </button>
              <button className="action-button" disabled={Boolean(busy) || !run?.id} onClick={() => run?.id && void runAction("Сборка куба", () => api.rebuildCube(run.id), (fresh) => { setRun(fresh); void refreshRun(fresh.id); })}>
                <Database size={20} />
                <span><strong>Собрать куб из готовых файлов</strong><small>Без повторного скачивания</small></span>
              </button>
              <button className="action-button" disabled={Boolean(busy) || !run?.id} onClick={() => run?.id && void runAction("Повторная классификация куба", () => api.reclassifyCube(run.id), (fresh) => { setRun(fresh); void refreshRun(fresh.id); })}>
                <RotateCcw size={20} />
                <span><strong>Переклассифицировать куб</strong><small>Перезаписать classified и БД</small></span>
              </button>
              <button className="action-button accent" disabled={Boolean(busy)} onClick={() => void runAction("Синхронизация нового месяца", syncNewMonth)}>
                <RefreshCcw size={20} />
                <span><strong>Синхронизировать новый месяц</strong><small>Следующий месяц по registry</small></span>
              </button>
            </div>
          </section>

          <section className="panel history-panel">
            <SectionTitle icon={<History />} title="Последние планы" />
            <div className="run-list">
              {runs.map((item) => (
                <button key={item.id} className={`run-row ${run?.id === item.id ? "active" : ""}`} onClick={() => { setRun(item); setTab("plan"); void refreshRun(item.id); }}>
                  <span><strong>{runTypeLabel(item.run_type)}</strong><small>{item.period_from} - {item.period_to}</small></span>
                  <Badge value={item.status} />
                </button>
              ))}
              {!runs.length ? <Empty text="Планов пока нет." /> : null}
            </div>
          </section>
        </aside>
      </main>
      {instructionOpen ? <ProductInstructionModal onClose={() => setInstructionOpen(false)} /> : null}
    </div>
  );
}

function monthCount(startYear: number, startMonth: number, endYear: number, endMonth: number) {
  if ((startYear > endYear) || (startYear === endYear && startMonth > endMonth)) return 0;
  return (endYear - startYear) * 12 + endMonth - startMonth + 1;
}

function emptyCondition(join: "and" | "or" = "and"): ClassifierCondition {
  return { join_with_prev: join, match_field: "", match_type: "contains", pattern: "" };
}

function nextPriority(rules: ClassifierRule[]) {
  const maxPriority = rules.reduce((max, rule) => Math.max(max, Number(rule.priority) || 0), 0);
  return maxPriority + 10;
}

function matchTypeLabel(type: string) {
  return matchTypes.find(([value]) => value === type)?.[1] ?? type;
}

function ruleModeLabel(mode: string) {
  return ruleModes.find(([value]) => value === mode)?.[1] ?? mode;
}

function ruleConditionSummary(rule: ClassifierRule) {
  const first = rule.conditions[0];
  if (first?.match_type === "otherwise") {
    const category = rule.category && rule.category !== "*" ? ` [${rule.category}]` : "";
    return `иначе${category}`;
  }
  if (!first?.match_field && !first?.pattern) return "условие не задано";
  const category = rule.category && rule.category !== "*" ? ` [${rule.category}]` : "";
  return `${first.match_field || "поле"} ${matchTypeLabel(first.match_type)} "${first.pattern || "..."}"${category}`;
}

function ruleActionSummary(rule: ClassifierRule) {
  return `${rule.target_column || "колонка"} = ${rule.set_value || "..."}`;
}

function SectionTitle(props: { icon: ReactNode; title: string; meta?: string; hint?: string }) {
  return (
    <div className="section-title">
      <div>{props.icon}<h2>{props.title}</h2>{props.hint ? <Hint text={props.hint} /> : null}</div>
      {props.meta ? <span>{props.meta}</span> : null}
    </div>
  );
}

function Hint(props: { text: string }) {
  return <button type="button" className="hint" data-tooltip={props.text} aria-label={props.text}><CircleHelp size={15} /></button>;
}

function FieldLabel(props: { text: string; hint: string }) {
  return <span className="field-label">{props.text}<Hint text={props.hint} /></span>;
}

function Toggle(props: { label: string; hint?: string; checked: boolean; onChange: (value: boolean) => void }) {
  return (
    <label className="check-row">
      <input type="checkbox" checked={props.checked} onChange={(event) => props.onChange(event.target.checked)} />
      {props.hint ? <FieldLabel text={props.label} hint={props.hint} /> : props.label}
    </label>
  );
}

function RunSummary(props: { run: PipelineRun | null; selectedCount: number; monthsCount: number; mode: Mode }) {
  const run = props.run;
  const total = run?.total_tasks ?? 0;
  const completed = run?.completed_tasks ?? 0;
  const failed = run?.failed_tasks ?? 0;
  const remaining = run?.remaining_tasks ?? Math.max(0, total - completed - failed);
  const progress = run?.progress ?? (total ? Math.round((completed / total) * 100) : 0);
  return (
    <div className="run-summary">
      <div className="run-heading">
        <span>{run ? runTypeLabel(run.run_type) : runTypeLabel(props.mode)}</span>
        <Badge value={run?.status ?? "planned"} />
      </div>
      <div className="progress"><span style={{ width: `${Math.min(100, progress)}%` }} /></div>
      <div className="summary-grid">
        <Metric label="Период" value={run ? `${run.period_from} - ${run.period_to}` : "не создан"} />
        <Metric label="Категории" value={String(run?.category_count ?? props.selectedCount)} />
        <Metric label="Месяцы" value={String(run?.month_count ?? props.monthsCount)} />
        <Metric label="Всего" value={String(total)} />
        <Metric label="Готово" value={String(completed)} />
        <Metric label="Ошибок" value={String(failed)} />
        <Metric label="Осталось" value={String(remaining)} />
      </div>
      <div className="current-step">
        <small>Текущий шаг</small>
        <strong>{run?.current_step || "ожидание"}</strong>
      </div>
    </div>
  );
}

function Metric(props: { label: string; value: string }) {
  return <div className="metric"><span>{props.label}</span><strong>{props.value}</strong></div>;
}

function Badge(props: { value: string }) {
  const value = props.value || "pending";
  return <span className={`badge ${value}`}>{statusLabels[value] ?? value}</span>;
}

function FilesTable(props: { files: ProjectFile[] }) {
  return (
    <FilterableTable
      rows={props.files}
      rowKey={(file) => file.path}
      emptyText="Нет файлов по текущим фильтрам."
      columns={[
        {
          id: "kind",
          label: "Тип",
          value: (file) => statusLabels[file.kind] ?? file.kind,
          render: (file) => <Badge value={file.kind} />
        },
        {
          id: "path",
          label: "Путь",
          value: (file) => file.relative_path ?? file.path,
          title: (file) => file.path
        },
        {
          id: "size",
          label: "Размер",
          value: (file) => Math.round(file.size / 1024),
          render: (file) => `${Math.round(file.size / 1024)} KB`,
          numeric: true
        },
        {
          id: "updated",
          label: "Обновлён",
          value: (file) => new Date(file.updated_at).toLocaleString("ru-RU"),
          sortValue: (file) => new Date(file.updated_at).getTime(),
          numeric: true
        }
      ]}
    />
  );
}

function TaskTable(props: { tasks: DownloadTask[]; onRetry: (taskId: string) => void }) {
  return (
    <FilterableTable
      className="task-table"
      rows={props.tasks}
      rowKey={(task) => task.id}
      emptyText="Нет задач по текущим фильтрам."
      columns={[
        { id: "category", label: "Категория", value: (task) => task.category_name, title: (task) => task.category_path },
        { id: "marketplace", label: "Marketplace", value: (task) => task.marketplace },
        { id: "month", label: "Месяц", value: (task) => monthLabel(task.year, task.month) },
        {
          id: "download",
          label: "Скачивание",
          value: (task) => statusLabels[task.download_status] ?? task.download_status,
          render: (task) => <Badge value={task.download_status} />
        },
        {
          id: "process",
          label: "Обработка",
          value: (task) => statusLabels[task.process_status] ?? task.process_status,
          render: (task) => <Badge value={task.process_status} />
        },
        {
          id: "classify",
          label: "Классиф.",
          value: (task) => statusLabels[task.classify_status] ?? task.classify_status,
          render: (task) => <Badge value={task.classify_status} />
        },
        {
          id: "save",
          label: "БД",
          value: (task) => statusLabels[task.save_status] ?? task.save_status,
          render: (task) => <Badge value={task.save_status} />
        },
        {
          id: "error",
          label: "Ошибка",
          value: (task) => task.error_message ?? "-",
          render: (task) => task.error_message ? <span className="error-text"><AlertTriangle size={14} />{task.error_message}</span> : "-"
        },
        {
          id: "action",
          label: "",
          value: () => "",
          render: (task) => <button className="tiny-button" onClick={() => props.onRetry(task.id)}>повтор</button>,
          filterable: false,
          sortable: false
        }
      ]}
    />
  );
}

function ExportWorkspace(props: {
  options: ExportOptions | null;
  selectedCategoryKeys: Set<string>;
  selectedColumns: Set<string>;
  filters: ExportFilterDraft[];
  periodFrom: string;
  periodTo: string;
  outputDir: string;
  splitByCategory: boolean;
  excludedCount: number;
  sortColumn: string | null;
  sortDirection: "asc" | "desc";
  preview: ExportPreview | null;
  artifacts: ExportArtifact[];
  confirmLarge: boolean;
  busy: boolean;
  onReloadOptions: () => void;
  onToggleCategory: (id: string) => void;
  onToggleAllCategories: () => void;
  onToggleColumn: (column: string) => void;
  onToggleAllColumns: () => void;
  onAddFilter: () => void;
  onFilterChange: (id: string, patch: Partial<ExportFilterDraft>) => void;
  onDeleteFilter: (id: string) => void;
  onPeriodFromChange: (value: string) => void;
  onPeriodToChange: (value: string) => void;
  onOutputDirChange: (value: string) => void;
  onSplitByCategoryChange: (value: boolean) => void;
  onConfirmLargeChange: (value: boolean) => void;
  onSort: (column: string, direction?: SortDirection) => void;
  onClearSort: () => void;
  onExcludeRow: (rowHash: string) => void;
  onClearExcluded: () => void;
  onPreview: () => void;
  onBuild: () => void;
}) {
  const options = props.options;
  const selectedColumnCount = options?.columns.filter((column) => props.selectedColumns.has(column)).length ?? 0;
  const selectedCategoryCount = options?.categories.filter((category) => props.selectedCategoryKeys.has(category.category_key)).length ?? 0;
  const needsLargeConfirm = Boolean(props.preview && props.preview.estimated_files > 10);
  return (
    <section className="panel stage-panel export-panel">
      <SectionTitle
        icon={<Download />}
        title="Выгрузка"
        meta={props.preview ? `${props.preview.total} строк` : "предпросмотр не собран"}
        hint="XLSX строится из сохранённого куба. Исключение строк действует только на текущую выгрузку."
      />

      <div className="export-settings">
        <div className="export-settings-main">
          <div className="form-grid two-cols">
            <label>
              Период с
              <input type="month" value={props.periodFrom} onChange={(event) => props.onPeriodFromChange(event.target.value)} min={options?.period_from ?? undefined} max={options?.period_to ?? undefined} />
            </label>
            <label>
              Период по
              <input type="month" value={props.periodTo} onChange={(event) => props.onPeriodToChange(event.target.value)} min={options?.period_from ?? undefined} max={options?.period_to ?? undefined} />
            </label>
          </div>
          <label>
            Папка
            <input value={props.outputDir} onChange={(event) => props.onOutputDirChange(event.target.value)} placeholder={options?.default_output_dir || "data/projects/.../exports"} />
          </label>
          <div className="export-mode-row">
            <Toggle label="Разными файлами по категориям" checked={props.splitByCategory} onChange={props.onSplitByCategoryChange} />
            <button className="ghost-button" onClick={props.onReloadOptions}><RefreshCcw size={17} />Обновить</button>
            <button className="ghost-button" disabled={props.busy || !options} onClick={props.onPreview}><Table2 size={17} />Предпросмотр</button>
            <button className="primary-inline-button" disabled={props.busy || !props.preview || (needsLargeConfirm && !props.confirmLarge)} onClick={props.onBuild}>
              <Download size={17} />
              Выгрузить
            </button>
          </div>
        </div>

        <div className="export-selectors">
          <div className="export-selector">
            <div className="selector-head">
              <strong>Категории</strong>
              <button className="tiny-button" onClick={props.onToggleAllCategories}>{selectedCategoryCount === (options?.categories.length ?? 0) ? "снять" : "все"}</button>
            </div>
            <div className="selector-list">
              {options?.categories.map((category) => (
                <label className="check-row export-check-row" key={category.category_key}>
                  <input type="checkbox" checked={props.selectedCategoryKeys.has(category.category_key)} onChange={() => props.onToggleCategory(category.category_key)} />
                  <span>
                    <strong>{category.category_name}</strong>
                    <small>{category.marketplace} · {category.rows_count} строк</small>
                  </span>
                </label>
              ))}
              {options && options.categories.length === 0 ? <Empty text="В БД нет категорий для проекта." /> : null}
            </div>
          </div>

          <div className="export-selector">
            <div className="selector-head">
              <strong>Колонки</strong>
              <button className="tiny-button" onClick={props.onToggleAllColumns}>{selectedColumnCount === (options?.columns.length ?? 0) ? "снять" : "все"}</button>
            </div>
            <div className="selector-list column-selector-list">
              {options?.columns.map((column) => (
                <label className="check-row export-check-row" key={column}>
                  <input type="checkbox" checked={props.selectedColumns.has(column)} onChange={() => props.onToggleColumn(column)} />
                  <span>{column}</span>
                </label>
              ))}
            </div>
          </div>
        </div>
      </div>

      {options?.warnings.length ? <div className="export-warning">{options.warnings.join(" ")}</div> : null}
      {props.preview?.warnings.length ? <div className="export-warning">{props.preview.warnings.join(" ")}</div> : null}

      <div className="export-summary-row">
        <Metric label="Категорий" value={String(selectedCategoryCount)} />
        <Metric label="Колонок" value={String(selectedColumnCount)} />
        <Metric label="Исключено" value={String(props.excludedCount)} />
        <Metric label="Файлов" value={props.preview ? String(props.preview.estimated_files) : "-"} />
      </div>

      <div className="export-filter-panel">
        <div className="selector-head">
          <strong>Фильтры строк</strong>
          <button className="tiny-button" disabled={!options?.columns.length} onClick={props.onAddFilter}><Plus size={14} />фильтр</button>
        </div>
        {props.filters.length ? (
          <div className="export-filter-list">
            {props.filters.map((filter) => (
              <div className="export-filter-row" key={filter.id}>
                <label>
                  Колонка
                  <select value={filter.column} onChange={(event) => props.onFilterChange(filter.id, { column: event.target.value })}>
                    {options?.columns.map((column) => <option key={column} value={column}>{column}</option>)}
                  </select>
                </label>
                <label>
                  Условие
                  <select value={filter.match_type} onChange={(event) => props.onFilterChange(filter.id, { match_type: event.target.value })}>
                    {exportFilterTypes.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
                  </select>
                </label>
                <label>
                  Значение
                  <input value={filter.value} onChange={(event) => props.onFilterChange(filter.id, { value: event.target.value })} placeholder={filter.match_type === "gt" ? "0" : "текст или число"} />
                </label>
                <button className="icon-button danger-inline" title="Удалить фильтр" onClick={() => props.onDeleteFilter(filter.id)}><Trash2 size={16} /></button>
              </div>
            ))}
          </div>
        ) : <span className="muted">Без дополнительных фильтров. Добавь фильтр, например `Продажи, шт` &gt; `0`.</span>}
      </div>

      {needsLargeConfirm ? (
        <div className="large-export-confirm">
          <Toggle label="Подтверждаю большую выгрузку" checked={props.confirmLarge} onChange={props.onConfirmLargeChange} />
        </div>
      ) : null}

      <div className="toolbar wrap">
        <button className="ghost-button" disabled={!props.excludedCount} onClick={props.onClearExcluded}><RotateCcw size={17} />Вернуть исключённые</button>
      </div>

      {props.preview ? (
        <ExportPreviewTable
          columns={props.preview.columns}
          rows={props.preview.rows}
          sortColumn={props.sortColumn}
          sortDirection={props.sortDirection}
          onSort={props.onSort}
          onClearSort={props.onClearSort}
          onExcludeRow={props.onExcludeRow}
        />
      ) : <Empty text="Собери предпросмотр, чтобы проверить строки перед XLSX." />}

      {props.preview ? <ExportBreakdownTable rows={props.preview.breakdown ?? []} /> : null}

      {props.artifacts.length ? (
        <div className="export-artifacts">
          <h3>Готовые файлы</h3>
          <div className="artifact-list">
            {props.artifacts.map((artifact) => (
              <a className="artifact-row" href={api.exportFileUrl(artifact.path)} key={artifact.path}>
                <FileSpreadsheet size={17} />
                <span>
                  <strong>{artifact.filename}</strong>
                  <small>{artifact.rows} строк · часть {artifact.part}/{artifact.parts}</small>
                </span>
              </a>
            ))}
          </div>
        </div>
      ) : null}
    </section>
  );
}

function ExportPreviewTable(props: {
  columns: string[];
  rows: Record<string, unknown>[];
  sortColumn: string | null;
  sortDirection: "asc" | "desc";
  onSort: (column: string, direction?: SortDirection) => void;
  onClearSort: () => void;
  onExcludeRow: (rowHash: string) => void;
}) {
  return (
    <FilterableTable
      className="export-preview-table"
      rows={props.rows}
      rowKey={(row, index) => String(row["__row_hash"] ?? index)}
      emptyText="Нет строк по текущим фильтрам."
      onSortChange={props.onSort}
      onSortClear={props.onClearSort}
      columns={[
        {
          id: "action",
          label: "",
          value: () => "",
          render: (row) => {
            const rowHash = String(row["__row_hash"] ?? "");
            return <button className="tiny-button" disabled={!rowHash} onClick={() => props.onExcludeRow(rowHash)}>убрать</button>;
          },
          className: "row-action-col",
          filterable: false,
          sortable: false
        },
        ...props.columns.map((column) => ({
          id: column,
          label: column,
          value: (row: Record<string, unknown>) => row[column]
        }))
      ]}
    />
  );
}

function ExportBreakdownTable(props: { rows: ExportPreview["breakdown"] }) {
  return (
    <div className="export-breakdown">
      <h3>Состав выгрузки</h3>
      <FilterableTable
        rows={props.rows}
        rowKey={(item) => `${item.period}-${item.category_key}-${item.marketplace_code}`}
        emptyText="Нет сочетаний период/категория/маркетплейс по текущим параметрам."
        columns={[
          { id: "period", label: "Период", value: (item) => item.period },
          { id: "category", label: "Категория", value: (item) => item.category_name, title: (item) => item.category_name },
          { id: "marketplace", label: "МП", value: (item) => item.marketplace },
          { id: "rows", label: "Строк", value: (item) => item.rows_count, numeric: true }
        ]}
      />
    </div>
  );
}

function DataQualityWorkspace(props: {
  projects: QualityProject[];
  selectedProject: string;
  report: QualityReport | null;
  loading: boolean;
  error: string | null;
  copied: boolean;
  onProjectChange: (value: string) => void;
  onReloadProjects: () => void;
  onRun: () => void;
  onCopySummary: () => void;
}) {
  const selected = props.projects.find((project) => project.project_name === props.selectedProject) ?? props.projects[0] ?? null;
  const report = props.report;
  return (
    <section className="panel stage-panel quality-panel">
      <SectionTitle
        icon={<CheckCircle2 />}
        title="Качество данных"
        meta={report ? `${formatNumber(report.total_rows)} строк` : "проверка не запускалась"}
        hint="Проверка отвечает на главный вопрос: можно ли доверять итоговому CSV перед анализом или выгрузкой."
      />

      <div className="quality-toolbar">
        <label>
          Проект
          <select value={selected?.project_name ?? ""} disabled={!props.projects.length || props.loading} onChange={(event) => props.onProjectChange(event.target.value)}>
            {props.projects.map((project) => (
              <option key={project.project_name} value={project.project_name}>
                {project.project_name}
              </option>
            ))}
          </select>
        </label>
        <button className="ghost-button" disabled={props.loading} onClick={props.onReloadProjects}>
          <RefreshCcw size={17} />
          Обновить
        </button>
        <button className="primary-inline-button" disabled={props.loading || !selected} onClick={props.onRun}>
          <CheckCircle2 size={17} />
          Проверить данные
        </button>
        <button className="ghost-button" disabled={!report || props.loading} onClick={props.onCopySummary}>
          <Copy size={17} />
          {props.copied ? "Скопировано" : "Сводка"}
        </button>
      </div>

      {props.error ? <div className="quality-error"><AlertTriangle size={16} />{props.error}</div> : null}
      {!props.projects.length && !props.loading ? <Empty text="Итоговые CSV пока не найдены." /> : null}

      {selected && !report ? (
        <div className="quality-source-note">
          <span>{qualitySourceLabel(selected.source_kind, selected.fallback_used)}</span>
          <code>{selected.path}</code>
        </div>
      ) : null}

      {props.loading ? <div className="quality-loading">Проверяем данные...</div> : null}

      {report ? <QualityReportView report={report} /> : props.projects.length && !props.loading ? <Empty text="Выбери проект и нажми «Проверить данные»." /> : null}
    </section>
  );
}

function QualityReportView(props: { report: QualityReport }) {
  const report = props.report;
  return (
    <div className="quality-report">
      <div className={`quality-status-card ${report.status.toLowerCase()}`}>
        <div>
          <QualityStatusBadge status={report.status} />
          <h3>{report.status_comment}</h3>
          <span>{qualitySourceLabel(report.source.kind, report.source.fallback_used)} · {formatNumber(report.source.file_count)} файл(ов)</span>
        </div>
        <code title={report.source.path}>{report.source.path}</code>
      </div>

      {report.warnings.length ? (
        <div className="quality-warning-list">
          {report.warnings.map((warning) => <span key={warning}>{warning}</span>)}
        </div>
      ) : null}

      <div className="quality-metrics">
        <QualityMetric label="Строк всего" value={formatNumber(report.total_rows)} />
        <QualityMetric label="Вес/объём найден" value={formatPercent(report.metrics.weight_volume.coverage_share)} detail={`${formatNumber(report.metrics.weight_volume.parsed_count)} строк`} />
        <QualityMetric label="Классифицировано" value={formatPercent(report.metrics.classification.coverage_share)} detail={`${formatNumber(report.metrics.classification.classified_count)} строк`} />
        <QualityMetric label="Аномалии" value={formatNumber(report.metrics.anomalies.count)} detail="вес/объём" />
        <QualityMetric
          label="Дубли"
          value={report.metrics.duplicates.checked ? formatNumber(report.metrics.duplicates.duplicate_rows) : "пропущено"}
          detail={report.metrics.duplicates.identifier_column || "нет ID"}
        />
        <QualityMetric label="Пропуски" value={formatNumber(report.metrics.empty_key_fields.rows_with_empty)} detail="ключевые поля" />
      </div>

      {report.skipped_checks.length ? (
        <div className="quality-skipped">
          <strong>Пропущенные проверки</strong>
          {report.skipped_checks.map((item) => (
            <span key={`${item.check}-${item.reason}`}>{item.check}: {item.reason}</span>
          ))}
        </div>
      ) : null}

      <div className="quality-section">
        <h3>Проблемы</h3>
        {report.problems.length ? <QualityProblemsTable problems={report.problems} /> : <Empty text="Проблемы не найдены." />}
      </div>

      <div className="quality-section">
        <h3>Примеры строк для проверки</h3>
        <QualityExamples report={report} />
      </div>
    </div>
  );
}

function QualityMetric(props: { label: string; value: string; detail?: string }) {
  return (
    <div className="quality-metric">
      <span>{props.label}</span>
      <strong>{props.value}</strong>
      {props.detail ? <small>{props.detail}</small> : null}
    </div>
  );
}

function QualityStatusBadge(props: { status: QualityReport["status"] }) {
  const label = props.status === "OK" ? "OK" : props.status === "WARNING" ? "WARNING" : "FAIL";
  const icon = props.status === "OK" ? <CheckCircle2 size={15} /> : <AlertTriangle size={15} />;
  return <span className={`quality-status-badge ${props.status.toLowerCase()}`}>{icon}{label}</span>;
}

function QualityProblemsTable(props: { problems: QualityProblem[] }) {
  return (
    <FilterableTable
      rows={props.problems}
      rowKey={(problem, index) => `${problem.type}-${index}`}
      emptyText="Проблемы не найдены."
      columns={[
        { id: "type", label: "Тип проблемы", value: (problem) => problem.type },
        { id: "count", label: "Строк", value: (problem) => problem.count, render: (problem) => formatNumber(problem.count), numeric: true },
        { id: "share", label: "Доля", value: (problem) => problem.share, render: (problem) => formatPercent(problem.share), numeric: true },
        { id: "comment", label: "Комментарий", value: (problem) => problem.comment, title: (problem) => problem.comment }
      ]}
    />
  );
}

function QualityExamples(props: { report: QualityReport }) {
  const groups = [
    { id: "unclassified", title: "Неклассифицированные товары", rows: props.report.examples.unclassified },
    { id: "missing-weight", title: "Товары без веса/объёма", rows: props.report.examples.missing_weight_volume },
    { id: "anomalies", title: "Аномальный вес/объём", rows: props.report.examples.anomalies },
    { id: "duplicates", title: "Дубли", rows: props.report.examples.duplicates }
  ];
  return (
    <div className="quality-examples">
      {groups.map((group) => (
        <details key={group.id} open={group.rows.length > 0}>
          <summary>
            <span>{group.title}</span>
            <strong>{formatNumber(group.rows.length)}</strong>
          </summary>
          {group.rows.length ? <SimpleTable columns={qualityExampleColumns(group.rows)} rows={group.rows} /> : <Empty text="Нет примеров." />}
        </details>
      ))}
    </div>
  );
}

function qualityExampleColumns(rows: Record<string, unknown>[]) {
  const columns: string[] = [];
  for (const row of rows) {
    for (const column of Object.keys(row)) {
      if (!columns.includes(column)) columns.push(column);
      if (columns.length >= 8) return columns;
    }
  }
  return columns;
}

function qualitySourceLabel(kind: string, fallbackUsed: boolean) {
  if (kind === "classified") return "использован classified CSV";
  if (fallbackUsed) return "classified не найден, использован merged CSV";
  if (kind === "merged") return "использован merged CSV";
  return kind;
}

function formatPercent(value: number | null | undefined) {
  return `${Math.round(Number(value || 0) * 1000) / 10}%`;
}

function formatNumber(value: number | null | undefined) {
  return new Intl.NumberFormat("ru-RU").format(Number(value || 0));
}

function CategorySourceEditor(props: {
  rows: CategorySourceRow[];
  selectedRow: CategorySourceRow | null;
  sourcePath: string;
  query: string;
  onQueryChange: (value: string) => void;
  onSelect: (id: string) => void;
  onAdd: () => void;
  onDelete: (id: string) => void;
  onReload: () => void;
  onSave: () => void;
  onChange: (id: string, patch: Partial<CategorySourceRow>) => void;
}) {
  const row = props.selectedRow;
  return (
    <section className="panel stage-panel">
      <SectionTitle icon={<FolderSync />} title="Справочник категорий" meta={`${props.rows.length} строк`} hint="Это единственный источник категорий: CSV в корне проекта. Excel больше не читается, чтобы не ловить дубли путей." />
      <div className="toolbar wrap">
        <label className="search-field">
          <Search size={17} />
          <input value={props.query} onChange={(event) => props.onQueryChange(event.target.value)} placeholder="Категория, marketplace, путь или фильтр" />
        </label>
        <button className="ghost-button" title="Добавить новую строку в CSV-справочник." onClick={props.onAdd}><Plus size={17} />Добавить</button>
        <button className="ghost-button" title="Заново прочитать CSV с диска." onClick={props.onReload}><RefreshCcw size={17} />Перечитать</button>
        <button className="ghost-button" title="Записать изменения в CSV и обновить локальный каталог DuckDB." onClick={props.onSave}><Save size={17} />Сохранить CSV</button>
      </div>
      <p className="path-note" title={props.sourcePath}>{props.sourcePath || "CSV ещё не выбран"}</p>
      <div className="editor-layout">
        {row ? (
          <div className="editor-form">
            <SectionTitle icon={<Settings />} title="Строка справочника" hint="После сохранения путь и фильтр обновят список категорий для загрузки." />
            <Toggle label="Активна в приложении" checked={row.active} onChange={(value) => props.onChange(row.id, { active: value })} />
            <Toggle label="FBS" checked={row.fbs} onChange={(value) => props.onChange(row.id, { fbs: value })} />
            <div className="form-grid two-cols">
              <label>Категория<input value={row.category_name} onChange={(event) => props.onChange(row.id, { category_name: event.target.value })} /></label>
              <label>Маркетплейс<select value={row.marketplace} onChange={(event) => props.onChange(row.id, { marketplace: event.target.value })}>{marketplaceOptions.map((option) => <option key={option}>{option}</option>)}</select></label>
              <label>От<input value={row.period_from} onChange={(event) => props.onChange(row.id, { period_from: event.target.value })} /></label>
              <label>До<input value={row.period_to} onChange={(event) => props.onChange(row.id, { period_to: event.target.value })} /></label>
            </div>
            <label>Путь<input value={row.path} onChange={(event) => props.onChange(row.id, { path: event.target.value })} /></label>
            <CatalogFilterBuilder sourceKey={row.id} value={row.filter_text} onChange={(value) => props.onChange(row.id, { filter_text: value })} />
            <label>Комментарий<textarea value={row.comment} onChange={(event) => props.onChange(row.id, { comment: event.target.value })} /></label>
            <button className="danger-button" title="Удалить строку из редактора. CSV изменится только после сохранения." onClick={() => props.onDelete(row.id)}>
              <Trash2 size={17} />Удалить строку
            </button>
          </div>
        ) : <Empty text="Выбери строку справочника или добавь новую." />}
        <FilterableTable
          className="editor-list"
          rows={props.rows}
          rowKey={(item) => item.id}
          emptyText="Нет строк справочника по текущим фильтрам."
          getRowClassName={(item) => row?.id === item.id ? "selected-row" : ""}
          onRowClick={(item) => props.onSelect(item.id)}
          columns={[
            { id: "active", label: "Активна", value: (item) => item.active ? "да" : "нет" },
            { id: "fbs", label: "FBS", value: (item) => item.fbs ? "да" : "нет" },
            { id: "category", label: "Категория", value: (item) => item.category_name || "-" },
            { id: "marketplace", label: "МП", value: (item) => item.marketplace || "-" },
            { id: "path", label: "Путь", value: (item) => item.path || "-", title: (item) => item.path || "-" },
            { id: "filter", label: "Фильтр", value: (item) => catalogFilterSummary(item.filter_text) }
          ]}
        />
      </div>
    </section>
  );
}

function catalogFilterSummary(value: string) {
  const draft = parseCatalogFilterText(value);
  if (draft.conditions.length === 0) return "-";
  return draft.conditions
    .map((condition) => `${condition.type === "notContains" ? "не содержит" : "содержит"} ${condition.value}`)
    .join(draft.operator === "OR" ? " или " : " и ");
}

function CatalogFilterBuilder(props: {
  sourceKey: string;
  value: string;
  onChange: (value: string) => void;
}) {
  const [draft, setDraft] = useState<CatalogFilterDraft>(() => parseCatalogFilterText(props.value));
  const previousSourceKey = useRef(props.sourceKey);

  useEffect(() => {
    setDraft((current) => {
      if (previousSourceKey.current !== props.sourceKey) {
        previousSourceKey.current = props.sourceKey;
        return parseCatalogFilterText(props.value);
      }
      if (props.value === serializeCatalogFilter(current)) return current;
      return parseCatalogFilterText(props.value);
    });
  }, [props.sourceKey, props.value]);

  function commit(next: CatalogFilterDraft) {
    const normalized = { operator: next.operator, conditions: next.conditions.slice(0, 2) };
    setDraft(normalized);
    props.onChange(serializeCatalogFilter(normalized));
  }

  function updateCondition(index: number, patch: Partial<CatalogFilterCondition>) {
    commit({
      ...draft,
      conditions: draft.conditions.map((condition, conditionIndex) => (conditionIndex === index ? { ...condition, ...patch } : condition))
    });
  }

  function addCondition() {
    if (draft.conditions.length >= 2) return;
    commit({ ...draft, conditions: [...draft.conditions, { type: "contains", value: "" }] });
  }

  function removeCondition(index: number) {
    commit({ ...draft, conditions: draft.conditions.filter((_, conditionIndex) => conditionIndex !== index) });
  }

  return (
    <div className="catalog-filter-builder">
      <div className="filter-builder-head">
        <span>Фильтр по названию</span>
        <button className="tiny-button" type="button" title="Добавить условие фильтра." onClick={addCondition} disabled={draft.conditions.length >= 2}>
          <Plus size={14} />Условие
        </button>
      </div>
      {draft.conditions.length > 1 ? (
        <div className="filter-operator" role="group" aria-label="Связь условий фильтра">
          <button type="button" className={draft.operator === "AND" ? "active" : ""} onClick={() => commit({ ...draft, operator: "AND" })}>И</button>
          <button type="button" className={draft.operator === "OR" ? "active" : ""} onClick={() => commit({ ...draft, operator: "OR" })}>ИЛИ</button>
        </div>
      ) : null}
      {draft.conditions.length === 0 ? (
        <button className="filter-empty-button" type="button" onClick={addCondition}>Без фильтра</button>
      ) : (
        <div className="filter-condition-list">
          {draft.conditions.map((condition, index) => (
            <div className="filter-condition-row" key={`${props.sourceKey}-${index}`}>
              <div className="filter-type-group" role="group" aria-label={`Тип условия ${index + 1}`}>
                {catalogFilterTypes.map(([type, label]) => (
                  <button
                    key={type}
                    type="button"
                    className={condition.type === type ? "active" : ""}
                    onClick={() => updateCondition(index, { type })}
                  >
                    {label}
                  </button>
                ))}
              </div>
              <input
                value={condition.value}
                onChange={(event) => updateCondition(index, { value: event.target.value })}
                placeholder="слово или фраза"
                aria-label={`Значение условия ${index + 1}`}
              />
              <button className="icon-button danger-inline" type="button" title="Удалить условие." onClick={() => removeCondition(index)}>
                <X size={16} />
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function ClassifierRulesEditor(props: {
  rules: ClassifierRule[];
  totalRules: number;
  selectedRule: ClassifierRule | null;
  rulesPath: string;
  query: string;
  onQueryChange: (value: string) => void;
  onSelect: (id: string) => void;
  onAdd: () => void;
  onAddPreset: (preset: ClassifierPreset) => void;
  onDuplicate: (rule: ClassifierRule) => void;
  onDelete: (id: string) => void;
  onChange: (id: string, patch: Partial<ClassifierRule>) => void;
  onConditionChange: (ruleId: string, index: number, patch: Partial<ClassifierCondition>) => void;
  onAddCondition: (ruleId: string) => void;
  onDeleteCondition: (ruleId: string, index: number) => void;
  onSave: () => void;
  externalFile: File | null;
  externalWriteXlsx: boolean;
  externalResult: ClassificationResponse | null;
  busy: boolean;
  onExternalFileChange: (file: File | null) => void;
  onExternalWriteXlsxChange: (value: boolean) => void;
  onExternalClassify: () => void;
}) {
  const rule = props.selectedRule;
  return (
    <section className="panel stage-panel">
      <SectionTitle icon={<FileSpreadsheet />} title="Классификатор" meta={`${props.rules.length}/${props.totalRules} правил`} hint="Правила сохраняются в classifiers/rules.csv. JSON дополнительных условий собирается автоматически из строк условий." />
      <div className="toolbar wrap">
        <label className="search-field">
          <Search size={17} />
          <input value={props.query} onChange={(event) => props.onQueryChange(event.target.value)} placeholder="Найти правило, колонку или текст" />
        </label>
        <button className="ghost-button" title="Создать пустое правило классификации." onClick={props.onAdd}><Plus size={17} />Пустое правило</button>
        <button className="ghost-button" title="Скопировать выбранное правило." disabled={!rule} onClick={() => rule && props.onDuplicate(rule)}><Copy size={17} />Дублировать</button>
        <button className="ghost-button" title="Записать правила в classifiers/rules.csv." onClick={props.onSave}><Save size={17} />Сохранить правила</button>
      </div>
      <div className="quick-start">
        <span>Быстро создать:</span>
        <button className="chip-button" onClick={() => props.onAddPreset("name-to-subcategory")}>по названию</button>
        <button className="chip-button" onClick={() => props.onAddPreset("sku-to-subcategory")}>по точному SKU</button>
        <button className="chip-button" onClick={() => props.onAddPreset("name-to-brand")}>бренд из названия</button>
        <button className="chip-button" onClick={() => props.onAddPreset("otherwise")}>иначе</button>
      </div>
      <p className="path-note" title={props.rulesPath}>{props.rulesPath || "Файл правил ещё не выбран"}</p>
      <div className="external-classifier">
        <div className="subsection-head">
          <div>
            <h3>Внешний файл</h3>
            <p>Загрузи CSV или XLSX, классификатор сразу создаст готовый CSV и покажет первые строки.</p>
          </div>
          <button className="ghost-button" disabled={!props.externalFile || props.busy} onClick={props.onExternalClassify}>
            <Upload size={17} />Обработать
          </button>
        </div>
        <div className="external-file-grid">
          <label>
            Файл для обработки
            <input
              type="file"
              accept=".csv,.xlsx,text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
              onChange={(event) => props.onExternalFileChange(event.target.files?.[0] ?? null)}
            />
          </label>
          <Toggle label="Сохранить копию XLSX" checked={props.externalWriteXlsx} onChange={props.onExternalWriteXlsxChange} />
        </div>
        {props.externalResult ? (
          <div className="external-result">
            <div className="result-paths">
              <span><strong>Готовый CSV</strong><code title={props.externalResult.output_file}>{props.externalResult.output_file}</code></span>
              {props.externalResult.output_xlsx ? <span><strong>XLSX</strong><code title={props.externalResult.output_xlsx}>{props.externalResult.output_xlsx}</code></span> : null}
            </div>
            <div className="result-actions">
              <a className="ghost-button" href={api.workflowFileUrl(props.externalResult.output_file)}>
                <Save size={17} />Скачать CSV
              </a>
              {props.externalResult.output_xlsx ? (
                <a className="ghost-button" href={api.workflowFileUrl(props.externalResult.output_xlsx)}>
                  <FileSpreadsheet size={17} />Скачать XLSX
                </a>
              ) : null}
            </div>
            <h3>Предпросмотр: {props.externalResult.preview.total} строк</h3>
            <SimpleTable columns={props.externalResult.preview.columns.slice(0, 9)} rows={props.externalResult.preview.rows} />
          </div>
        ) : null}
      </div>
      <div className="editor-layout">
        {rule ? (
          <div className="editor-form">
            <SectionTitle icon={<Settings />} title="Правило" hint="Заполни правило как фразу: если в колонке найден текст, то записать нужное значение в нужную колонку." />
            <Toggle label="Правило активно" checked={rule.active} onChange={(value) => props.onChange(rule.id, { active: value })} />
            <datalist id="classifier-columns">
              {commonClassifierColumns.map((column) => <option key={column} value={column} />)}
            </datalist>
            <div className="rule-block">
              <h3>1. Когда применять</h3>
              <div className="form-grid two-cols">
                <label>Для категории<input value={rule.category} onChange={(event) => props.onChange(rule.id, { category: event.target.value })} placeholder="* или название категории" /></label>
                <label>Порядок<input type="number" value={rule.priority} onChange={(event) => props.onChange(rule.id, { priority: Number(event.target.value) })} /></label>
              </div>
            </div>
            <div className="rule-block">
              <div className="subsection-head">
                <h3>Условия</h3>
                <button className="tiny-button" title="Добавить дополнительное условие AND/OR." onClick={() => props.onAddCondition(rule.id)}><Plus size={14} />условие</button>
              </div>
              <div className="condition-list">
                {rule.conditions.map((condition, index) => (
                  <div className="condition-row" key={`${rule.id}-${index}`}>
                    {index === 0 ? (
                      <span className="condition-prefix">Если</span>
                    ) : (
                      <label>Связка<select value={condition.join_with_prev} onChange={(event) => props.onConditionChange(rule.id, index, { join_with_prev: event.target.value })}><option value="and">И</option><option value="or">ИЛИ</option></select></label>
                    )}
                    <label>Где искать<input list="classifier-columns" value={condition.match_field} disabled={condition.match_type === "otherwise"} onChange={(event) => props.onConditionChange(rule.id, index, { match_field: event.target.value })} placeholder="Название, SKU, Бренд..." /></label>
                    <label>Как искать<select value={condition.match_type} onChange={(event) => {
                      const matchType = event.target.value;
                      props.onConditionChange(rule.id, index, matchType === "otherwise" ? { match_type: matchType, match_field: "", pattern: "" } : { match_type: matchType });
                    }}>{matchTypes.map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
                    <label>Что искать<input value={condition.pattern} disabled={condition.match_type === "otherwise"} onChange={(event) => props.onConditionChange(rule.id, index, { pattern: event.target.value })} placeholder={condition.match_type === "otherwise" ? "не нужно" : "Например: лимон"} /></label>
                    <button className="tiny-button danger-inline" title="Удалить условие." disabled={rule.conditions.length <= 1} onClick={() => props.onDeleteCondition(rule.id, index)}><Trash2 size={14} /></button>
                  </div>
                ))}
              </div>
            </div>
            <div className="rule-block">
              <h3>2. Что записать</h3>
              <div className="form-grid two-cols">
                <label>Куда записать<input list="classifier-columns" value={rule.target_column} onChange={(event) => props.onChange(rule.id, { target_column: event.target.value })} placeholder="Например: Подкатегория" /></label>
                <label>Что записать<input value={rule.set_value} onChange={(event) => props.onChange(rule.id, { set_value: event.target.value })} placeholder="Например: кислота" /></label>
              </div>
            </div>
            <div className="rule-block">
              <h3>3. Как применять</h3>
              <div className="form-grid two-cols">
                <label>Режим<select value={rule.mode} onChange={(event) => props.onChange(rule.id, { mode: event.target.value })}>{ruleModes.map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
                <label>Заметка<input value={rule.comment} onChange={(event) => props.onChange(rule.id, { comment: event.target.value })} placeholder="Для себя, можно пусто" /></label>
              </div>
            </div>
            <button className="danger-button" title="Удалить выбранное правило. Файл изменится только после сохранения." onClick={() => props.onDelete(rule.id)}>
              <Trash2 size={17} />Удалить правило
            </button>
          </div>
        ) : <Empty text="Выбери правило или создай новое." />}
        <FilterableTable
          className="editor-list"
          rows={props.rules}
          rowKey={(item) => item.id}
          emptyText="Нет правил по текущим фильтрам."
          getRowClassName={(item) => rule?.id === item.id ? "selected-row" : ""}
          onRowClick={(item) => props.onSelect(item.id)}
          columns={[
            { id: "active", label: "Вкл", value: (item) => item.active ? "да" : "нет" },
            { id: "priority", label: "Порядок", value: (item) => item.priority, numeric: true },
            { id: "condition", label: "Если", value: ruleConditionSummary, title: ruleConditionSummary },
            { id: "action", label: "Записать", value: ruleActionSummary, title: ruleActionSummary },
            { id: "mode", label: "Режим", value: (item) => ruleModeLabel(item.mode) }
          ]}
        />
      </div>
    </section>
  );
}

function CubeTable(props: { items: CubeItem[] }) {
  return (
    <FilterableTable
      rows={props.items}
      rowKey={(item) => item.id}
      emptyText="Нет срезов куба по текущим фильтрам."
      columns={[
        { id: "month", label: "Месяц", value: (item) => monthLabel(item.year, item.month) },
        { id: "marketplace", label: "Marketplace", value: (item) => item.marketplace },
        { id: "category", label: "Категория", value: (item) => item.category_name },
        { id: "rows", label: "Строк", value: (item) => item.rows_count, numeric: true },
        {
          id: "source",
          label: "Источник",
          value: (item) => item.source_processed_file_path ?? "-",
          title: (item) => item.source_processed_file_path ?? "-"
        }
      ]}
    />
  );
}

function SimpleTable(props: { columns: string[]; rows: Record<string, unknown>[] }) {
  return (
    <FilterableTable
      rows={props.rows}
      rowKey={(_, index) => String(index)}
      emptyText="Нет строк по текущим фильтрам."
      columns={props.columns.map((column) => ({
        id: column,
        label: column,
        value: (row: Record<string, unknown>) => row[column]
      }))}
    />
  );
}

function visibleProductColumns(columns: string[]) {
  const visible = columns.filter((column) => !column.startsWith("__"));
  return visible.length ? visible : columns;
}

function ProductInstructionModal(props: { onClose: () => void }) {
  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={props.onClose}>
      <section className="instruction-modal" role="dialog" aria-modal="true" aria-labelledby="product-instruction-title" onMouseDown={(event) => event.stopPropagation()}>
        <div className="modal-head">
          <div>
            <h2 id="product-instruction-title">Инструкция по MPStats Workflow</h2>
            <p>Коротко: выбираешь что скачать, запускаешь план, проверяешь БД и при необходимости настраиваешь классификатор.</p>
          </div>
          <div className="result-actions">
            <a className="ghost-button" href="/docs/USER_GUIDE.md" target="_blank" rel="noreferrer">
              <BookOpen size={17} />Полная инструкция
            </a>
            <button className="icon-button" aria-label="Закрыть инструкцию" onClick={props.onClose}><X size={18} /></button>
          </div>
        </div>
        <div className="instruction-content">
          <section className="instruction-section">
            <h3>1. Проект и доступ</h3>
            <p>В поле «Название проекта» напиши понятное имя выгрузки. По нему приложение раскладывает файлы, планы и записи в БД. Cookie нужен только для скачивания из MPStats: вставляешь его, сохраняешь настройки и дальше работаешь через кнопки.</p>
          </section>
          <section className="instruction-section">
            <h3>2. Справочник</h3>
            <p>Во вкладке «Справочник» лежит список категорий и путей MPStats. Если нужна новая категория, добавь строку, укажи маркетплейс, путь и фильтр, затем нажми «Сохранить CSV». После этого категория появится во вкладке «Категории».</p>
          </section>
          <section className="instruction-section">
            <h3>3. Загрузка</h3>
            <p>Для старых периодов выбери «Историческая загрузка», отметь категории и нажми «Создать план». Потом проверь вкладку «План загрузки» и нажми «Запустить». Для следующего месяца используй режим «Ежемесячное обновление» и кнопку синхронизации.</p>
          </section>
          <section className="instruction-section">
            <h3>4. БД / Куб</h3>
            <p>После обработки каждая задача сохраняется в DuckDB. Во вкладке «БД / Куб» видно, какие месячные срезы уже сохранены, можно открыть предпросмотр первых строк и найти товар по SKU, названию или бренду.</p>
          </section>
          <section className="instruction-section">
            <h3>5. Классификатор</h3>
            <p>Правило читается как обычная фраза: «если в колонке Название есть лимон, записать в колонку Тип значение Кислота». Обычно хватает одного условия. Несколько условий нужны, когда надо сузить правило: например «Название содержит мыло» и «Название не содержит хозяйственное». Здесь же можно загрузить внешний CSV или XLSX и сразу получить обработанный файл без запуска пайплайна.</p>
            <div className="example-box">
              <strong>Пример</strong>
              <span>Где искать: Название</span>
              <span>Как искать: содержит</span>
              <span>Что искать: лимон</span>
              <span>Куда записать: Тип</span>
              <span>Что записать: Кислота</span>
            </div>
          </section>
          <section className="instruction-section">
            <h3>6. Если что-то пошло не так</h3>
            <p>Сначала смотри статус задачи в «Плане загрузки». Ошибки можно повторить кнопкой «Повторить ошибки», одну задачу - кнопкой «повтор» в строке. Если файлы уже готовы, но БД пустая, используй «Собрать куб из готовых файлов».</p>
          </section>
        </div>
      </section>
    </div>
  );
}

function Notice(props: { tone: "error" | "success"; text: string; onClose: () => void }) {
  return (
    <div className={`notice ${props.tone}`}>
      <span>{props.text}</span>
      <button onClick={props.onClose}>Закрыть</button>
    </div>
  );
}

function Empty(props: { text: string }) {
  return <div className="empty">{props.text}</div>;
}
