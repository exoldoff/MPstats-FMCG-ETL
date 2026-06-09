import {
  AlertTriangle,
  Archive,
  BarChart3,
  BookOpen,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  CircleHelp,
  Copy,
  Database,
  Download,
  FileSpreadsheet,
  FolderSync,
  Github,
  History,
  ListChecks,
  LoaderCircle,
  PanelLeftClose,
  PanelLeftOpen,
  Pause,
  Play,
  Plus,
  RefreshCcw,
  RotateCcw,
  Save,
  Search,
  Send,
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
  ExportArtifact,
  ExportBuildJob,
  ExportColumnFilter,
  ExportFormat,
  ExportOptions,
  ExportPayload,
  ExportPreview,
  ExportTemplate,
  ExportTemplatePayload,
  ManualOverride,
  PipelineRun,
  PipelineSettings,
  ProductSearch,
  ProjectSummary,
  ProjectFile,
  QualityProblem,
  QualityProject,
  QualityReport,
  ReportArtifact,
  ReportBuildResponse,
  ReportOptions,
  ReportPayload,
  ReportPreview,
  ReportType,
  SmartPlan,
  SmartPlanStatus,
  SmartPlanTask
} from "./api";

type Mode = "historical_backfill" | "monthly_sync";
type Tab = "projects" | "categories" | "catalog" | "plan" | "files" | "cube" | "reports" | "export" | "classifier" | "quality";
type DataTab = "files" | "cube" | "reports" | "export" | "quality";
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

const smartPlanFilters: Array<{ value: SmartPlanStatus | "all"; label: string }> = [
  { value: "all", label: "Все" },
  { value: "ready", label: "Готовые" },
  { value: "missing", label: "Нет файлов" },
  { value: "stale", label: "Устарели" },
  { value: "failed", label: "Ошибки" },
  { value: "incomplete", label: "Неполные" }
];

const matchTypes = [
  ["contains", "содержит"],
  ["not_contains", "не содержит"],
  ["regex", "regex"],
  ["equals", "равно"],
  ["startswith", "начинается с"],
  ["gt", ">"],
  ["gte", ">="],
  ["lt", "<"],
  ["lte", "<="],
  ["otherwise", "если пусто"]
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
const ruleCategorySeparator = " | ";

type ClassifierPreset = "name-to-subcategory" | "sku-to-subcategory" | "name-to-brand" | "otherwise";
type CatalogFilterType = (typeof catalogFilterTypes)[number][0];
type CatalogFilterOperator = "AND" | "OR";
type CatalogFilterCondition = { type: CatalogFilterType; value: string };
type CatalogFilterDraft = { operator: CatalogFilterOperator; conditions: CatalogFilterCondition[] };
type ExportFilterDraft = { id: string; column: string; match_type: string; value: string };
type CubeMatrixCell = { status: "ready" | "missing"; period: string; rowsCount: number; title: string };
type CubeMatrixRow = { categoryKey: string; categoryName: string; cells: CubeMatrixCell[]; missingCount: number };
type CubeMatrix = { marketplaces: string[]; rows: CubeMatrixRow[]; missingCount: number; totalCells: number; targetPeriods: Record<string, string> };
type PipelineOperationKind = "start" | "pause" | "stop" | "resume" | "retry" | "rebuild" | "reclassify" | "sync";
type EditableWorkspace = "catalog" | "classifier";
type UnsavedChangesPrompt = {
  kind: EditableWorkspace;
  proceed: () => void | Promise<void>;
};
type PipelineOperation = {
  kind: PipelineOperationKind;
  label: string;
  detail: string;
  runId: string | null;
  startedAt: number;
  finishedAt: number | null;
};

const commonClassifierColumns = ["Название", "SKU", "Артикул", "Бренд", "Категория", "Вес, кг", "Вес, кг (ед.)", "Подкатегория", "Тип", "Вид мяса", "SKU-группа"];

function isYandexMarketplace(value: string) {
  return ["ям", "яндекс", "яндекс маркет", "яндекс.маркет", "ym"].includes(value.trim().toLowerCase());
}

function editorSnapshot<T>(value: T): string {
  return JSON.stringify(value);
}

function restoreSnapshot<T>(snapshot: string | null): T | null {
  if (!snapshot) return null;
  return JSON.parse(snapshot) as T;
}

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
  stopping: "остановка...",
  stopped: "остановлено",
  succeeded: "готово",
  completed_with_errors: "с ошибками",
  ready: "готово",
  missing: "нет файлов",
  stale: "устарело",
  incomplete: "неполно",
  heavy: "тяжёлая"
};

const activeRunStatuses = new Set(["running", "pausing", "stopping"]);
const passiveCubeActions = new Set(["Предпросмотр БД", "Поиск в БД"]);
const activeTaskStatuses = new Set(["downloading", "processing", "classifying", "saving_to_db", "running", "pausing", "stopping"]);

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
    hint: "Processed-файлы после правил из вкладки Классификатор. Именно они сохраняются в Данные -> Куб."
  },
  export: {
    label: "Выгрузки",
    hint: "Готовые XLSX из Данные -> Выгрузка. Это файлы для передачи, анализа или ручной работы."
  },
  other: {
    label: "Прочее",
    hint: "Вспомогательные файлы проекта, которые не относятся к основному пути raw -> processed -> classified -> куб."
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

function sleep(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function addLoadError(errors: string[], label: string, reason: unknown) {
  errors.push(`${label}: ${errorText(reason)}`);
}

function isPipelineRun(value: unknown): value is PipelineRun {
  return isRecord(value) && typeof value.id === "string" && typeof value.status === "string" && typeof value.total_tasks === "number";
}

function liveSmartPlanStatus(task: SmartPlanTask) {
  return [task.pipeline_status, task.download_status, task.process_status, task.classify_status, task.save_status].find((status) =>
    activeTaskStatuses.has(status)
  ) ?? null;
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

function cubeMonthLabel(year: number, month: number) {
  return `${String(month).padStart(2, "0")}.${year}`;
}

function cubeMonthValue(item: Pick<CubeItem, "year" | "month">) {
  return item.year * 12 + item.month;
}

function marketplaceSortKey(marketplace: string) {
  const order: Record<string, number> = { Ozon: 1, WB: 2, "Яндекс.Маркет": 3, YM: 3 };
  return order[marketplace] ?? 100;
}

function buildCubeMatrix(items: CubeItem[]): CubeMatrix {
  const marketplaces = [...new Set(items.map((item) => item.marketplace))]
    .sort((left, right) => marketplaceSortKey(left) - marketplaceSortKey(right) || left.localeCompare(right, "ru"));
  const targets = new Map<string, number>();
  const latest = new Map<string, CubeItem>();
  const categories = new Map<string, string>();

  for (const item of items) {
    const value = cubeMonthValue(item);
    const target = targets.get(item.marketplace);
    if (target === undefined || value > target) targets.set(item.marketplace, value);

    const categoryName = item.category_name || item.category_key || "Без категории";
    const categoryKey = categoryName;
    categories.set(categoryKey, categoryName);
    const key = `${categoryKey}\u0000${item.marketplace}`;
    const previous = latest.get(key);
    if (!previous || value > cubeMonthValue(previous)) latest.set(key, item);
  }

  const targetPeriods = Object.fromEntries(
    marketplaces.map((marketplace) => {
      const target = targets.get(marketplace) ?? 0;
      const year = Math.floor((target - 1) / 12);
      const month = target - year * 12;
      return [marketplace, target ? cubeMonthLabel(year, month) : "-"];
    })
  );

  const rows = [...categories.entries()]
    .sort((left, right) => left[1].localeCompare(right[1], "ru"))
    .map(([categoryKey, categoryName]) => {
      const cells = marketplaces.map((marketplace) => {
        const item = latest.get(`${categoryKey}\u0000${marketplace}`);
        const target = targets.get(marketplace);
        const isReady = Boolean(item && target !== undefined && cubeMonthValue(item) === target);
        const period = item ? cubeMonthLabel(item.year, item.month) : "нет данных";
        const expected = targetPeriods[marketplace] ?? "-";
        return {
          status: isReady ? "ready" : "missing",
          period,
          rowsCount: item?.rows_count ?? 0,
          title: isReady ? `Актуальный срез: ${period}` : `Не хватает актуального среза ${expected}; последний в кубе: ${period}`
        } satisfies CubeMatrixCell;
      });
      return {
        categoryKey,
        categoryName,
        cells,
        missingCount: cells.filter((cell) => cell.status === "missing").length
      } satisfies CubeMatrixRow;
    });

  const missingCount = rows.reduce((total, row) => total + row.missingCount, 0);
  return { marketplaces, rows, missingCount, totalCells: rows.length * marketplaces.length, targetPeriods };
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

function isDataTab(tab: Tab): tab is DataTab {
  return tab === "files" || tab === "cube" || tab === "reports" || tab === "export" || tab === "quality";
}

export function App() {
  const current = monthNow();
  const [mode, setMode] = useState<Mode>("historical_backfill");
  const [tab, setTab] = useState<Tab>("plan");
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
  const [savedCatalogSnapshot, setSavedCatalogSnapshot] = useState<string | null>(null);
  const [pipelineSettings, setPipelineSettings] = useState<PipelineSettings>(defaultPipelineSettings);
  const [classifierRules, setClassifierRules] = useState<ClassifierRule[]>([]);
  const [rulesPath, setRulesPath] = useState("");
  const [selectedRuleId, setSelectedRuleId] = useState<string | null>(null);
  const [manualOverrides, setManualOverrides] = useState<ManualOverride[]>([]);
  const [manualOverridesPath, setManualOverridesPath] = useState("");
  const [selectedManualOverrideId, setSelectedManualOverrideId] = useState<string | null>(null);
  const [classifierQuery, setClassifierQuery] = useState("");
  const [savedClassifierSnapshot, setSavedClassifierSnapshot] = useState<string | null>(null);
  const [savedManualOverridesSnapshot, setSavedManualOverridesSnapshot] = useState<string | null>(null);
  const [externalClassifierFile, setExternalClassifierFile] = useState<File | null>(null);
  const [externalClassifierWriteXlsx, setExternalClassifierWriteXlsx] = useState(false);
  const [externalClassifierResult, setExternalClassifierResult] = useState<ClassificationResponse | null>(null);
  const [run, setRun] = useState<PipelineRun | null>(null);
  const [runs, setRuns] = useState<PipelineRun[]>([]);
  const [smartPlan, setSmartPlan] = useState<SmartPlan | null>(null);
  const [smartPlanFilter, setSmartPlanFilter] = useState<SmartPlanStatus | "all">("all");
  const [files, setFiles] = useState<ProjectFile[]>([]);
  const [fileKindFilter, setFileKindFilter] = useState<FileKindFilter>("all");
  const [cube, setCube] = useState<CubeItem[]>([]);
  const [projects, setProjects] = useState<ProjectSummary[]>([]);
  const [projectQuery, setProjectQuery] = useState("");
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
  const [exportFormat, setExportFormat] = useState<ExportFormat>("xlsx");
  const [exportExcludedRows, setExportExcludedRows] = useState<Set<string>>(new Set());
  const [exportSortColumn, setExportSortColumn] = useState<string | null>(null);
  const [exportSortDirection, setExportSortDirection] = useState<"asc" | "desc">("asc");
  const [exportPreview, setExportPreview] = useState<ExportPreview | null>(null);
  const [exportArtifacts, setExportArtifacts] = useState<ExportArtifact[]>([]);
  const [exportProgress, setExportProgress] = useState<ExportBuildJob | null>(null);
  const [exportTemplates, setExportTemplates] = useState<ExportTemplate[]>([]);
  const [exportTemplateName, setExportTemplateName] = useState("");
  const [exportConfirmLarge, setExportConfirmLarge] = useState(false);
  const [reportOptions, setReportOptions] = useState<ReportOptions | null>(null);
  const [reportType, setReportType] = useState<ReportType>("category_month");
  const [reportCategoryKeys, setReportCategoryKeys] = useState<Set<string>>(new Set());
  const [reportPeriodFrom, setReportPeriodFrom] = useState("");
  const [reportPeriodTo, setReportPeriodTo] = useState("");
  const [reportOutputDir, setReportOutputDir] = useState("");
  const [reportFormat, setReportFormat] = useState<ExportFormat>("xlsx");
  const [reportMaxRows, setReportMaxRows] = useState(5000);
  const [reportPreview, setReportPreview] = useState<ReportPreview | null>(null);
  const [reportArtifacts, setReportArtifacts] = useState<ReportArtifact[]>([]);
  const [busy, setBusy] = useState<string | null>(null);
  const [pipelineOperation, setPipelineOperation] = useState<PipelineOperation | null>(null);
  const [unsavedChangesPrompt, setUnsavedChangesPrompt] = useState<UnsavedChangesPrompt | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [instructionOpen, setInstructionOpen] = useState(false);
  const [leftRailCollapsed, setLeftRailCollapsed] = useState(false);
  const [pipelineSettingsOpen, setPipelineSettingsOpen] = useState(false);
  const exportDefaultOutputDirRef = useRef("");
  const previousRunStatusRef = useRef<string | null>(null);

  useEffect(() => {
    void initialLoad();
  }, []);

  useEffect(() => {
    if (!run?.id) return;
    void refreshRun(run.id);
  }, [smartPlanFilter]);

  useEffect(() => {
    if (!run?.id || !isActiveRunStatus(run.status)) return;
    const id = window.setInterval(() => void refreshRun(run.id), 1500);
    return () => window.clearInterval(id);
  }, [run?.id, run?.status, smartPlanFilter]);

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
    if (!pipelineOperation?.runId || pipelineOperation.finishedAt || run?.id !== pipelineOperation.runId) return;
    if (isActiveRunStatus(run.status)) return;
    setPipelineOperation((current) =>
      current?.runId === run.id && !current.finishedAt ? { ...current, finishedAt: Date.now() } : current
    );
    void refreshFilesAndCube(run.project_name || projectName);
    if (tab === "cube" || products) {
      void loadDbPreview().catch((exc) => setError(`Предпросмотр БД: ${errorText(exc)}`));
    }
  }, [pipelineOperation?.runId, pipelineOperation?.finishedAt, run?.id, run?.status, tab]);

  useEffect(() => {
    if ((tab === "catalog" || tab === "classifier") && catalogRows.length === 0) {
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
    void loadExportTemplates();
  }, [tab, projectName]);

  useEffect(() => {
    if (tab !== "reports") return;
    void loadReportOptions();
  }, [tab, projectName]);

  useEffect(() => {
    if (tab !== "quality") return;
    void loadQualityProjects();
  }, [tab]);

  useEffect(() => {
    if (tab !== "projects") return;
    void loadProjects();
  }, [tab]);

  async function initialLoad() {
    setError(null);
    const loadErrors: string[] = [];
    const [settingsResult, pipelineResult, categoryResult, rulesResult, manualOverridesResult, projectsResult] = await Promise.allSettled([
      api.getWorkflowSettings(),
      api.getPipelineSettings(),
      api.listCategories(),
      api.getClassifierRules(),
      api.getManualOverrides(),
      api.listProjects()
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
      setSavedClassifierSnapshot(editorSnapshot(rulesResult.value.rules));
      setSelectedRuleId((prev) => (prev && rulesResult.value.rules.some((rule) => rule.id === prev) ? prev : null));
    } else {
      addLoadError(loadErrors, "Правила классификатора", rulesResult.reason);
    }

    if (manualOverridesResult.status === "fulfilled") {
      setManualOverridesPath(manualOverridesResult.value.path);
      setManualOverrides(manualOverridesResult.value.overrides);
      setSavedManualOverridesSnapshot(editorSnapshot(manualOverridesResult.value.overrides));
      setSelectedManualOverrideId((prev) =>
        prev && manualOverridesResult.value.overrides.some((override) => override.id === prev) ? prev : null
      );
    } else {
      addLoadError(loadErrors, "Ручные правки SKU", manualOverridesResult.reason);
    }

    if (projectsResult.status === "fulfilled") {
      setProjects(projectsResult.value.projects);
    } else {
      addLoadError(loadErrors, "Проекты", projectsResult.reason);
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

    try {
      const cubeResponse = await api.listCube(loadedProjectName);
      setCube(cubeResponse.items);
    } catch (exc) {
      addLoadError(loadErrors, "Куб", exc);
    }

    setError(loadErrors.length ? loadErrors.join(" | ") : null);
  }

  async function refreshRun(runId: string, statusFilter: SmartPlanStatus | "all" = smartPlanFilter): Promise<string | null> {
    try {
      const [freshRun, smartPlanResponse] = await Promise.all([api.getRun(runId), api.getSmartPlan(runId, statusFilter)]);
      setRun(freshRun);
      setSmartPlan(smartPlanResponse);
      return null;
    } catch (exc) {
      const message = errorText(exc);
      setError(`Обновление запуска: ${message}`);
      return message;
    }
  }

  async function refreshFilesAndCube(targetProjectName = projectName) {
    try {
      const [fileResponse, cubeResponse] = await Promise.all([api.listFiles(targetProjectName), api.listCube(targetProjectName)]);
      setFiles(fileResponse.files);
      setCube(cubeResponse.items);
    } catch (exc) {
      setError(`Файлы и куб: ${errorText(exc)}`);
    }
  }

  async function refreshCube(targetProjectName = projectName) {
    try {
      const cubeResponse = await api.listCube(targetProjectName);
      setCube(cubeResponse.items);
    } catch (exc) {
      setError(`Куб: ${errorText(exc)}`);
    }
  }

  async function loadProjects() {
    try {
      const response = await api.listProjects();
      setProjects(response.projects);
      return response;
    } catch (exc) {
      setError(`Проекты: ${errorText(exc)}`);
      throw exc;
    }
  }

  async function openProject(targetProjectName: string, nextTab: Tab | null = "plan") {
    setProjectName(targetProjectName);
    setRun(null);
    setSmartPlan(null);
    setProducts(null);
    setReportPreview(null);
    setReportArtifacts([]);
    await api.saveWorkflowSettings(workflowSettingsPayload(targetProjectName));
    const [runResponse, cubeResponse, fileResponse] = await Promise.all([
      api.listRuns(targetProjectName),
      api.listCube(targetProjectName),
      api.listFiles(targetProjectName)
    ]);
    setRuns(runResponse.runs);
    setCube(cubeResponse.items);
    setFiles(fileResponse.files);
    if (runResponse.runs[0]) {
      setRun(runResponse.runs[0]);
      await refreshRun(runResponse.runs[0].id, "all");
    }
    if (nextTab) setTab(nextTab);
    return { project_name: targetProjectName };
  }

  async function createProject(targetProjectName: string) {
    const created = await api.createProject(targetProjectName);
    await loadProjects();
    await openProject(created.project_name, null);
    setTab("projects");
    return created;
  }

  async function deleteProject(targetProjectName: string, deleteFiles: boolean) {
    const response = await api.deleteProject(targetProjectName, deleteFiles);
    await loadProjects();
    if (targetProjectName === projectName) {
      setProjectName("mpstats");
      setRun(null);
      setSmartPlan(null);
      setFiles([]);
      setCube([]);
      setProducts(null);
      const settings = await api.saveWorkflowSettings({
        cookie,
        project_name: "mpstats",
        workflow_mode: mode,
        start_year: startYear,
        start_month: startMonth,
        end_year: endYear,
        end_month: endMonth
      });
      setProjectName(settings.project_name || "mpstats");
    }
    return response;
  }

  async function deleteRun(runId: string) {
    const response = await api.deleteRun(runId);
    const runResponse = await api.listRuns(projectName);
    setRuns(runResponse.runs);
    if (run?.id === runId) {
      const nextRun = runResponse.runs[0] ?? null;
      setRun(nextRun);
      setSmartPlan(null);
      if (nextRun) await refreshRun(nextRun.id, "all");
    }
    return response;
  }

  async function deleteProjectFile(file: ProjectFile, deleteCube: boolean) {
    return api.deleteFile(projectName || "mpstats", file.path, deleteCube);
  }

  async function deleteCubeItem(item: CubeItem) {
    setProducts(null);
    return api.deleteCubeEntry(item.id);
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
      const previousDefault = exportDefaultOutputDirRef.current;
      exportDefaultOutputDirRef.current = response.default_output_dir || "";
      setExportOutputDir((prev) => {
        const currentValue = prev.trim();
        if (!currentValue || currentValue === previousDefault) {
          return response.default_output_dir || "";
        }
        return prev;
      });
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

  async function loadExportTemplates(targetProjectName = projectName) {
    try {
      const response = await api.listExportTemplates(targetProjectName || "mpstats");
      setExportTemplates(response.templates);
    } catch (exc) {
      setError(`Шаблоны выгрузки: ${errorText(exc)}`);
    }
  }

  async function loadReportOptions() {
    try {
      const response = await api.getReportOptions(projectName || "mpstats");
      setReportOptions(response);
      setReportPeriodFrom((prev) => prev || response.period_from || "");
      setReportPeriodTo((prev) => prev || response.period_to || "");
      setReportOutputDir((prev) => prev || response.default_output_dir || "");
      setReportCategoryKeys((prev) => {
        const available = new Set(response.categories.map((category) => category.category_key));
        const retained = [...prev].filter((key) => available.has(key));
        const heavy = response.categories.filter((category) => category.is_heavy).map((category) => category.category_key);
        return new Set(retained.length ? retained : heavy.length ? heavy : response.categories.map((category) => category.category_key));
      });
    } catch (exc) {
      setError(`Отчёты: ${errorText(exc)}`);
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

  function normalizedExportFilters(options: ExportOptions): ExportColumnFilter[] {
    return exportFilters
      .map((filter) => ({ column: filter.column, value: filter.value.trim(), match_type: filter.match_type }))
      .filter((filter) => filter.value && options.columns.includes(filter.column));
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
    return {
      project_name: projectName || "mpstats",
      category_keys: [...exportCategoryKeys],
      period_from: exportPeriodFrom || null,
      period_to: exportPeriodTo || null,
      selected_columns: selectedColumns,
      filters: normalizedExportFilters(exportOptions),
      excluded_row_hashes: [...exportExcludedRows],
      sort_column: exportSortColumn,
      sort_direction: exportSortDirection,
      split_by_category: exportSplitByCategory,
      export_format: exportFormat,
      output_dir: exportOutputDir || null,
      confirm_large_export: exportConfirmLarge,
      limit,
      offset: 0
    };
  }

  function buildReportPayload(limit = 100): ReportPayload {
    if (!reportOptions) {
      throw new Error("Настройки отчётов ещё не загружены.");
    }
    return {
      project_name: projectName || "mpstats",
      report_type: reportType,
      category_keys: [...reportCategoryKeys],
      period_from: reportPeriodFrom || null,
      period_to: reportPeriodTo || null,
      export_format: reportFormat,
      output_dir: reportOutputDir || null,
      max_rows: reportMaxRows,
      limit,
      offset: 0
    };
  }

  function buildExportTemplatePayload(name: string): ExportTemplatePayload {
    if (!exportOptions) {
      throw new Error("Настройки выгрузки ещё не загружены.");
    }
    const selectedColumns = selectedExportColumns();
    if (!selectedColumns.length) {
      throw new Error("Выбери хотя бы одну колонку для шаблона.");
    }
    if (!exportCategoryKeys.size) {
      throw new Error("Выбери хотя бы одну категорию для шаблона.");
    }
    return {
      name: name.trim(),
      project_name: projectName || "mpstats",
      category_keys: [...exportCategoryKeys],
      period_from: exportPeriodFrom || null,
      period_to: exportPeriodTo || null,
      selected_columns: selectedColumns,
      filters: normalizedExportFilters(exportOptions),
      sort_column: exportSortColumn,
      sort_direction: exportSortDirection,
      split_by_category: exportSplitByCategory,
      export_format: exportFormat,
      output_dir: exportOutputDir || null
    };
  }

  function payloadFromExportTemplate(template: ExportTemplate, limit = 100): ExportPayload {
    return {
      project_name: template.project_name,
      category_keys: template.category_keys,
      period_from: template.period_from ?? null,
      period_to: template.period_to ?? null,
      selected_columns: template.selected_columns,
      filters: template.filters,
      excluded_row_hashes: [],
      sort_column: template.sort_column ?? null,
      sort_direction: template.sort_direction,
      split_by_category: template.split_by_category,
      export_format: template.export_format ?? "xlsx",
      output_dir: template.output_dir ?? null,
      confirm_large_export: false,
      limit,
      offset: 0
    };
  }

  function applyExportTemplate(template: ExportTemplate) {
    const availableCategories = new Set(exportOptions?.categories.map((category) => category.category_key) ?? template.category_keys);
    const availableColumns = new Set(exportOptions?.columns ?? template.selected_columns);
    const categoryKeys = template.category_keys.filter((key) => availableCategories.has(key));
    const columns = template.selected_columns.filter((column) => availableColumns.has(column));
    setExportCategoryKeys(new Set(categoryKeys.length ? categoryKeys : template.category_keys));
    setExportSelectedColumns(new Set(columns.length ? columns : template.selected_columns));
    setExportPeriodFrom(template.period_from ?? "");
    setExportPeriodTo(template.period_to ?? "");
    setExportOutputDir(template.output_dir ?? "");
    setExportSplitByCategory(Boolean(template.split_by_category));
    setExportFormat(template.export_format ?? "xlsx");
    setExportFilters(template.filters.map((filter, index) => ({ ...filter, id: `template-filter-${template.id}-${index}` })));
    setExportSortColumn(template.sort_column ?? null);
    setExportSortDirection(template.sort_direction === "desc" ? "desc" : "asc");
    setExportExcludedRows(new Set());
    setExportConfirmLarge(false);
    setExportPreview(null);
    setExportArtifacts([]);
    setExportTemplateName(template.name);
  }

  async function saveExportTemplate() {
    const name = exportTemplateName.trim();
    if (!name) {
      throw new Error("Введи название шаблона.");
    }
    const saved = await api.saveExportTemplate(buildExportTemplatePayload(name));
    setExportTemplateName(saved.name);
    await loadExportTemplates(saved.project_name);
    return saved;
  }

  async function deleteExportTemplate(template: ExportTemplate) {
    const response = await api.deleteExportTemplate(template.id, template.project_name);
    await loadExportTemplates(template.project_name);
    if (exportTemplateName === template.name) setExportTemplateName("");
    return response;
  }

  async function loadExportPreview() {
    setExportProgress(null);
    const response = await api.previewExport(buildExportPayload(100));
    setExportPreview(response);
    if (response.estimated_files <= 10) setExportConfirmLarge(false);
    return response;
  }

  async function waitForExportJob(jobId: string) {
    let job = await api.getExportBuildJob(jobId);
    setExportProgress(job);
    while (job.status === "queued" || job.status === "running") {
      await sleep(700);
      job = await api.getExportBuildJob(jobId);
      setExportProgress(job);
    }
    if (job.status === "failed") {
      throw new Error(job.error || "Выгрузка завершилась ошибкой.");
    }
    if (!job.result) {
      throw new Error("Выгрузка завершилась без результата.");
    }
    return job.result;
  }

  async function startExportJob(payload: ExportPayload) {
    setExportArtifacts([]);
    const job = await api.startExportBuild(payload);
    setExportProgress(job);
    return waitForExportJob(job.id);
  }

  async function buildExportFiles() {
    const response = await startExportJob(buildExportPayload(100));
    setExportArtifacts(response.artifacts);
    setExportPreview((prev) => (prev ? { ...prev, total: response.total, estimated_files: response.estimated_files, warnings: response.warnings } : prev));
    return response;
  }

  async function buildExportFromTemplate(template: ExportTemplate) {
    applyExportTemplate(template);
    const response = await startExportJob(payloadFromExportTemplate(template, 100));
    setExportArtifacts(response.artifacts);
    setExportPreview(null);
    return response;
  }

  async function loadReportPreview() {
    setReportArtifacts([]);
    const response = await api.previewReport(buildReportPayload(100));
    setReportPreview(response);
    return response;
  }

  async function buildReportFile() {
    const response: ReportBuildResponse = await api.buildReport(buildReportPayload(100));
    setReportArtifacts(response.artifacts);
    setReportPreview((prev) => (prev ? { ...prev, total: response.source_total, warnings: response.warnings } : prev));
    await refreshCube(projectName);
    return response;
  }

  async function loadCategorySource() {
    try {
      const response = await api.getCategorySource();
      setCatalogPath(response.path);
      setCatalogRows(response.rows);
      setSavedCatalogSnapshot(editorSnapshot(response.rows));
      setSelectedCatalogId((prev) => prev ?? response.rows[0]?.id ?? null);
    } catch (exc) {
      setError(`Справочник категорий: ${errorText(exc)}`);
    }
  }

  async function saveCategorySource() {
    const response = await api.saveCategorySource(catalogRows);
    setCatalogPath(response.path);
    setCatalogRows(response.rows);
    setSavedCatalogSnapshot(editorSnapshot(response.rows));
    setSelectedCatalogId(response.rows[0]?.id ?? null);
    const categoryResponse = await api.listCategories();
    setCategories(categoryResponse.categories);
    return response;
  }

  async function runAction<T>(label: string, action: () => Promise<T>, onSuccess?: (value: T) => void): Promise<T | null> {
    setBusy(label);
    setError(null);
    setMessage(null);
    try {
      const result = await action();
      onSuccess?.(result);
      const backgroundRun = isPipelineRun(result) && isActiveRunStatus(result.status);
      setMessage(backgroundRun ? `${label}: запущено, прогресс открыт` : `${label}: готово`);
      if (!backgroundRun) {
        if (tab === "files" || tab === "cube") {
          await refreshFilesAndCube();
        } else if (tab === "plan") {
          await refreshCube();
        }
        if (tab === "cube" && !passiveCubeActions.has(label)) {
          await loadDbPreview();
        }
      }
      return result;
    } catch (exc) {
      setError(errorText(exc));
      return null;
    } finally {
      setBusy(null);
    }
  }

  async function runPipelineAction(
    kind: PipelineOperationKind,
    label: string,
    detail: string,
    action: () => Promise<PipelineRun>,
    onSuccess?: (value: PipelineRun) => void
  ) {
    setPipelineOperation({ kind, label, detail, runId: run?.id ?? null, startedAt: Date.now(), finishedAt: null });
    const result = await runAction(label, action, (fresh) => {
      setRun(fresh);
      setPipelineOperation((current) =>
        current ? { ...current, runId: fresh.id, finishedAt: isActiveRunStatus(fresh.status) ? null : Date.now() } : current
      );
      onSuccess?.(fresh);
    });
    if (!result) {
      setPipelineOperation((current) => (current?.label === label ? null : current));
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
  const classifierCategoryOptions = useMemo(() => {
    const names = new Set<string>();
    for (const row of catalogRows) {
      const name = row.category_name.trim();
      if (name) names.add(name);
    }
    if (!names.size) {
      for (const category of categories) {
        const name = category.category_name.trim();
        if (name) names.add(name);
      }
    }
    return [...names].sort((left, right) => left.localeCompare(right, "ru"));
  }, [catalogRows, categories]);
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
  const filteredManualOverrides = useMemo(() => {
    const text = classifierQuery.trim().toLowerCase();
    if (!text) return manualOverrides;
    return manualOverrides.filter((override) => {
      const haystack = [
        override.priority,
        override.match_field,
        override.match_value,
        override.target_column,
        override.set_value,
        override.mode,
        override.comment
      ]
        .join(" ")
        .toLowerCase();
      return haystack.includes(text);
    });
  }, [manualOverrides, classifierQuery]);
  const selectedRule = useMemo(() => classifierRules.find((rule) => rule.id === selectedRuleId) ?? null, [classifierRules, selectedRuleId]);
  const selectedManualOverride = useMemo(
    () => manualOverrides.find((override) => override.id === selectedManualOverrideId) ?? null,
    [manualOverrides, selectedManualOverrideId]
  );
  const catalogDirty = useMemo(
    () => savedCatalogSnapshot !== null && editorSnapshot(catalogRows) !== savedCatalogSnapshot,
    [catalogRows, savedCatalogSnapshot]
  );
  const classifierDirty = useMemo(
    () =>
      (savedClassifierSnapshot !== null && editorSnapshot(classifierRules) !== savedClassifierSnapshot) ||
      (savedManualOverridesSnapshot !== null && editorSnapshot(manualOverrides) !== savedManualOverridesSnapshot),
    [classifierRules, manualOverrides, savedClassifierSnapshot, savedManualOverridesSnapshot]
  );

  useEffect(() => {
    if (!catalogDirty && !classifierDirty) return;
    const handler = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", handler);
    return () => window.removeEventListener("beforeunload", handler);
  }, [catalogDirty, classifierDirty]);

  function dirtyWorkspaceForCurrentTab(): EditableWorkspace | null {
    if (tab === "catalog" && catalogDirty) return "catalog";
    if (tab === "classifier" && classifierDirty) return "classifier";
    return null;
  }

  function guardUnsavedChanges(proceed: () => void | Promise<void>, forcedKind?: EditableWorkspace | null) {
    const kind = forcedKind ?? dirtyWorkspaceForCurrentTab();
    if (!kind) {
      void proceed();
      return;
    }
    setUnsavedChangesPrompt({ kind, proceed });
  }

  function changeTab(nextTab: Tab, afterChange?: () => void | Promise<void>) {
    const proceed = async () => {
      setTab(nextTab);
      await afterChange?.();
    };
    const kind = nextTab === tab && !afterChange ? null : dirtyWorkspaceForCurrentTab();
    guardUnsavedChanges(proceed, kind);
  }

  function discardEditorChanges(kind: EditableWorkspace) {
    if (kind === "catalog") {
      const rows = restoreSnapshot<CategorySourceRow[]>(savedCatalogSnapshot);
      if (!rows) return;
      setCatalogRows(rows);
      setSelectedCatalogId((prev) => (prev && rows.some((row) => row.id === prev) ? prev : rows[0]?.id ?? null));
      return;
    }
    const rules = restoreSnapshot<ClassifierRule[]>(savedClassifierSnapshot);
    const overrides = restoreSnapshot<ManualOverride[]>(savedManualOverridesSnapshot);
    if (!rules || !overrides) return;
    setClassifierRules(rules);
    setManualOverrides(overrides);
    setSelectedRuleId((prev) => (prev && rules.some((rule) => rule.id === prev) ? prev : null));
    setSelectedManualOverrideId((prev) => (prev && overrides.some((override) => override.id === prev) ? prev : null));
  }

  async function saveDirtyWorkspace(kind: EditableWorkspace): Promise<boolean> {
    if (kind === "catalog") {
      return Boolean(await runAction("Сохранение справочника", saveCategorySource));
    }
    return saveClassifierWorkspace();
  }

  async function saveCategorySourceFromEditor() {
    return Boolean(await runAction("Сохранение справочника", saveCategorySource));
  }

  async function confirmUnsavedSave() {
    const prompt = unsavedChangesPrompt;
    if (!prompt) return;
    const saved = await saveDirtyWorkspace(prompt.kind);
    if (!saved) return;
    setUnsavedChangesPrompt(null);
    await prompt.proceed();
  }

  async function confirmUnsavedDiscard() {
    const prompt = unsavedChangesPrompt;
    if (!prompt) return;
    discardEditorChanges(prompt.kind);
    setUnsavedChangesPrompt(null);
    await prompt.proceed();
  }
  const filteredProjects = useMemo(() => {
    const text = projectQuery.trim().toLowerCase();
    if (!text) return projects;
    return projects.filter((project) =>
      `${project.project_name} ${project.data_path} ${project.latest_period ?? ""}`.toLowerCase().includes(text)
    );
  }, [projects, projectQuery]);
  const projectOptions = useMemo(() => {
    const names = projects.map((project) => project.project_name);
    if (projectName && !names.includes(projectName)) names.unshift(projectName);
    return names;
  }, [projects, projectName]);
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

  function toggleReportCategory(id: string) {
    setReportCategoryKeys((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
    setReportPreview(null);
    setReportArtifacts([]);
  }

  function toggleAllExportCategories() {
    const all = exportOptions?.categories.map((category) => category.category_key) ?? [];
    setExportCategoryKeys((prev) => (prev.size === all.length ? new Set() : new Set(all)));
  }

  function toggleAllReportCategories() {
    const all = reportOptions?.categories.map((category) => category.category_key) ?? [];
    setReportCategoryKeys((prev) => (prev.size === all.length ? new Set() : new Set(all)));
    setReportPreview(null);
    setReportArtifacts([]);
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

  function changeExportFormat(value: ExportFormat) {
    setExportFormat(value);
    setExportPreview(null);
    setExportArtifacts([]);
    setExportProgress(null);
    setExportConfirmLarge(false);
  }

  function changeReportType(value: ReportType) {
    setReportType(value);
    setReportPreview(null);
    setReportArtifacts([]);
  }

  function changeReportFormat(value: ExportFormat) {
    setReportFormat(value);
    setReportPreview(null);
    setReportArtifacts([]);
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
    setCatalogRows((prev) => prev.map((row) => {
      if (row.id !== id) return row;
      const next = { ...row, ...patch };
      if (patch.marketplace !== undefined) next.fbs = isYandexMarketplace(patch.marketplace) ? false : true;
      if (isYandexMarketplace(next.marketplace)) next.fbs = false;
      return next;
    }));
  }

  function addCatalogRow() {
    const id = `new-category-${Date.now()}`;
    const next: CategorySourceRow = {
      id,
      active: true,
      category_name: "",
      marketplace: "WB",
      fbs: true,
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

  async function saveClassifierRules(): Promise<boolean> {
    const selectedIndex = classifierRules.findIndex((rule) => rule.id === selectedRuleId);
    const response = await runAction("Сохранение правил классификатора", () => api.saveClassifierRules(classifierRules), (response) => {
      setRulesPath(response.path);
      setClassifierRules(response.rules);
      setSavedClassifierSnapshot(editorSnapshot(response.rules));
      setSelectedRuleId(selectedIndex >= 0 ? response.rules[selectedIndex]?.id ?? null : null);
    });
    return Boolean(response);
  }

  function updateManualOverride(id: string, patch: Partial<ManualOverride>) {
    setManualOverrides((prev) => prev.map((override) => (override.id === id ? { ...override, ...patch } : override)));
  }

  function addManualOverride(kind: "classification" | "sku-group" = "classification") {
    const id = `new-manual-override-${Date.now()}`;
    const next: ManualOverride = {
      id,
      active: true,
      priority: nextManualPriority(manualOverrides),
      match_field: "Артикул",
      match_value: "",
      target_column: kind === "sku-group" ? "SKU-группа" : "Подкатегория",
      set_value: "",
      mode: "overwrite",
      comment: ""
    };
    setManualOverrides((prev) => [...prev, next]);
    setSelectedManualOverrideId(id);
  }

  function deleteManualOverride(id: string) {
    setManualOverrides((prev) => prev.filter((override) => override.id !== id));
    setSelectedManualOverrideId((prev) => (prev === id ? null : prev));
  }

  async function saveManualOverrides(): Promise<boolean> {
    const selectedIndex = manualOverrides.findIndex((override) => override.id === selectedManualOverrideId);
    const response = await runAction("Сохранение ручных правок SKU", () => api.saveManualOverrides(manualOverrides), (response) => {
      setManualOverridesPath(response.path);
      setManualOverrides(response.overrides);
      setSavedManualOverridesSnapshot(editorSnapshot(response.overrides));
      setSelectedManualOverrideId(selectedIndex >= 0 ? response.overrides[selectedIndex]?.id ?? null : null);
    });
    return Boolean(response);
  }

  async function saveClassifierWorkspace(): Promise<boolean> {
    const rulesChanged = savedClassifierSnapshot !== null && editorSnapshot(classifierRules) !== savedClassifierSnapshot;
    const overridesChanged = savedManualOverridesSnapshot !== null && editorSnapshot(manualOverrides) !== savedManualOverridesSnapshot;
    if (rulesChanged && !(await saveClassifierRules())) return false;
    if (overridesChanged && !(await saveManualOverrides())) return false;
    return true;
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
    setSmartPlanFilter("all");
    setTab("plan");
    await refreshRun(created.id, "all");
    await refreshCube(projectName);
    const runResponse = await api.listRuns(projectName);
    setRuns(runResponse.runs);
  }

  async function syncNewMonth() {
    await saveCurrentWorkflowSettings();
    const created = await api.monthlySync({ project_name: projectName, settings: pipelineSettings, start_immediately: true, wait: false });
    setRun(created);
    setSmartPlanFilter("all");
    setTab("plan");
    await refreshRun(created.id, "all");
    await refreshCube(projectName);
    return created;
  }

  function workflowSettingsPayload(targetProjectName = projectName) {
    return {
      cookie,
      project_name: targetProjectName,
      workflow_mode: mode,
      start_year: startYear,
      start_month: startMonth,
      end_year: endYear,
      end_month: endMonth
    };
  }

  function saveCurrentWorkflowSettings() {
    return api.saveWorkflowSettings(workflowSettingsPayload());
  }

  const runIsActive = isActiveRunStatus(run?.status);
  const runIsRunning = run?.status === "running";
  const runIsPaused = run?.status === "paused";
  const runHasTasks = Boolean(run?.id && (run.total_tasks ?? 0) > 0);
  const runHasRemainingWork = Boolean(run?.id && ((run.remaining_tasks ?? 0) > 0 || (run.failed_tasks ?? 0) > 0));
  const operationRun = pipelineOperation && (!pipelineOperation.runId || run?.id === pipelineOperation.runId) ? run : null;
  const activeOperationKind = pipelineOperation && !pipelineOperation.finishedAt ? pipelineOperation.kind : null;
  const canCreatePlan = !busy && !runIsActive && (mode === "monthly_sync" || (selected.size > 0 && monthsCount > 0));
  const canStartRun = !busy && !runIsActive && runHasTasks && runHasRemainingWork;
  const canPauseRun = !busy && Boolean(run?.id) && runIsRunning;
  const canStopRun = !busy && Boolean(run?.id) && (runIsRunning || run?.status === "pausing" || runIsPaused);
  const canResumeRun = !busy && Boolean(run?.id) && runIsPaused;
  const canRetryErrors = !busy && Boolean(run?.id) && !runIsActive && (run?.failed_tasks ?? 0) > 0;
  const canRebuildCube = !busy && runHasTasks && !runIsActive;
  const canSyncNewMonth = !busy && !runIsActive;
  const createPlanDisabledTitle =
    mode === "historical_backfill" && !selected.size
      ? "Сначала выбери категории."
      : mode === "historical_backfill" && monthsCount <= 0
        ? "Проверь диапазон месяцев."
        : runIsActive
          ? "Дождись завершения текущего запуска или поставь его на паузу."
          : undefined;
  const runBusyTitle = runIsActive ? "Дождись завершения текущей операции или поставь запуск на паузу." : undefined;

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
        <div className="header-actions">
          <a className="author-credit" href="https://github.com/exoldoff" target="_blank" rel="noreferrer" title="Автор проекта на GitHub">
            <Github size={17} />
            Made by <strong>@exoldoff</strong>
          </a>
          <a className="social-credit" href="https://t.me/exoldoff" target="_blank" rel="noreferrer" aria-label="Telegram @exoldoff" title="Telegram @exoldoff">
            <Send size={17} />
          </a>
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
        </div>
      </header>

      {error ? <Notice tone="error" text={error} onClose={() => setError(null)} /> : null}
      {message ? <Notice tone="success" text={message} onClose={() => setMessage(null)} /> : null}
      {pipelineOperation ? (
        <PipelineOperationModal
          operation={pipelineOperation}
          run={operationRun}
          onClose={() => setPipelineOperation(null)}
          onOpenPlan={() => changeTab("plan")}
          onOpenCube={() => changeTab("cube")}
        />
      ) : null}
      {unsavedChangesPrompt ? (
        <UnsavedChangesModal
          kind={unsavedChangesPrompt.kind}
          busy={Boolean(busy)}
          onSave={() => void confirmUnsavedSave()}
          onDiscard={() => void confirmUnsavedDiscard()}
          onCancel={() => setUnsavedChangesPrompt(null)}
        />
      ) : null}

      <main className={`workflow-grid ${leftRailCollapsed ? "left-collapsed" : ""}`}>
        <aside className={`left-rail ${leftRailCollapsed ? "collapsed" : ""}`}>
          <section className="panel rail-control-panel">
            <button
              className="icon-button"
              title={leftRailCollapsed ? "Развернуть настройки" : "Свернуть настройки"}
              aria-label={leftRailCollapsed ? "Развернуть настройки" : "Свернуть настройки"}
              onClick={() => setLeftRailCollapsed((value) => !value)}
            >
              {leftRailCollapsed ? <PanelLeftOpen size={18} /> : <PanelLeftClose size={18} />}
            </button>
            {!leftRailCollapsed ? (
              <div>
                <strong>Настройки</strong>
                <small>Проект, доступ и pipeline</small>
              </div>
            ) : null}
          </section>

          {!leftRailCollapsed ? (
          <>
          <section className="panel">
            <SectionTitle icon={<Settings />} title="Проект и доступ" hint="Project name разделяет файлы, manifest и записи БД. Cookie нужен только для скачивания из MPStats." />
            <div className="form-grid">
              <label>
                <FieldLabel text="Текущий проект" hint="Выбор только из созданных проектов. Новый проект создаётся в разделе «Проекты» кнопкой «Создать»." />
                <select
                  value={projectName}
                  disabled={Boolean(busy) || projectOptions.length === 0}
                  onChange={(event) => {
                    const nextProjectName = event.target.value;
                    if (nextProjectName && nextProjectName !== projectName) {
                      void runAction("Открытие проекта", () => openProject(nextProjectName, null));
                    }
                  }}
                >
                  {projectOptions.map((name) => (
                    <option key={name} value={name}>{name}</option>
                  ))}
                </select>
              </label>
              <label>
                <FieldLabel text="MPStats cookie" hint="Cookie текущей авторизованной сессии MPStats. Нужен только для скачивания; если устарел, задачи будут падать с ошибкой доступа." />
                <textarea className="cookie-input" value={cookie} onChange={(event) => setCookie(event.target.value)} placeholder="Вставь cookie из MPStats" />
              </label>
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
            <div className="collapsible-title">
              <div><ListChecks size={18} /><h2>Настройки pipeline</h2><Hint text="Эти параметры управляют повторными запусками: что можно пересобирать, сколько ждать MPStats и какие файлы считать готовыми." /></div>
              <button className="tiny-button" type="button" onClick={() => setPipelineSettingsOpen((value) => !value)}>
                {pipelineSettingsOpen ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                {pipelineSettingsOpen ? "Скрыть" : "Открыть"}
              </button>
            </div>
            {pipelineSettingsOpen ? (
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
            ) : (
              <div className="settings-summary">
                <span>{pipelineSettings.overwrite_raw ? "raw: заново" : "raw: reuse"}</span>
                <span>{pipelineSettings.overwrite_processed ? "processed: заново" : "processed: reuse"}</span>
                <span>{pipelineSettings.overwrite_db ? "БД: overwrite" : "БД: без дублей"}</span>
                <span>{pipelineSettings.retry_count} повтор(а), {pipelineSettings.timeout_seconds} сек</span>
              </div>
            )}
          </section>

          <section className="panel">
            <SectionTitle icon={<FileSpreadsheet />} title="Правила и справочник" hint="Редактирование вынесено в центральные вкладки, чтобы не работать с сырым CSV или JSON." />
            <div className="mini-actions">
              <button className="ghost-button" title="Открывает полноценный редактор правил классификатора." onClick={() => changeTab("classifier")}>
                <FileSpreadsheet size={17} />
                Правила
              </button>
	              <button className="ghost-button" title="Открывает CSV-справочник категорий и путей." onClick={() => changeTab("catalog", loadCategorySource)}>
	                <FolderSync size={17} />
	                Справочник
	              </button>
	              <button className="ghost-button" title="Показать сохранённые проекты, кубы, файлы и удаления." onClick={() => changeTab("projects", loadProjects)}>
	                <Archive size={17} />
	                Проекты
	              </button>
	            </div>
	          </section>
          </>
          ) : null}
        </aside>

        <section className="center-stage">
          <nav className="tabs main-tabs" aria-label="Основные разделы">
            <button className={tab === "projects" ? "active" : ""} title="Список проектов, выбор и удаление." onClick={() => changeTab("projects", loadProjects)}>Проекты</button>
            <button className={tab === "plan" ? "active" : ""} onClick={() => changeTab("plan")}>Умный план</button>
            <button className={tab === "categories" ? "active" : ""} title="Выбор активных путей для исторической загрузки." onClick={() => changeTab("categories")}>Категории</button>
            <button className={isDataTab(tab) ? "active" : ""} title="Куб, отчёты, файлы, выгрузка и проверка качества." onClick={() => changeTab("cube")}>Данные</button>
            <button className={tab === "classifier" ? "active" : ""} title="Правила классификатора без ручного JSON." onClick={() => changeTab("classifier")}>Классификатор</button>
          </nav>

          {isDataTab(tab) ? (
            <DataSubnav
              activeTab={tab}
              onSelect={(nextTab) => {
                changeTab(nextTab, nextTab === "files" ? refreshFilesAndCube : undefined);
              }}
            />
          ) : null}

	          {tab === "projects" ? (
	            <ProjectsWorkspace
	              projects={filteredProjects}
	              totalProjects={projects.length}
	              currentProjectName={projectName}
	              query={projectQuery}
	              busy={Boolean(busy)}
	              onQueryChange={setProjectQuery}
	              onReload={() => void runAction("Обновление списка проектов", loadProjects)}
	              onCreate={(name) => void runAction("Создание проекта", () => createProject(name))}
	              onOpen={(name) => void runAction("Открытие проекта", () => openProject(name))}
	              onDelete={(name, deleteFiles) => void runAction("Удаление проекта", () => deleteProject(name, deleteFiles))}
	            />
	          ) : null}

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
              onReload={() => guardUnsavedChanges(loadCategorySource, catalogDirty ? "catalog" : null)}
              onSave={() => void saveCategorySourceFromEditor()}
              onChange={updateCatalogRow}
              dirty={catalogDirty}
            />
          ) : null}

          {tab === "plan" ? (
            <section className="panel stage-panel">
              <SectionTitle
                icon={<Table2 />}
                title="Умный план"
                meta={smartPlan ? `${smartPlan.summary.total} задач` : run ? `${run.total_tasks} задач` : "план не создан"}
                hint="Сверяет желаемые задачи с локальными файлами и БД: готово, нет файлов, устарело, ошибка или неполно."
              />
              <div className="plan-setup">
                <div className="plan-setup-head">
                  <div>
                    <strong>{mode === "monthly_sync" ? "Новый месяц" : "Период плана"}</strong>
                    <small>{mode === "monthly_sync" ? "следующий месяц определяется по registry" : `${selected.size} категорий, ${monthsCount} месяцев`}</small>
                  </div>
                  <button
                    className="primary-inline-button"
                    disabled={!canCreatePlan}
                    title={createPlanDisabledTitle}
                    onClick={() => void runAction("Создание плана", createPlan)}
                  >
                    <ListChecks size={17} />
                    Создать план
                  </button>
                </div>
                {mode === "historical_backfill" ? (
                  <div className="period-grid compact-period-grid">
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
                  <div className="sync-note compact-sync-note">
                    <strong>Месяц выбирать не нужно.</strong>
                    <span>Берём последний сохранённый месяц и создаём задачи на следующий.</span>
                  </div>
                )}
              </div>
              {smartPlan ? <SmartPlanOverview plan={smartPlan} /> : null}
              <CubeMatrixTable items={cube} />
              <div className="toolbar wrap">
                {smartPlanFilters.map((filter) => (
                  <button key={filter.value} className={`chip-button ${smartPlanFilter === filter.value ? "active" : ""}`} onClick={() => setSmartPlanFilter(filter.value)}>
                    {filter.label}
                    {smartPlan && filter.value !== "all" ? <small>{smartPlan.summary[filter.value]}</small> : null}
                  </button>
                ))}
              </div>
              {smartPlan?.tasks.length ? (
                <SmartPlanTable
                  tasks={smartPlan.tasks}
                  onRetry={(taskId) => void runAction("Повтор задачи", () => api.retryTask(taskId), (fresh) => { setRun(fresh); void refreshRun(fresh.id); })}
                />
              ) : (
                <Empty text={run ? "Нет задач по текущему фильтру умного плана." : "Создай план, и здесь появятся задачи category × marketplace × month."} />
              )}
            </section>
          ) : null}

          {tab === "files" ? (
            <section className="panel stage-panel">
              <SectionTitle
                icon={<Archive />}
                title="Файлы"
                meta={`${filteredFiles.length}/${files.length} файлов`}
                hint="Рабочий путь web-app: raw -> processed -> classified -> куб -> XLSX выгрузка. Legacy merged-файлы здесь скрыты, куб теперь хранится в БД."
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
              {filteredFiles.length ? (
                <FilesTable
                  files={filteredFiles}
                  busy={Boolean(busy)}
                  onDelete={(file, deleteCube) => void runAction("Удаление файла", () => deleteProjectFile(file, deleteCube))}
                />
              ) : <Empty text={files.length ? "В выбранном типе файлов пока нет." : "Файлы появятся после скачивания и обработки."} />}
            </section>
          ) : null}

          {tab === "cube" ? (
            <section className="panel stage-panel">
              <SectionTitle icon={<Database />} title="Куб" meta={`${cube.length} срезов`} />
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
              {cube.length ? (
                <CubeTable
                  items={cube}
                  busy={Boolean(busy)}
                  onDelete={(item) => void runAction("Удаление среза куба", () => deleteCubeItem(item))}
                />
              ) : <Empty text="После сохранения задач здесь появится registry куба." />}
              {products ? (
                <div className="db-results">
                  <h3>{productResultTitle}: {products.total} записей</h3>
                  <SimpleTable columns={visibleProductColumns(products.columns)} rows={products.rows} />
                </div>
              ) : null}
            </section>
          ) : null}

          {tab === "reports" ? (
            <ReportsWorkspace
              options={reportOptions}
              reportType={reportType}
              selectedCategoryKeys={reportCategoryKeys}
              periodFrom={reportPeriodFrom}
              periodTo={reportPeriodTo}
              outputDir={reportOutputDir}
              exportFormat={reportFormat}
              maxRows={reportMaxRows}
              preview={reportPreview}
              artifacts={reportArtifacts}
              busy={Boolean(busy)}
              onReportTypeChange={changeReportType}
              onToggleCategory={toggleReportCategory}
              onToggleAllCategories={toggleAllReportCategories}
              onPeriodFromChange={setReportPeriodFrom}
              onPeriodToChange={setReportPeriodTo}
              onOutputDirChange={setReportOutputDir}
              onExportFormatChange={changeReportFormat}
              onMaxRowsChange={setReportMaxRows}
              onReloadOptions={loadReportOptions}
              onPreview={() => void runAction("Предпросмотр отчёта", loadReportPreview)}
              onBuild={() => void runAction(`Отчёт ${reportFormat.toUpperCase()}`, buildReportFile)}
            />
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
              exportFormat={exportFormat}
              excludedCount={exportExcludedRows.size}
              sortColumn={exportSortColumn}
              sortDirection={exportSortDirection}
              preview={exportPreview}
              artifacts={exportArtifacts}
              progress={exportProgress}
              templates={exportTemplates}
              templateName={exportTemplateName}
              confirmLarge={exportConfirmLarge}
              busy={Boolean(busy)}
              onTemplateNameChange={setExportTemplateName}
              onSaveTemplate={() => void runAction("Сохранение шаблона выгрузки", saveExportTemplate)}
              onApplyTemplate={applyExportTemplate}
              onBuildTemplate={(template) => void runAction(`Выгрузка ${(template.export_format ?? "xlsx").toUpperCase()} по шаблону`, () => buildExportFromTemplate(template))}
              onDeleteTemplate={(template) => {
                if (window.confirm(`Удалить шаблон «${template.name}»?`)) {
                  void runAction("Удаление шаблона выгрузки", () => deleteExportTemplate(template));
                }
              }}
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
              onExportFormatChange={changeExportFormat}
              onConfirmLargeChange={setExportConfirmLarge}
              onSort={toggleExportSort}
              onClearSort={() => {
                setExportSortColumn(null);
                setExportSortDirection("asc");
              }}
              onExcludeRow={excludeExportRow}
              onClearExcluded={() => setExportExcludedRows(new Set())}
              onPreview={() => void runAction("Предпросмотр выгрузки", loadExportPreview)}
              onBuild={() => void runAction(`Выгрузка ${exportFormat.toUpperCase()}`, buildExportFiles)}
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
              categoryOptions={classifierCategoryOptions}
              manualOverrides={filteredManualOverrides}
              totalManualOverrides={manualOverrides.length}
              selectedManualOverride={selectedManualOverride}
              manualOverridesPath={manualOverridesPath}
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
              onSave={() => void saveClassifierRules()}
              onManualSelect={setSelectedManualOverrideId}
              onManualAdd={addManualOverride}
              onManualChange={updateManualOverride}
              onManualDelete={deleteManualOverride}
              onManualSave={() => void saveManualOverrides()}
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
              dirty={classifierDirty}
            />
          ) : null}
        </section>

        <aside className="right-rail">
          <section className="panel flow-panel">
            <SectionTitle icon={<Play />} title="Текущий запуск" />
            <RunSummary run={run} selectedCount={selectedCategories.length} monthsCount={monthsCount} mode={mode} />
            <div className="action-stack">
              <div className="action-group">
                <span className="action-group-label">План</span>
                <button className="action-button" disabled={!canCreatePlan} title={createPlanDisabledTitle} onClick={() => void runAction("Создание плана", createPlan)}>
                  <ListChecks size={20} />
                  <span><strong>Создать план</strong><small>Проверить manifest и файлы</small></span>
                </button>
                <button
                  className={`action-button ${activeOperationKind === "start" ? "is-working" : ""}`}
                  disabled={!canStartRun}
                  title={runBusyTitle}
                  onClick={() => run?.id && void runPipelineAction("start", "Запуск", "Выполняются ожидающие задачи и ошибки текущего плана.", () => api.startRun(run.id), (fresh) => { void refreshRun(fresh.id); })}
                >
                  {activeOperationKind === "start" ? <LoaderCircle className="spin" size={20} /> : <Play size={20} />}
                  <span><strong>{activeOperationKind === "start" ? "Запуск идёт" : "Запустить"}</strong><small>Ожидающие задачи и ошибки</small></span>
                </button>
                <button className="action-button" disabled={!canPauseRun} title={runBusyTitle} onClick={() => run?.id && void runAction("Пауза", () => api.pauseRun(run.id), setRun)}>
                  <Pause size={20} />
                  <span><strong>Пауза</strong><small>Остановится между стадиями</small></span>
                </button>
                <button className="action-button danger-action" disabled={!canStopRun} onClick={() => run?.id && void runAction("Остановка", () => api.stopRun(run.id), (fresh) => { setRun(fresh); void refreshRun(fresh.id); })}>
                  <X size={20} />
                  <span><strong>Остановить</strong><small>Завершить текущий pipeline</small></span>
                </button>
              </div>
              <div className="action-group">
                <span className="action-group-label">Восстановление</span>
                <button
                  className={`action-button ${activeOperationKind === "resume" ? "is-working" : ""}`}
                  disabled={!canResumeRun}
                  onClick={() => run?.id && void runPipelineAction("resume", "Продолжение", "Продолжается незавершённый запуск.", () => api.resumeRun(run.id), (fresh) => { void refreshRun(fresh.id); })}
                >
                  {activeOperationKind === "resume" ? <LoaderCircle className="spin" size={20} /> : <SkipForward size={20} />}
                  <span><strong>{activeOperationKind === "resume" ? "Продолжение идёт" : "Продолжить"}</strong><small>Незавершённый запуск</small></span>
                </button>
                <button
                  className={`action-button ${activeOperationKind === "retry" ? "is-working" : ""}`}
                  disabled={!canRetryErrors}
                  onClick={() => run?.id && void runPipelineAction("retry", "Повтор ошибок", "Перезапускаются только задачи, которые завершились ошибкой.", () => api.retryErrors(run.id), (fresh) => { void refreshRun(fresh.id); })}
                >
                  {activeOperationKind === "retry" ? <LoaderCircle className="spin" size={20} /> : <RotateCcw size={20} />}
                  <span><strong>{activeOperationKind === "retry" ? "Повтор идёт" : "Повторить ошибки"}</strong><small>Только задачи с ошибкой</small></span>
                </button>
              </div>
              <div className="action-group">
                <span className="action-group-label">Куб и классификация</span>
                <button
                  className={`action-button ${activeOperationKind === "rebuild" ? "is-working" : ""}`}
                  disabled={!canRebuildCube}
                  title={runBusyTitle}
                  onClick={() => run?.id && void runPipelineAction("rebuild", "Сборка куба", "Готовые classified-файлы сохраняются в локальный куб без повторного скачивания.", () => api.rebuildCube(run.id), (fresh) => { void refreshRun(fresh.id); })}
                >
                  {activeOperationKind === "rebuild" ? <LoaderCircle className="spin" size={20} /> : <Database size={20} />}
                  <span><strong>{activeOperationKind === "rebuild" ? "Сборка куба идёт" : "Собрать куб из готовых файлов"}</strong><small>Без повторного скачивания</small></span>
                </button>
                <button
                  className={`action-button ${activeOperationKind === "reclassify" ? "is-working" : ""}`}
                  disabled={!canRebuildCube}
                  title={runBusyTitle}
                  onClick={() => run?.id && void runPipelineAction("reclassify", "Повторная классификация куба", "Применяются текущие правила: classified-файлы и срезы БД будут заменены по задачам плана.", () => api.reclassifyCube(run.id), (fresh) => { void refreshRun(fresh.id); })}
                >
                  {activeOperationKind === "reclassify" ? <LoaderCircle className="spin" size={20} /> : <RotateCcw size={20} />}
                  <span><strong>{activeOperationKind === "reclassify" ? "Переклассификация идёт" : "Переклассифицировать куб"}</strong><small>Перезаписать classified и БД</small></span>
                </button>
              </div>
              <div className="action-group">
                <span className="action-group-label">Ежемесячно</span>
                <button
                  className={`action-button accent ${activeOperationKind === "sync" ? "is-working" : ""}`}
                  disabled={!canSyncNewMonth}
                  title={runBusyTitle}
                  onClick={() => void runPipelineAction("sync", "Синхронизация нового месяца", "Создаётся и запускается план на следующий месяц по registry куба.", syncNewMonth, (fresh) => { void refreshRun(fresh.id); })}
                >
                  {activeOperationKind === "sync" ? <LoaderCircle className="spin" size={20} /> : <RefreshCcw size={20} />}
                  <span><strong>{activeOperationKind === "sync" ? "Синхронизация идёт" : "Синхронизировать новый месяц"}</strong><small>Следующий месяц по registry</small></span>
                </button>
              </div>
            </div>
          </section>

          <section className="panel history-panel">
            <SectionTitle icon={<History />} title="Последние планы" />
            <div className="run-list">
              {runs.map((item) => (
                <div key={item.id} className={`run-row ${run?.id === item.id ? "active" : ""}`}>
                  <button className="run-select-button" onClick={() => guardUnsavedChanges(() => { setRun(item); setTab("plan"); void refreshRun(item.id); })}>
                    <span><strong>{runTypeLabel(item.run_type)}</strong><small>{item.period_from} - {item.period_to}</small></span>
                    <Badge value={item.status} />
                  </button>
                  <button
                    className="icon-button danger-inline"
                    disabled={Boolean(busy) || isActiveRunStatus(item.status)}
                    title={isActiveRunStatus(item.status) ? "Выполняющийся план нельзя удалить." : "Удалить план и его задачи."}
                    onClick={() => {
                      if (window.confirm(`Удалить план «${runTypeLabel(item.run_type)}» за ${item.period_from} - ${item.period_to}? Файлы и куб останутся на месте.`)) {
                        void runAction("Удаление плана", () => deleteRun(item.id));
                      }
                    }}
                  >
                    <Trash2 size={16} />
                  </button>
                </div>
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

function ruleCategoryValues(value: string): string[] {
  const text = value.trim();
  if (!text || text === "*") return [];
  const parts = text.includes("|") || text.includes(";") ? text.split(/[|;]/) : [text];
  const seen = new Set<string>();
  const categories: string[] = [];
  for (const part of parts) {
    const category = part.trim();
    if (!category || category === "*" || seen.has(category)) continue;
    seen.add(category);
    categories.push(category);
  }
  return categories;
}

function serializeRuleCategories(values: string[]) {
  const categories = values.map((value) => value.trim()).filter(Boolean);
  return categories.length ? categories.join(ruleCategorySeparator) : "*";
}

function ruleCategoryChoices(options: string[], selected: string[]) {
  const seen = new Set<string>();
  const choices: string[] = [];
  for (const value of [...options, ...selected]) {
    const category = value.trim();
    if (!category || seen.has(category)) continue;
    seen.add(category);
    choices.push(category);
  }
  return choices;
}

function toggleRuleCategoryValue(currentValue: string, category: string, checked: boolean, options: string[]) {
  const selected = new Set(ruleCategoryValues(currentValue));
  if (checked) selected.add(category);
  else selected.delete(category);
  const ordered = ruleCategoryChoices(options, [...selected]).filter((value) => selected.has(value));
  return serializeRuleCategories(ordered);
}

function nextPriority(rules: ClassifierRule[]) {
  const maxPriority = rules.reduce((max, rule) => Math.max(max, Number(rule.priority) || 0), 0);
  return maxPriority + 10;
}

function nextManualPriority(overrides: ManualOverride[]) {
  const maxPriority = overrides.reduce((max, override) => Math.max(max, Number(override.priority) || 0), 0);
  return maxPriority + 10;
}

function matchTypeLabel(type: string) {
  return matchTypes.find(([value]) => value === type)?.[1] ?? type;
}

function isNumericMatchType(type: string) {
  return ["gt", "gte", "lt", "lte"].includes(type);
}

function ruleModeLabel(mode: string) {
  return ruleModes.find(([value]) => value === mode)?.[1] ?? mode;
}

function ruleHasOtherwise(rule: ClassifierRule) {
  return rule.conditions.some((condition) => condition.match_type === "otherwise");
}

function ruleConditionSummary(rule: ClassifierRule) {
  const first = rule.conditions[0];
  if (ruleHasOtherwise(rule)) {
    return `если «${rule.target_column || "целевая колонка"}» пусто`;
  }
  if (!first?.match_field && !first?.pattern) return "условие не задано";
  return `${first.match_field || "поле"} ${matchTypeLabel(first.match_type)} "${first.pattern || "..."}"`;
}

function ruleCategorySummary(rule: ClassifierRule) {
  const categories = ruleCategoryValues(rule.category);
  return categories.length ? categories.join(", ") : "Все";
}

function ruleActionSummary(rule: ClassifierRule) {
  return `${rule.target_column || "колонка"} = ${rule.set_value || "..."}`;
}

function manualOverrideConditionSummary(override: ManualOverride) {
  return `${override.match_field || "поле"} = ${override.match_value || "..."}`;
}

function manualOverrideActionSummary(override: ManualOverride) {
  return `${override.target_column || "колонка"} = ${override.set_value || "..."}`;
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

function Toggle(props: { label: string; hint?: string; checked: boolean; disabled?: boolean; onChange: (value: boolean) => void }) {
  return (
    <label className={`check-row${props.disabled ? " disabled" : ""}`}>
      <input type="checkbox" checked={props.checked} disabled={props.disabled} onChange={(event) => props.onChange(event.target.checked)} />
      {props.hint ? <FieldLabel text={props.label} hint={props.hint} /> : props.label}
    </label>
  );
}

function DataSubnav(props: { activeTab: DataTab; onSelect: (tab: DataTab) => void }) {
  const items: Array<{ tab: DataTab; label: string; icon: ReactNode }> = [
    { tab: "cube", label: "Куб", icon: <Database size={16} /> },
    { tab: "reports", label: "Отчёты", icon: <BarChart3 size={16} /> },
    { tab: "files", label: "Файлы", icon: <Archive size={16} /> },
    { tab: "export", label: "Выгрузка", icon: <Download size={16} /> },
    { tab: "quality", label: "Качество", icon: <CheckCircle2 size={16} /> }
  ];
  return (
    <nav className="data-subnav" aria-label="Данные">
      {items.map((item) => (
        <button key={item.tab} className={props.activeTab === item.tab ? "active" : ""} onClick={() => props.onSelect(item.tab)}>
          {item.icon}
          {item.label}
        </button>
      ))}
    </nav>
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

function ProjectsWorkspace(props: {
  projects: ProjectSummary[];
  totalProjects: number;
  currentProjectName: string;
  query: string;
  busy: boolean;
  onQueryChange: (value: string) => void;
  onReload: () => void;
  onCreate: (projectName: string) => void;
  onOpen: (projectName: string) => void;
  onDelete: (projectName: string, deleteFiles: boolean) => void;
}) {
  const [deleteFiles, setDeleteFiles] = useState(true);
  const [draftName, setDraftName] = useState("");

  function confirmDelete(project: ProjectSummary) {
    const fileText = deleteFiles ? `\n\nПапка файлов тоже будет удалена:\n${project.data_path}` : "\n\nФайлы проекта останутся на диске.";
    const ok = window.confirm(`Удалить проект «${project.project_name}» из локальной БД?${fileText}`);
    if (ok) props.onDelete(project.project_name, deleteFiles);
  }

  function submitCreate() {
    const projectName = draftName.trim();
    if (!projectName) return;
    props.onCreate(projectName);
    setDraftName("");
  }

  return (
    <section className="panel stage-panel projects-panel">
      <SectionTitle
        icon={<Archive />}
        title="Проекты"
        meta={`${props.projects.length}/${props.totalProjects}`}
        hint="Здесь создаются и выбираются рабочие области: планы, задачи, срезы куба, строки БД и локальные файлы."
      />
      <div className="project-create-panel">
        <label>
          <FieldLabel text="Новый проект" hint="Напиши понятное имя рабочего набора. После создания он появится в списке и станет текущим." />
          <input
            value={draftName}
            onChange={(event) => setDraftName(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter") {
                event.preventDefault();
                submitCreate();
              }
            }}
            placeholder="Например: Сахар"
          />
        </label>
        <button className="primary-inline-button" disabled={props.busy || !draftName.trim()} onClick={submitCreate}>
          <Plus size={17} />
          Создать
        </button>
      </div>
      <div className="toolbar wrap">
        <label className="search-field">
          <Search size={17} />
          <input value={props.query} onChange={(event) => props.onQueryChange(event.target.value)} placeholder="Найти проект или путь" />
        </label>
        <Toggle label="Удалять файлы" checked={deleteFiles} onChange={setDeleteFiles} />
        <button className="ghost-button" disabled={props.busy} onClick={props.onReload}><RefreshCcw size={17} />Обновить</button>
      </div>
      <FilterableTable
        className="project-table"
        rows={props.projects}
        rowKey={(project) => project.project_name}
        getRowClassName={(project) => project.project_name === props.currentProjectName ? "selected-row" : ""}
        emptyText="Проекты пока не найдены."
        columns={[
          {
            id: "project",
            label: "Проект",
            value: (project) => project.project_name,
            render: (project) => (
              <span className="project-name-cell">
                <strong>{project.project_name}</strong>
                {project.project_name === props.currentProjectName ? <small>текущий</small> : null}
              </span>
            )
          },
          {
            id: "actions",
            label: "Действия",
            value: () => "",
            filterable: false,
            sortable: false,
            render: (project) => (
              <div className="table-actions">
                <button className="tiny-button" disabled={props.busy} onClick={() => props.onOpen(project.project_name)}>Открыть</button>
                <button className="tiny-button danger-inline" disabled={props.busy} onClick={() => confirmDelete(project)}>Удалить</button>
              </div>
            )
          },
          { id: "period", label: "Период", value: (project) => project.latest_period ? `${project.first_period ?? "?"} - ${project.latest_period}` : "-", sortValue: (project) => project.latest_period ?? "" },
          { id: "cube", label: "Куб", value: (project) => project.cube_slices_count, render: (project) => `${formatNumber(project.cube_slices_count)} срезов`, numeric: true },
          { id: "rows", label: "Строк БД", value: (project) => project.product_rows_count, render: (project) => formatNumber(project.product_rows_count), numeric: true },
          { id: "tasks", label: "Задачи", value: (project) => project.tasks_count, render: (project) => formatNumber(project.tasks_count), numeric: true },
          { id: "files", label: "Файлы", value: (project) => project.files_count, render: (project) => `${formatNumber(project.files_count)} / ${formatBytes(project.files_size)}`, numeric: true },
          { id: "updated", label: "Обновлён", value: (project) => project.latest_activity ?? "-", render: (project) => formatDateTime(project.latest_activity) },
          { id: "path", label: "Папка", value: (project) => project.data_path, title: (project) => project.data_path }
        ]}
      />
    </section>
  );
}

function FilesTable(props: { files: ProjectFile[]; busy: boolean; onDelete: (file: ProjectFile, deleteCube: boolean) => void }) {
  function confirmDelete(file: ProjectFile) {
    const name = file.relative_path ?? file.path;
    if (!window.confirm(`Удалить файл «${name}»?`)) return;
    const deleteCube =
      file.kind === "classified" &&
      window.confirm("Если этот classified-файл уже сохранён в куб, удалить связанный срез и строки БД тоже?");
    props.onDelete(file, deleteCube);
  }

  return (
    <FilterableTable
      rows={props.files}
      rowKey={(file) => file.path}
      emptyText="Нет файлов по текущим фильтрам."
      columns={[
        {
          id: "actions",
          label: "",
          value: () => "",
          render: (file) => (
            <button className="icon-button danger-inline" disabled={props.busy} title="Удалить файл" onClick={() => confirmDelete(file)}>
              <Trash2 size={16} />
            </button>
          ),
          className: "row-action-col",
          filterable: false,
          sortable: false
        },
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

function SmartPlanOverview(props: { plan: SmartPlan }) {
  const summary = props.plan.summary;
  const cards: Array<{ status: SmartPlanStatus; label: string }> = [
    { status: "ready", label: "Готово" },
    { status: "missing", label: "Нет файлов" },
    { status: "stale", label: "Устарели" },
    { status: "failed", label: "Ошибки" },
    { status: "incomplete", label: "Неполные" }
  ];
  return (
    <div className="smart-plan-overview">
      <div className="smart-plan-summary">
        {cards.map((card) => (
          <div className={`smart-plan-card ${card.status}`} key={card.status}>
            <span>{card.label}</span>
            <strong>{summary[card.status]}</strong>
          </div>
        ))}
        <div className="smart-plan-card db">
          <span>В БД</span>
          <strong>{summary.saved_to_db}</strong>
        </div>
      </div>
      <div className={`recommended-action ${props.plan.recommended_action.key}`}>
        <div>
          {summary.failed || summary.stale ? <AlertTriangle size={18} /> : <CheckCircle2 size={18} />}
          <span>Рекомендованное действие</span>
        </div>
        <strong>{props.plan.recommended_action.label}</strong>
        <p>{props.plan.recommended_action.detail}</p>
      </div>
    </div>
  );
}

function CubeMatrixTable(props: { items: CubeItem[] }) {
  const matrix = useMemo(() => buildCubeMatrix(props.items), [props.items]);
  const readyCount = Math.max(0, matrix.totalCells - matrix.missingCount);
  return (
    <div className="cube-matrix-block">
      <div className="cube-matrix-head">
        <div>
          <Table2 size={18} />
          <h3>Матрица куба</h3>
        </div>
        <span>{matrix.totalCells ? `${readyCount}/${matrix.totalCells} актуальны` : "нет срезов"}</span>
      </div>
      {matrix.rows.length && matrix.marketplaces.length ? (
        <div className="cube-matrix-wrap">
          <table className="cube-matrix">
            <thead>
              <tr>
                <th>Категория</th>
                {matrix.marketplaces.map((marketplace) => (
                  <th key={marketplace}>
                    <span>{marketplace}</span>
                    <small>{matrix.targetPeriods[marketplace]}</small>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {matrix.rows.map((row) => (
                <tr key={row.categoryKey} className={row.missingCount ? "has-missing" : ""}>
                  <td title={row.categoryName}>{row.categoryName}</td>
                  {row.cells.map((cell, index) => (
                    <td key={`${row.categoryKey}-${matrix.marketplaces[index]}`}>
                      <span className={`cube-matrix-cell ${cell.status}`} title={cell.title}>
                        <span aria-hidden="true">{cell.status === "ready" ? "✅" : "❌"}</span>
                        <strong>{cell.period}</strong>
                        {cell.rowsCount ? <small>{cell.rowsCount} строк</small> : null}
                      </span>
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <Empty text="В кубе пока нет срезов для матрицы." />
      )}
    </div>
  );
}

function SmartPlanTable(props: { tasks: SmartPlanTask[]; onRetry: (taskId: string) => void }) {
  return (
    <FilterableTable
      className="task-table"
      rows={props.tasks}
      rowKey={(task) => task.task_id}
      emptyText="Нет задач по текущим фильтрам."
      columns={[
        {
          id: "smart_status",
          label: "Статус",
          value: (task) => {
            const liveStatus = liveSmartPlanStatus(task);
            return statusLabels[liveStatus ?? task.smart_status] ?? liveStatus ?? task.smart_status;
          },
          render: (task) => <Badge value={liveSmartPlanStatus(task) ?? task.smart_status} />
        },
        { id: "category", label: "Категория", value: (task) => task.category_name, title: (task) => task.category_path },
        { id: "marketplace", label: "Marketplace", value: (task) => task.marketplace },
        { id: "month", label: "Месяц", value: (task) => monthLabel(task.year, task.month) },
        {
          id: "files",
          label: "Файлы",
          value: (task) => [task.raw_file.exists ? "raw" : "", task.processed_file.exists ? "processed" : "", task.classified_file.exists ? "classified" : ""].filter(Boolean).join(" "),
          render: (task) => <SmartPlanFileChain task={task} />
        },
        {
          id: "db",
          label: "БД",
          value: (task) => task.has_cube ? "в БД" : "нет",
          render: (task) => task.has_cube ? <span className="db-cell">{task.cube_rows_count} строк</span> : <span className="muted-cell">нет</span>,
          numeric: true
        },
        {
          id: "reason",
          label: "Причина",
          value: (task) => {
            const liveStatus = liveSmartPlanStatus(task);
            return liveStatus ? `Идёт: ${statusLabels[liveStatus] ?? liveStatus}` : task.reason;
          },
          title: (task) => {
            const liveStatus = liveSmartPlanStatus(task);
            return liveStatus ? `Идёт: ${statusLabels[liveStatus] ?? liveStatus}. ${task.reason}` : task.reason;
          },
          render: (task) => {
            const liveStatus = liveSmartPlanStatus(task);
            return liveStatus ? <span className="live-reason">Идёт: {statusLabels[liveStatus] ?? liveStatus}</span> : task.reason;
          }
        },
        {
          id: "action",
          label: "Действие",
          value: (task) => liveSmartPlanStatus(task) ? "Дождись завершения операции" : task.recommended_action,
          render: (task) => (
            <div className="task-action-cell">
              <span>{liveSmartPlanStatus(task) ? "Дождись завершения операции" : task.recommended_action}</span>
              {task.smart_status === "failed" && !liveSmartPlanStatus(task) ? <button className="tiny-button" onClick={() => props.onRetry(task.task_id)}>повтор</button> : null}
            </div>
          )
        },
        {
          id: "pipeline_status",
          label: "Pipeline",
          value: (task) => statusLabels[task.pipeline_status] ?? task.pipeline_status,
          render: (task) => <Badge value={task.pipeline_status} />
        }
      ]}
    />
  );
}

function SmartPlanFileChain(props: { task: SmartPlanTask }) {
  const liveStatus = liveSmartPlanStatus(props.task);
  return (
    <div className={`file-chain ${liveStatus ?? props.task.smart_status}`}>
      <SmartPlanFilePill label="raw" file={props.task.raw_file} />
      <SmartPlanFilePill label="processed" file={props.task.processed_file} />
      <SmartPlanFilePill label="classified" file={props.task.classified_file} />
    </div>
  );
}

function SmartPlanFilePill(props: { label: string; file: SmartPlanTask["raw_file"] }) {
  const exists = props.file.exists && props.file.size > 0;
  const title = props.file.path ? `${props.file.path}${props.file.updated_at ? `, ${new Date(props.file.updated_at).toLocaleString("ru-RU")}` : ""}` : "Файл не задан";
  return (
    <span className={`file-pill ${exists ? "exists" : "missing"}`} title={title}>
      {props.label}
    </span>
  );
}

function ReportsWorkspace(props: {
  options: ReportOptions | null;
  reportType: ReportType;
  selectedCategoryKeys: Set<string>;
  periodFrom: string;
  periodTo: string;
  outputDir: string;
  exportFormat: ExportFormat;
  maxRows: number;
  preview: ReportPreview | null;
  artifacts: ReportArtifact[];
  busy: boolean;
  onReportTypeChange: (value: ReportType) => void;
  onToggleCategory: (id: string) => void;
  onToggleAllCategories: () => void;
  onPeriodFromChange: (value: string) => void;
  onPeriodToChange: (value: string) => void;
  onOutputDirChange: (value: string) => void;
  onExportFormatChange: (value: ExportFormat) => void;
  onMaxRowsChange: (value: number) => void;
  onReloadOptions: () => void;
  onPreview: () => void;
  onBuild: () => void;
}) {
  const options = props.options;
  const selectedCategoryCount = options?.categories.filter((category) => props.selectedCategoryKeys.has(category.category_key)).length ?? 0;
  const heavyCategories = options?.categories.filter((category) => category.is_heavy) ?? [];
  const selectedReport = options?.reports.find((report) => report.type === props.reportType) ?? options?.reports[0] ?? null;
  return (
    <section className="panel stage-panel reports-panel">
      <SectionTitle
        icon={<BarChart3 />}
        title="Отчёты"
        meta={props.preview ? `${formatNumber(props.preview.total)} строк` : `${heavyCategories.length} тяжёлых категорий`}
        hint="Отчёты строятся SQL-агрегацией из куба. Excel получает готовую сводную таблицу, а не миллионы raw-строк."
      />

      <div className="reports-overview">
        <Metric label="Категорий в кубе" value={String(options?.categories.length ?? 0)} />
        <Metric label="Тяжёлых" value={String(heavyCategories.length)} />
        <Metric label="Выбрано" value={String(selectedCategoryCount)} />
        <Metric label="Порог среза" value={formatNumber(options?.heavy_slice_rows_limit ?? 0)} />
      </div>

      {heavyCategories.length ? (
        <div className="heavy-category-list">
          <div className="selector-head">
            <strong>Тяжёлые категории</strong>
            <span>{formatNumber(heavyCategories.reduce((total, category) => total + Number(category.rows_count || 0), 0))} строк</span>
          </div>
          <FilterableTable
            rows={heavyCategories}
            rowKey={(category) => category.category_key}
            emptyText="Тяжёлых категорий пока нет."
            columns={[
              { id: "status", label: "Режим", value: () => "heavy", render: () => <Badge value="heavy" /> },
              { id: "category", label: "Категория", value: (category) => category.category_name, title: (category) => category.heavy_reason ?? category.category_name },
              { id: "rows", label: "Строк", value: (category) => category.rows_count, render: (category) => formatNumber(category.rows_count), numeric: true },
              { id: "slices", label: "Срезов", value: (category) => category.slices_count, numeric: true },
              { id: "reports", label: "Отчёты", value: (category) => category.reports_built_at ?? "", render: (category) => category.reports_built_at ? formatDateTime(category.reports_built_at) : "доступны" }
            ]}
          />
        </div>
      ) : (
        <div className="export-warning">Тяжёлых категорий пока не найдено. Отчёты всё равно можно строить по любым сохранённым срезам куба.</div>
      )}

      <div className="reports-settings">
        <div className="reports-main-settings">
          <div className="form-grid two-cols">
            <label>
              Тип отчёта
              <select value={props.reportType} onChange={(event) => props.onReportTypeChange(event.target.value as ReportType)}>
                {options?.reports.map((report) => <option key={report.type} value={report.type}>{report.label}</option>)}
              </select>
            </label>
            <label>
              Топ SKU, строк
              <input type="number" min={100} max={100000} value={props.maxRows} onChange={(event) => props.onMaxRowsChange(Number(event.target.value))} />
            </label>
            <label>
              Период с
              <input type="month" value={props.periodFrom} onChange={(event) => props.onPeriodFromChange(event.target.value)} min={options?.period_from ?? undefined} max={options?.period_to ?? undefined} />
            </label>
            <label>
              Период по
              <input type="month" value={props.periodTo} onChange={(event) => props.onPeriodToChange(event.target.value)} min={options?.period_from ?? undefined} max={options?.period_to ?? undefined} />
            </label>
          </div>
          {selectedReport ? <p className="reports-description">{selectedReport.description}</p> : null}
          <label>
            Папка
            <input value={props.outputDir} onChange={(event) => props.onOutputDirChange(event.target.value)} placeholder={options?.default_output_dir || "data/projects/.../reports"} />
          </label>
          <div className="export-mode-row">
            <div className="format-switch" aria-label="Формат отчёта">
              <button className={props.exportFormat === "xlsx" ? "active" : ""} type="button" onClick={() => props.onExportFormatChange("xlsx")}>XLSX</button>
              <button className={props.exportFormat === "csv" ? "active" : ""} type="button" onClick={() => props.onExportFormatChange("csv")}>CSV</button>
            </div>
            <button className="ghost-button" onClick={props.onReloadOptions}><RefreshCcw size={17} />Обновить</button>
            <button className="ghost-button" disabled={props.busy || !options} onClick={props.onPreview}><Table2 size={17} />Предпросмотр</button>
            <button className="primary-inline-button" disabled={props.busy || !props.preview} onClick={props.onBuild}>
              <Download size={17} />
              {props.exportFormat.toUpperCase()}
            </button>
          </div>
        </div>

        <div className="export-selector reports-category-selector">
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
                  <small>{category.is_heavy ? "тяжёлая · " : ""}{formatNumber(category.rows_count)} строк · {category.slices_count} срезов</small>
                </span>
              </label>
            ))}
            {options && options.categories.length === 0 ? <Empty text="В кубе нет категорий для отчётов." /> : null}
          </div>
        </div>
      </div>

      {options?.warnings.length ? <div className="export-warning">{options.warnings.join(" ")}</div> : null}
      {props.preview?.warnings.length ? <div className="export-warning">{props.preview.warnings.join(" ")}</div> : null}

      {props.preview ? (
        <div className="reports-preview">
          <h3>{props.preview.report_label}: {formatNumber(props.preview.total)} агрегированных строк</h3>
          <SimpleTable columns={props.preview.columns} rows={props.preview.rows} />
        </div>
      ) : <Empty text="Собери предпросмотр, чтобы увидеть готовую агрегированную таблицу." />}

      {props.artifacts.length ? (
        <div className="export-artifacts">
          <h3>Готовые отчёты</h3>
          <div className="artifact-list">
            {props.artifacts.map((artifact) => (
              <a className="artifact-row" href={api.reportFileUrl(artifact.path)} key={artifact.path}>
                <FileSpreadsheet size={17} />
                <span>
                  <strong>{artifact.filename}</strong>
                  <small>{artifact.format.toUpperCase()} · {artifact.report_label} · {formatNumber(artifact.rows)} строк</small>
                </span>
              </a>
            ))}
          </div>
        </div>
      ) : null}
    </section>
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
  exportFormat: ExportFormat;
  excludedCount: number;
  sortColumn: string | null;
  sortDirection: "asc" | "desc";
  preview: ExportPreview | null;
  artifacts: ExportArtifact[];
  progress: ExportBuildJob | null;
  templates: ExportTemplate[];
  templateName: string;
  confirmLarge: boolean;
  busy: boolean;
  onTemplateNameChange: (value: string) => void;
  onSaveTemplate: () => void;
  onApplyTemplate: (template: ExportTemplate) => void;
  onBuildTemplate: (template: ExportTemplate) => void;
  onDeleteTemplate: (template: ExportTemplate) => void;
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
  onExportFormatChange: (value: ExportFormat) => void;
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
  const formatLabel = props.exportFormat.toUpperCase();
  return (
    <section className="panel stage-panel export-panel">
      <SectionTitle
        icon={<Download />}
        title="Выгрузка"
        meta={props.preview ? `${props.preview.total} строк` : "предпросмотр не собран"}
        hint="XLSX строится из сохранённого куба. Исключение строк действует только на текущую выгрузку."
      />

      <div className="export-templates-panel">
        <div className="template-save-row">
          <label>
            <FieldLabel text="Шаблон" hint="Сохраняет текущий период, категории, колонки, фильтры, сортировку, папку и режим разбиения." />
            <input value={props.templateName} onChange={(event) => props.onTemplateNameChange(event.target.value)} placeholder="Например: Ежемесячный отчёт" />
          </label>
          <button className="primary-inline-button" disabled={props.busy || !options || !props.templateName.trim()} onClick={props.onSaveTemplate}>
            <Save size={17} />
            Сохранить
          </button>
        </div>
        {props.templates.length ? (
          <div className="template-list">
            {props.templates.map((template) => (
              <div className="template-row" key={template.id}>
                <span>
                  <strong>{template.name}</strong>
                  <small>{(template.export_format ?? "xlsx").toUpperCase()} · {template.category_keys.length} категорий · {template.selected_columns.length} колонок · {template.period_from || "с начала"} - {template.period_to || "по последний"}</small>
                </span>
                <div className="table-actions">
                  <button className="tiny-button" disabled={props.busy} onClick={() => props.onApplyTemplate(template)}>Применить</button>
                  <button className="tiny-button" disabled={props.busy} onClick={() => props.onBuildTemplate(template)}>{(template.export_format ?? "xlsx").toUpperCase()}</button>
                  <button className="tiny-button danger-inline" disabled={props.busy} onClick={() => props.onDeleteTemplate(template)}>Удалить</button>
                </div>
              </div>
            ))}
          </div>
        ) : <span className="muted">Шаблонов пока нет. Настрой выгрузку и сохрани её здесь.</span>}
      </div>

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
            <div className="format-switch" aria-label="Формат выгрузки">
              <button className={props.exportFormat === "xlsx" ? "active" : ""} type="button" onClick={() => props.onExportFormatChange("xlsx")}>XLSX</button>
              <button className={props.exportFormat === "csv" ? "active" : ""} type="button" onClick={() => props.onExportFormatChange("csv")}>CSV</button>
            </div>
            <button className="ghost-button" onClick={props.onReloadOptions}><RefreshCcw size={17} />Обновить</button>
            <button className="ghost-button" disabled={props.busy || !options} onClick={props.onPreview}><Table2 size={17} />Предпросмотр</button>
            <button className="primary-inline-button" disabled={props.busy || !props.preview || (needsLargeConfirm && !props.confirmLarge)} onClick={props.onBuild}>
              <Download size={17} />
              {formatLabel}
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
      {props.progress ? <ExportProgress job={props.progress} /> : null}

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
                  <small>{(artifact.format ?? props.exportFormat).toUpperCase()} · {artifact.rows} строк · часть {artifact.part}/{artifact.parts}</small>
                </span>
              </a>
            ))}
          </div>
        </div>
      ) : null}
    </section>
  );
}

function ExportProgress(props: { job: ExportBuildJob }) {
  const progress = Math.max(0, Math.min(100, Number(props.job.progress) || 0));
  const rowsText = props.job.total_rows
    ? `${formatNumber(props.job.completed_rows)} / ${formatNumber(props.job.total_rows)} строк`
    : "строки считаются";
  const filesText = props.job.total_files
    ? `${props.job.completed_files} / ${props.job.total_files} файлов`
    : "файлы считаются";
  return (
    <div className={`export-progress ${props.job.status}`}>
      <div className="export-progress-head">
        <strong>{props.job.status === "succeeded" ? "Выгрузка готова" : props.job.status === "failed" ? "Выгрузка не завершилась" : props.job.current_step}</strong>
        <span>{progress.toFixed(progress % 1 ? 1 : 0)}%</span>
      </div>
      <div className="export-progress-track"><span style={{ width: `${progress}%` }} /></div>
      <div className="export-progress-meta">
        <span>{rowsText}</span>
        <span>{filesText}</span>
      </div>
      {props.job.error ? <p>{props.job.error}</p> : null}
    </div>
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
        title="Качество"
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
          detail={report.metrics.duplicates.checked ? "полная строка" : "нет колонок"}
        />
        <QualityMetric label="Пустые поля" value={formatNumber(report.metrics.empty_key_fields.rows_with_empty)} detail="ключевые поля" />
      </div>

      {report.skipped_checks.length ? (
        <div className="quality-skipped">
          <strong>Не проверялось</strong>
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
    { id: "duplicates", title: "Полные дубли строк", rows: props.report.examples.duplicates }
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

function formatBytes(value: number | null | undefined) {
  const bytes = Number(value || 0);
  if (bytes < 1024) return `${formatNumber(bytes)} Б`;
  if (bytes < 1024 * 1024) return `${formatNumber(Math.round(bytes / 102.4) / 10)} КБ`;
  if (bytes < 1024 * 1024 * 1024) return `${formatNumber(Math.round(bytes / 104857.6) / 10)} МБ`;
  return `${formatNumber(Math.round(bytes / 107374182.4) / 10)} ГБ`;
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("ru-RU");
}

function formatDate(value: string | null | undefined) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString("ru-RU");
}

function CategorySourceEditor(props: {
  rows: CategorySourceRow[];
  selectedRow: CategorySourceRow | null;
  sourcePath: string;
  query: string;
  dirty: boolean;
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
      <SectionTitle icon={<FolderSync />} title="Справочник категорий" meta={props.dirty ? `${props.rows.length} строк · не сохранено` : `${props.rows.length} строк`} hint="Это единственный источник категорий: CSV в корне проекта. Excel больше не читается, чтобы не ловить дубли путей." />
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
            <Toggle label="FBS" hint="Выключать у нефудовых категорий. У Яндекс.Маркета FBS не используется." checked={isYandexMarketplace(row.marketplace) ? false : row.fbs} disabled={isYandexMarketplace(row.marketplace)} onChange={(value) => props.onChange(row.id, { fbs: value })} />
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
  categoryOptions: string[];
  manualOverrides: ManualOverride[];
  totalManualOverrides: number;
  selectedManualOverride: ManualOverride | null;
  manualOverridesPath: string;
  query: string;
  dirty: boolean;
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
  onManualSelect: (id: string) => void;
  onManualAdd: (kind?: "classification" | "sku-group") => void;
  onManualChange: (id: string, patch: Partial<ManualOverride>) => void;
  onManualDelete: (id: string) => void;
  onManualSave: () => void;
  externalFile: File | null;
  externalWriteXlsx: boolean;
  externalResult: ClassificationResponse | null;
  busy: boolean;
  onExternalFileChange: (file: File | null) => void;
  onExternalWriteXlsxChange: (value: boolean) => void;
  onExternalClassify: () => void;
}) {
  const rule = props.selectedRule;
  const manualOverride = props.selectedManualOverride;
  const selectedRuleCategories = rule ? ruleCategoryValues(rule.category) : [];
  const categoryChoices = rule ? ruleCategoryChoices(props.categoryOptions, selectedRuleCategories) : props.categoryOptions;
  return (
    <section className="panel stage-panel">
      <SectionTitle
        icon={<FileSpreadsheet />}
        title="Классификатор"
        meta={props.dirty ? `${props.rules.length}/${props.totalRules} правил · ${props.manualOverrides.length}/${props.totalManualOverrides} правок · не сохранено` : `${props.rules.length}/${props.totalRules} правил · ${props.manualOverrides.length}/${props.totalManualOverrides} правок`}
        hint="Правила сохраняются в classifiers/rules.csv, ручные правки SKU — в classifiers/manual_overrides.csv."
      />
      <div className="toolbar wrap">
        <label className="search-field">
          <Search size={17} />
          <input value={props.query} onChange={(event) => props.onQueryChange(event.target.value)} placeholder="Найти правило, колонку или текст" />
        </label>
        <button className="ghost-button" title="Создать новое правило классификации." onClick={props.onAdd}><Plus size={17} />Создать новое</button>
        <button className="ghost-button" title="Скопировать выбранное правило." disabled={!rule} onClick={() => rule && props.onDuplicate(rule)}><Copy size={17} />Дублировать</button>
        <button className="ghost-button" title="Записать правила в classifiers/rules.csv." onClick={props.onSave}><Save size={17} />Сохранить правила</button>
      </div>
      <div className="quick-start">
        <span>Быстро создать:</span>
        <button className="chip-button" onClick={() => props.onAddPreset("name-to-subcategory")}>по названию</button>
        <button className="chip-button" onClick={() => props.onAddPreset("sku-to-subcategory")}>по точному SKU</button>
        <button className="chip-button" onClick={() => props.onAddPreset("name-to-brand")}>бренд из названия</button>
        <button className="chip-button" onClick={() => props.onAddPreset("otherwise")}>если пусто</button>
      </div>
      <p className="path-note" title={props.rulesPath}>{props.rulesPath || "Файл правил ещё не выбран"}</p>
      <datalist id="classifier-columns">
        {commonClassifierColumns.map((column) => <option key={column} value={column} />)}
      </datalist>
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
      <div className="manual-overrides">
        <div className="subsection-head">
          <div>
            <h3>Ручные правки SKU</h3>
            <p>Применяются после правил и сохраняются при повторной классификации.</p>
          </div>
          <div className="row-actions">
            <button className="ghost-button" title="Создать ручную правку для конкретного SKU или артикула." onClick={() => props.onManualAdd("classification")}><Plus size={17} />Правка</button>
            <button className="ghost-button" title="Создать правку, которая записывает общую группу SKU." onClick={() => props.onManualAdd("sku-group")}><ListChecks size={17} />Объединение SKU</button>
            <button className="ghost-button" title="Записать ручные правки в classifiers/manual_overrides.csv." onClick={props.onManualSave}><Save size={17} />Сохранить правки</button>
          </div>
        </div>
        <p className="path-note" title={props.manualOverridesPath}>{props.manualOverridesPath || "Файл ручных правок ещё не создан"}</p>
        <div className="manual-override-layout">
          {manualOverride ? (
            <div className="manual-override-form">
              <SectionTitle icon={<Settings />} title="Ручная правка" hint="Правка ищет точное значение в выбранной колонке и записывает новое значение после автоматических правил." />
              <Toggle label="Правка активна" checked={manualOverride.active} onChange={(value) => props.onManualChange(manualOverride.id, { active: value })} />
              <div className="form-grid two-cols">
                <label>Где искать<input list="classifier-columns" value={manualOverride.match_field} onChange={(event) => props.onManualChange(manualOverride.id, { match_field: event.target.value })} placeholder="Артикул или SKU" /></label>
                <label>Что искать<input value={manualOverride.match_value} onChange={(event) => props.onManualChange(manualOverride.id, { match_value: event.target.value })} placeholder="Точный SKU/артикул" /></label>
                <label>Куда записать<input list="classifier-columns" value={manualOverride.target_column} onChange={(event) => props.onManualChange(manualOverride.id, { target_column: event.target.value })} placeholder="Подкатегория, SKU-группа..." /></label>
                <label>Что записать<input value={manualOverride.set_value} onChange={(event) => props.onManualChange(manualOverride.id, { set_value: event.target.value })} placeholder="Новое значение" /></label>
                <label>Режим<select value={manualOverride.mode} onChange={(event) => props.onManualChange(manualOverride.id, { mode: event.target.value })}>{ruleModes.map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
                <label>Порядок<input type="number" value={manualOverride.priority} onChange={(event) => props.onManualChange(manualOverride.id, { priority: Number(event.target.value) })} /></label>
              </div>
              <label className="full-width-label">Заметка<input value={manualOverride.comment} onChange={(event) => props.onManualChange(manualOverride.id, { comment: event.target.value })} placeholder="Для себя, можно пусто" /></label>
              <button className="danger-button" title="Удалить выбранную ручную правку. Файл изменится только после сохранения." onClick={() => props.onManualDelete(manualOverride.id)}>
                <Trash2 size={17} />Удалить правку
              </button>
            </div>
          ) : (
            <div className="classifier-empty-state compact-empty">
              <CircleHelp size={24} />
              <h3>Выбери правку или создай новую</h3>
              <button className="ghost-button" onClick={() => props.onManualAdd("classification")}>
                <Plus size={17} />Создать правку
              </button>
            </div>
          )}
          <FilterableTable
            className="editor-list"
            rows={props.manualOverrides}
            rowKey={(item) => item.id}
            emptyText="Нет ручных правок по текущему фильтру."
            getRowClassName={(item) => manualOverride?.id === item.id ? "selected-row" : ""}
            onRowClick={(item) => props.onManualSelect(item.id)}
            columns={[
              { id: "active", label: "Вкл", value: (item) => item.active ? "да" : "нет" },
              { id: "priority", label: "Порядок", value: (item) => item.priority, numeric: true },
              { id: "match", label: "SKU", value: manualOverrideConditionSummary, title: manualOverrideConditionSummary },
              { id: "action", label: "Записать", value: manualOverrideActionSummary, title: manualOverrideActionSummary },
              { id: "mode", label: "Режим", value: (item) => ruleModeLabel(item.mode) }
            ]}
          />
        </div>
      </div>
      <div className="editor-layout">
        {rule ? (
          <div className="editor-form">
            <SectionTitle icon={<Settings />} title="Правило" hint="Обычное правило проверяет выбранную колонку. Правило «если пусто» заполняет целевую колонку только там, где после правил выше ещё пусто." />
            <Toggle label="Правило активно" checked={rule.active} onChange={(value) => props.onChange(rule.id, { active: value })} />
            <div className="rule-block">
              <h3>1. Когда применять</h3>
              <div className="form-grid two-cols">
                <div className="category-picker">
                  <FieldLabel text="Для категории" hint="Список берётся из CSV-справочника категорий. Если выбрано «Все», правило применяется без ограничения по категории." />
                  <div className="category-picker-head">
                    <label className="category-checkbox">
                      <input
                        type="checkbox"
                        checked={selectedRuleCategories.length === 0}
                        onChange={(event) =>
                          props.onChange(rule.id, {
                            category: event.target.checked ? "*" : serializeRuleCategories(categoryChoices.slice(0, 1))
                          })
                        }
                      />
                      <span>Все</span>
                    </label>
                    <small>{selectedRuleCategories.length ? `${selectedRuleCategories.length} выбрано` : "без ограничения"}</small>
                  </div>
                  <div className="category-checkbox-list">
                    {categoryChoices.length ? (
                      categoryChoices.map((category) => (
                        <label className="category-checkbox" key={category}>
                          <input
                            type="checkbox"
                            checked={selectedRuleCategories.includes(category)}
                            onChange={(event) =>
                              props.onChange(rule.id, {
                                category: toggleRuleCategoryValue(rule.category, category, event.target.checked, props.categoryOptions)
                              })
                            }
                          />
                          <span title={category}>{category}</span>
                        </label>
                      ))
                    ) : (
                      <span className="muted-cell">Категории не найдены</span>
                    )}
                  </div>
                </div>
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
	                  condition.match_type === "otherwise" ? (
	                    <div className="otherwise-row" key={`${rule.id}-${index}`}>
	                      <span className="condition-prefix">Если пусто</span>
	                      <div className="otherwise-copy">
	                        <strong>Если поле «{rule.target_column || "целевая колонка"}» осталось пустым</strong>
	                        <small>записать «{rule.set_value || "значение не задано"}». Это fallback-строка, она не ищет текст и никогда не перезаписывает уже заполненные значения.</small>
	                      </div>
	                      <button className="tiny-button danger-inline" title="Удалить fallback-условие." disabled={rule.conditions.length <= 1} onClick={() => props.onDeleteCondition(rule.id, index)}><Trash2 size={14} /></button>
	                    </div>
	                  ) : (
	                    <div className="condition-row" key={`${rule.id}-${index}`}>
	                      {index === 0 ? (
	                        <span className="condition-prefix">Если</span>
	                      ) : (
	                        <label>Связка<select value={condition.join_with_prev} onChange={(event) => props.onConditionChange(rule.id, index, { join_with_prev: event.target.value })}><option value="and">И</option><option value="or">ИЛИ</option></select></label>
	                      )}
	                      <label>Где искать<input list="classifier-columns" value={condition.match_field} onChange={(event) => props.onConditionChange(rule.id, index, { match_field: event.target.value })} placeholder="Название, SKU, Вес, кг..." /></label>
	                      <label>Как искать<select value={condition.match_type} onChange={(event) => {
	                        const matchType = event.target.value;
	                        props.onConditionChange(rule.id, index, matchType === "otherwise" ? { match_type: matchType, match_field: "", pattern: "" } : { match_type: matchType });
	                      }}>{matchTypes.map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
	                      <label>Что искать<input value={condition.pattern} onChange={(event) => props.onConditionChange(rule.id, index, { pattern: event.target.value })} placeholder={isNumericMatchType(condition.match_type) ? "Например: 10 или 10 кг" : "Например: лимон"} /></label>
	                      <button className="tiny-button danger-inline" title="Удалить условие." disabled={rule.conditions.length <= 1} onClick={() => props.onDeleteCondition(rule.id, index)}><Trash2 size={14} /></button>
	                    </div>
	                  )
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
	              {ruleHasOtherwise(rule) ? <p className="rule-note">Для fallback-строки движок всегда заполняет только пустые ячейки целевой колонки, даже если в CSV остался режим «перезаписать».</p> : null}
	            </div>
            <button className="danger-button" title="Удалить выбранное правило. Файл изменится только после сохранения." onClick={() => props.onDelete(rule.id)}>
              <Trash2 size={17} />Удалить правило
            </button>
          </div>
        ) : (
          <div className="classifier-empty-state">
            <CircleHelp size={26} />
            <h3>Выбери существующее правило или нажми создать новое</h3>
            <button className="ghost-button" onClick={props.onAdd}>
              <Plus size={17} />Создать новое
            </button>
          </div>
        )}
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
            { id: "category", label: "Категория", value: ruleCategorySummary, title: ruleCategorySummary },
            { id: "condition", label: "Если", value: ruleConditionSummary, title: ruleConditionSummary },
            { id: "action", label: "Записать", value: ruleActionSummary, title: ruleActionSummary },
            { id: "mode", label: "Режим", value: (item) => ruleModeLabel(item.mode) }
          ]}
        />
      </div>
    </section>
  );
}

function CubeTable(props: { items: CubeItem[]; busy: boolean; onDelete: (item: CubeItem) => void }) {
  function confirmDelete(item: CubeItem) {
    const label = `${monthLabel(item.year, item.month)} · ${item.marketplace} · ${item.category_name}`;
    if (window.confirm(`Удалить срез куба «${label}» и строки БД? Исходные файлы останутся на диске.`)) {
      props.onDelete(item);
    }
  }

  return (
    <FilterableTable
      rows={props.items}
      rowKey={(item) => item.id}
      emptyText="Нет срезов куба по текущим фильтрам."
      columns={[
        {
          id: "actions",
          label: "",
          value: () => "",
          render: (item) => (
            <button className="icon-button danger-inline" disabled={props.busy} title="Удалить срез куба" onClick={() => confirmDelete(item)}>
              <Trash2 size={16} />
            </button>
          ),
          className: "row-action-col",
          filterable: false,
          sortable: false
        },
        { id: "month", label: "Месяц", value: (item) => monthLabel(item.year, item.month) },
        { id: "marketplace", label: "Marketplace", value: (item) => item.marketplace },
        { id: "category", label: "Категория", value: (item) => item.category_name },
        { id: "rows", label: "Строк", value: (item) => item.rows_count, numeric: true },
        {
          id: "mode",
          label: "Режим",
          value: (item) => item.is_heavy ? "heavy" : "standard",
          render: (item) => item.is_heavy ? <Badge value="heavy" /> : <span className="muted-cell">standard</span>,
          title: (item) => item.heavy_reason ?? "Обычный срез."
        },
        {
          id: "reports",
          label: "Отчёты",
          value: (item) => item.reports_built_at ?? "",
          render: (item) => item.reports_built_at ? formatDateTime(item.reports_built_at) : item.is_heavy ? "доступны" : "-",
          title: (item) => item.reports_built_at ? `Последний отчёт: ${formatDateTime(item.reports_built_at)}` : "Отчёт можно построить во вкладке Данные -> Отчёты."
        },
        {
          id: "days",
          label: "Дней",
          value: (item) => item.days_loaded ?? 0,
          render: cubeDaysLabel,
          title: (item) => item.data_actual_until ? `Данные актуальны по ${formatDate(item.data_actual_until)}` : "Покрытие по дням пока не сохранено.",
          numeric: true
        },
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

function cubeDaysLabel(item: CubeItem) {
  if (item.days_loaded == null || item.days_in_month == null) return "-";
  const base = `${formatNumber(item.days_loaded)}/${formatNumber(item.days_in_month)}`;
  return item.data_actual_until ? `${base} · по ${formatDate(item.data_actual_until)}` : base;
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

function UnsavedChangesModal(props: {
  kind: EditableWorkspace;
  busy: boolean;
  onSave: () => void;
  onDiscard: () => void;
  onCancel: () => void;
}) {
  const title = props.kind === "catalog" ? "Сохранить изменения справочника?" : "Сохранить изменения правил?";
  const detail =
    props.kind === "catalog"
      ? "В справочнике категорий есть правки, которые ещё не записаны в CSV."
      : "В классификаторе есть правки, которые ещё не записаны в classifiers/rules.csv.";
  return (
    <div className="modal-backdrop confirm-backdrop" role="presentation">
      <section className="confirm-modal" role="dialog" aria-modal="true" aria-labelledby="unsaved-changes-title">
        <div className="modal-head">
          <div>
            <h2 id="unsaved-changes-title">{title}</h2>
            <p>{detail}</p>
          </div>
          <button className="icon-button" aria-label="Остаться в редакторе" disabled={props.busy} onClick={props.onCancel}><X size={18} /></button>
        </div>
        <div className="confirm-content">
          <AlertTriangle size={20} />
          <p>Если уйти без сохранения, локальные правки в этом редакторе будут сброшены до последней сохранённой версии.</p>
        </div>
        <div className="confirm-actions">
          <button className="ghost-button" disabled={props.busy} onClick={props.onCancel}>Остаться</button>
          <button className="ghost-button danger-inline" disabled={props.busy} onClick={props.onDiscard}>Не сохранять</button>
          <button className="primary-inline-button" disabled={props.busy} onClick={props.onSave}><Save size={17} />Сохранить</button>
        </div>
      </section>
    </div>
  );
}

function PipelineOperationModal(props: {
  operation: PipelineOperation;
  run: PipelineRun | null;
  onClose: () => void;
  onOpenPlan: () => void;
  onOpenCube: () => void;
}) {
  const run = props.run;
  const operationProgress = run?.operation_progress?.kind === props.operation.kind ? run.operation_progress : null;
  const total = operationProgress?.total_files ?? run?.total_tasks ?? 0;
  const completed = operationProgress?.completed_files ?? run?.completed_tasks ?? 0;
  const failed = operationProgress?.failed_files ?? run?.failed_tasks ?? 0;
  const remaining = operationProgress?.remaining_files ?? run?.remaining_tasks ?? Math.max(0, total - completed - failed);
  const progress = Math.max(
    0,
    Math.min(100, operationProgress?.progress ?? run?.progress ?? (total ? Math.round((completed / total) * 100) : 0))
  );
  const status = run?.status ?? (props.operation.finishedAt ? "succeeded" : "running");
  const active = isActiveRunStatus(status) || (!props.operation.finishedAt && status === "running");
  const steps = pipelineOperationSteps(props.operation.kind);
  const currentStepIndex = pipelineOperationStepIndex(props.operation.kind, run?.current_step ?? "", status);
  const completedMetric = props.operation.kind === "reclassify" ? "Переклассифицировано" : "Готово";

  return (
    <div className="modal-backdrop operation-backdrop" role="presentation">
      <section className="operation-modal" role="dialog" aria-modal="true" aria-labelledby="pipeline-operation-title">
        <div className="modal-head">
          <div>
            <h2 id="pipeline-operation-title">{props.operation.label}</h2>
            <p>{props.operation.detail}</p>
          </div>
          <button className="icon-button" aria-label={active ? "Скрыть прогресс" : "Закрыть прогресс"} onClick={props.onClose}><X size={18} /></button>
        </div>
        <div className="operation-content">
          <div className={`operation-status ${status}`}>
            <div className="operation-progress-head">
              <div>
                <strong>{statusLabels[status] ?? status}</strong>
                <span>{run?.current_step || (active ? "Старт операции" : "Операция завершена")}</span>
              </div>
              <b>{progress}%</b>
            </div>
            <div className="operation-progress-track"><span style={{ width: `${progress}%` }} /></div>
            <div className="operation-metrics">
              {operationProgress ? <span>Файлов: <strong>{total}</strong></span> : null}
              <span>{completedMetric}: <strong>{completed}</strong></span>
              <span>Осталось: <strong>{remaining}</strong></span>
              <span>Ошибок: <strong>{failed}</strong></span>
            </div>
          </div>
          <ol className="operation-steps">
            {steps.map((step, index) => {
              const stepState = !active
                ? status === "failed" || status === "completed_with_errors"
                  ? index < currentStepIndex ? "done" : index === currentStepIndex ? "current" : ""
                  : "done"
                : index < currentStepIndex ? "done" : index === currentStepIndex ? "current" : "";
              return <li className={stepState} key={step}>{step}</li>;
            })}
          </ol>
          {props.operation.kind === "reclassify" ? (
            <div className="operation-note">
              <AlertTriangle size={16} />
              <span>Во время переклассификации classified-файлы и срезы БД заменяются по задачам плана. Предпросмотр куба обновится после статуса «готово».</span>
            </div>
          ) : null}
        </div>
        <div className="operation-actions">
          <button className="ghost-button" onClick={props.onOpenPlan}>Умный план</button>
          <button className="ghost-button" onClick={props.onOpenCube}>Куб</button>
          <button className="primary-inline-button" onClick={props.onClose}>{active ? "Скрыть" : "Закрыть"}</button>
        </div>
      </section>
    </div>
  );
}

function pipelineOperationSteps(kind: PipelineOperationKind) {
  if (kind === "reclassify") {
    return ["Проверка processed-файлов", "Применение правил классификатора", "Замена срезов БД", "Обновление куба в интерфейсе"];
  }
  if (kind === "rebuild") {
    return ["Проверка classified-файлов", "Сохранение срезов в БД", "Обновление registry", "Обновление куба в интерфейсе"];
  }
  return ["Скачивание", "Обработка", "Классификация", "Сохранение в БД"];
}

function pipelineOperationStepIndex(kind: PipelineOperationKind, currentStep: string, status: string) {
  if (!isActiveRunStatus(status)) return pipelineOperationSteps(kind).length - 1;
  const text = currentStep.toLowerCase();
  if (text.includes("сохран")) return kind === "reclassify" ? 2 : 3;
  if (text.includes("классиф")) return kind === "reclassify" ? 1 : 2;
  if (text.includes("обработ")) return 1;
  return 0;
}

function ProductInstructionModal(props: { onClose: () => void }) {
  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={props.onClose}>
      <section className="instruction-modal" role="dialog" aria-modal="true" aria-labelledby="product-instruction-title" onMouseDown={(event) => event.stopPropagation()}>
        <div className="modal-head">
          <div>
            <h2 id="product-instruction-title">Инструкция по MPStats Workflow</h2>
            <p>Коротко: выбираешь что скачать, запускаешь план, проверяешь раздел «Данные» и при необходимости настраиваешь классификатор.</p>
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
            <p>Новый проект создаётся в разделе «Проекты» кнопкой «Создать». Слева выбирается только уже созданный проект: по нему приложение раскладывает файлы, планы и записи в БД. Cookie нужен только для скачивания из MPStats: вставляешь его, сохраняешь настройки и дальше работаешь через кнопки.</p>
          </section>
          <section className="instruction-section">
            <h3>2. Справочник</h3>
            <p>Справочник лежит в блоке «Правила и справочник» слева. Если нужна новая категория, добавь строку, укажи маркетплейс, путь и фильтр, затем нажми «Сохранить CSV». После этого категория появится в разделе «Категории».</p>
          </section>
          <section className="instruction-section">
            <h3>3. Загрузка</h3>
            <p>Для старых периодов выбери «Историческая загрузка», отметь категории и нажми «Создать план». Потом проверь вкладку «Умный план» и нажми «Запустить». Для следующего месяца используй режим «Ежемесячное обновление» и кнопку синхронизации.</p>
          </section>
          <section className="instruction-section">
            <h3>4. Данные → Куб</h3>
            <p>После обработки каждая задача сохраняется в DuckDB. В разделе «Данные» открой «Куб»: там видно, какие месячные срезы уже сохранены, можно открыть предпросмотр первых строк и найти товар по SKU, названию или бренду.</p>
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
