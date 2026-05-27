export type Category = {
  category_id: string;
  category_name: string;
  marketplace: string;
  mp_code: string;
  path: string;
  filter_json?: string | null;
  fbs?: boolean | null;
  period_from?: string | null;
  period_to?: string | null;
};

export type CategorySourceRow = {
  id: string;
  active: boolean;
  category_name: string;
  marketplace: string;
  fbs: boolean;
  period_from: string;
  period_to: string;
  comment: string;
  path: string;
  filter_text: string;
  path2: string;
  filter2_text: string;
  actualization: string;
};

export type ClassifierCondition = {
  join_with_prev: "and" | "or" | string;
  match_field: string;
  match_type: "contains" | "not_contains" | "regex" | "equals" | "startswith" | string;
  pattern: string;
};

export type ClassifierRule = {
  id: string;
  active: boolean;
  priority: number;
  category: string;
  target_column: string;
  set_value: string;
  mode: "fill_empty" | "overwrite" | string;
  comment: string;
  conditions: ClassifierCondition[];
};

export type WorkflowSettings = {
  cookie: string;
  project_name: string;
  workflow_mode?: "historical_backfill" | "monthly_sync" | string;
  start_year?: number | null;
  start_month?: number | null;
  end_year?: number | null;
  end_month?: number | null;
};

export type ProjectSummary = {
  project_name: string;
  is_current: boolean;
  data_path: string;
  has_files: boolean;
  files_count: number;
  files_size: number;
  pipeline_runs_count: number;
  app_runs_count: number;
  total_runs_count: number;
  tasks_count: number;
  cube_slices_count: number;
  cube_rows_count: number;
  product_rows_count: number;
  schedules_count: number;
  first_period?: string | null;
  latest_period?: string | null;
  latest_activity?: string | null;
};

export type ProjectDeleteResponse = {
  project_name: string;
  deleted: Record<string, number>;
  deleted_file_paths: string[];
  skipped_file_paths: string[];
};

export type PipelineSettings = {
  overwrite_raw: boolean;
  overwrite_processed: boolean;
  overwrite_db: boolean;
  max_parallel_downloads: number;
  retry_count: number;
  timeout_seconds: number;
  pause_between_requests: number;
  max_weight_kg: number;
};

export type PipelineRun = {
  id: string;
  project_name: string;
  run_type: "historical_backfill" | "monthly_sync" | string;
  period_from?: string;
  period_to?: string;
  status: string;
  total_tasks: number;
  completed_tasks: number;
  failed_tasks: number;
  remaining_tasks?: number;
  current_step?: string | null;
  category_count?: number;
  month_count?: number;
  progress?: number;
  is_active?: boolean;
};

export type DownloadTask = {
  id: string;
  run_id: string;
  project_name: string;
  marketplace: string;
  marketplace_code: string;
  category_name: string;
  category_path: string;
  category_id: string;
  category_key: string;
  year: number;
  month: number;
  status: string;
  download_status: string;
  process_status: string;
  classify_status: string;
  save_status: string;
  raw_file_path?: string | null;
  processed_file_path?: string | null;
  classified_file_path?: string | null;
  rows_count?: number | null;
  error_message?: string | null;
};

export type SmartPlanStatus = "ready" | "missing" | "stale" | "failed" | "incomplete";

export type SmartPlanFile = {
  path?: string | null;
  exists: boolean;
  size: number;
  updated_at?: string | null;
};

export type SmartPlanTask = {
  task_id: string;
  run_id: string;
  project_name: string;
  marketplace: string;
  marketplace_code: string;
  category_name: string;
  category_path: string;
  category_id: string;
  category_key: string;
  year: number;
  month: number;
  pipeline_status: string;
  download_status: string;
  process_status: string;
  classify_status: string;
  save_status: string;
  rows_count: number;
  error_message?: string | null;
  smart_status: SmartPlanStatus;
  reason: string;
  recommended_action: string;
  has_cube: boolean;
  cube_rows_count: number;
  cube_saved_at?: string | null;
  raw_file: SmartPlanFile;
  processed_file: SmartPlanFile;
  classified_file: SmartPlanFile;
};

export type SmartPlanSummary = Record<SmartPlanStatus, number> & {
  total: number;
  saved_to_db: number;
  ready_for_db: number;
};

export type SmartPlanAction = {
  key: string;
  label: string;
  detail: string;
};

export type SmartPlan = {
  run_id: string;
  generated_at: string;
  summary: SmartPlanSummary;
  recommended_action: SmartPlanAction;
  tasks: SmartPlanTask[];
};

export type ProjectFile = {
  path: string;
  relative_path?: string;
  kind: string;
  size: number;
  updated_at: string;
};

export type QualityStatus = "OK" | "WARNING" | "FAIL";

export type QualityProject = {
  project_name: string;
  source_kind: "classified" | "merged" | string;
  source_scope: "legacy" | "project_files" | string;
  file_count: number;
  path: string;
  paths: string[];
  fallback_used: boolean;
  updated_at?: string | null;
};

export type QualityProblem = {
  type: string;
  count: number;
  share: number;
  comment: string;
};

export type QualitySkippedCheck = {
  check: string;
  reason: string;
};

export type QualityReport = {
  project_name: string;
  status: QualityStatus;
  status_comment: string;
  source: {
    kind: "classified" | "merged" | string;
    scope: "legacy" | "project_files" | string;
    path: string;
    paths: string[];
    file_count: number;
    fallback_used: boolean;
  };
  total_rows: number;
  metrics: {
    empty_key_fields: { rows_with_empty: number; share: number; fields: Array<Record<string, unknown>> };
    weight_volume: { columns: string[]; parsed_count: number; missing_count: number; coverage_share: number; missing_share: number };
    anomalies: { columns: string[]; count: number; zero_or_negative: number; too_large: number; suspicious: number };
    classification: { columns: string[]; classified_count: number; unclassified_count: number; coverage_share: number; unclassified_share: number };
    duplicates: { checked: boolean; identifier_column?: string | null; duplicate_rows: number; duplicate_keys: number; share: number };
  };
  problems: QualityProblem[];
  skipped_checks: QualitySkippedCheck[];
  examples: {
    unclassified: Record<string, unknown>[];
    missing_weight_volume: Record<string, unknown>[];
    anomalies: Record<string, unknown>[];
    duplicates: Record<string, unknown>[];
  };
  warnings: string[];
  summary: string;
};

export type CubeItem = {
  id: string;
  project_name: string;
  year: number;
  month: number;
  marketplace: string;
  marketplace_code: string;
  category_key: string;
  category_name: string;
  rows_count: number;
  saved_to_db_at: string;
  source_processed_file_path?: string | null;
};

export type ProductSearch = {
  columns: string[];
  rows: Record<string, unknown>[];
  total: number;
  run_id?: string | null;
};

export type ExportCategoryOption = {
  category_key: string;
  category_name: string;
  marketplace_code: string;
  marketplace: string;
  rows_count: number;
};

export type ExportColumnFilter = {
  column: string;
  match_type: "contains" | "not_contains" | "equals" | "startswith" | "gt" | "gte" | "lt" | "lte" | string;
  value: string;
};

export type ExportOptions = {
  project_name: string;
  default_output_dir: string;
  columns: string[];
  selected_columns: string[];
  categories: ExportCategoryOption[];
  period_from?: string | null;
  period_to?: string | null;
  warnings: string[];
  excel_max_rows: number;
};

export type ExportPreview = {
  columns: string[];
  rows: Record<string, unknown>[];
  total: number;
  estimated_files: number;
  breakdown: ExportBreakdownItem[];
  warnings: string[];
};

export type ExportArtifact = {
  path: string;
  filename: string;
  rows: number;
  part: number;
  parts: number;
  category_key?: string | null;
  category_name?: string | null;
  marketplace?: string | null;
};

export type ExportBreakdownItem = {
  year: number;
  month: number;
  period: string;
  category_key: string;
  category_name: string;
  marketplace_code: string;
  marketplace: string;
  rows_count: number;
};

export type ExportBuildResponse = {
  artifacts: ExportArtifact[];
  total: number;
  estimated_files: number;
  output_dir: string;
  split_by_category: boolean;
  breakdown: ExportBreakdownItem[];
  warnings: string[];
};

export type ExportPayload = {
  project_name: string;
  category_keys: string[];
  period_from?: string | null;
  period_to?: string | null;
  selected_columns: string[];
  filters: ExportColumnFilter[];
  excluded_row_hashes: string[];
  sort_column?: string | null;
  sort_direction: "asc" | "desc" | string;
  split_by_category: boolean;
  limit?: number;
  offset?: number;
  output_dir?: string | null;
  confirm_large_export?: boolean;
};

export type FilePreview = {
  file: string;
  columns: string[];
  rows: Record<string, unknown>[];
  total: number;
};

export type ClassificationResponse = {
  run_id: string;
  status: string;
  input_file: string;
  output_file: string;
  output_xlsx?: string;
  preview: FilePreview;
  result?: Record<string, unknown>;
};

const API_BASE = import.meta.env.VITE_API_BASE ?? "";

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...(options?.headers ?? {}) },
    ...options
  });

  const text = await response.text();
  const trimmed = text.trim();
  const contentType = response.headers.get("content-type") ?? "";
  const looksLikeJson = contentType.includes("application/json") || trimmed.startsWith("{") || trimmed.startsWith("[");

  if (!trimmed) {
    if (!response.ok) {
      throw new Error(`${response.status} ${response.statusText}`);
    }
    return {} as T;
  }

  if (!looksLikeJson) {
    const hint = path.startsWith("/api/")
      ? "Похоже, backend отдаёт frontend-страницу вместо API. Перезапусти локальное приложение."
      : "Сервер вернул не JSON.";
    throw new Error(`${hint} Endpoint: ${path}`);
  }

  let payload: unknown;
  try {
    payload = JSON.parse(trimmed);
  } catch {
    throw new Error(`Некорректный JSON от API. Endpoint: ${path}`);
  }

  if (!response.ok) {
    throw new Error(extractApiError(payload, response.statusText));
  }

  return payload as T;
}

function extractApiError(payload: unknown, fallback: string) {
  if (payload && typeof payload === "object" && "detail" in payload) {
    const detail = (payload as { detail?: unknown }).detail;
    if (typeof detail === "string") return detail;
    if (detail !== undefined) return JSON.stringify(detail);
  }
  return fallback;
}

export const api = {
  workflowFileUrl: (path: string) => `${API_BASE}/api/workflow/download-file?path=${encodeURIComponent(path)}`,
  exportFileUrl: (path: string) => `${API_BASE}/api/exports/download-file?path=${encodeURIComponent(path)}`,
  getWorkflowSettings: () => request<WorkflowSettings>("/api/workflow/settings"),
  saveWorkflowSettings: (payload: WorkflowSettings) =>
    request<WorkflowSettings>("/api/workflow/settings", { method: "PUT", body: JSON.stringify(payload) }),
  getPipelineSettings: () => request<PipelineSettings>("/api/workflow/pipeline/settings"),
  savePipelineSettings: (payload: PipelineSettings) =>
    request<PipelineSettings>("/api/workflow/pipeline/settings", { method: "PUT", body: JSON.stringify(payload) }),
  listProjects: () => request<{ projects: ProjectSummary[] }>("/api/projects"),
  deleteProject: (projectName: string, deleteFiles: boolean) =>
    request<ProjectDeleteResponse>(
      `/api/projects?project_name=${encodeURIComponent(projectName)}&delete_files=${encodeURIComponent(String(deleteFiles))}`,
      { method: "DELETE" }
    ),
  listCategories: () => request<{ categories: Category[] }>("/api/workflow/categories"),
  syncCategories: () => request<{ imported: number; source?: string | null }>("/api/workflow/categories/sync", { method: "POST" }),
  getCategorySource: () => request<{ path: string; rows: CategorySourceRow[] }>("/api/workflow/categories/source"),
  saveCategorySource: (rows: CategorySourceRow[]) =>
    request<{ path: string; rows: CategorySourceRow[]; imported: number }>("/api/workflow/categories/source", {
      method: "PUT",
      body: JSON.stringify({ rows })
    }),
  createPlan: (payload: {
    project_name: string;
    run_type: string;
    category_ids: string[];
    start_year: number;
    start_month: number;
    end_year: number;
    end_month: number;
    settings: PipelineSettings;
  }) => request<PipelineRun>("/api/workflow/pipeline/plans", { method: "POST", body: JSON.stringify(payload) }),
  monthlySync: (payload: { project_name: string; settings: PipelineSettings; start_immediately: boolean; wait: boolean }) =>
    request<PipelineRun>("/api/workflow/pipeline/monthly-sync", { method: "POST", body: JSON.stringify(payload) }),
  listRuns: (projectName: string) => request<{ runs: PipelineRun[] }>(`/api/workflow/pipeline/runs?project_name=${encodeURIComponent(projectName)}`),
  getRun: (runId: string) => request<PipelineRun>(`/api/workflow/pipeline/runs/${runId}`),
  getSmartPlan: (runId: string, status = "all") =>
    request<SmartPlan>(`/api/workflow/pipeline/runs/${runId}/smart-plan?status=${encodeURIComponent(status)}`),
  listTasks: (runId: string, taskFilter: string) =>
    request<{ tasks: DownloadTask[] }>(`/api/workflow/pipeline/runs/${runId}/tasks?task_filter=${encodeURIComponent(taskFilter)}`),
  startRun: (runId: string, wait = false) =>
    request<PipelineRun>(`/api/workflow/pipeline/runs/${runId}/start`, { method: "POST", body: JSON.stringify({ wait }) }),
  pauseRun: (runId: string) => request<PipelineRun>(`/api/workflow/pipeline/runs/${runId}/pause`, { method: "POST" }),
  resumeRun: (runId: string, wait = false) =>
    request<PipelineRun>(`/api/workflow/pipeline/runs/${runId}/resume`, { method: "POST", body: JSON.stringify({ wait }) }),
  retryErrors: (runId: string, wait = false) =>
    request<PipelineRun>(`/api/workflow/pipeline/runs/${runId}/retry-errors`, { method: "POST", body: JSON.stringify({ wait }) }),
  retryTask: (taskId: string, wait = false) =>
    request<PipelineRun>(`/api/workflow/pipeline/tasks/${taskId}/retry`, { method: "POST", body: JSON.stringify({ wait }) }),
  rebuildCube: (runId: string, wait = false) =>
    request<PipelineRun>(`/api/workflow/pipeline/runs/${runId}/rebuild-cube`, { method: "POST", body: JSON.stringify({ wait }) }),
  reclassifyCube: (runId: string, wait = false) =>
    request<PipelineRun>(`/api/workflow/pipeline/runs/${runId}/reclassify-cube`, { method: "POST", body: JSON.stringify({ wait }) }),
  getExportOptions: (projectName: string) => request<ExportOptions>(`/api/exports/options?project_name=${encodeURIComponent(projectName)}`),
  previewExport: (payload: ExportPayload) =>
    request<ExportPreview>("/api/exports/preview", { method: "POST", body: JSON.stringify(payload) }),
  buildExport: (payload: ExportPayload) =>
    request<ExportBuildResponse>("/api/exports/build", { method: "POST", body: JSON.stringify(payload) }),
  listFiles: (projectName: string) => request<{ root: string; files: ProjectFile[] }>(`/api/workflow/pipeline/files?project_name=${encodeURIComponent(projectName)}`),
  listCube: (projectName: string) => request<{ items: CubeItem[] }>(`/api/workflow/pipeline/cube?project_name=${encodeURIComponent(projectName)}`),
  listQualityProjects: () => request<{ projects: QualityProject[] }>("/api/quality/projects"),
  getQualityReport: (projectName: string) => request<QualityReport>(`/api/quality/report?project_name=${encodeURIComponent(projectName)}`),
  getRules: () => request<{ path: string; content: string }>("/api/rules"),
  saveRules: (content: string) =>
    request<{ path: string; content: string }>("/api/rules", {
      method: "PUT",
      body: JSON.stringify({ content })
    }),
  getClassifierRules: () => request<{ path: string; rules: ClassifierRule[] }>("/api/classifier/rules"),
  saveClassifierRules: (rules: ClassifierRule[]) =>
    request<{ path: string; rules: ClassifierRule[] }>("/api/classifier/rules", {
      method: "PUT",
      body: JSON.stringify({ rules })
    }),
  classifyExternalFile: (projectName: string, file: File, writeXlsx: boolean) => {
    const params = new URLSearchParams();
    params.set("project_name", projectName);
    params.set("filename", file.name || "external.csv");
    params.set("write_xlsx", String(writeXlsx));
    return request<ClassificationResponse>(`/api/workflow/classify-upload?${params.toString()}`, {
      method: "POST",
      headers: { "Content-Type": file.type || "application/octet-stream" },
      body: file
    });
  },
  searchProducts: (params: URLSearchParams) => request<ProductSearch>(`/api/products?${params.toString()}`)
};
