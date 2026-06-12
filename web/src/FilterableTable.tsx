import { ArrowDown, ArrowUp, Filter, RotateCcw, Search, X } from "lucide-react";
import type { MouseEvent as ReactMouseEvent, PointerEvent as ReactPointerEvent, ReactNode } from "react";
import { useMemo, useRef, useState } from "react";

export type SortDirection = "asc" | "desc";

export type FilterableColumn<T> = {
  id: string;
  label: string;
  value: (row: T) => unknown;
  sortValue?: (row: T) => unknown;
  render?: (row: T) => ReactNode;
  title?: (row: T) => string;
  className?: string;
  sortable?: boolean;
  filterable?: boolean;
  numeric?: boolean;
};

type ColumnFilterState = {
  text: string;
  selectedValues: string[] | null;
  valueSearch: string;
};

type SortState = {
  columnId: string;
  direction: SortDirection;
} | null;

type FilterableTableProps<T> = {
  columns: FilterableColumn<T>[];
  rows: T[];
  rowKey?: (row: T, index: number) => string;
  className?: string;
  emptyText?: string;
  getRowClassName?: (row: T) => string;
  onRowClick?: (row: T) => void;
  onTextFilterChange?: (columnId: string, value: string) => void;
  onSortChange?: (columnId: string, direction: SortDirection) => void;
  onSortClear?: () => void;
};

const LIST_LIMIT = 120;
const MIN_COLUMN_WIDTH = 90;
const MAX_COLUMN_WIDTH = 720;

type ResizeState = {
  columnId: string;
  pointerId?: number;
  startX: number;
  startWidth: number;
};

export function FilterableTable<T>(props: FilterableTableProps<T>) {
  const [openColumnId, setOpenColumnId] = useState<string | null>(null);
  const [filters, setFilters] = useState<Record<string, ColumnFilterState>>({});
  const [sort, setSort] = useState<SortState>(null);
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});
  const resizeState = useRef<ResizeState | null>(null);
  const columnsKey = props.columns
    .map((column) => `${column.id}:${column.filterable === false ? 0 : 1}:${column.sortable === false ? 0 : 1}:${column.numeric ? 1 : 0}`)
    .join("|");

  const openColumn = props.columns.find((column) => column.id === openColumnId);
  const openColumnValues = useMemo(() => (
    openColumn?.filterable === false || !openColumn ? [] : columnValues(openColumn, props.rows)
  ), [openColumnId, props.rows, columnsKey]);

  const visibleRows = useMemo(() => {
    const filtered = props.rows.filter((row) =>
      props.columns.every((column) => {
        if (column.filterable === false) return true;
        const filterState = filters[column.id];
        if (!filterState) return true;
        const value = cellText(column.value(row));
        const normalizedValue = normalizeText(value);
        if (filterState.text.trim() && !normalizedValue.includes(normalizeText(filterState.text))) {
          return false;
        }
        if (filterState.selectedValues && !filterState.selectedValues.includes(value)) {
          return false;
        }
        return true;
      })
    );

    if (!sort) return filtered;
    const sortedColumn = props.columns.find((column) => column.id === sort.columnId);
    if (!sortedColumn) return filtered;
    return [...filtered].sort((left, right) => compareValues(
      sortedColumn.sortValue ? sortedColumn.sortValue(left) : sortedColumn.value(left),
      sortedColumn.sortValue ? sortedColumn.sortValue(right) : sortedColumn.value(right),
      sort.direction,
      sortedColumn.numeric
    ));
  }, [filters, props.rows, sort, columnsKey]);

  const activeFilterCount = Object.values(filters).filter((filterState) => isFilterActive(filterState)).length;

  function columnFilter(columnId: string): ColumnFilterState {
    return filters[columnId] ?? { text: "", selectedValues: null, valueSearch: "" };
  }

  function patchFilter(columnId: string, patch: Partial<ColumnFilterState>) {
    setFilters((current) => {
      const nextState = { ...columnFilterFrom(current, columnId), ...patch };
      const next = { ...current };
      if (shouldKeepFilterState(nextState)) next[columnId] = nextState;
      else delete next[columnId];
      return next;
    });
  }

  function setColumnText(columnId: string, value: string) {
    patchFilter(columnId, { text: value });
    props.onTextFilterChange?.(columnId, value);
  }

  function clearColumnFilter(columnId: string) {
    setFilters((current) => {
      const next = { ...current };
      delete next[columnId];
      return next;
    });
    props.onTextFilterChange?.(columnId, "");
  }

  function clearAll() {
    setFilters({});
    setSort(null);
    for (const column of props.columns) props.onTextFilterChange?.(column.id, "");
    props.onSortClear?.();
  }

  function setColumnSort(columnId: string, direction: SortDirection) {
    setSort({ columnId, direction });
    props.onSortChange?.(columnId, direction);
  }

  function startColumnResize(event: ReactPointerEvent<HTMLButtonElement>, columnId: string) {
    resizeState.current = {
      columnId,
      pointerId: event.pointerId,
      startX: event.clientX,
      startWidth: resizeStartWidth(event.currentTarget, columnId)
    };
    event.currentTarget.setPointerCapture(event.pointerId);
    document.body.classList.add("column-resize-active");
    event.preventDefault();
    event.stopPropagation();
  }

  function moveColumnResize(event: ReactPointerEvent<HTMLButtonElement>) {
    const state = resizeState.current;
    if (!state || state.pointerId !== event.pointerId) return;
    updateColumnResize(event.clientX);
  }

  function stopColumnResize(event: ReactPointerEvent<HTMLButtonElement>) {
    const state = resizeState.current;
    if (!state || state.pointerId !== event.pointerId) return;
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
    resizeState.current = null;
    document.body.classList.remove("column-resize-active");
    event.preventDefault();
    event.stopPropagation();
  }

  function startColumnMouseResize(event: ReactMouseEvent<HTMLButtonElement>, columnId: string) {
    if (event.button !== 0 || resizeState.current) return;
    resizeState.current = {
      columnId,
      startX: event.clientX,
      startWidth: resizeStartWidth(event.currentTarget, columnId)
    };
    document.body.classList.add("column-resize-active");
    const handleMouseMove = (moveEvent: MouseEvent) => updateColumnResize(moveEvent.clientX);
    const handleMouseUp = () => {
      document.removeEventListener("mousemove", handleMouseMove);
      resizeState.current = null;
      document.body.classList.remove("column-resize-active");
    };
    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp, { once: true });
    event.preventDefault();
    event.stopPropagation();
  }

  function resizeStartWidth(handle: HTMLButtonElement, columnId: string) {
    const headerCell = handle.closest("th");
    return headerCell?.getBoundingClientRect().width ?? columnWidths[columnId] ?? MIN_COLUMN_WIDTH;
  }

  function updateColumnResize(clientX: number) {
    const state = resizeState.current;
    if (!state) return;
    const width = Math.min(MAX_COLUMN_WIDTH, Math.max(MIN_COLUMN_WIDTH, Math.round(state.startWidth + clientX - state.startX)));
    setColumnWidths((current) => (current[state.columnId] === width ? current : { ...current, [state.columnId]: width }));
  }

  function toggleValue(columnId: string, value: string) {
    const current = columnFilter(columnId);
    const allValues = columnId === openColumnId ? openColumnValues : columnValuesForId(props.columns, props.rows, columnId);
    const selected = current.selectedValues ?? allValues;
    const nextSelected = selected.includes(value) ? selected.filter((item) => item !== value) : [...selected, value];
    patchFilter(columnId, { selectedValues: nextSelected });
  }

  return (
    <div className={`filterable-table ${props.className ?? ""}`}>
      <div className="filterable-table-toolbar">
        <span>Показано {visibleRows.length} из {props.rows.length}</span>
        {activeFilterCount || sort ? (
          <button type="button" className="tiny-button" onClick={clearAll}>
            <RotateCcw size={14} />Сбросить
          </button>
        ) : null}
      </div>
      <div className="table-wrap">
        <table>
          <colgroup>
            {props.columns.map((column) => (
              <col key={column.id} style={columnWidths[column.id] ? { width: `${columnWidths[column.id]}px` } : undefined} />
            ))}
          </colgroup>
          <thead>
            <tr>
              {props.columns.map((column) => {
                const filterState = columnFilter(column.id);
                const isOpen = openColumnId === column.id;
                const isActive = isFilterActive(filterState) || sort?.columnId === column.id;
                const columnWidth = columnWidths[column.id];
                const columnStyle = columnWidth ? { width: `${columnWidth}px`, minWidth: `${columnWidth}px` } : undefined;
                return (
                  <th key={column.id} className={column.className} style={columnStyle}>
                    <div className="filterable-th">
                      <button
                        type="button"
                        className={`filterable-th-button ${isActive ? "active" : ""}`}
                        onClick={() => setOpenColumnId(isOpen ? null : column.id)}
                        disabled={column.filterable === false && column.sortable === false}
                        title={`Фильтр и сортировка: ${column.label}`}
                      >
                        <span>{column.label}</span>
                        {sort?.columnId === column.id ? (sort.direction === "asc" ? <ArrowUp size={13} /> : <ArrowDown size={13} />) : null}
                        {column.filterable !== false ? <Filter size={13} /> : null}
                      </button>
                      <button
                        type="button"
                        className="column-resize-handle"
                        title={`Изменить ширину: ${column.label}`}
                        aria-label={`Изменить ширину: ${column.label}`}
                        onPointerDown={(event) => startColumnResize(event, column.id)}
                        onPointerMove={moveColumnResize}
                        onPointerUp={stopColumnResize}
                        onPointerCancel={stopColumnResize}
                        onMouseDown={(event) => startColumnMouseResize(event, column.id)}
                      />
                      {isOpen ? (
                        <ColumnFilterMenu
                          column={column}
                          filterState={filterState}
                          sort={sort}
                          values={openColumnValues}
                          onClose={() => setOpenColumnId(null)}
                          onSort={setColumnSort}
                          onTextChange={setColumnText}
                          onValueSearchChange={(value) => patchFilter(column.id, { valueSearch: value })}
                          onToggleValue={toggleValue}
                          onSelectAll={() => patchFilter(column.id, { selectedValues: null })}
                          onSelectNone={() => patchFilter(column.id, { selectedValues: [] })}
                          onClear={() => clearColumnFilter(column.id)}
                        />
                      ) : null}
                    </div>
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((row, index) => (
              <tr
                key={props.rowKey ? props.rowKey(row, index) : String(index)}
                className={props.getRowClassName?.(row)}
                onClick={props.onRowClick ? () => props.onRowClick?.(row) : undefined}
              >
                {props.columns.map((column) => {
                  const columnWidth = columnWidths[column.id];
                  const columnStyle = columnWidth ? { width: `${columnWidth}px`, minWidth: `${columnWidth}px` } : undefined;
                  return (
                    <td key={column.id} className={column.className} style={columnStyle} title={column.title ? column.title(row) : cellText(column.value(row))}>
                      {column.render ? column.render(row) : cellText(column.value(row))}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
        {!visibleRows.length ? <div className="filterable-empty">{props.emptyText ?? "Нет строк по текущим фильтрам."}</div> : null}
      </div>
    </div>
  );
}

function ColumnFilterMenu<T>(props: {
  column: FilterableColumn<T>;
  filterState: ColumnFilterState;
  sort: SortState;
  values: string[];
  onClose: () => void;
  onSort: (columnId: string, direction: SortDirection) => void;
  onTextChange: (columnId: string, value: string) => void;
  onValueSearchChange: (value: string) => void;
  onToggleValue: (columnId: string, value: string) => void;
  onSelectAll: () => void;
  onSelectNone: () => void;
  onClear: () => void;
}) {
  const selectedValues = props.filterState.selectedValues;
  const valueSearch = normalizeText(props.filterState.valueSearch);
  const values = props.values.filter((value) => !valueSearch || normalizeText(value).includes(valueSearch)).slice(0, LIST_LIMIT);
  const selectedCount = selectedValues ? selectedValues.length : props.values.length;

  return (
    <div className="column-filter-menu">
      <div className="column-filter-head">
        <strong>{props.column.label}</strong>
        <button type="button" className="icon-button" onClick={props.onClose} aria-label="Закрыть фильтр">
          <X size={15} />
        </button>
      </div>
      {props.column.sortable !== false ? (
        <div className="column-filter-sort">
          <button type="button" className={props.sort?.columnId === props.column.id && props.sort.direction === "asc" ? "active" : ""} onClick={() => props.onSort(props.column.id, "asc")}>
            <ArrowUp size={14} />А-Я
          </button>
          <button type="button" className={props.sort?.columnId === props.column.id && props.sort.direction === "desc" ? "active" : ""} onClick={() => props.onSort(props.column.id, "desc")}>
            <ArrowDown size={14} />Я-А
          </button>
        </div>
      ) : null}
      {props.column.filterable !== false ? (
        <>
          <label className="filter-menu-field">
            <span>Текст</span>
            <input value={props.filterState.text} onChange={(event) => props.onTextChange(props.column.id, event.target.value)} placeholder="содержит..." />
          </label>
          <label className="filter-menu-field">
            <span>Список</span>
            <div className="filter-menu-search">
              <Search size={14} />
              <input value={props.filterState.valueSearch} onChange={(event) => props.onValueSearchChange(event.target.value)} placeholder="найти значение" />
            </div>
          </label>
          <div className="filter-menu-actions">
            <button type="button" onClick={props.onSelectAll}>Все</button>
            <button type="button" onClick={props.onSelectNone}>Снять</button>
            <button type="button" onClick={props.onClear}>Очистить</button>
          </div>
          <div className="filter-menu-values" role="listbox" aria-label={`Значения ${props.column.label}`}>
            {values.map((value) => (
              <label key={value} className="filter-menu-value">
                <input type="checkbox" checked={!selectedValues || selectedValues.includes(value)} onChange={() => props.onToggleValue(props.column.id, value)} />
                <span title={value}>{value}</span>
              </label>
            ))}
          </div>
          <small className="filter-menu-count">Выбрано {selectedCount} из {props.values.length}</small>
        </>
      ) : null}
    </div>
  );
}

function columnValues<T>(column: FilterableColumn<T>, rows: T[]) {
  const values = new Set<string>();
  for (const row of rows) values.add(cellText(column.value(row)));
  return [...values].sort(compareText);
}

function columnValuesForId<T>(columns: FilterableColumn<T>[], rows: T[], columnId: string) {
  const column = columns.find((item) => item.id === columnId);
  if (!column || column.filterable === false) return [];
  return columnValues(column, rows);
}

function columnFilterFrom(filters: Record<string, ColumnFilterState>, columnId: string): ColumnFilterState {
  return filters[columnId] ?? { text: "", selectedValues: null, valueSearch: "" };
}

function isFilterActive(filterState: ColumnFilterState) {
  return Boolean(filterState.text.trim() || filterState.selectedValues !== null);
}

function shouldKeepFilterState(filterState: ColumnFilterState) {
  return Boolean(isFilterActive(filterState) || filterState.valueSearch.trim());
}

function normalizeText(value: string) {
  return value.trim().toLocaleLowerCase("ru-RU");
}

function cellText(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  if (value instanceof Date) return value.toLocaleString("ru-RU");
  return String(value);
}

function compareText(left: string, right: string) {
  return left.localeCompare(right, "ru-RU", { numeric: true, sensitivity: "base" });
}

function compareValues(leftRaw: unknown, rightRaw: unknown, direction: SortDirection, numeric = false) {
  const leftText = cellText(leftRaw);
  const rightText = cellText(rightRaw);
  const leftNumber = Number(String(leftText).replace(",", "."));
  const rightNumber = Number(String(rightText).replace(",", "."));
  const canCompareAsNumber = numeric || (!Number.isNaN(leftNumber) && !Number.isNaN(rightNumber));
  let result = compareText(leftText, rightText);
  if (canCompareAsNumber) {
    if (Number.isNaN(leftNumber) && Number.isNaN(rightNumber)) result = 0;
    else if (Number.isNaN(leftNumber)) result = 1;
    else if (Number.isNaN(rightNumber)) result = -1;
    else result = leftNumber - rightNumber;
  }
  return direction === "asc" ? result : -result;
}
