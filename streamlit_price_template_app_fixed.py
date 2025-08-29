"""
Streamlit Price Parser with Per-Supplier Templates (Fixed & Extended)
--------------------------------------------------------------------
- Fixes IndentationError and duplicate-column header issues
- Adds multi-price mapping, explicit dimensions (length/width/height)
- Adds output column renaming in template/UI

Run locally:
  pip install -r requirements.txt
  streamlit run streamlit_price_template_app_fixed.py

requirements.txt:
  streamlit>=1.36
  pandas>=2.2
  openpyxl>=3.1
  numpy>=1.26
  # For legacy .xls (only if needed):
  # xlrd==1.2.0
"""

from __future__ import annotations
import io
import json
import os
import re
import hashlib
from dataclasses import dataclass, asdict
from typing import Dict, Optional, List, Any

import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------- Config ----------------------------
TEMPLATE_DIR = "templates"
STANDARD_FIELDS = [
    "supplier",
    "sheet",
    "header_row",
    "name_col",
    "size_col",
    "length_col",
    "width_col",
    "height_col",
    "price_col",
    "price_cols",
    "price_labels",
    "currency_col",
    "sku_col",
    "qty_col",
    "uom_col",
    "regex_extract_size_from",
    "size_regex",
    "price_regex",
    "price_multiplier",
    "drop_na_price",
    "skip_top_rows",
    "skip_bottom_rows",
    "filters",
    "output_renames",
]

# ---------------------------- Utilities ----------------------------

def ensure_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass


def file_fingerprint(file_bytes: bytes) -> str:
    return hashlib.sha1(file_bytes).hexdigest()[:12]


@dataclass
class PriceTemplate:
    supplier: str
    sheet: Optional[str] = None
    header_row: int = 0  # zero-based index in pandas

    # core mappings
    name_col: Optional[str] = None
    size_col: Optional[str] = None

    # explicit dimensions (optional)
    length_col: Optional[str] = None
    width_col: Optional[str] = None
    height_col: Optional[str] = None

    # prices: legacy single and new multi
    price_col: Optional[str] = None
    price_cols: Optional[List[str]] = None
    price_labels: Optional[List[str]] = None

    currency_col: Optional[str] = None
    sku_col: Optional[str] = None
    qty_col: Optional[str] = None
    uom_col: Optional[str] = None

    # extraction helpers
    regex_extract_size_from: Optional[str] = None  # None | "name_col" | "size_col"
    size_regex: Optional[str] = None
    price_regex: Optional[str] = None
    price_multiplier: float = 1.0

    drop_na_price: bool = True
    skip_top_rows: int = 0
    skip_bottom_rows: int = 0

    # optional filters like {"price_min": 0, "include_text": "PVC"}
    filters: Dict[str, Any] = None

    # optional output renaming
    output_renames: Optional[Dict[str, str]] = None

    def to_json(self) -> str:
        d = asdict(self)
        return json.dumps(d, ensure_ascii=False, indent=2)

    @staticmethod
    def from_json(s: str) -> "PriceTemplate":
        data = json.loads(s)
        return PriceTemplate(**data)


# ---------------------------- Parsing Helpers ----------------------------

def try_read_excel(uploaded_file, sheet_name: Optional[str] = None, header_row: int = 0,
                   skip_top: int = 0, skip_bottom: int = 0) -> pd.DataFrame:
    """Read Excel without trusting headers, then set user-chosen header row.
    Ensures column names are non-empty and unique to satisfy Arrow/Streamlit.
    """
    xls = pd.ExcelFile(uploaded_file)
    target_sheet = sheet_name or xls.sheet_names[0]
    df_raw = xls.parse(target_sheet, header=None, dtype=str)

    # trim top/bottom rows visually
    if skip_top:
        df_raw = df_raw.iloc[skip_top:]
    if skip_bottom:
        df_raw = df_raw.iloc[:-skip_bottom] if skip_bottom < len(df_raw) else df_raw.iloc[:0]

    # safe header row after trimming
    if header_row >= len(df_raw):
        header_row = 0

    header_series = df_raw.iloc[header_row].fillna("")

    # clean & deduplicate headers
    names = [' '.join(str(x).split()) for x in header_series.tolist()]
    seen: Dict[str, int] = {}
    cleaned: List[str] = []
    for i, n in enumerate(names):
        if not n or re.match(r"^(unnamed(:.*)?|none|nan)$", n, re.IGNORECASE):
            n = f"col_{i+1}"
        if n in seen:
            seen[n] += 1
            n = f"{n}_{seen[n]}"
        else:
            seen[n] = 0
        cleaned.append(n)

    df = df_raw.iloc[header_row + 1:].copy()
    df.columns = cleaned
    df = df.reset_index(drop=True)

    # drop completely empty columns
    df = df.dropna(axis=1, how="all")

    # final safety: ensure uniqueness after downstream changes
    if len(df.columns) != len(set(df.columns)):
        seen2: Dict[str, int] = {}
        new_cols: List[str] = []
        for c in list(df.columns):
            n = str(c)
            if n in seen2:
                seen2[n] += 1
                n = f"{n}_{seen2[n]}"
            else:
                seen2[n] = 0
            new_cols.append(n)
        df.columns = new_cols

    return df


def normalize_price(val: Any, price_regex: Optional[str]) -> Optional[float]:
    if val is None:
        return None
    s = str(val)
    if not s or s.strip().lower() in {"nan", "none", ""}:
        return None
    if price_regex:
        m = re.search(price_regex, s)
        if m:
            s = m.group(1)
    s = s.replace(" ", "").replace("\u00a0", "").replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        return float(s)
    except Exception:
        return None


def extract_size(row: pd.Series, t: PriceTemplate) -> Optional[str]:
    src = None
    if t.regex_extract_size_from == "name_col" and t.name_col:
        src = row.get(t.name_col)
    elif t.regex_extract_size_from == "size_col" and t.size_col:
        src = row.get(t.size_col)
    if src is None:
        return row.get(t.size_col) if t.size_col else None
    if not t.size_regex:
        return src
    m = re.search(t.size_regex, str(src))
    if m:
        return m.group(0)
    return None


def apply_template(df: pd.DataFrame, t: PriceTemplate) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    # name
    if t.name_col and t.name_col in df.columns:
        out["name"] = df[t.name_col].astype(str)
    else:
        out["name"] = ""

    # explicit dimensions
    for src, dst in [(t.length_col, "length"), (t.width_col, "width"), (t.height_col, "height")]:
        if src and src in df.columns:
            out[dst] = df[src]
        else:
            out[dst] = ""

    # legacy size (string)
    out["size"] = df.apply(lambda r: extract_size(r, t), axis=1)

    # prices (multi or single)
    created_price_cols: List[str] = []
    effective_price_cols: List[str] = []
    if t.price_cols:
        effective_price_cols = [c for c in t.price_cols if c in df.columns]
    elif t.price_col and t.price_col in df.columns:
        effective_price_cols = [t.price_col]

    if t.price_labels and len(t.price_labels) >= len(effective_price_cols):
        labels = t.price_labels[:len(effective_price_cols)]
    else:
        labels = effective_price_cols[:]

    # dedup labels
    seen: Dict[str, int] = {}
    final_labels: List[str] = []
    for i, lab in enumerate(labels):
        n = str(lab) if lab else f"price_{i+1}"
        if n in seen:
            seen[n] += 1
            n = f"{n}_{seen[n]}"
        else:
            seen[n] = 0
        final_labels.append(n)

    for src_col, out_name in zip(effective_price_cols, final_labels):
        prices = df[src_col].apply(lambda v: normalize_price(v, t.price_regex))
        if t.price_multiplier and t.price_multiplier != 1:
            prices = prices.apply(lambda x: x * t.price_multiplier if x is not None else x)
        out[out_name] = prices
        created_price_cols.append(out_name)

    # currency and other fields
    if t.currency_col and t.currency_col in df.columns:
        out["currency"] = df[t.currency_col]
    else:
        out["currency"] = ""

    for src, dst in [(t.sku_col, "sku"), (t.qty_col, "qty"), (t.uom_col, "uom")]:
        if src and src in df.columns:
            out[dst] = df[src]
        else:
            out[dst] = ""

    # optional filtering
    if t.drop_na_price and created_price_cols:
        out = out.dropna(subset=created_price_cols, how="all")

    if t.filters:
        f = t.filters
        if isinstance(f, dict):
            if created_price_cols:
                row_max = out[created_price_cols].max(axis=1, skipna=True)
            else:
                row_max = pd.Series([np.nan] * len(out), index=out.index)
            if "price_min" in f:
                try:
                    minv = float(f["price_min"])
                    out = out[row_max.isna() | (row_max >= minv)]
                except Exception:
                    pass
            if "price_max" in f:
                try:
                    maxv = float(f["price_max"])
                    out = out[row_max.isna() | (row_max <= maxv)]
                except Exception:
                    pass
            if txt := f.get("include_text"):
                out = out[out["name"].astype(str).str.contains(str(txt), case=False, na=False)]
            if excl := f.get("exclude_text"):
                out = out[~out["name"].astype(str).str.contains(str(excl), case=False, na=False)]

    # apply custom renames for output columns
    if getattr(t, "output_renames", None):
        rename_map = {k: v for k, v in (t.output_renames or {}).items() if v and str(v).strip() and k in out.columns}
        if rename_map:
            out = out.rename(columns=rename_map)
            # ensure uniqueness after renaming
            seen3: Dict[str, int] = {}
            new_cols2: List[str] = []
            for c in list(out.columns):
                n = str(c)
                if n in seen3:
                    seen3[n] += 1
                    n = f"{n}_{seen3[n]}"
                else:
                    seen3[n] = 0
                new_cols2.append(n)
            out.columns = new_cols2

    out = out.reset_index(drop=True)
    return out


# ---------------------------- UI ----------------------------

st.set_page_config(page_title="Прайс‑парсер с шаблонами", layout="wide")
st.title("🧩 Прайс‑парсер с шаблонами (Streamlit)")

with st.sidebar:
    st.header("Шаги")
    st.markdown("1) Загрузка файла\n\n2) Подготовка листа и заголовка\n\n3) Настройка шаблона\n\n4) Предпросмотр\n\n5) Сохранить / Экспорт")
    st.divider()
    st.caption("Подсказка: для .xls может понадобиться xlrd==1.2.0")

# Session state
ss = st.session_state
if "templates_cache" not in ss:
    ss.templates_cache = {}
if "active_template" not in ss:
    ss.active_template = None

# 1) Upload
uploaded = st.file_uploader("Загрузите Excel (.xlsx/.xls)", type=["xlsx", "xls"], accept_multiple_files=False)

if uploaded:
    uploaded_bytes = uploaded.getvalue()
    fp = file_fingerprint(uploaded_bytes)
    xls = pd.ExcelFile(io.BytesIO(uploaded_bytes))
    st.success(f"Файл загружен ✅ | Пальчик: {fp} | Листы: {', '.join(xls.sheet_names)}")

    # 2) Sheet + header row + trimming
    st.subheader("🧾 Подготовка листа")
    colA, colB, colC, colD = st.columns([2, 1, 1, 1])
    with colA:
        sheet = st.selectbox("Лист", options=xls.sheet_names)
    with colB:
        skip_top = st.number_input("Пропустить верхних строк", min_value=0, max_value=500, value=0)
    with colC:
        skip_bottom = st.number_input("Пропустить нижних строк", min_value=0, max_value=500, value=0)
    with colD:
        header_row = st.number_input(
            "Строка заголовка (после пропуска)",
            min_value=0,
            max_value=100,
            value=0,
            help="Нулевая = первая видимая строка после пропуска",
        )

    df_preview = try_read_excel(
        io.BytesIO(uploaded_bytes),
        sheet_name=sheet,
        header_row=header_row,
        skip_top=skip_top,
        skip_bottom=skip_bottom,
    )

    st.caption("Предпросмотр таблицы после выбора строки заголовка и обрезки:")
    st.dataframe(df_preview.head(50), use_container_width=True)

    # 3) Template config
    st.subheader("🧩 Настройка шаблона")

    # Auto-suggest supplier name from filename
    default_supplier = re.sub(r"\.[^.]+$", "", uploaded.name)
    default_supplier = re.sub(r"[^\w\-]+", " ", default_supplier).strip()

    with st.expander("Загрузка/Сохранение шаблонов", expanded=True):
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            supplier = st.text_input("Поставщик (имя шаблона)", value=default_supplier or "Поставщик")
        with c2:
            # discover local templates
            ensure_dir(TEMPLATE_DIR)
            try:
                available = [f for f in os.listdir(TEMPLATE_DIR) if f.endswith(".json")]
                available = ["— выбрать —"] + sorted(available)
            except Exception:
                available = ["— выбрать —"]
            chosen_name = st.selectbox("Загрузить локальный шаблон", options=available)
            if chosen_name != "— выбрать —":
                try:
                    with open(os.path.join(TEMPLATE_DIR, chosen_name), "r", encoding="utf-8") as f:
                        t_loaded = PriceTemplate.from_json(f.read())
                    ss.active_template = t_loaded
                    st.success(f"Шаблон загружен: {chosen_name}")
                except Exception as e:
                    st.error(f"Не удалось загрузить шаблон: {e}")
        with c3:
            up_json = st.file_uploader("Импорт шаблона (.json)", type=["json"], accept_multiple_files=False, key="tmplup")
            if up_json is not None:
                try:
                    t_loaded = PriceTemplate.from_json(up_json.getvalue().decode("utf-8"))
                    ss.active_template = t_loaded
                    st.success("Импортирован шаблон из JSON")
                except Exception as e:
                    st.error(f"Ошибка импорта: {e}")

    # current template or new
    t: PriceTemplate = ss.active_template or PriceTemplate(
        supplier=supplier,
        sheet=sheet,
        header_row=int(header_row),
        skip_top_rows=int(skip_top),
        skip_bottom_rows=int(skip_bottom),
        filters={},
    )

    # refresh fields
    t.supplier = supplier
    t.sheet = sheet
    t.header_row = int(header_row)
    t.skip_top_rows = int(skip_top)
    t.skip_bottom_rows = int(skip_bottom)

    st.markdown("**Соответствие столбцов:**")
    cols = list(df_preview.columns)
    ca, cb, cc = st.columns(3)
    with ca:
        t.name_col = st.selectbox(
            "Наименование",
            options=["— нет —"] + cols,
            index=(cols.index(t.name_col) + 1) if (t.name_col in cols) else 0,
        )
        t.sku_col = st.selectbox(
            "Артикул (если есть)",
            options=["— нет —"] + cols,
            index=(cols.index(t.sku_col) + 1) if (t.sku_col in cols) else 0,
        )
        t.currency_col = st.selectbox(
            "Валюта (если есть)",
            options=["— нет —"] + cols,
            index=(cols.index(t.currency_col) + 1) if (t.currency_col in cols) else 0,
        )
    with cb:
        t.size_col = st.selectbox(
            "Размер (строка, если есть)",
            options=["— нет —"] + cols,
            index=(cols.index(t.size_col) + 1) if (t.size_col in cols) else 0,
        )
        t.length_col = st.selectbox(
            "Длина (если есть)",
            options=["— нет —"] + cols,
            index=(cols.index(t.length_col) + 1) if (t.length_col in cols) else 0,
        )
        t.width_col = st.selectbox(
            "Ширина (если есть)",
            options=["— нет —"] + cols,
            index=(cols.index(t.width_col) + 1) if (t.width_col in cols) else 0,
        )
        t.height_col = st.selectbox(
            "Высота (если есть)",
            options=["— нет —"] + cols,
            index=(cols.index(t.height_col) + 1) if (t.height_col in cols) else 0,
        )
    with cc:
        preselected_prices = t.price_cols or ([t.price_col] if (t.price_col and t.price_col in cols) else [])
        t.price_cols = st.multiselect(
            "Цены (можно несколько)",
            options=cols,
            default=[c for c in preselected_prices if c in cols],
        )
        labels: List[str] = []
        if t.price_cols:
            st.caption("Имена выходных столбцов цен (по умолчанию берём имена исходных столбцов):")
            for i, csrc in enumerate(t.price_cols):
                default_label = (
                    t.price_labels[i]
                    if (t.price_labels and i < len(t.price_labels) and t.price_labels[i])
                    else csrc
                )
                labels.append(
                    st.text_input(
                        f"Название для цены {i+1} ({csrc})",
                        value=default_label,
                        key=f"lbl_{i}_{csrc}",
                    )
                )
        t.price_labels = labels
        t.qty_col = st.selectbox(
            "Кол-во (если есть)",
            options=["— нет —"] + cols,
            index=(cols.index(t.qty_col) + 1) if (t.qty_col in cols) else 0,
        )
        t.uom_col = st.selectbox(
            "Ед.изм. (если есть)",
            options=["— нет —"] + cols,
            index=(cols.index(t.uom_col) + 1) if (t.uom_col in cols) else 0,
        )
        t.price_regex = st.text_input(
            "Regex для цены (опционально)",
            value=t.price_regex or "([0-9]+[.,]?[0-9]*)",
            help="Если в ячейке есть текст с валютой — вытащим число по первой группе",
        )
        t.price_multiplier = float(
            st.number_input(
                "Множитель цены",
                value=float(t.price_multiplier or 1.0),
                step=0.1,
                help="Напр.: 0.01 если цена указана за 100 шт.",
            )
        )

    # Output column renaming (optional)
    with st.expander("Переименование выходных столбцов (не влияет на исходный Excel)", expanded=False):
        st.caption("Оставьте пустым, если переименовывать не нужно. Для цен используйте поля 'Название для цены' выше.")
        base_fields = ["name", "size", "length", "width", "height", "sku", "qty", "uom", "currency"]
        renames = (t.output_renames or {}) if isinstance(getattr(t, "output_renames", None), dict) else {}
        new_map: Dict[str, str] = {}
        cols_ren = st.columns(3)
        for idx, key in enumerate(base_fields):
            with cols_ren[idx % 3]:
                new_map[key] = st.text_input(f"Переименовать '{key}' в", value=renames.get(key, ""), key=f"rn_{key}")
        t.output_renames = new_map

    # Size extraction (regex) block
    st.markdown("**Извлечение размера по шаблону (опционально):**")
    cd, ce = st.columns([2, 3])
    with cd:
        src_choice = st.selectbox(
            "Извлекать размер из",
            options=["— не извлекать —", "name_col", "size_col"],
            index={None: 0, "name_col": 1, "size_col": 2}.get(t.regex_extract_size_from or None, 0),
        )
        t.regex_extract_size_from = None if src_choice == "— не извлекать —" else src_choice
    with ce:
        t.size_regex = st.text_input(
            "Regex для размера",
            value=t.size_regex or r"(\d+\s?[xX]\s?\d+)|(\d+(?:[.,]\d+)?)\s?(mm|cm|m)\b",
            help="Например: 10x20, 10 x 20, или 12 mm",
        )

    # Filters block
    st.markdown("**Фильтры (опционально):**")
    cf, cg, ch, ci = st.columns(4)
    with cf:
        drop_na = st.checkbox("Убирать пустые цены", value=bool(t.drop_na_price))
        t.drop_na_price = drop_na
    with cg:
        pmin = st.text_input("Мин. цена", value=str((t.filters or {}).get("price_min", "")))
    with ch:
        pmax = st.text_input("Макс. цена", value=str((t.filters or {}).get("price_max", "")))
    with ci:
        inc = st.text_input("Включать текст", value=(t.filters or {}).get("include_text", ""))
        exc = st.text_input("Исключать текст", value=(t.filters or {}).get("exclude_text", ""))

    t.filters = {}
    if pmin.strip():
        t.filters["price_min"] = pmin
    if pmax.strip():
        t.filters["price_max"] = pmax
    if inc.strip():
        t.filters["include_text"] = inc
    if exc.strip():
        t.filters["exclude_text"] = exc

    # 4) Apply template preview
    st.subheader("🔎 Предпросмотр стандартизованных данных")

    # normalize "— нет —" -> None
    for fld in [
        "name_col",
        "size_col",
        "length_col",
        "width_col",
        "height_col",
        "price_col",
        "currency_col",
        "sku_col",
        "qty_col",
        "uom_col",
    ]:
        v = getattr(t, fld)
        if v == "— нет —":
            setattr(t, fld, None)

    standardized = apply_template(df_preview, t)
    st.dataframe(standardized.head(200), use_container_width=True)
    st.caption(f"Всего строк после фильтров: {len(standardized)}")

    # 5) Save / Export
    st.subheader("💾 Сохранение и экспорт")

    csa, csb, csc, csd = st.columns(4)
    with csa:
        if st.button("Сохранить локально (./templates)"):
            try:
                ensure_dir(TEMPLATE_DIR)
                fname = re.sub(r"[^\w\-]+", "_", t.supplier.strip()) or "template"
                suffix = f"__{re.sub(r'[^\w\-]+','_', (t.sheet or 'any'))}"
                path = os.path.join(TEMPLATE_DIR, f"{fname}{suffix}.json")
                with open(path, "w", encoding="utf-8") as f:
                    f.write(t.to_json())
                st.success(f"Шаблон сохранён: {path}")
                ss.active_template = t
            except Exception as e:
                st.error(f"Ошибка сохранения: {e}")
    with csb:
        st.download_button(
            "⬇️ Скачать шаблон JSON",
            data=t.to_json().encode("utf-8"),
            file_name=f"template_{re.sub(r'[^\\w\\-]+','_', t.supplier)}.json",
            mime="application/json",
        )
    with csc:
        csv_bytes = standardized.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "⬇️ Экспорт CSV",
            data=csv_bytes,
            file_name=f"{re.sub(r'[^\\w\\-]+','_', t.supplier)}_export.csv",
            mime="text/csv",
        )
    with csd:
        xbuf = io.BytesIO()
        with pd.ExcelWriter(xbuf, engine="openpyxl") as writer:
            standardized.to_excel(writer, index=False, sheet_name="export")
        st.download_button(
            "⬇️ Экспорт Excel",
            data=xbuf.getvalue(),
            file_name=f"{re.sub(r'[^\\w\\-]+','_', t.supplier)}_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    st.divider()
    st.markdown("#### 🤖 Автоприменение шаблона")
    st.caption(
        "При повторной загрузке прайса поставщика вы можете сразу загрузить его шаблон из списка или импортировать JSON. "
        "Если имя листа и структура совпадают — данные подтянутся автоматически."
    )

else:
    st.info("Загрузите Excel-файл поставщика, чтобы начать.")
