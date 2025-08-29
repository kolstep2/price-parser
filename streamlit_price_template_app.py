"""
Streamlit Price Parser with Per-Supplier Templates
-------------------------------------------------
Run locally:
  pip install -r requirements.txt
  streamlit run streamlit_price_template_app.py

requirements.txt (create this file alongside the app):
  streamlit>=1.36
  pandas>=2.2
  openpyxl>=3.1
  numpy>=1.26

Notes:
- Templates can be stored locally in the ./templates folder (automatic) or downloaded/uploaded as JSON via the UI.
- Works for .xlsx and .xls (via openpyxl for .xlsx). For .xls you may need xlrd < 2.0.0; if necessary add "xlrd==1.2.0" to requirements.
- If deploying to Streamlit Cloud (no persistent disk), prefer downloading templates and re-uploading them when needed.
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

TEMPLATE_DIR = "templates"
STANDARD_FIELDS = [
    "supplier",
    "sheet",
    "header_row",
    "name_col",
    "size_col",
    "price_col",
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
]

# ---------- Utilities ----------

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
    name_col: Optional[str] = None
    size_col: Optional[str] = None
    price_col: Optional[str] = None
    currency_col: Optional[str] = None
    sku_col: Optional[str] = None
    qty_col: Optional[str] = None
    uom_col: Optional[str] = None

    # extraction helpers
    regex_extract_size_from: Optional[str] = None  # one of: None, "name_col", "size_col"
    size_regex: Optional[str] = None               # e.g., r"(\d+\s?[xX]\s?\d+)|\b(\d+(?:[.,]\d+)?)\s?(mm|cm|m)\b"
    price_regex: Optional[str] = None              # e.g., r"([0-9]+[.,]?[0-9]*)"
    price_multiplier: float = 1.0                  # convert per-100 units, etc.

    drop_na_price: bool = True
    skip_top_rows: int = 0
    skip_bottom_rows: int = 0

    # optional filters like {"price_min": 0, "include_text": "PVC"}
    filters: Dict[str, Any] = None

    def to_json(self) -> str:
        d = asdict(self)
        return json.dumps(d, ensure_ascii=False, indent=2)

    @staticmethod
    def from_json(s: str) -> "PriceTemplate":
        data = json.loads(s)
        return PriceTemplate(**data)


# ---------- Template Persistence ----------

def list_templates() -> List[str]:
    ensure_dir(TEMPLATE_DIR)
    try:
        files = [f for f in os.listdir(TEMPLATE_DIR) if f.endswith(".json")]
        return sorted(files)
    except Exception:
        return []


def save_template(t: PriceTemplate) -> str:
    ensure_dir(TEMPLATE_DIR)
    fname = re.sub(r"[^\w\-]+", "_", t.supplier.strip()) or "template"
    # allow multiple versions per supplier by sheet name
    suffix = f"__{re.sub(r'[^\w\-]+','_', (t.sheet or 'any'))}"
    path = os.path.join(TEMPLATE_DIR, f"{fname}{suffix}.json")
    with open(path, "w", encoding="utf-8") as f:
        f.write(t.to_json())
    return path


def load_template(path_or_name: str) -> PriceTemplate:
    path = path_or_name
    if not os.path.exists(path):
        path = os.path.join(TEMPLATE_DIR, path_or_name)
    with open(path, "r", encoding="utf-8") as f:
        return PriceTemplate.from_json(f.read())


# ---------- Parsing Helpers ----------

def try_read_excel(uploaded_file, sheet_name: Optional[str] = None, header_row: int = 0,
                   skip_top: int = 0, skip_bottom: int = 0) -> pd.DataFrame:
    # we read without header first to allow user to choose header row robustly
    xls = pd.ExcelFile(uploaded_file)
    target_sheet = sheet_name or xls.sheet_names[0]
    # read all rows then manually set header
    df_raw = xls.parse(target_sheet, header=None, dtype=str)

    # apply top/bottom trimming visually
    if skip_top:
        df_raw = df_raw.iloc[skip_top:]
    if skip_bottom:
        df_raw = df_raw.iloc[:-skip_bottom] if skip_bottom < len(df_raw) else df_raw.iloc[:0]

    # set header row AFTER trimming
    if header_row >= len(df_raw):
        header_row = 0
    headers = df_raw.iloc[header_row].astype(str).fillna("")
    df = df_raw.iloc[header_row + 1 :].copy()
    df.columns = headers
    df = df.reset_index(drop=True)
    # drop completely empty columns
    df = df.dropna(axis=1, how="all")
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
    # replace comma decimal, strip spaces
    s = s.replace(" ", "").replace("\u00a0", "").replace(",", ".")
    # remove non number / dot / minus
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
    out = pd.DataFrame()
    if t.name_col and t.name_col in df.columns:
        out["name"] = df[t.name_col].astype(str)
    else:
        out["name"] = np.nan
    out["size"] = df.apply(lambda r: extract_size(r, t), axis=1)

    if t.price_col and t.price_col in df.columns:
        prices = df[t.price_col].apply(lambda v: normalize_price(v, t.price_regex))
    else:
        prices = pd.Series([None] * len(df))
    if t.price_multiplier and t.price_multiplier != 1:
        prices = prices.apply(lambda x: x * t.price_multiplier if x is not None else x)
    out["price"] = prices

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
    if t.drop_na_price:
        out = out[~out["price"].isna()]
    if t.filters:
        f = t.filters
        if isinstance(f, dict):
            if "price_min" in f:
                out = out[(out["price"].isna()) | (out["price"] >= float(f["price_min"]))]
            if "price_max" in f:
                out = out[(out["price"].isna()) | (out["price"] <= float(f["price_max"]))]
            if txt := f.get("include_text"):
                out = out[out["name"].astype(str).str.contains(str(txt), case=False, na=False)]
            if excl := f.get("exclude_text"):
                out = out[~out["name"].astype(str).str.contains(str(excl), case=False, na=False)]

    # clean index
    out = out.reset_index(drop=True)
    return out


# ---------- UI ----------

st.set_page_config(page_title="Прайс-парсер с шаблонами", layout="wide")
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
    # read basic info
    uploaded_bytes = uploaded.getvalue()
    fp = file_fingerprint(uploaded_bytes)
    xls = pd.ExcelFile(io.BytesIO(uploaded_bytes))
    st.success(f"Файл загружен ✅ | Пальчик: {fp} | Листы: {', '.join(xls.sheet_names)}")

    # 2) Sheet + header row + trimming
    st.subheader("🧾 Подготовка листа")
    colA, colB, colC, colD = st.columns([2,1,1,1])
    with colA:
        sheet = st.selectbox("Лист", options=xls.sheet_names)
    with colB:
        skip_top = st.number_input("Пропустить верхних строк", min_value=0, max_value=500, value=0)
    with colC:
        skip_bottom = st.number_input("Пропустить нижних строк", min_value=0, max_value=500, value=0)
    with colD:
        header_row = st.number_input("Строка заголовка (после пропуска)", min_value=0, max_value=100, value=0,
                                     help="Нулевая = первая видимая строка после пропуска")

    df_preview = try_read_excel(io.BytesIO(uploaded_bytes), sheet_name=sheet, header_row=header_row,
                                skip_top=skip_top, skip_bottom=skip_bottom)

    st.caption("Предпросмотр таблицы после выбора строки заголовка и обрезки:")
    st.dataframe(df_preview.head(50), use_container_width=True)

    # 3) Template config
    st.subheader("🧩 Настройка шаблона")

    # Auto-suggest supplier name from filename
    default_supplier = re.sub(r"\.[^.]+$", "", uploaded.name)
    default_supplier = re.sub(r"[^\w\-]+", " ", default_supplier).strip()

    with st.expander("Загрузка/Сохранение шаблонов", expanded=True):
        c1, c2, c3 = st.columns([1,1,2])
        with c1:
            supplier = st.text_input("Поставщик (имя шаблона)", value=default_supplier or "Поставщик")
        with c2:
            # discover local templates
            available = ["— выбрать —"] + list_templates()
            chosen_name = st.selectbox("Загрузить локальный шаблон", options=available)
            if chosen_name != "— выбрать —":
                try:
                    t_loaded = load_template(chosen_name)
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
    t: PriceTemplate = ss.active_template or PriceTemplate(supplier=supplier, sheet=sheet, header_row=header_row,
                                                          skip_top_rows=skip_top, skip_bottom_rows=skip_bottom,
                                                          filters={})

    # Refresh template sheet/header if changed
    t.supplier = supplier
    t.sheet = sheet
    t.header_row = int(header_row)
    t.skip_top_rows = int(skip_top)
    t.skip_bottom_rows = int(skip_bottom)

    st.markdown("**Соответствие столбцов:**")
    cols = list(df_preview.columns)
    ca, cb, cc = st.columns(3)
    with ca:
        t.name_col = st.selectbox("Наименование", options=["— нет —"] + cols, index=(cols.index(t.name_col) + 1) if t.name_col in cols else 0)
        t.size_col = st.selectbox("Размер (если есть)", options=["— нет —"] + cols, index=(cols.index(t.size_col) + 1) if t.size_col in cols else 0)
        t.sku_col = st.selectbox("Артикул (если есть)", options=["— нет —"] + cols, index=(cols.index(t.sku_col) + 1) if t.sku_col in cols else 0)
    with cb:
        t.price_col = st.selectbox("Цена", options=["— нет —"] + cols, index=(cols.index(t.price_col) + 1) if t.price_col in cols else 0)
        t.currency_col = st.selectbox("Валюта (если есть)", options=["— нет —"] + cols, index=(cols.index(t.currency_col) + 1) if t.currency_col in cols else 0)
        t.qty_col = st.selectbox("Кол-во (если есть)", options=["— нет —"] + cols, index=(cols.index(t.qty_col) + 1) if t.qty_col in cols else 0)
    with cc:
        t.uom_col = st.selectbox("Ед.изм. (если есть)", options=["— нет —"] + cols, index=(cols.index(t.uom_col) + 1) if t.uom_col in cols else 0)
        t.price_regex = st.text_input("Regex для цены (опционально)", value=t.price_regex or "([0-9]+[.,]?[0-9]*)",
                                      help="Если в ячейке есть текст с валютой — вытащим число по первой группе")
        t.price_multiplier = float(st.number_input("Множитель цены", value=float(t.price_multiplier or 1.0), step=0.1,
                                                  help="Напр.: 0.01 если цена указана за 100 шт."))

    st.markdown("**Извлечение размера по шаблону (опционально):**")
    cd, ce = st.columns([2,3])
    with cd:
        src_choice = st.selectbox("Извлекать размер из", options=["— не извлекать —", "name_col", "size_col"],
                                  index={None:0, "name_col":1, "size_col":2}.get(t.regex_extract_size_from or None, 0))
        t.regex_extract_size_from = None if src_choice == "— не извлекать —" else src_choice
    with ce:
        t.size_regex = st.text_input("Regex для размера", value=t.size_regex or r"(\d+\s?[xX]\s?\d+)|(\d+(?:[.,]\d+)?)\s?(mm|cm|m)\b",
                                     help="Например: 10x20, 10 x 20, или 12 mm")

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
    # handle "— нет —" selections
    for fld in ["name_col","size_col","price_col","currency_col","sku_col","qty_col","uom_col"]:
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
                path = save_template(t)
                st.success(f"Шаблон сохранён: {path}")
                ss.active_template = t
            except Exception as e:
                st.error(f"Ошибка сохранения: {e}")
    with csb:
        st.download_button("⬇️ Скачать шаблон JSON", data=t.to_json().encode("utf-8"),
                           file_name=f"template_{re.sub(r'[^\w\-]+','_', t.supplier)}.json",
                           mime="application/json")
    with csc:
        # Export CSV
        csv_bytes = standardized.to_csv(index=False).encode("utf-8-sig")
        st.download_button("⬇️ Экспорт CSV", data=csv_bytes, file_name=f"{re.sub(r'[^\w\-]+','_', t.supplier)}_export.csv",
                           mime="text/csv")
    with csd:
        # Export Excel
        xbuf = io.BytesIO()
        with pd.ExcelWriter(xbuf, engine="openpyxl") as writer:
            standardized.to_excel(writer, index=False, sheet_name="export")
        st.download_button("⬇️ Экспорт Excel", data=xbuf.getvalue(), file_name=f"{re.sub(r'[^\w\-]+','_', t.supplier)}_export.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    st.divider()
    st.markdown("#### 🤖 Автоприменение шаблона")
    st.caption("При повторной загрузке прайса поставщика вы можете сразу загрузить его шаблон из списка или импортировать JSON."
               " Если имя листа и структура совпадают — данные подтянутся автоматически.")

else:
    st.info("Загрузите Excel-файл поставщика, чтобы начать.")
