import os
import re
import logging
from typing import List, Optional, Tuple
import pandas as pd
import pdfplumber
import camelot
import hashlib
from sentence_transformers import SentenceTransformer, util
import torch

# --- model init ---
model = SentenceTransformer(r"C:/Users/cb1152/OneDrive - Eide Bailly LLP/Desktop/California SCO/venv/Lib/site-packages/sentence_transformers/all-MiniLM-L6-v2")

# ========== CONFIG ==========
PDF_DIR = r"C:/Users/cb1152/OneDrive - Eide Bailly LLP/Current Projects/State of California/Comp Abs/Downloads/Detail of Apportionment PDFs/PY Recreate"
OUTPUT_XLSX = os.path.join(PDF_DIR, "all_pdfs_extracted.xlsx")
CAMELot_FLAVORS = [("stream", {"edge_tol": 500}), ("stream", {"edge_tol": 200}), ("lattice", {})]
YEARS_TO_FIND = ["2023-24", "2024-25", "2025-26"]
HEADER_GROUP_TOL = 18
VALUE_X_TOL = 60
X_MARGIN = 10
Y_PADDING_TOP = 6
Y_PADDING_BOTTOM = 6
MAX_SHEETNAME_LEN = 31
# ============================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ---------- helpers ----------
def token_to_regex(tok: str) -> re.Pattern:
    a, b = tok.split('-')
    return re.compile(rf"{re.escape(a)}\D*{re.escape(b)}\D*$")

def clean_text(x) -> str:
    return "" if pd.isna(x) else str(x).strip().replace("\u00A0", " ")

def semantic_similarity(a: str, b: str) -> float:
    emb_a = model.encode(a, convert_to_tensor=True, normalize_embeddings=True)
    emb_b = model.encode(b, convert_to_tensor=True, normalize_embeddings=True)
    return float(util.cos_sim(emb_a, emb_b).item())

def is_semantic_duplicate(df: pd.DataFrame, seen_texts: List[str], threshold: float = 0.95) -> bool:
    text = df.astype(str).apply(lambda s: " ".join(s), axis=1).str.cat(sep=" ")
    for prev_text in seen_texts:
        if semantic_similarity(text, prev_text) > threshold:
            return True
    seen_texts.append(text)
    return False

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.map(clean_text)
    df = df.loc[~df.eq("").all(axis=1), ~df.eq("").all(axis=0)]  # drop empty rows/cols
    return df.reset_index(drop=True)

def safe_sheet_name(name: str) -> str:
    return re.sub(r'[:\\/?*\[\]]', '_', name)[:MAX_SHEETNAME_LEN]

def is_incomplete(df: pd.DataFrame, min_rows=2, min_cols=2) -> bool:
    return df.shape[0] < min_rows or df.shape[1] < min_cols or (df != "").sum().sum() < min_rows

def hash_df(df: pd.DataFrame) -> str:
    """Create a simple hash fingerprint for a DataFrame's content."""
    text = df.astype(str).apply(lambda s: "|".join(s), axis=1).str.cat(sep="\n")
    return hashlib.md5(text.encode()).hexdigest()

def is_duplicate(df: pd.DataFrame, seen_hashes: set) -> bool:
    """Check if DataFrame has already been seen based on text hash."""
    h = hash_df(df)
    if h in seen_hashes:
        return True
    seen_hashes.add(h)
    return False

def is_junk_table(df: pd.DataFrame) -> bool:
    """Heuristic filter to drop numeric-only or duplicate total tables."""
    if df.empty:
        return True

    # Count non-empty values in first column
    first_col = df.iloc[:, 0].astype(str).str.strip()
    non_empty = first_col[first_col != ""]
    
    # If first column is mostly empty or very short, reject
    if len(non_empty) <= 2 or len(non_empty) < 0.2 * len(df):
        return True

    # Flatten all text and check if it has any alphabetic characters
    text = " ".join(df.astype(str).agg(" ".join, axis=1))
    if not re.search(r"[A-Za-z]", text):
        return True

    # If the table is almost identical to the previous totals pattern
    if df.astype(str).apply(lambda s: s.str.contains(r"\$\d")).sum().sum() > 0.8 * df.size:
        return True

    return False


def find_year_headers(page, target_years: List[str]) -> List[dict]:
    words = page.extract_words()
    patterns = [token_to_regex(y) for y in target_years]
    return [
        {**{k: float(w[k]) for k in ("x0", "x1", "top", "bottom")}, "text": w["text"].strip()}
        for w in words if any(p.search(w["text"].strip()) for p in patterns)
    ]

def group_by_y(headers, tol=12):
    """Merge header fragments that are within tolerance."""
    headers = sorted(headers, key=lambda h: h["top"])
    groups = []
    current = [headers[0]]

    for h in headers[1:]:
        if abs(h["top"] - current[-1]["top"]) <= tol:
            current.append(h)
        else:
            groups.append(current)
            current = [h]
    groups.append(current)
    return groups

def find_numeric_words(page) -> List[dict]:
    pat = re.compile(r"^\$?-?\d[\d,]*(\.\d{2})?$|^-$")
    return [
        {**{k: float(w[k]) for k in ("x0", "x1", "top", "bottom")}, "text": w["text"]}
        for w in page.extract_words() if pat.match(w["text"].replace(",", ""))
    ]

def pdf_coords(height: float, top: float, bottom: float) -> Tuple[float, float]:
    y1, y2 = height - top, height - bottom
    return max(y1, y2), min(y1, y2)

def unify_candidates(candidates: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if not candidates: return None
    score = lambda df: (df.notna() & df.ne("")).sum().sum() + df.shape[1] * 2
    valid = [(score(clean_dataframe(df)), clean_dataframe(df)) for df in candidates if not df.empty]
    if not valid: return None
    valid.sort(key=lambda x: x[0], reverse=True)
    best_cols = valid[0][1].shape[1]
    merged = pd.concat([df for _, df in valid if df.shape[1] == best_cols], ignore_index=True)
    return clean_dataframe(merged)

def camelot_candidates(pdf_path, page_str, table_areas=None, columns=None) -> List[pd.DataFrame]:
    candidates = []
    for flavor, params in CAMELot_FLAVORS:
        try:
            tables = camelot.read_pdf(pdf_path, pages=page_str, flavor=flavor, strip_text="\n",
                                      table_areas=table_areas, columns=[columns] if columns else None, **params)
            candidates += [t.df for t in tables] if tables.n > 0 else []
        except Exception as e:
            logger.debug(f"Camelot {flavor} failed on page {page_str}: {e}")
    return candidates

# ---------- main extraction ----------
def extract_tables(pdf_path: str) -> List[pd.DataFrame]:
    logger.info(f"Opening {os.path.basename(pdf_path)}")
    with pdfplumber.open(pdf_path) as pdf:
        file_dfs, last_df = [], None

        for page_num, page in enumerate(pdf.pages, 1):
            logger.info(f"  → Page {page_num}/{len(pdf.pages)}")
            headers = group_by_y(find_year_headers(page, YEARS_TO_FIND))
            nums = find_numeric_words(page)

            seen_hashes = set()
            seen_texts = []  # NEW: semantic duplicates tracking

            for gi, group in enumerate(headers, 1):
                try:
                    hdr_top, hdr_bottom = min(h["top"] for h in group), max(h["bottom"] for h in group)
                    next_top = min((g[0]["top"] for g in headers if g[0]["top"] > hdr_top), default=page.height)
                    matched = [
                        (v["x0"] if abs(v["x0"] - h["x0"]) <= VALUE_X_TOL else (h["x0"] + h["x1"]) / 2)
                        for h in group if (v := min((vv for vv in nums if vv["top"] > hdr_bottom),
                                                    key=lambda vv: abs(vv["x0"] - h["x0"]), default=None))
                    ]
                    if not matched: continue

                    cols = sorted(set(round(float(x), 2) for x in matched))
                    left, right = max(0, min(cols) - X_MARGIN), min(page.width, max(cols) + X_MARGIN)
                    top_pdf, bottom_pdf = pdf_coords(page.height, hdr_top - Y_PADDING_TOP, next_top - Y_PADDING_BOTTOM)
                    area = [f"{left:.2f},{top_pdf:.2f},{right:.2f},{bottom_pdf:.2f}"]
                    cols_str = ",".join(f"{c:.2f}" for c in cols)

                    candidates = camelot_candidates(pdf_path, str(page_num), area, cols_str) or \
                                 camelot_candidates(pdf_path, str(page_num)) or \
                                 [pd.DataFrame(page.extract_table() or [])]

                    df = unify_candidates([c for c in candidates if c is not None])
                    if df is None or is_incomplete(df) or is_junk_table(df) or is_duplicate(df, seen_hashes):
                        continue

                    if is_semantic_duplicate(df, seen_texts):
                        logger.info(f"    Group {gi}: skipped semantically duplicate table")
                        continue

                    # Skip repeated fragment (simple structure check)
                    if last_df is not None and df.equals(last_df):
                        continue

                    df.columns = range(df.shape[1])
                    file_dfs += [df, pd.DataFrame([[""] * df.shape[1]])]
                    last_df = df
                    logger.info(f"    Group {gi}: appended table shape {df.shape}")

                except Exception as e:
                    logger.exception(f"    Error processing group {gi} on page {page_num}: {e}")
    return file_dfs

# ---------- main runner ----------
def main():
    pdf_files = [os.path.join(PDF_DIR, f) for f in os.listdir(PDF_DIR) if f.lower().endswith(".pdf")]
    logger.info(f"Found {len(pdf_files)} PDFs to process.")
    with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as writer:
        for pdf_path in pdf_files:
            pdf_name = os.path.basename(pdf_path)
            logger.info(f"\nProcessing {pdf_name}")
            dfs = extract_tables(pdf_path)

            if not dfs:
                logger.warning(f"No tables found for {pdf_name}")
                continue

            max_cols = max(df.shape[1] for df in dfs)
            dfs = [df.reindex(columns=range(max_cols), fill_value="").astype(str) for df in dfs]
            final = pd.concat(dfs, ignore_index=True)
            sheet = safe_sheet_name(os.path.splitext(pdf_name)[0])
            final.to_excel(writer, sheet_name=sheet, index=False, header=False)
            ws = writer.sheets[sheet]

            try:
                d6 = clean_text(final.iat[5, 3]) if final.shape >= (6, 4) else ""
                if not d6:
                    ws.set_tab_color("red")
                    logger.info(f"  D6 empty → marked '{sheet}' red")
                else:
                    logger.info(f"  D6: {d6}")
            except Exception:
                ws.set_tab_color("red")
                logger.info(f"  D6 check failed → marked '{sheet}' red")

    logger.info(f"\n✅ Extraction complete: {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()


"""
import os
import pdfplumber
import pandas as pd
import camelot

# Define paths
#pdf_dir = "C:/Users/cb1152/OneDrive - Eide Bailly LLP/Current Projects/State of California/Comp Abs/Downloads/Detail of Apportionment PDFs/PY Recreate"
pdf_dir = "C:/Users/cb1152/Downloads/RJ"
output_excel = "C:/Users/cb1152/Downloads/output_all1.xlsx"

# Initialize Excel writer
with pd.ExcelWriter(output_excel, engine='xlsxwriter') as writer:
    
    workbook = writer.book  # Get workbook object for formatting
    
    # Loop through all PDFs in the directory
    for pdf_file in os.listdir(pdf_dir):
        if pdf_file.lower().endswith(".pdf"):
            pdf_path = os.path.join(pdf_dir, pdf_file)
            print(f"Processing: {pdf_file}")
            
            # Get total page count
            with pdfplumber.open(pdf_path) as pdf:
                total_pages = len(pdf.pages)
            
            df_list = []
            
            # Extract tables from each page
            for page_num in range(1, total_pages + 1):  
                page_str = str(page_num)
                
                # Try extracting tables using stream flavor first
                tables_stream = camelot.read_pdf(pdf_path, pages=page_str, flavor="stream", edge_tol=500)
                
                # If no tables found, try lattice
                if tables_stream.n == 0:
                    tables_lattice = camelot.read_pdf(pdf_path, pages=page_str, flavor="lattice")
                    tables = tables_lattice if tables_lattice.n > 0 else None
                else:
                    tables = tables_stream
                
                # Process extracted tables
                if tables:
                    print(f"  Page {page_num}: {tables.n} table(s) found.")
                    df_list.extend([table.df for table in tables])
                    
                    # Add an empty row to separate pages
                    df_list.append(pd.DataFrame([[""] * df_list[-1].shape[1]]))  
                else:
                    print(f"  Page {page_num}: No tables found.")
            
            # Save extracted data to a new sheet in the Excel file
            if df_list:
                final_df = pd.concat(df_list, ignore_index=True)
                sheet_name = os.path.splitext(pdf_file)[0][:31]  # Excel sheet names max length = 31
                final_df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)

                # Check if D6 is empty/null
                try:
                    d6_value = final_df.iloc[5, 3] if final_df.shape[0] > 5 and final_df.shape[1] > 3 else None
                except IndexError:
                    d6_value = None  # Handle case where D6 doesn't exist

                # If D6 is null, set tab color to red
                if pd.isna(d6_value) or d6_value == "":
                    worksheet = writer.sheets[sheet_name]
                    worksheet.set_tab_color('red')

                print(f"  Data saved to sheet: {sheet_name}")
            else:
                print(f"  No tables extracted from {pdf_file}.")
    
    print(f"All PDFs processed. Extracted data saved to {output_excel}")


"""





#***************************************************************************************************
"""
import os
import re
import logging
from typing import List, Optional, Tuple
import pandas as pd
import pdfplumber
import camelot

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ========== CONFIG ==========
PDF_DIR = r"C:/Users/cb1152/OneDrive - Eide Bailly LLP/Current Projects/State of California/Comp Abs/Downloads/Detail of Apportionment PDFs/PY Recreate"
OUTPUT_XLSX = r"C:/Users/cb1152/Downloads/output_all2026.xlsx"
CAMELot_FLAVORS = [
    ("stream", {"edge_tol": 500}),
    ("stream", {"edge_tol": 200}),
    ("lattice", {}),
]
YEARS_TO_FIND = ["2023-24", "2024-25", "2025-26"]  # canonical tokens (used for fuzzy regex)
X_MARGIN = 10        # horizontal padding (points)
Y_PADDING_TOP = 6    # vertical padding above header row (points)
Y_PADDING_BOTTOM = 6
HEADER_GROUP_TOL = 18  # px tolerance to group headers on same row
VALUE_X_TOL = 60       # px tolerance when matching header -> numeric value
MIN_NON_EMPTY_CELLS = 4
MAX_SHEETNAME_LEN = 31
# ============================

def sanitize_cell(x):
    if pd.isna(x):
        return ""
    return str(x).strip().replace("\u00A0", " ")

def is_valid_pdf(file_path: str) -> bool:
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) < 1024:
            return False
        with open(file_path, "rb") as f:
            header = f.read(5)
            if not header.startswith(b"%PDF"):
                return False
        return True
    except Exception:
        return False

# ---------- helpers ----------
def token_to_regex(tok: str) -> re.Pattern:
    a, b = tok.split('-')
    return re.compile(rf"{re.escape(a)}\D*{re.escape(b)}\D*$")

def find_year_headers_on_page(page: pdfplumber.page.Page, target_years: List[str]) -> List[dict]:
    #Return list of header word dicts (each has x0,x1,top,bottom,text) matching any target_year fuzzy regex.
    words = page.extract_words()
    patterns = [token_to_regex(tok) for tok in target_years]
    headers = []
    for w in words:
        text = w["text"].strip()
        if any(p.search(text) for p in patterns):
            headers.append({"x0": float(w["x0"]), "x1": float(w["x1"]),
                            "top": float(w["top"]), "bottom": float(w["bottom"]),
                            "text": text})
    return headers

def to_pdf_coords(page_height: float, top: float, bottom: float) -> Tuple[float, float]:
    y_top_pdf = page_height - top
    y_bottom_pdf = page_height - bottom
    if y_top_pdf < y_bottom_pdf:
        y_top_pdf, y_bottom_pdf = y_bottom_pdf, y_top_pdf
    return y_top_pdf, y_bottom_pdf

def group_by_vertical_position(items: List[dict], tolerance: float = HEADER_GROUP_TOL) -> List[List[dict]]:
    #Group items with similar 'top' into header rows.
    if not items:
        return []
    items_sorted = sorted(items, key=lambda w: w["top"])
    groups = []
    current = [items_sorted[0]]
    for w in items_sorted[1:]:
        if abs(w["top"] - current[-1]["top"]) <= tolerance:
            current.append(w)
        else:
            groups.append(current)
            current = [w]
    groups.append(current)
    return groups

# ---------- Camelot candidate extraction ----------
def candidate_tables_from_camelot(pdf_path: str, page_str: str, table_areas: Optional[List[str]] = None, columns: Optional[str] = None) -> List[pd.DataFrame]:
    candidates = []
    for flavor, params in CAMELot_FLAVORS:
        try:
            kwargs = dict(params)
            if table_areas:
                kwargs["table_areas"] = table_areas
            if columns:
                # pass columns as string (Camelot accepts either)
                kwargs["columns"] = columns
            logger.debug(f" camelot: flavor={flavor} params={kwargs} pages={page_str}")
            tables = camelot.read_pdf(pdf_path, pages=page_str, flavor=flavor, **{k: v for k, v in kwargs.items()})
            if tables and tables.n > 0:
                for t in tables:
                    candidates.append(t.df)
        except Exception as e:
            logger.warning(f"Camelot {flavor} failed on {page_str} (table_areas={table_areas}): {e}")
    return candidates

def candidate_from_pdfplumber(pdf_path: str, page_number: int) -> Optional[pd.DataFrame]:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[page_number - 1]
            table = page.extract_table()
            if table:
                return pd.DataFrame(table)
            text = page.extract_text()
            if text:
                lines = [ln for ln in text.splitlines() if ln.strip()]
                parsed = []
                for ln in lines:
                    cols = re.split(r'\s{2,}', ln.strip())
                    if len(cols) > 1:
                        parsed.append(cols)
                if parsed:
                    maxc = max(len(r) for r in parsed)
                    parsed_norm = [r + [""]*(maxc - len(r)) for r in parsed]
                    return pd.DataFrame(parsed_norm)
    except Exception as e:
        logger.warning(f"pdfplumber extract failed on page {page_number}: {e}")
    return None

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.map(lambda x: "" if pd.isna(x) else str(x).strip())
    if df.shape[1] == 1:
        new_rows = []
        for _, row in df.iterrows():
            cell = row.iloc[0]
            parts = re.split(r'\s{2,}|\s(?=\$?\d)', cell)
            new_rows.append(parts)
        max_len = max(len(r) for r in new_rows)
        df = pd.DataFrame([r + [""]*(max_len - len(r)) for r in new_rows])
    mask_row = ~(df.apply(lambda r: all(c == "" for c in r), axis=1))
    df = df[mask_row].reset_index(drop=True)
    mask_col = ~(df.apply(lambda c: all(c == ""), axis=0))
    return df.loc[:, mask_col]

def score_table(df: pd.DataFrame) -> int:
    if df is None or df.shape[0] == 0 or df.shape[1] == 0:
        return 0
    df_clean = df.map(lambda x: "" if pd.isna(x) else str(x).strip())
    non_empty_cells = (df_clean != "").sum().sum()
    return non_empty_cells + df.shape[1] * 2

def unify_candidate_tables(candidates: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if not candidates:
        return None
    scored = [(score_table(df), idx, df) for idx, df in enumerate(candidates)]
    scored.sort(reverse=True, key=lambda x: x[0])
    top_score, top_idx, top_df = scored[0]
    top_cols = top_df.shape[1]
    consistent = [df for _, _, df in scored if df.shape[1] == top_cols and score_table(df) > 0]
    if len(consistent) > 1:
        concat_df = pd.concat(consistent, ignore_index=True)
        return clean_dataframe(concat_df)
    else:
        return clean_dataframe(top_df)

def pad_to_same_width(df_list: List[pd.DataFrame]) -> List[pd.DataFrame]:
    if not df_list:
        return df_list
    max_cols = max(df.shape[1] for df in df_list)
    padded = []
    for df in df_list:
        if df.shape[1] < max_cols:
            extra = pd.DataFrame([[""]*(max_cols-df.shape[1])] * df.shape[0])
            df2 = pd.concat([df.reset_index(drop=True), extra.reset_index(drop=True)], axis=1)
            padded.append(df2)
        else:
            padded.append(df)
    return padded

def safe_sheet_name(name: str) -> str:
    s = re.sub(r'[:\\/?*\[\]]', '_', name)
    return s[:MAX_SHEETNAME_LEN]

# ---------- enhanced extraction using header groups ----------
def find_numeric_values_on_page(page: pdfplumber.page.Page) -> List[dict]:
    #Return list of numeric-like words ($n, n, or '-')
    words = page.extract_words()
    num_pat = re.compile(r"^\$?-?\d[\d,]*(\.\d{2})?$|^-$")
    numeric = [ {"x0": float(w["x0"]), "x1": float(w["x1"]), "top": float(w["top"]),
                  "bottom": float(w["bottom"]), "text": w["text"]} for w in words if num_pat.match(w["text"].replace(",", ""))]
    return numeric

def extract_tables_from_pdf(pdf_path: str) -> List[pd.DataFrame]:
    logger.info(f"Opening {os.path.basename(pdf_path)}")
    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
    file_dfs: List[pd.DataFrame] = []

    for page_num in range(1, total_pages + 1):
        page_str = str(page_num)
        logger.info(f" Processing page {page_num}/{total_pages}")

        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[page_num - 1]
            # find header tokens (all occurrences)
            headers = find_year_headers_on_page(page, YEARS_TO_FIND)

            # group headers by vertical position -> header rows (each group indicates a separate table structure)
            header_groups = group_by_vertical_position(headers, tolerance=HEADER_GROUP_TOL)

            # find numeric candidates on this page
            numeric_values = find_numeric_values_on_page(page)

            # For each header group, attempt to infer columns & table_area and extract
            for gi, group in enumerate(header_groups, start=1):
                try:
                    # header group's vertical bounds
                    hdr_top = min(h["top"] for h in group)
                    hdr_bottom = max(h["bottom"] for h in group)
                    # For vertical extent of this table, try to find next header group's top as bottom bound
                    next_group_tops = [g[0]["top"] for g in header_groups if g[0]["top"] > hdr_top]
                    next_top = min(next_group_tops) if next_group_tops else page.height
                    # find matching numeric column x0 positions for each header in this group
                    matched_positions = []
                    for h in group:
                        # candidates below the header
                        below = [v for v in numeric_values if v["top"] > hdr_bottom]
                        if not below:
                            continue
                        # choose the closest by X distance
                        closest = min(below, key=lambda v: abs(v["x0"] - h["x0"]))
                        # if it's within tolerance, use numeric x0, else fallback to header center
                        if abs(closest["x0"] - h["x0"]) <= VALUE_X_TOL:
                            matched_positions.append(closest["x0"])
                        else:
                            matched_positions.append((h["x0"] + h["x1"]) / 2.0)

                    if not matched_positions:
                        logger.info(f"  Page {page_num} group {gi}: no matched numeric positions—skipping group.")
                        continue

                    # build columns (use unique sorted x positions)
                    cols_sorted = sorted(list(dict.fromkeys([round(float(x), 2) for x in matched_positions])))
                    cols_str = ",".join([f"{c:.2f}" for c in cols_sorted])

                    # determine left/right/top/bottom for table_area in PDF coords
                    left = max(0.0, min(cols_sorted) - X_MARGIN)
                    right = min(page.width, max(cols_sorted) + X_MARGIN)
                    # top: take header top minus padding; bottom: either next_top or page height minus some margin
                    top_pdf, bottom_pdf = to_pdf_coords(page.height, hdr_top - Y_PADDING_TOP, next_top - Y_PADDING_BOTTOM)

                    table_area = [f"{left:.2f},{top_pdf:.2f},{right:.2f},{bottom_pdf:.2f}"]

                    logger.info(f"  Page {page_num} group {gi}: headers={[h['text'] for h in group]}")
                    logger.info(f"    inferred columns={cols_sorted}")
                    logger.info(f"    inferred table_area={table_area}")

                    # Try Camelot extraction for this specific group (will iterate flavors inside)
                    candidates = candidate_tables_from_camelot(pdf_path, page_str, table_areas=table_area, columns=cols_str)

                    # Fallbacks: if nothing found, try without guidance and finally pdfplumber
                    if not candidates:
                        logger.info(f"    Group {gi}: header-guided extraction returned nothing; trying unguided flavors...")
                        candidates = candidate_tables_from_camelot(pdf_path, page_str, table_areas=None, columns=None)

                    if not candidates:
                        p_df = candidate_from_pdfplumber(pdf_path, page_num)
                        if p_df is not None:
                            candidates.append(p_df)

                    final_df = unify_candidate_tables(candidates)
                    if final_df is not None and final_df.shape[0] > 0:
                        # normalize columns to simple integer names to avoid duplicate-name concat issues later
                        final_df.columns = list(range(final_df.shape[1]))
                        file_dfs.append(final_df)
                        # add separator row
                        file_dfs.append(pd.DataFrame([[""] * final_df.shape[1]]))
                        logger.info(f"    Group {gi}: extracted table shape {final_df.shape}")
                    else:
                        logger.info(f"    Group {gi}: no valid table after unification (candidates={len(candidates)})")
                except Exception as e:
                    logger.exception(f"    Error processing page {page_num} group {gi}: {e}")

        # If we didn't find any header groups or didn't extract anything, fall back to the original per-page pipeline
        if not header_groups or not any(isinstance(df, pd.DataFrame) for df in file_dfs):
            logger.info("  No header groups or no group extractions — trying page-level extraction fallbacks.")
            candidates = []
            # try unguided Camelot flavors
            candidates.extend(candidate_tables_from_camelot(pdf_path, page_str, table_areas=None, columns=None))
            # try pdfplumber fallback
            if not candidates:
                p_df = candidate_from_pdfplumber(pdf_path, page_num)
                if p_df is not None:
                    candidates.append(p_df)
            final_df = unify_candidate_tables(candidates)
            if final_df is not None and final_df.shape[0] > 0:
                final_df.columns = list(range(final_df.shape[1]))
                file_dfs.append(final_df)
                file_dfs.append(pd.DataFrame([[""] * final_df.shape[1]]))
                logger.info(f"  Page-level fallback: extracted table shape {final_df.shape}")

    return file_dfs

# ---------- main (writes excel) ----------
def main():
    with pd.ExcelWriter(OUTPUT_XLSX, engine='xlsxwriter') as writer:
        workbook = writer.book
        for pdf_file in sorted(os.listdir(PDF_DIR)):
            if not pdf_file.lower().endswith(".pdf"):
                continue
            pdf_path = os.path.join(PDF_DIR, pdf_file)
            logger.info(f"Processing file: {pdf_file}")

            if not is_valid_pdf(pdf_path):
                logger.warning(f"Skipping invalid or empty PDF: {pdf_file}")
                continue

            try:
                dfs = extract_tables_from_pdf(pdf_path)
                if not dfs:
                    logger.warning(f"No tables extracted from {pdf_file}")
                    continue
                # pad to same width, then concat into one sheet per PDF
                dfs = pad_to_same_width(dfs)
                final_df = pd.concat(dfs, ignore_index=True)
                # write sheet
                sheet_name = safe_sheet_name(os.path.splitext(pdf_file)[0])
                final_df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
                worksheet = writer.sheets[sheet_name]

                # Robust D6 check
                try:
                    d6_value = None
                    if final_df.shape[0] > 5 and final_df.shape[1] > 3:
                        raw = final_df.iat[5, 3]
                        d6_value = sanitize_cell(raw)
                except Exception:
                    d6_value = None

                if not d6_value:
                    worksheet.set_tab_color('red')
                    logger.info(f"  Marked sheet '{sheet_name}' red because D6 empty.")
                else:
                    logger.info(f"  D6 value: {d6_value[:50] if d6_value else ''}")

            except Exception as e:
                logger.exception(f"Failed processing {pdf_file}: {e}")

    logger.info("All done.")

if __name__ == "__main__":
    main()

"""