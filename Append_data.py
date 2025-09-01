#!/usr/bin/env python3
"""
Append mixed Excel/data files in the current folder, optionally remove duplicates by a chosen column,
and save as .xlsx or .csv with safe timestamped filenames.

Supported input formats (auto-detected by extension):
  - .xlsx, .xlsm (engine: openpyxl)
  - .xls          (engine: xlrd==1.2.0 required)
  - .xlsb         (engine: pyxlsb)
  - .ods          (engine: odf)
  - .csv, .tsv, .txt (delimited; delimiter auto-detected: , ; \t |)

The window will stay open until you press a key (even on errors), which is ideal when double-clicking.
"""

from pathlib import Path
from datetime import datetime
import sys
import os
import time
import traceback
import pandas as pd
import csv

# ------------------ Helper Utilities ------------------ #

def timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def clean_column_names(cols):
    """Strip whitespace and deduplicate duplicate column names by suffixing .1, .2, ..."""
    seen = {}
    out = []
    for c in cols:
        name = str(c).strip()
        if name in seen:
            seen[name] += 1
            name = f"{name}.{seen[name]}"
        else:
            seen[name] = 0
        out.append(name)
    return out

SUPPORTED_PATTERNS = [
    "*.xlsx", "*.xlsm", "*.xls", "*.xlsb", "*.ods",
    "*.csv", "*.tsv", "*.txt"
]

def discover_data_files():
    """Find supported files in current directory. Skip temp/hidden and previous outputs."""
    cwd = Path.cwd()
    files = []
    for pat in SUPPORTED_PATTERNS:
        files.extend([p for p in cwd.glob(pat) if not p.name.startswith("~$")])
    # Exclude files that look like outputs from previous runs
    files = [p for p in files if "_Appended_data" not in p.stem]
    # Sort for stable processing
    return sorted(files, key=lambda p: p.name.lower())

def sniff_delimiter(file_path: Path, encoding: str = "utf-8-sig"):
    """Try to detect delimiter among common candidates."""
    try:
        with open(file_path, "r", encoding=encoding, errors="replace") as f:
            sample = f.read(32768)
        # If empty, fallback to comma
        if not sample:
            return ","
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        # Fallback to comma if sniffer fails
        return ","

def read_delimited_safely(path: Path):
    """Read CSV/TSV/TXT with delimiter & encoding detection; return DataFrame or None."""
    suffix = path.suffix.lower()
    # Known delimiter for .tsv
    fixed_sep = "\t" if suffix == ".tsv" else None

    encodings_to_try = ["utf-8-sig", "utf-8", "cp1252", "latin1"]
    last_err = None
    for enc in encodings_to_try:
        try:
            sep = fixed_sep if fixed_sep else sniff_delimiter(path, enc)
            df = pd.read_csv(path, sep=sep, encoding=enc)
            df.columns = clean_column_names(df.columns)
            df.insert(0, "Source_File", path.name)
            return df
        except Exception as e:
            last_err = e
            continue

    print(f"[WARN] Skipping '{path.name}': could not read delimited file. Last error: {last_err}")
    return None

def read_excel_safely(path: Path):
    """Read first sheet of Excel-like files using the appropriate engine; return DataFrame or None."""
    suffix = path.suffix.lower()
    try:
        if suffix in [".xlsx", ".xlsm"]:
            df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
        elif suffix == ".xls":
            # Requires xlrd==1.2.0
            df = pd.read_excel(path, sheet_name=0, engine="xlrd")
        elif suffix == ".xlsb":
            # Requires pyxlsb
            df = pd.read_excel(path, sheet_name=0, engine="pyxlsb")
        elif suffix == ".ods":
            # Requires odfpy
            df = pd.read_excel(path, sheet_name=0, engine="odf")
        else:
            print(f"[WARN] Unhandled Excel suffix for '{path.name}'.")
            return None

        df.columns = clean_column_names(df.columns)
        df.insert(0, "Source_File", path.name)
        return df
    except ImportError as e:
        # Engine missing
        need = ""
        if suffix in [".xlsx", ".xlsm"]:
            need = "openpyxl"
        elif suffix == ".xls":
            need = "xlrd==1.2.0"
        elif suffix == ".xlsb":
            need = "pyxlsb"
        elif suffix == ".ods":
            need = "odfpy"
        print(f"[WARN] Missing dependency for '{path.name}' ({need}). Install it and re-run. Details: {e}")
        return None
    except Exception as e:
        print(f"[WARN] Skipping '{path.name}': {e}")
        return None

def read_any_supported(path: Path):
    """Router for any supported file extension."""
    suffix = path.suffix.lower()
    if suffix in [".xlsx", ".xlsm", ".xls", ".xlsb", ".ods"]:
        return read_excel_safely(path)
    elif suffix in [".csv", ".tsv", ".txt"]:
        return read_delimited_safely(path)
    else:
        print(f"[WARN] Unsupported file type: {path.name}")
        return None

def choose_save_format():
    """Ask user for output format (.xlsx or .csv). Default: xlsx."""
    print("\n1) In which format do you want to save the file?")
    print("   [1] Excel (.xlsx)  [Default]")
    print("   [2] CSV (.csv)")
    choice = input("   Enter 1 or 2 (press Enter for default): ").strip()
    return "csv" if choice == "2" else "xlsx"

def ask_remove_duplicates():
    """Ask user whether to remove duplicates."""
    print("\n2) Do you want to remove duplicates?")
    choice = input("   Type Y for Yes, N for No (default N): ").strip().lower()
    return choice == "y"

def select_dedupe_column(columns):
    """
    Show columns with indices, let user choose one by number or name.
    Returns the selected column name (str) or None if invalid/empty.
    """
    print("\n3) Select the column header to use for duplicate removal.\n")
    for i, col in enumerate(columns):
        print(f"   [{i}] {col}")
    raw = input("\n   Enter the column number or exact column name: ").strip()
    if raw == "":
        return None

    if raw.isdigit():
        idx = int(raw)
        if 0 <= idx < len(columns):
            return columns[idx]
        else:
            print("[ERROR] Invalid index.")
            return None

    lowered = {c.lower(): c for c in columns}
    if raw.lower() in lowered:
        return lowered[raw.lower()]
    print("[ERROR] Column not found.")
    return None

def safe_output_name(base_name: str, ext: str):
    """
    Build safe output file name in current directory with timestamp.
    Example: base_name='Appended_data', ext='xlsx' -> 'Appended_data_20250101_120000.xlsx'
    """
    ts = timestamp()
    return str(Path.cwd() / f"{base_name}_{ts}.{ext}")

def keep_window_open():
    """
    Keep the console window open until user confirms.
    Uses 'pause' on Windows for consistent behavior when double-clicking.
    """
    try:
        print()
        if os.name == "nt":
            os.system("pause")  # 'Press any key to continue . . .'
        else:
            input("Press Enter to exit...")
    except Exception:
        time.sleep(10)

# ------------------ Main Workflow ------------------ #

def main():
    print("\n=== Mixed File Appender & Optional De-duplicator ===\n")
    print("Supported inputs: .xlsx .xlsm .xls .xlsb .ods .csv .tsv .txt\n")

    files = discover_data_files()
    if not files:
        print("No supported files found in the current folder.\n"
              "Place your data files in the same directory as this script.")
        sys.exit(1)

    print("Detected files:")
    for f in files:
        print(f"  • {f.name}")

    # Step 1: Ask output format
    out_fmt = choose_save_format()  # 'xlsx' or 'csv'

    # Read & append
    dfs = []
    for f in files:
        df = read_any_supported(f)
        if df is not None and not df.empty:
            dfs.append(df)

    if not dfs:
        print("\nNo readable/non-empty files were found. Exiting.")
        sys.exit(1)

    # Align on column names, outer-style concat (missing values become NaN)
    appended = pd.concat(dfs, ignore_index=True, sort=False)
    original_rowcount = len(appended)

    # Step 2: Ask to remove duplicates
    do_dedupe = ask_remove_duplicates()
    if not do_dedupe:
        folder_name = Path.cwd().name
        out_path = safe_output_name(f"{folder_name}_Appended_data", out_fmt)
        if out_fmt == "xlsx":
            appended.to_excel(out_path, index=False)
        else:
            appended.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\n✔ Appended data saved (no de-duplication): {out_path}")
        print(f"   Rows: {original_rowcount:,} | Columns: {appended.shape[1]}")
        return

    # Step 3: After appending, remove duplicates by selected column
    columns = list(appended.columns)
    selected = select_dedupe_column(columns)
    if not selected:
        print("\n[ERROR] No valid column selected. Exiting without saving.")
        sys.exit(1)

    before = len(appended)
    deduped = appended.drop_duplicates(subset=[selected], keep="first").reset_index(drop=True)
    after = len(deduped)
    removed = before - after

    # Step 4: Save
    out_path = safe_output_name("Appended_data", out_fmt)
    if out_fmt == "xlsx":
        deduped.to_excel(out_path, index=False)
    else:
        deduped.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"\n✔ Appended data saved with de-duplication on column: '{selected}'")
    print(f"   File: {out_path}")
    print(f"   Rows before: {before:,} | Rows after: {after:,} | Removed: {removed:,}")
    print(f"   Columns: {deduped.shape[1]}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except SystemExit:
        pass
    except Exception as e:
        print("\n[UNEXPECTED ERROR] The script encountered an error:")
        print(f"  {e}")
        traceback.print_exc()
    finally:
        keep_window_open()
