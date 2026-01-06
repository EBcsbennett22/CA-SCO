import pandas as pd

def get_excel_sheets(file_path):
    """Extracts sheet names from an Excel file."""
    try:
        excel_file = pd.ExcelFile(file_path)
        return excel_file.sheet_names  # List of sheet names
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []
