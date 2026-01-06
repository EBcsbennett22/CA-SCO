from config import WORKING_DIRECTORY
from file_utils import list_files
from excel_processor import get_excel_sheets
from lessee_JE import process_lessee_JE
import pandas as pd
import os

def main():
    excel_files = list_files(WORKING_DIRECTORY, file_spec="*.xlsx", include_subdirs=True)
    all_data = []

    # Process Excel Files
    for file in excel_files:
        sheet_names = get_excel_sheets(file)
        for sheet in sheet_names:
            formatted_path = f"{file}|||{sheet}$'"
            all_data.append({
                "Sheet Name": sheet,
                "Filename": file,
                "Filepath": formatted_path  
            })

    # Convert to DataFrame
    df_summary = pd.DataFrame(all_data, columns=["Sheet Name", "Filename", "Filepath"])

    # Filter files that contain "\lessee\" in their path
    df_lessee = df_summary[df_summary["Filename"].str.contains(r"\\lessee\\", case=False, na=False, regex=True)]

    # Sub DataFrames based on Sheet Name conditions
    df_journal_ls = df_lessee[
        df_lessee["Sheet Name"].str.contains("Journal", case=False, na=False, regex=True) & 
        df_lessee["Sheet Name"].str.contains("LS", case=False, na=False, regex=True)
    ]

    df_department_note = df_lessee[
        df_lessee["Sheet Name"].str.contains("LS-Department Note disclos", case=False, na=False, regex=True)
    ]

    df_lease_ls = df_lessee[
        df_lessee["Sheet Name"].str.contains("Lease", case=False, na=False, regex=True) & 
        df_lessee["Sheet Name"].str.contains("LS", case=False, na=False, regex=True)
    ]

    # Save Results
    df_summary.to_csv("file_summary.csv", index=False)  # Full dataset
    df_lessee.to_csv("lessee_files.csv", index=False)  # Lessee dataset
    df_journal_ls.to_csv("journal_ls_files.csv", index=False)  # Journal & LS dataset
    df_department_note.to_csv("department_note_files.csv", index=False)  # Department Note dataset
    df_lease_ls.to_csv("lease_ls_files.csv", index=False)  # Lease & LS dataset

    #loop through sheets for LS JE's
    for sheet in df_journal_ls:
        lessee_JE = process_lessee_JE(sheet.[Sheet Name], filepath)



    print(f"Processed {len(excel_files)} Excel files.")
    print(f"Total records: {len(df_summary)} | Lessee records: {len(df_lessee)}")
    print(f"Journal & LS: {len(df_journal_ls)} | Department Note: {len(df_department_note)} | Lease & LS: {len(df_lease_ls)}")
    print("Results saved to respective CSV files.")

if __name__ == "__main__":
    main()
