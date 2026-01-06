import os
import pandas as pd

def check_first_row_data(directory, output_file):
    excel_files = [f for f in os.listdir(directory) if f.endswith(('.xls', '.xlsx'))]
    results = []
    
    for file in excel_files:
        file_path = os.path.join(directory, file)
        try:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name, header=None)  # Read without headers
                if not df.iloc[0, :2].isnull().all():  # Check if A1 or B1 has any data
                    results.append(f"{file} - Sheet: {sheet_name}")
        except Exception as e:
            print(f"Error processing {file}: {e}")
    
    # Write results to a text file
    with open(output_file, "w") as f:
        for line in results:
            f.write(line + "\n")
    
    print(f"Results saved to {output_file}")

# Specify your directory here
directory = "C:/Users/cb1152/OneDrive - Eide Bailly LLP/Current Projects/State of California/Capital Assets/Capital Asset Aggregation/Inputs/All Report 18's Excel/Report18s"
output_file = "first_row.txt"
check_first_row_data(directory, output_file)