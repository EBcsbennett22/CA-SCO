import duckdb
import pandas as pd
import math
con = duckdb.connect(database='C:/Users/cb1152/Downloads/OneDrive_2025-11-03/combined_leave_data.duckdb', read_only=False)
con.execute("""
    CREATE OR REPLACE TABLE non_clas_employees AS
    SELECT * 
    FROM read_csv_auto(
        'C:/Users/cb1152/Downloads/OneDrive_2025-11-03/Non-Clas Employees/non_clas_employees.csv',
        all_varchar=True
    );
""")


#con.execute("""
#    CREATE OR REPLACE TABLE all_comp_abs AS
#    SELECT * 
#    FROM read_csv_auto(
#        'C:/Users/cb1152/Downloads/OneDrive_2025-11-03/combined_leave_data.csv',
#        all_varchar=True
#    );
#""")
