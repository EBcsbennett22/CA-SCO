import duckdb
import pandas as pd
import math

con = duckdb.connect(database='C:/Users/cb1152/Downloads/OneDrive_2025-11-03/combined_leave_data.duckdb')
df = con.execute("""
WITH full_position_number as (
    SELECT *,
        CASE WHEN LENGTH(CAST("Position Number" AS VARCHAR)) = 12 THEN CONCAT('0', CAST("Position Number" AS VARCHAR)) 
                WHEN LENGTH(CAST("Position Number" AS VARCHAR)) = 11 THEN CONCAT('00', CAST("Position Number" AS VARCHAR)) 
                WHEN LENGTH(CAST("Position Number" AS VARCHAR)) = 10 THEN CONCAT('000', CAST("Position Number" AS VARCHAR)) 
                ELSE CAST("Position Number" AS VARCHAR) END AS "Full Position Number"                
    FROM all_comp_abs                     
)
                               
, with_FY AS (
    SELECT
        -- Fiscal Year
        CONCAT(
            CAST(
                CASE 
                    WHEN CAST(SUBSTR("Leave Period", 6, 2) AS INT) < 7 
                        THEN CAST(CAST(SUBSTR("Leave Period", 1, 4) AS INT) - 1 AS VARCHAR)
                    ELSE SUBSTR("Leave Period", 1, 4)
                END AS VARCHAR
            ),
            '-',
            CAST(
                CASE 
                    WHEN CAST(SUBSTR("Leave Period", 6, 2) AS INT) < 7 
                        THEN SUBSTR("Leave Period", 1, 4)
                    ELSE CAST(CAST(SUBSTR("Leave Period", 1, 4) AS INT) + 1 AS VARCHAR)
                END AS VARCHAR
            )
        ) AS FY,

        LEFT("Full Position Number", 3) AS "Agency Code",

        STRPTIME("Leave Period" || '-01', '%Y-%m-%d') AS period_date,

        a.*
    FROM full_position_number a
    WHERE "Time Base" != '??' AND "Time Base" != 'INT' -- Excluding intermittent employees from avg pay calculation
    AND SUBSTR("Leave Period", 6, 2) = '06'
),
                 
distinct_employees as (
     SELECT DISTINCT             
        FY,
        "Agency Code",
        "CFIS/ORG Code",
        UEID, 
        "Hourly Rate" 
      FROM with_FY        
    )
                 
SELECT 
    FY,
    "Agency Code",
    "CFIS/ORG Code",
    AVG(CAST("Hourly Rate" AS DOUBLE)) AS avg_pay_rate           
FROM distinct_employees
GROUP BY FY, "Agency Code", "CFIS/ORG Code"
ORDER BY FY DESC, "Agency Code", "CFIS/ORG Code"
""").df()

# Debug and print preview
#print(df.to_string(index=False))

# ✅ Write to CSV

output_path = r"C:/Users/cb1152/Downloads/OneDrive_2025-11-03/pay_rates.csv"
base_name, ext = output_path.rsplit('.', 1)  # split filename & extension

# Define your maximum rows per CSV
max_rows = 1_045_000

total_rows = len(df)
num_parts = math.ceil(total_rows / max_rows)

print(f"📊 Total rows: {total_rows:,} — splitting into {num_parts} file(s)")

# Handle empty dataframe gracefully
if total_rows == 0:
    # Save an empty file with headers to the original path
    df.to_csv(output_path, index=False)
    print(f"⚠️ Dataframe is empty. Saved an empty file with headers to: {output_path}")
else:
    for i in range(num_parts):
        start_row = i * max_rows
        end_row = min(start_row + max_rows, total_rows)
        part_df = df.iloc[start_row:end_row]

        # Only append _ptX when there are multiple parts
        if num_parts == 1:
            part_filename = output_path
        else:
            part_filename = f"{base_name}_pt{i+1}.{ext}"

        part_df.to_csv(part_filename, index=False)

        print(f"✅ Saved {part_filename} — rows {start_row+1:,}–{end_row:,} ({len(part_df):,} rows)")

    print("🎉 All parts saved successfully!")
