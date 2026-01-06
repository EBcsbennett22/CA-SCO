import duckdb # pyright: ignore[reportMissingImports]
import pandas as pd
import math

con = duckdb.connect(database='C:/Users/cb1152/Downloads/OneDrive_2025-11-03/combined_leave_data.duckdb')
#df = con.execute("""
#                 WITH beginning_balances AS (
#                     SELECT CONCAT(LEFT("Leave Period", 4), \'-\', TRY_CAST(LEFT("Leave Period", 4) as INT) + 1) as FY, UEID, "Position Number", "Leave Benefit ID", "Begin Balance"
#                     FROM all_comp_abs 
#                      WHERE RIGHT("Leave Period", 2) = '07'
#                  ) 
#                 , pre_agg AS (
#                 SELECT *,
#                    CASE 
#                        WHEN CAST(SUBSTR("Leave Period", 6, 2) AS INTEGER) < 7 THEN
#                          CONCAT(
#                            CAST(CAST(SUBSTR("Leave Period", 1, 4) AS INTEGER) - 1 AS VARCHAR),
#                            '-',
#                            SUBSTR("Leave Period", 1, 4)
#                          )
#                        ELSE
#                          CONCAT(
#                            SUBSTR("Leave Period", 1, 4),
#                            '-',
#                            CAST(CAST(SUBSTR("Leave Period", 1, 4) AS INTEGER) + 1 AS VARCHAR)
#                          )
#                      END AS FY
#                  FROM all_comp_abs
#                 )
#                 , aggregated AS (
#                 SELECT 
#                    FY, UEID, "Department Name", "Position Number", "Leave Benefit Name", "Leave Benefit ID", "Base Pay", "Hourly Rate",
#                    SUM(CAST("Leave Earned" as DOUBLE)) as "Leave Earned",
#                    SUM(CAST("Leave Used" as DOUBLE)) as "Leave Used",
#                    SUM(CAST("Misc Amt" as DOUBLE)) as "Misc Amt",
#                    SUM(CAST("End Balance" as DOUBLE)) as "End Balance"
#                 FROM pre_agg
#                 GROUP BY FY, UEID, "Department Name", "Position Number", "Leave Benefit Name", "Leave Benefit ID", "Base Pay", "Hourly Rate"
#                 ORDER BY UEID, FY desc
#                )
#                 
#                , final AS ( 
#                  SELECT a.FY, a.UEID, a."Department Name", a."Position Number", a."Leave Benefit Name", a."Leave Benefit ID", a."Base Pay", a."Hourly Rate",
#                      bb."Begin Balance" as "Beginning Balance",
#                      a."Leave Earned",
#                      a."Leave Used",
#                      a."Misc Amt"
#                  FROM aggregated a
#                  LEFT JOIN beginning_balances bb
#                  ON a.FY = bb.FY 
#                    AND a.UEID = bb.UEID 
#                    AND a."Position Number" = bb."Position Number" 
#                    AND a."Leave Benefit ID" = bb."Leave Benefit ID"
#                 -- WHERE a.FY IN ('2024-2025')
#                  ORDER BY a.UEID, a.FY desc
#                 )
#                -- SELECT * FROM final
#                -- SELECT COUNT(DISTINCT (UEID, FY, "Position Number")) AS distinct_combos FROM final
#                SELECT UEID, FY, COUNT(DISTINCT "Position Number") AS "Number of Positions", STRING_AGG(DISTINCT "Position Number", ', ') AS "List of Positions" FROM final GROUP BY UEID, FY HAVING COUNT(DISTINCT "Position Number") > 1 ORDER BY FY, UEID
#                 """).df()

df = con.execute("""
    WITH base AS (
        SELECT
            *,
            -- Fiscal Year (same logic you had)
            CONCAT(
                CASE 
                    WHEN CAST(SUBSTR("Leave Period", 6, 2) AS INT) < 7 
                        THEN CAST(CAST(SUBSTR("Leave Period", 1, 4) AS INT) - 1 AS VARCHAR)
                    ELSE SUBSTR("Leave Period", 1, 4)
                END,
                '-',
                CASE 
                    WHEN CAST(SUBSTR("Leave Period", 6, 2) AS INT) < 7 
                        THEN SUBSTR("Leave Period", 1, 4)
                    ELSE CAST(CAST(SUBSTR("Leave Period", 1, 4) AS INT) + 1 AS VARCHAR)
                END
            ) AS FY,
            CASE WHEN LENGTH(CAST("Position Number" AS VARCHAR)) = 12 THEN CONCAT('0', CAST("Position Number" AS VARCHAR)) 
                 WHEN LENGTH(CAST("Position Number" AS VARCHAR)) = 11 THEN CONCAT('00', CAST("Position Number" AS VARCHAR)) 
                 WHEN LENGTH(CAST("Position Number" AS VARCHAR)) = 10 THEN CONCAT('000', CAST("Position Number" AS VARCHAR)) 
                 ELSE CAST("Position Number" AS VARCHAR) END AS "Full Position Number"
        FROM all_comp_abs
        WHERE SUBSTR("Leave Period", 6, 2) = '06'  -- Only June
    )
    , final as (
        SELECT *,
            LEFT(CAST("Full Position Number" AS VARCHAR), 3) AS agency_code
        FROM base        
    )

    SELECT
        FY,
        agency_code,
        "CFIS/ORG Code",
        COUNT(DISTINCT UEID) AS employee_count
    FROM final
    GROUP BY FY, agency_code, "CFIS/ORG Code"
    ORDER BY FY DESC, agency_code;          
""").df()

#print(df.to_string(index=False))


# ✅ (OPTIONAL) Write to CSV
output_path = r"C:/Users/cb1152/Downloads/OneDrive_2025-11-03/count_employees_by_CFIS.csv"
df.to_csv(output_path, index=False)
print(f"✅ CSV successfully saved to: {output_path} — rows in CSV: {len(df):,}")