import duckdb
import pandas as pd
import math
con = duckdb.connect(database='C:/Users/cb1152/Downloads/OneDrive_2025-11-03/combined_leave_data.duckdb')

query = """
WITH pre_agg AS (
    SELECT *,
        CONCAT(
            CAST(
                CASE 
                    WHEN CAST(SUBSTR("Leave Period", 6, 2) AS INT) < 7 
                        THEN CAST(CAST(SUBSTR("Leave Period", 1, 4) AS INT) - 1 AS VARCHAR)
                    ELSE CAST(SUBSTR("Leave Period", 1, 4) AS VARCHAR)
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
        ) AS FY
    FROM all_comp_abs
    WHERE LEFT("Position Number", 3) NOT IN (
        '001','002','003','004','007','012','019','035','063','275','276','294',
        '298','313','320','321','323','324','339','341','345','358','402','532',
        '693','801','815','823'
    )
),
-- 051 leading zeroes should not be excluded

eb_setup AS (
    SELECT FY, UEID, "Position Number", "Leave Benefit ID", "End Balance"
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY FY, UEID, "Position Number", "Leave Benefit ID"
                ORDER BY "Leave Period" DESC
            ) AS rn
        FROM pre_agg
    ) t
    WHERE rn = 1
),

movements AS (
    SELECT 
        FY,
        UEID,
        "Position Number",
        "Leave Benefit ID",
        SUM(CAST("Leave Earned" AS DOUBLE)) AS total_earned,
        SUM(CAST("Leave Used" AS DOUBLE))   AS total_used,
        SUM(CAST("Misc Amt" AS DOUBLE))     AS total_misc
    FROM pre_agg
    GROUP BY FY, UEID, "Position Number", "Leave Benefit ID"
),

aggregated AS (
    SELECT 
        p.FY,
        p.UEID,
        p."Department Name",
        p."Position Number",
        p."Leave Benefit Name",
        p."Leave Benefit ID",
        p."Base Pay",
        p."Hourly Rate",
        SUM(CAST(p."Leave Earned" AS DOUBLE)) AS "Leave Earned",
        SUM(CAST(p."Leave Used" AS DOUBLE))   AS "Leave Used",
        SUM(CAST(p."Misc Amt" AS DOUBLE))     AS "Misc Amt"
    FROM pre_agg p
    GROUP BY p.FY, p.UEID, p."Department Name", p."Position Number",
             p."Leave Benefit Name", p."Leave Benefit ID",
             p."Base Pay", p."Hourly Rate"
),

final AS (
    SELECT 
        a.FY,
        a.UEID,
        a."Department Name",
        a."Position Number",
        a."Leave Benefit Name",
        a."Leave Benefit ID",
        a."Base Pay",
        a."Hourly Rate",

        /* New computed beginning balance */
        (       
        CAST(eb."End Balance" AS DOUBLE)
        - COALESCE(m.total_earned, 0)
        + COALESCE(m.total_used, 0)
        + CASE 
            WHEN m.total_misc < 0 THEN ABS(m.total_misc)
            ELSE -COALESCE(m.total_misc, 0)      
        END
        ) AS "Beginning Balance",

        a."Leave Earned",
        a."Leave Used",
        a."Misc Amt",

        eb."End Balance" AS "Ending Balance"
    FROM aggregated a
    LEFT JOIN eb_setup eb
      ON a.FY = eb.FY
     AND a.UEID = eb.UEID
     AND a."Position Number" = eb."Position Number"
     AND a."Leave Benefit ID" = eb."Leave Benefit ID"

    LEFT JOIN movements m
      ON a.FY = m.FY
     AND a.UEID = m.UEID
     AND a."Position Number" = m."Position Number"
     AND a."Leave Benefit ID" = m."Leave Benefit ID"

    WHERE a.FY = '2022-2023'
    ORDER BY a.UEID, a.FY DESC
)

SELECT * FROM final;
"""




df = con.execute(query).df()

#print(df.head(20).to_string(index=False, max_colwidth=10))



# ✅ Write to CSV
output_path = r"C:/Users/cb1152/Downloads/OneDrive_2025-11-03/leave_data_by_FY.csv"
base_name = output_path.rsplit('.', 1)[0]  # remove ".csv" extension

# Define your maximum rows per CSV
max_rows = 1_045_000

total_rows = len(df)
num_parts = math.ceil(total_rows / max_rows)

print(f"📊 Total rows: {total_rows:,} — splitting into {num_parts} file(s)")
for i in range(num_parts):
    start_row = i * max_rows
    end_row = min(start_row + max_rows, total_rows)
    part_df = df.iloc[start_row:end_row]

    part_filename = f"{base_name}_pt{i+1}.csv"
    part_df.to_csv(part_filename, index=False)

    print(f"✅ Saved {part_filename} — rows {start_row+1:,}–{end_row:,} ({len(part_df):,} rows)")

print("🎉 All parts saved successfully!")
""""""
