# This script automates the process of extracting leave benefit information from a dump of excel files, adding those to a duckdb database, and then running several analysis scripts to generate compiled reports.
import subprocess
import sys
from pathlib import Path

#Processes that run as part of the compensated absenses aggregation. Processes can be activated/deactivated as needed and run in a single place here.
scripts = [
    (Path(r"C:/Users/cb1152/OneDrive - Eide Bailly LLP/Desktop/California SCO/Leave Benefits/compile_comp_abs.py"), False),
    (Path(r"C:/Users/cb1152/OneDrive - Eide Bailly LLP/Desktop/California SCO/Leave Benefits/create_duckdb.py"), False),
    (Path(r"C:/Users/cb1152/OneDrive - Eide Bailly LLP/Desktop/California SCO/Leave Benefits/count_employees.py"), True),
    (Path(r"C:/Users/cb1152/OneDrive - Eide Bailly LLP/Desktop/California SCO/Leave Benefits/leave_balance_by_agency.py"), True),
    (Path(r"C:/Users/cb1152/OneDrive - Eide Bailly LLP/Desktop/California SCO/Leave Benefits/leave_earned_used.py"), True),
    (Path(r"C:/Users/cb1152/OneDrive - Eide Bailly LLP/Desktop/California SCO/Leave Benefits/average_agency_liability.py"), True),
    (Path(r"C:/Users/cb1152/OneDrive - Eide Bailly LLP/Desktop/California SCO/Leave Benefits/pay_rates.py"), True),
]

print(f"Controller Python: {sys.executable}\n")

for script_path, run in scripts:
    if not run:
        print(f"⏭️ Skipping {script_path.name}\n")
        continue
    if not script_path.exists():
        raise FileNotFoundError(f"❌ Not found: {script_path}")

    print(f"▶️ Running {script_path.name} ...")
    subprocess.run([sys.executable, str(script_path)], check=True, cwd=str(script_path.parent))
    print(f"✅ Completed: {script_path.name}\n")


