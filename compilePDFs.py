import os
from pypdf import PdfWriter

# Define the directory containing the PDFs
directory = "C:/Users/cb1152/OneDrive - Eide Bailly LLP/Current Projects/State of California/Comp Abs/Downloads/Detail of Apportionment PDFs/PY Recreate"
output = "C:/Users/cb1152/OneDrive - Eide Bailly LLP/Current Projects/State of California/Comp Abs/Downloads/Detail of Apportionment PDFs"
# Get a list of all PDF files in the directory
pdf_files = [f for f in os.listdir(directory) if f.endswith(".pdf")]

# Sort files to ensure correct order (optional)
pdf_files.sort()

# Create a PdfWriter object
writer = PdfWriter()

# Loop through each PDF and append it
for pdf in pdf_files:
    writer.append(os.path.join(directory, pdf))

# Save the merged PDF
output_path = os.path.join(output, "Detail of Appropriations.pdf")
writer.write(output_path)
writer.close()

print(f"PDFs merged successfully into {output_path}")