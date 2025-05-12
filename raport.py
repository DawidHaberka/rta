from fpdf import FPDF
import os

folder_path = "delay_analysis"
output_pdf = "raport_loty.pdf"

pdf = FPDF()
pdf.set_auto_page_break(auto=True, margin=15)

# Przejście po wszystkich plikach .png
for file in sorted(os.listdir(folder_path)):
    if file.endswith("_wykres.png"):
        flight_code = file.split('_')[0]

        pdf.add_page()
        pdf.set_font("Arial", size=12)
        pdf.cell(0, 10, f"Wykres lotu: {flight_code}", ln=True)

        # Wstawienie obrazka
        img_path = os.path.join(folder_path, file)
        pdf.image(img_path, x=10, y=30, w=180)
pdf.output(output_pdf)

print(f"✅ Wygenerowano raport PDF: {output_pdf}")
