from fpdf import FPDF
from datetime import datetime, timedelta
import os
from datetime import date

from numpy import float64

class AnalyticReport:

    def __init__(self, ne_corner: tuple, sw_corner: tuple, figures, filename="reports/newsletter_draft.pdf"):
        self.ne_lat = ne_corner[0]
        self.ne_lng = ne_corner[1]
        self.sw_lat = sw_corner[0]
        self.sw_lng = sw_corner[1]
        self.Figures = figures
        self.width = 210
        self.height = 297
        self.date = str(date.today())
        self.filename = filename


    def build_report(self):
        pdf = FPDF()

        # Header and Title Page
        pdf.add_page()
        pdf.image("../airbnb_reports/newsletter_features/letterhead1.png", 0, 0, self.width)
        create_title(self.date, pdf)
        fig = self.Figures[0]
        pdf.image(fig, fig.x_pos, fig.y_pos, fig.width)
        #pdf.image('../airbnb_reports/newsletter_features/median_price_and_occ_by_guestno_miami.png', 0, 90, WIDTH)

        print("built")

    def setWidth(self, w: int):
        self.width = w
    
    def setHeight(self, h: int):
        self.height = h

    def getWidth(self):
        return self.width
    
    def getHeight(self):
        return self.height

    def create_title(day, pdf):
        # Unicode is not yet supported in the py3k version; use windows-1252 standard font
        pdf.set_font('Arial', '', 24)  
        pdf.ln(60)
        pdf.write(5, f"Airbnb Analytics Report")
        pdf.ln(10)
        pdf.set_font('Arial', '', 16)
        pdf.write(4, f'{day}')
        pdf.ln(5)

    
