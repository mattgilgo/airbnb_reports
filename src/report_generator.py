# File acting as Main function for newsletter generator

import pandas as pd
import matplotlib.pyplot as plt
import folium
import numpy as np
import seaborn as sns
import plotly
import os
import fastparquet
import warnings
import geopy
from geopy.point import Point
import time
from fpdf import FPDF
from datetime import datetime, timedelta, date
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

class AnalyticReport:

    def __init__(self, lat: float, lng: float):
        return
        