import pandas as pd
import matplotlib.pyplot as plt
import folium
import numpy as np
import seaborn as sns
import os
import fastparquet
import warnings
import geopy
from geopy.point import Point
import time
from pandas.core.common import SettingWithCopyWarning
import plotly
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta, date
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

# Load in price data
price_data = pd.read_parquet('C:/Users/mattg/Desktop/Hobbies/airbnb_reports/bucket_data/prices')

# Convert aws pull time column to datetime
start_time = time.time()
price_data['pull_time'] = pd.to_datetime(price_data['pull_time'])
print('done with datatype change')
middle_time = time.time()
price_data['pull_time'] = price_data['pull_time'].dt.date ## Do we actually need this step if we structure it like the occupancy data? (greater than/less than pull on dates)
print('done with change to date')
end_time = time.time()
print('Start Time '+ str(start_time))
print('Middle Time '+ str(middle_time-start_time))
print('End Time '+ str(end_time-start_time))

# Grab next month of data from last weeks pull vs this weeks pull to see how price has changed week over week
last_day_from = pd.to_datetime('05/31/2022')
fwd_looking_days = 28
pull_time_current = pd.to_datetime('05/05/2022') # Would be set as date for "today" in the future
fwd_looking_date_current = pull_time_current + timedelta(days=fwd_looking_days)
pull_time_old = last_day_from - timedelta(days=7)
fwd_looking_date_old = pull_time_old + timedelta(days=fwd_looking_days)
price_data_last30_current = price_data[(price_data['check_in'] > str(pull_time_current)) & (price_data['check_in'] <= str(fwd_looking_date_current))]
price_data_last30_old = price_data[(price_data['check_in'] > str(pull_time_old)) & (price_data['check_in'] <= str(fwd_looking_date_old))]
print(price_data_last30_current.shape)
print(price_data_last30_old.shape)
price_data_previous_pull_avgs = price_data_last30_current.groupby(['id','day_of_week']).mean().reset_index()
print(price_data_previous_pull_avgs.shape)
price_data_current_pull_avgs = price_data_last30_old.groupby(['id','day_of_week']).mean().reset_index()
print(price_data_current_pull_avgs.shape)
price_trend = price_data_previous_pull_avgs.merge(price_data_current_pull_avgs, how='inner', on='id')
print(price_trend.shape)
price_trend.to_csv('price_trend_miami_for_table_weekly.csv')

# Add calculated columns finding changes week over week
price_trend['cleaning_fee_delta'] = price_trend['cleaning_fee_y'] - price_trend['cleaning_fee_x']
price_trend['service_fee_delta'] = price_trend['service_fee_y'] - price_trend['service_fee_x']
price_trend['total_price_delta'] = price_trend['total_price_y'] - price_trend['total_price_x']
price_trend['cleaning_fee_delta_pct'] = price_trend['cleaning_fee_delta']/price_trend['cleaning_fee_x']*100
price_trend['service_fee_delta_pct'] = price_trend['service_fee_delta']/price_trend['service_fee_x']*100
price_trend['total_price_delta_pct'] = price_trend['total_price_delta']/price_trend['total_price_x']*100
price_trend['display_price_delta_pct'] = price_trend['total_price_delta_pct'] - price_trend['service_fee_delta_pct'] - price_trend['cleaning_fee_delta_pct']

# Read in real estate data for quick mapping of location attributes
data_w_listing_loc = pd.read_csv('post_mort_and_roi_calcs_june19.csv')
data_w_listing_loc = data_w_listing_loc[['id','zipcode','City','Metro','CountyName','State']]

# Merge price data with location data
price_trend['id'] = price_trend['id'].astype('float64')
price_trend_wloc = price_trend.merge(data_w_listing_loc, how='inner', on='id')

## Groupbys for tables
# Price Trends week over week by Zipcode
price_trends_by_zip = price_trend_wloc.groupby('zipcode').mean(['cleaning_fee_delta_pct','service_fee_delta_pct','total_price_delta_pct']).reset_index()

# Price Trends week over week by Zipcode and Day of Week
price_trends_by_zip_dow = price_trend_wloc.pivot_table(index=['zipcode','day_of_week_x'], values='total_price_delta_pct', aggfunc='mean')
