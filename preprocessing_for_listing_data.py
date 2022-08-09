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
import plotly.graph_objects as go
import plotly.express as px
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)


# Load in listing data
nc_dir = 'C:/Users/mattg/Desktop/Hobbies/airbnb_reports/bucket_data/listings/north_carolina'
first_file = ''
for file in os.listdir(nc_dir):
    first_file = os.path.join(nc_dir,file)
    break
listing_data = pd.read_parquet(first_file)
print(listing_data.shape)
for file in os.listdir(nc_dir):
    next_file = os.path.join(nc_dir,file)
    if next_file != first_file:
        next_listing = pd.read_parquet(next_file)
        listing_data = listing_data.append(next_listing)
print(listing_data.shape)

ne_dir = 'C:/Users/mattg/Desktop/Hobbies/airbnb_reports/bucket_data/listings/vt_nh'
for file in os.listdir(ne_dir):
    next_file = os.path.join(ne_dir,file)
    next_listing = pd.read_parquet(next_file)
    listing_data = listing_data.append(next_listing)
print(listing_data.shape)

miami_dir = 'C:/Users/mattg/Desktop/Hobbies/airbnb_reports/bucket_data/listings/miami'
for file in os.listdir(miami_dir):
    next_file = os.path.join(miami_dir,file)
    next_listing = pd.read_parquet(next_file)
    listing_data = listing_data.append(next_listing)
print(listing_data.shape)

## Clean up listing data
# Shared vs Private Bath Labeling
conditions = [
    listing_data['baths'].str.contains('shared', na=False),
    listing_data['baths'].str.contains('Shared', na=False)
]
values = ['Shared', 'Shared']
listing_data['baths_type'] = np.select(conditions, values, default='Private')

# Regional Locations for our 3 areas, won't need this in the long run
conditions_loc = [
    listing_data['top_lat'] > 35.5,
    listing_data['top_lat'] < 28
]
values_loc = ['New England', 'Miami']
listing_data['Region'] = np.select(conditions_loc, values_loc, default='Carolinas')

# Labeling Half-baths
conditions_halfbath = [
    listing_data['baths'].str.contains('Half-bath', na=False),
    listing_data['baths'].str.contains('Shared half-bath', na=False),
    listing_data['baths'].str.contains('Private half-bath', na=False)
]
values_halfbath = [0.5,0.5,0.5]
listing_data['baths'] = np.select(conditions_halfbath, values_halfbath, default=listing_data['baths'])

# Converting listing attributes to number values for mathematical use cases
listing_data['baths_no'] = listing_data['baths'].str.split(' ').str[0]
listing_data['beds_no'] = listing_data['beds'].str.split(' ').str[0]
listing_data['guest_no'] = listing_data['title'].str.split(' ').str[0]
listing_data['guest_no'] = listing_data['guest_no'].astype('float')

# Cleaning up id and town entries. Town is user entered, but can probably get around this w/ geopy
listing_data['id'] = listing_data['id'].astype('float64')
listing_data['id'] = listing_data['id'].astype('str')
listing_data['town'] = listing_data['town'].str.lower()

# Get rid of data that won't give us a location with geopy
listing_data = listing_data[listing_data.lat.isna() == False]

# Load in occupancy data
occ_data = pd.read_parquet('C:/Users/mattg/Desktop/Hobbies/airbnb_reports/bucket_data/occupancy')
occ_data['id'] = occ_data['id'].astype('str')

# Load in price data
price_data = pd.read_parquet('C:/Users/mattg/Desktop/Hobbies/airbnb_reports/bucket_data/prices')
price_data['id'] = price_data['id'].astype('str')

# Set zipcode using geopy
listing_data.to_csv('listings_preprocessed.csv')
start_time = time.time()
listing_data['zipcode'] = listing_data.apply(lambda row: set_zipcode(row['lat'], row['lng']), axis=1)
end_time = time.time()
listing_data.to_csv('listings_preprocessed_w_zips.csv')
print('Time to run was ' + str(end_time-start_time))

# Getting each listings home value using zillow data (https://www.zillow.com/research/data/)
home_value_1bed = pd.read_csv('resource_data/zillow_zip_onebed.csv')
home_value_2bed = pd.read_csv('resource_data/zillow_zip_twobed.csv')
home_value_3bed = pd.read_csv('resource_data/zillow_zip_threebed.csv')
home_value_4bed = pd.read_csv('resource_data/zillow_zip_fourbed.csv')
home_value_5plusbed = pd.read_csv('resource_data/zillow_zip_fiveplusbed.csv')
# One beds
home_value_1bed_reduced = home_value_1bed[['RegionName','City','Metro','CountyName','State','4/30/2022']]
home_value_1bed_reduced['num_beds'] = 1
# Two beds
home_value_2bed_reduced = home_value_2bed[['RegionName','City','Metro','CountyName','State','4/30/2022']]
home_value_2bed_reduced['num_beds'] = 2
# Three beds
home_value_3bed_reduced = home_value_3bed[['RegionName','City','Metro','CountyName','State','4/30/2022']]
home_value_3bed_reduced['num_beds'] = 3
# Four beds
home_value_4bed_reduced = home_value_4bed[['RegionName','City','Metro','CountyName','State','4/30/2022']]
home_value_4bed_reduced['num_beds'] = 4
# Five+ beds
home_value_5plusbed_reduced = home_value_5plusbed[['RegionName','City','Metro','CountyName','State','4/30/2022']]
home_value_5plusbed_reduced['num_beds'] = 5
# Append all values together into one dataframe
home_values = home_value_1bed_reduced.append(home_value_2bed_reduced)
home_values = home_values.append(home_value_3bed_reduced)
home_values = home_values.append(home_value_4bed_reduced)
home_values = home_values.append(home_value_5plusbed_reduced)
# Drop any duplicates from home value table
home_values = home_values[['RegionName','City','Metro','CountyName','State','4/30/2022','num_beds']]
home_values = home_values.drop_duplicates()
home_values.to_csv('home_values_combined.csv')

# Set home value of listing
listing_data = pd.read_csv('listings_preprocessed_w_zips.csv')
listing_data = listing_data[listing_data['zipcode'].notna()]
listing_data = listing_data[~listing_data['zipcode'].str.contains(':')]
listing_data['zipcode'] = listing_data['zipcode'].astype('int')
listing_data = listing_data.merge(home_values, how='inner', left_on=['zipcode','beds_no'], right_on=['RegionName','num_beds'])
print(listing_data.shape)
listing_data['avg_home_value'] = listing_data.apply(lambda row: set_avg_home_val_w_zip(home_values, row['zipcode'], row['beds_no']),axis=1)
listing_data = listing_data[listing_data['avg_home_value'].notna()]
listing_data['id'] = listing_data['id'].astype('str')

# Merge occupancy rate to listing id
occ_rate = occ_data.groupby('id')['available'].apply(lambda row: np.sum(row)/len(row)).reset_index()
occ_rate['id'] = occ_rate['id'].astype('str')
combined_data = listing_data.merge(occ_rate, on = 'id')
combined_data.rename(columns = {'available':'occupancy_rate'}, inplace = True)

# Merge pricing values to listing id
cleaning_fee = price_data.groupby(['id'])['cleaning_fee'].median().reset_index()
cleaning_fee.rename(columns = {'cleaning_fee':'median_cleaning_fee'}, inplace = True)
service_fee = price_data.groupby(['id'])['service_fee'].median().reset_index()
service_fee.rename(columns = {'service_fee':'median_service_fee'}, inplace = True)
combined_data = combined_data.merge(cleaning_fee, on='id')
combined_data = combined_data.merge(service_fee, on='id')
combined_data['median_total_price'] = combined_data['price'] + combined_data['median_cleaning_fee'] + combined_data['median_service_fee']

# Adding avg mortgage and median ROI
combined_data['avg_30_yr_mort'] = combined_data.apply(lambda row: calculate_mortgage(row['avg_home_value'], 5, 30), axis=1)
combined_data['monthly_maintenance'] = combined_data.apply(lambda row: calculate_monthly_maintenance(row['avg_home_value']), axis=1)
combined_data['monthly_tax'] = combined_data.apply(lambda row: calculate_monthly_taxes(row['avg_home_value'], row['State']), axis=1)
combined_data['median_ROI'] = combined_data.apply(lambda row: calculate_roi(row['median_total_price'], row['occupancy_rate'], row['avg_30_yr_mort'], row['monthly_maintenance'], row['monthly_tax']), axis=1)
combined_data['zipcode'] = combined_data['zipcode'].astype('str')
combined_data = combined_data.to_csv('post_mort_and_roi_calcs.csv')


### Can do tons of groupbys and vizs with "combineddata" table above acting as a master table





######################################
############# FUNCTIONS ##############
######################################


# Various functions that are pretty helpful imo

def append_fig_to_html(list_of_figs):
    for fig in list_of_figs:
        with open("reports/report_draft_june19.html",'a') as f:
            f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))


property_tax_map = {
    'AL' : 0.0037,
    'AK' : 0.0098,
    'AZ' : 0.0060,
    'AK' : 0.0061,
    'CA' : 0.0070,
    'CO' : 0.0052,
    'CT' : 0.0173,
    'DE' : 0.0059,
    'FL' : 0.0086,
    'GA' : 0.0087,
    'HI' : 0.0031,
    'ID' : 0.0065,
    'IL' : 0.0197,
    'IN' : 0.0081,
    'IA' : 0.0143,
    'KS' : 0.0128,
    'KY' : 0.0078,
    'LA' : 0.0051,
    'ME' : 0.0120,
    'MD' : 0.0101,
    'MA' : 0.0108,
    'MI' : 0.0131,
    'MN' : 0.0105,
    'MS' : 0.0063,
    'MO' : 0.0096,
    'MT' : 0.0074,
    'NE' : 0.0154,
    'NV' : 0.0056,
    'NH' : 0.0189,
    'NJ' : 0.0213,
    'NM' : 0.0059,
    'NY' : 0.0130,
    'NC' : 0.0078,
    'ND' : 0.0088,
    'OH' : 0.0152,
    'OK' : 0.0083,
    'OR' : 0.0091,
    'PA' : 0.0143,
    'RI' : 0.0137,
    'SC' : 0.0053,
    'SD' : 0.0114,
    'TN' : 0.0063,
    'TX' : 0.0160,
    'UT' : 0.0056,
    'VT' : 0.0176,
    'VA' : 0.0084,
    'WA' : 0.0084,
    'WV' : 0.0053,
    'WI' : 0.0153,
    'WY' : 0.0051,
    'DC' : 0.0058
}

def calculate_monthly_maintenance(home_value):
    monthly_maintenance = home_value/100/12
    return monthly_maintenance

def calculate_monthly_taxes(home_value, state_id, property_tax_map=property_tax_map):
    tax_rate = property_tax_map.get(state_id)
    monthly_tax = home_value*tax_rate/12
    return monthly_tax

def calculate_mortgage(home_value, interest_rate, num_years, down_payment_pct=0):
    if home_value is None:
        return None
    
    per_payment_interest = 0
    loan_value = 0


    if down_payment_pct >= 1:
        down_payment = down_payment_pct/100 * home_value
        loan_value = home_value - down_payment
    else:
        down_payment = down_payment_pct * home_value
        loan_value = home_value - down_payment
    
    if loan_value/home_value < 0.80: 
        # insert pmi calc here
        pmi = 0.01  # using near average value here
        pmi_cost = 0.0007*home_value    # shot in the dark after interpolating nerdwallet calculator
        if interest_rate >= 1:
            per_payment_interest = interest_rate/100/12
        else:
            per_payment_interest = interest_rate/12
        num_months = num_years*12
        mortgage = loan_value*(per_payment_interest*(1+per_payment_interest)**num_months)/((1+per_payment_interest)**num_months-1) + pmi_cost

        mortgage = np.round(mortgage, 2)
        return mortgage
    else:
        if interest_rate >= 1:
            per_payment_interest = interest_rate/100/12
        else:
            per_payment_interest = interest_rate/12
        num_months = num_years*12
        mortgage = loan_value*(per_payment_interest*(1+per_payment_interest)**num_months)/((1+per_payment_interest)**num_months-1)

        mortgage = np.round(mortgage, 2)
        return mortgage

def calculate_roi(airbnb_daily_price, occupancy_rate, monthly_mortgage, monthly_maintenence=0, monthly_taxes=0):
    gross_rev = airbnb_daily_price * occupancy_rate * 365/12
    net_rev = gross_rev - monthly_maintenence - monthly_taxes
    profit = net_rev - monthly_mortgage
    roi = profit/monthly_mortgage
    roi = roi
    return roi

def set_location_desc(lat,long):
    from geopy.geocoders import Nominatim
    geolocator = Nominatim(user_agent="geoapiExercises")
    lat = str(lat)
    long = str(long)
    location = geolocator.reverse(lat+","+long)
    address = location.raw['address']
    city = address.get('city', '')
    state = address.get('state', '')
    country = address.get('country')
    country_code = address.get('country_code')
    zipcode = address.get('postcode', '')
    return city, state, country, country_code, zipcode

def set_city(lat, long):
    from geopy.geocoders import Nominatim
    geolocator = Nominatim(user_agent="geoapiExercises")
    #start_time = time.time()
    lat = str(lat)
    #print("---Latitude casted to string at %s seconds ---" % (time.time() - start_time))

    long = str(long)
    #print("---Longitude casted at %s seconds ---" % (time.time() - start_time))

    location = geolocator.reverse(lat+","+long)
    #print("---Get location json from geopy at %s seconds ---" % (time.time() - start_time))

    address = location.raw['address']
    #print("---Get address json at %s seconds ---" % (time.time() - start_time))

    city = address.get('city', '')
    #print("---Get city value at %s seconds ---" % (time.time() - start_time))
    return city

def set_state(lat, long):
    from geopy.geocoders import Nominatim
    geolocator = Nominatim(user_agent="geoapiExercises")
    lat = str(lat)
    long = str(long)
    location = geolocator.reverse(Point(lat,long))
    address = location.raw['address']
    state = address.get('state', '')
    return state

def set_country(lat, long):
    from geopy.geocoders import Nominatim
    geolocator = Nominatim(user_agent="geoapiExercises")
    lat = str(lat)
    long = str(long)
    location = geolocator.reverse(Point(lat,long))
    address = location.raw['address']
    country = address.get('country')
    return country

def set_country_code(lat, long):
    from geopy.geocoders import Nominatim
    geolocator = Nominatim(user_agent="geoapiExercises")
    lat = str(lat)
    long = str(long)
    location = geolocator.reverse(lat+","+long)
    address = location.raw['address']
    country_code = address.get('country_code')
    return country_code

def set_zipcode(lat, long):
    from geopy.geocoders import Nominatim
    geolocator = Nominatim(user_agent="geoapiExercises")
    lat = str(lat)
    long = str(long)
    location = geolocator.reverse(Point(lat,long))
    address = location.raw['address']
    zipcode = address.get('postcode', '')
    return zipcode

def set_interpolated_state(top_lat,bottom_lat,left_long,right_long):
    lat = (top_lat+bottom_lat)/2
    long = (left_long+right_long)/2
    state = set_state(lat, long)
    return state


def set_avg_home_val_w_city(home_values, city, state, num_beds):
    if num_beds <= 5:
        desired_row = home_values[(home_values['RegionName'] == city) & (home_values['State'] == state) & (home_values['num_beds'] == num_beds)]
        avg_value = desired_row['2022-04-30']
        if len(avg_value) == 0:
            return None
        else:
            return avg_value.iloc[0]
    else:
        desired_row = home_values[(home_values['RegionName'] == city) & (home_values['State'] == state) & (home_values['num_beds'] == 5)]
        avg_value = desired_row['2022-04-30']
        if len(avg_value) == 0:
            return None
        else:
            return avg_value.iloc[0]*num_beds/5  # <-- improve this with linear regression later on

def set_avg_home_val_w_zip(home_values, zipcode, num_beds):
    if num_beds <= 5:
        desired_row = home_values[(home_values['RegionName'] == zipcode) & (home_values['num_beds'] == num_beds)]
        avg_value = desired_row['4/30/2022']
        if avg_value.empty:
            return None
        else:
            return avg_value.iloc[0]
    else:
        desired_row = home_values[(home_values['RegionName'] == zipcode) & (home_values['num_beds'] == 5)]
        avg_value = desired_row['4/30/2022']
        if avg_value.empty:
            return None
        else:
            return avg_value.iloc[0]*num_beds/5  # <-- improve this with linear regression later on


def list_options_for_dash(df_series):
    options = []
    value = 0
    for i in df_series:
        if value == 0:
            value = i
        town = {'label':i, 'value':i}
        dict_copy = town.copy()
        options.append(dict_copy)
    return options, value

def med_price_occ_by_guests(df, location: str):
    df_guests = df.groupby(['guest_no'])['median_total_price'].median().reset_index()
    df_occ = df.groupby(['guest_no'])['occupancy_rate'].median().reset_index()

    med_price_occ_by_guests = go.Figure(data=[
        go.Bar(name='Total Price', x=df_guests['guest_no'], y=df_guests['median_total_price'], yaxis='y', offsetgroup=1),
        go.Bar(name='Occupancy Rate', x=df_occ['guest_no'], y=df_occ['occupancy_rate'], yaxis='y2', offsetgroup=2),
    ],
        layout={
            'xaxis': {'title': '# of Guests'},
            'yaxis': {'title': 'Total Price'},
            'yaxis2': {'title': 'Occupancy Rate', 'overlaying': 'y', 'side': 'right'}
        }
    )

    # Change the bar mode
    med_price_occ_by_guests.update_layout(title_text='Median Price and Occupancy by # of Guests in '+location, barmode='group')
    filename = "newsletter_features/"+location+"_median_price_and_occ_by_guestno_june19.png"
    med_price_occ_by_guests.write_image(filename, engine='kaleido')
    #miami_fig.show()

def avg_30yrmort_by_guests(df, location: str):
    df_guests_mort = df.groupby(['guest_no'])['avg_30_yr_mort'].median().reset_index()

    avg_mort_by_guests_fig = go.Figure(data=[
        go.Bar(name='Total Price', x=df_guests_mort['guest_no'], y=df_guests_mort['avg_30_yr_mort'])
    ],
        layout={
            'xaxis': {'title': '# of Guests'},
            'yaxis': {'title': 'Monthly Mortgage Cost ($)'},
        }
    )

    # Change the bar mode
    avg_mort_by_guests_fig.update_layout(title_text='Avg 30-Year Mortgage by # of Guests in '+location)
    filename = "newsletter_features/"+location+"_monthly_mortgage_by_guestno_june19.png"
    avg_mort_by_guests_fig.write_image(filename, engine='kaleido')
    #miami_fig.show()


def avg_roi_fig_generator(df, location: str, groupbycol='zipcode', filename_end="_zips_roi_fig_june19.png"):
    #df['zipcode'] = df.apply(lambda row: set_zipcode(row['lat'], row['lng']), axis=1)
    zips_roi_df = df.groupby(groupbycol)[['median_ROI']].mean().reset_index()

    zips_roi_fig = go.Figure(data=[
        go.Bar(name='Display Price', x=zips_roi_df[groupbycol], y=zips_roi_df['median_ROI']),
        ],
        
        layout={
            'xaxis': {'title': groupbycol},
            'yaxis': {'title': 'ROI (1 = 100%)'},
        }
    )

    # Change the bar mode
    zips_roi_fig.update_layout(title_text='Average ROI in '+location+' by '+groupbycol, barmode='stack')
    filename = "newsletter_features/"+location+filename_end
    zips_roi_fig.write_image(filename, engine='kaleido')
    #zips_roi_fig.show()


def pricing_fig_generator(df, location: str, groupbycol='zipcode', filename_end="_zips_cleaning_fig_june19.png"):
    #df['zipcode'] = df.apply(lambda row: set_zipcode(row['lat'], row['lng']), axis=1)

    zips_cleaning_df = df.groupby([groupbycol])[['price','median_cleaning_fee','median_service_fee']].median().reset_index()

    zips_cleaning_fig = go.Figure(data=[
        go.Bar(name='Display Price', x=zips_cleaning_df[groupbycol], y=zips_cleaning_df['price']),
        go.Bar(name='Cleaning Fee', x=zips_cleaning_df[groupbycol], y=zips_cleaning_df['median_cleaning_fee']),
        go.Bar(name='Service Fee', x=zips_cleaning_df[groupbycol], y=zips_cleaning_df['median_service_fee']),
    ],
        layout={
            'xaxis': {'title': groupbycol},
            'yaxis': {'title': 'Total Price ($)'},
        }
    )

    # Change the bar mode
    zips_cleaning_fig.update_layout(title_text='Median Pricing in '+location+' by '+groupbycol, barmode='stack')
    filename = "newsletter_features/"+location+filename_end
    zips_cleaning_fig.write_image(filename, engine='kaleido')
    #miami_zips_cleaning_fig.show()

def roi_bubble_plot(df, filename_end="roi_bubble_fig_june19.png"):
    import plotly.express as px
    #df['City'] = df.apply(lambda row: set_city(row['lat'], row['lng'], axis=1))
    df_group = df.groupby(['City','State','zipcode','guest_no'])['avg_30_yr_mort','median_ROI'].mean().reset_index()

    roi_bubble_fig = px.scatter(df_group, x="guest_no", y="median_ROI",
                size="avg_30_yr_mort", color="State",
                    hover_name="zipcode")
    
    roi_bubble_fig.update_layout(title_text='ROI vs Guest Number, sized by the Avg Monthly Mortgage', xaxis=dict(title='Guest Number for Listing'), yaxis=dict(title='Average ROI (1 = 100%)'))
    filename = "newsletter_features/"+filename_end
    roi_bubble_fig.write_image(filename, engine='kaleido')
    roi_bubble_fig.show()

def listing_count_bubble_plot(df, filename_end="listing_count_bubble_fig_june19.png"):
    import plotly.express as px
    #df['City'] = df.apply(lambda row: set_city(row['lat'], row['lng'], axis=1))
    df_group = df.groupby(['City','State','zipcode','guest_no'])['id'].count().reset_index()

    listing_count_bubble_fig = px.scatter(df_group, x="guest_no", y="id",
                    color="State",
                    hover_name="zipcode")
    
    listing_count_bubble_fig.update_layout(title_text='Listing Count vs Guest Number, sized by the Avg Monthly Mortgage', xaxis=dict(title='Guest Number for Listing'), yaxis=dict(title='# of Listings'))
    filename = "newsletter_features/"+filename_end
    listing_count_bubble_fig.write_image(filename, engine='kaleido')
    listing_count_bubble_fig.show()

