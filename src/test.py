"""


"""

datasets = \
{
    "datasets": [
        {
            "paths": "price_by_zipcode_weekoverweek_deltas.parquet",
            "plots": 
                {
                    "plot_type": "choropleth",
                    "args": {
                        "title": "Total Price Changes W/W",
                        "locations":'zipcode',
                        "color": "total_price_delta_pct",
                        "color_continuous_scale": "RdYlGn",
                        "featureidkey": "properties.ZCTA5CE10",
                        "range_color": [-100,100],
                        "scope": "usa",
                        "output_filename": "newsletter_features/total_price_weekly_changes.html"
                    }
                }
        },      
        
        {
            "paths": "occupancy_by_zipcode_weekoverweek_deltas.parquet",
            "plots":
                {
                    "plot_type": "choropleth",
                    "args": {
                        "title": "Occupancy Rate Changes W/W",
                        "locations":'zipcode',
                        "color": "available_delta_pct",
                        "color_continuous_scale": "RdYlGn",
                        "featureidkey": "properties.ZCTA5CE10",
                        "range_color": [-100,100],
                        "scope": "usa",
                        "output_filename": "newsletter_features/occupancy_weekly_changes.html"
                    }
                }    
        },      
        
        {
            "paths": "price_by_zipcode_and_dayofweek.parquet",
            "plots": 
                {
                    "plot_type": "line",
                    "args": {
                        "title": "Price Rate Changes W/W Monday-Sunday",
                        "x": "day_of_week",
                        "y": "total_price_delta_pct",
                        "color": "zipcode",
                        "output_filename": "newsletter_features/total_price_weekly_changes_dow.html"
                    }
                },
        },       
        
        {
            "paths": "occupancy_by_zipcode_and_dayofweek.parquet",
            "plots":
                {
                    "plot_type": "line",
                    "args": {
                        "title": "Occupancy Rate Changes W/W Monday-Sunday",
                        "x": "day_of_week",
                        "y": "available_delta_pct",
                        "color": "zipcode",
                        "output_filename": "newsletter_features/occupancy_weekly_changes_dow.html"
                    }
                }
        },         
        
        {
            "paths": "price_by_zipcode.parquet",
            "plots": {
                    "plot_type": "Table",
                    "args": {
                        "title": "Recent Pricing Trends (%) (W/W)",
                        "columnwidth": 50,
                        "columnorder": [0,1,2,3,4],
                        "header": {
                            "height": 40,
                            "values": [['<b>Zip Code</b>'], ['<b>Display Price</b>'], ['<b>Cleaning Fee</b>'], ['<b>Service Fee</b>'],['<b>Total Price</b>']],
                            "line": {
                                "color": "rgb(50,50,50)"
                            },
                            "align": 'left',
                            "font": {
                                "color": "rgb(45,45,45)",
                                "size": 14
                            }
                        },
                        "cells": {
                            "values": ["cities", "vals[0]", "vals[1]", "vals[2]", "vals[3]"],
                            "line" : {
                                "color": "#506784"
                            },
                            "align":"left",  
                            "font": {
                                "family": "Arial", 
                                "size": 14, 
                                "color":"conditional_red_or_green",
                            },
                            "format": ["None", ",.2f"],
                            "height": 30,
                            "fill": {
                                "color":"rgb(245,245,245)"
                            }
                        }
                    }
            }
        },         
        
        {
            "paths": "occupancy_by_zipcode.parquet",
            "plots": {
                    "plot_type": "Table",
                    "args": {
                        "title": "Recent Occupancy Rate Trends (%) (W/W)",
                        "columnwidth": 50,
                        "columnorder": [0,1,2,3,4],
                        "header": {
                            "height": 40,
                            "values": [['<b>City</b>'], ['<b>Availability</b>'], ['<b>Check-in Availability</b>'], ['<b>Check-out Availability</b>'],['<b>Bookable</b>']],
                            "line": {
                                "color": "rgb(50,50,50)"
                            },
                            "align": 'left',
                            "font": {
                                "color": "rgb(45,45,45)",
                                "size": 14
                            }
                        },
                        "cells": {
                            "values": ["cities", "vals[0]", "vals[1]", "vals[2]", "vals[3]"],
                            "line" : {
                                "color": "#506784"
                            },
                            "align":"left",  
                            "font": {
                                "family": "Arial", 
                                "size": 14, 
                                "color":"conditional_red_or_green",
                            },
                            "format": ["None", ",.2f"],
                            "height": 30,
                            "fill": {
                                "color":"rgb(245,245,245)"
                            }
                        }
                    }
                }
        },         
        
        {
            "paths": ["price_by_guest_no.parquet", "occupancy_by_guest_no.parquet"],
            "plots":
                [{
                    "plot_type": "Figure",
                    "args": {
                        "data": {
                            "traces": [{
                                "plot1": {
                                    "plot_type": "Bar",
                                    "title": "Total Price",
                                    "x": "guest_no",
                                    "y": "median_total_price",
                                    "yaxis": "y",
                                    "offsetgroup": 1
                                },                                 
                                
                                "plot2": {
                                    "plot_type": "Bar",
                                    "title": "Occupancy Rate",
                                    "x": "guest_no",
                                    "y": "occupancy_rate",
                                    "yaxis": "y2",
                                    "offsetgroup": 2
                                }
                            }]
                        },
                        "layout": {
                                    'xaxis': {'title': '# of Guests'},
                                    'yaxis': {'title': 'Total Price'},
                                    'yaxis2': {'title': 'Occupancy Rate', 'overlaying': 'y', 'side': 'right'}
                            }
                        },
                        "title": "Median Price and Occupancy by # of Guests",
                        "location": ""
                }]
        }
    ]
}


import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
import json
from urllib.request import urlopen

#with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
#    counties = json.load(response)

with urlopen('https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/fl_florida_zip_codes_geo.min.json') as response:
    zipcodes = json.load(response)

for dataset_config in datasets:
    dfs = []
    df = None
    for path in dataset_config["path"] :
        df = pd.read_parquet(dataset_config['path'])
        dfs = dfs.append[df]
    if len(dfs) == 1:
        df = dfs[0]
    for plot_config in dataset_config['plots']:

        if plot_config['plot_type'] == "line":
            fig = px.line(df, 
            x=plot_config['args']['day_of_week'], 
            y=plot_config['args']['total_price_delta_pct'], 
            color=plot_config['args']['zipcode']
            )
            fig.write_html(plot_config['args']['output_filename'])

        elif plot_config['plot_type'] == "cloropleth":
            fig = px.choropleth(df,
            geojson=zipcodes,
            locations=plot_config['args']['locations'],
            color=plot_config['args']['color'],
            color_continuous_scale=plot_config['args']['color_continuous_scale'],
            featureidkey=plot_config['args']['properties.ZCTA5CE10'],
            range_color=plot_config['args']['range_color'],
            scope=plot_config['args']['scope']
            )
            fig.write_html(plot_config['args']['output_filename'])

        elif plot_config['plot_type'] == "Table":
            go.Table(
                 columnwidth= [50]+[50]+[50]+[50]+[50],
                 columnorder=[0, 1, 2, 3, 4],
                 header = dict(height = 40,
                               values = [['<b>Zip Code</b>'], ['<b>Monday</b>'],['<b>Tuesday</b>'],['<b>Wednesday</b>'],['<b>Thursday</b>'],['<b>Friday</b>'],['<b>Saturday</b>'],['<b>Sunday</b>']],
                               line = dict(color='rgb(50,50,50)'),
                               align = ['left']*5,
                               font = dict(color=['rgb(45,45,45)']*4, size=14),
                             
                              ),
                 cells = dict(values=[df['0'],
                            df['1'],
                            df['2'],
                            df['3'],
                            df['4'],
                            df['5'],
                            df['6']],
                              line = dict(color='#506784'),
                              align = ['left']*5,
                              
                              #font = dict(family="Arial", size=14, color=font_color),
                              format = [None, ",.2f"],  #add % sign here
                              height = 30,
                              fill = dict(color='rgb(245,245,245)'))
                             )
        
        elif plot_config['plot_type'] == "Figure":
            for df in dfs:
                
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
        else:
            print('Plot type not available in automated script at the moment.')
        
        



"""






def func(x, y):
    return y, x


json = {"x": "hello", "y": "man"}
assert func(**json) == func(x="hello", y="man")


def func(x, y, **kwargs):
    for k, v in kwargs:
        print(f"{k} is {v}!")
    return y, x












"""

#def plot_builder(**kwargs):
#    for k