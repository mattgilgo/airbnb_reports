import json
import pandas as pd

## What to change when migrating to AWS
# Update paths to parquet tables in aws once created
# Update "day_of_week_x" to whatever variable we have it named
# Potentially add zipcode list as variable to filter data down
# Bonus: decide on new way to plotly occupancy with price

datasets = \
{
    "datasets": [
    {
            "paths": ["joined_viz_table.csv"],
            "plots":[
                {
                    "plot_type": "choropleth",
                    "args": {
                        "title": "Occupancy Rate Changes W/W",
                        "locations":"zipcode", # use with OpenDataDE geojsons
                        #"locations":"fips", # use with plotly geojsons
                        "color": "occ_pct_change",
                        "color_continuous_scale": "RdYlGn",
                        "featureidkey": "properties.ZCTA5CE10",
                        "range_color": [-100,100],
                        "scope": "usa",
                        "html_filename": "newsletter_features/choropleth_occ_rate_config_generated_cloud_100pctrange.html",
                        "png_filename": "newsletter_features/choropleth_occ_rate_config_generated_cloud_100pctrange.png"
                    }
                }]
    },
    {
            "paths": ["joined_viz_table.csv"],
            "plots":[
                {
                    "plot_type": "choropleth",
                    "args": {
                        "title": "Total Price Changes W/W",
                        "locations":"zipcode", # use with OpenDataDE geojsons
                        #"locations":"fips", # use with plotly geojsons
                        "color": "avg_nightly_price_pct_change",
                        "color_continuous_scale": "RdYlGn",
                        "featureidkey": "properties.ZCTA5CE10",
                        "range_color": [-100,100],
                        "scope": "usa",
                        "html_filename": "newsletter_features/choropleth_price_config_generated_cloud_100pctrange.html",
                        "png_filename": "newsletter_features/choropleth_price_config_generated_cloud_100pctrange.png"
                    }
                }]
    },
    {
            "paths": ['joined_viz_table.csv'],
            "plots": [{
                    "plot_type": "Table",
                    "args": {
                        "title": "Recent Occupancy Rate Trends (%) (W/W)",
                        "indexcol": 'zipcode',
                        "colnames": ['available_delta_pct','available_for_checkin_delta_pct','available_for_checkout_delta_pct','bookable_delta_pct'],
                        #"columnwidth": 50, #dont need, table changes dynamically with page in html
                        #"columnorder": [0,1,2,3,4], # dont need
                        "header": {
                            #"height": 40, # dont need
                            "values": [['<b>Zipcode</b>'], ['<b>Availability</b>'], ['<b>Check-in Availability</b>'], ['<b>Check-out Availability</b>'],['<b>Bookable</b>']],
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
                            "values": ["index", "vals.iloc[0]", "vals.iloc[1]", "vals.iloc[2]", "vals.iloc[3]"],
                            "line" : {
                                "color": "#506784"
                            },
                            "align": "left",  
                            "font": {
                                "family": "Arial", 
                                #"size": 14, # dont need
                                "color": "conditional_red_or_green",
                                "conditional_direction": "negative"
                            },
                            "format": ["None", ",.2f"],
                            #"height": 30, # dont need
                            "fill": {
                                "color":"rgb(245,245,245)"
                            }
                        },
                        "html_filename": "newsletter_features/occ_rate_trend_weekly_table_config_generated.html",
                        "png_filename": "newsletter_features/occ_rate_trend_weekly_table_config_generated.png"
                    }
                }]
    },
    {
            "paths": ['joined_viz_table.csv'],
            "plots": [{
                    "plot_type": "Table",
                    "args": {
                        "title": "Recent Pricing Trends (%) (W/W)",
                        "indexcol": 'zipcode',
                        "colnames": ['display_price_delta_pct','cleaning_fee_delta_pct','service_fee_delta_pct','total_price_delta_pct'],
                        #"columnwidth": 50, #dont need, table changes dynamically with page in html
                        #"columnorder": [0,1,2,3,4], # dont need
                        "header": {
                            #"height": 40, # dont need
                            "values": [['<b>Zipcode</b>'], ['<b>Display Price</b>'], ['<b>Cleaning Fee</b>'], ['<b>Service Fee</b>'],['<b>Total Price</b>']],
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
                            "values": ["index", "vals.iloc[0]", "vals.iloc[1]", "vals.iloc[2]", "vals.iloc[3]"],
                            "line" : {
                                "color": "#506784"
                            },
                            "align": "left",  
                            "font": {
                                "family": "Arial", 
                                #"size": 14, # dont need
                                "color": "conditional_red_or_green",
                                "conditional_direction": "positive"
                            },
                            "format": ["None", ",.2f"],
                            #"height": 30, # dont need
                            "fill": {
                                "color":"rgb(245,245,245)"
                            }
                        },
                        "html_filename": "newsletter_features/price_trend_weekly_table_config_generated.html",
                        "png_filename": "newsletter_features/price_trend_weekly_table_config_generated.png"
                    }
                }]
    },
    {
            "paths": ["joined_viz_table.csv"],
            "plots":
                [{
                    "plot_type": "Figure",
                    "args": {
                        "data": {
                            "traces": [
                                {
                                    "plot_type": "Bar",
                                    "title": "Total Price",
                                    "x": "Guest Number",
                                    #"y": ["Display Price","Cleaning Fee","Service Fee"],
                                    "y": "Total Price",
                                    "yaxis": "y",
                                    "offsetgroup": 1,
                                    "dataframe_path_position": 0
                                },
                                {
                                    "plot_type": "Bar",
                                    "title": "Occupancy Rate",
                                    "x": "Guest Number",
                                    "y": "Occupancy Rate",
                                    "yaxis": "y2",
                                    "offsetgroup": 2,
                                    "dataframe_path_position": 1
                                }
                            ]
                        },
                        
                        "layout": {
                                    'xaxis': {'title': '# of Guests'},
                                    'yaxis': {'title': 'Prices'},
                                    'yaxis2': {'title': 'Occupancy Rate', 'overlaying': 'y', 'side': 'right'}
                        },
                        
                        "title": "Median Price and Occupancy by # of Guests",
                        #"location": "", # use for specified zips later
                        #"html_filename": "two_dataset_figure_med_total_price_occ_by_guests.html", toggle to use when Total Price for y is true
                        "html_filename": "newsletter_features/two_dataset_figure_med_all_prices_occ_by_guests_config_generated.html",
                        #"png_filename": "two_dataset_figure_med_total_price_occ_by_guests.png", toggle to use when Total Price for y is true
                        "png_filename": "newsletter_features/two_dataset_figure_med_all_prices_occ_by_guests_config_generated.png",
                        "barmode": "group"
                        },
                }]
    }
    ]
}      

# Day of week line plots, add in at later date
"""
    {  
            "paths": ["joined_viz_table.csv"],
            "plots":[
                {
                    "plot_type": "line",
                    "args": {
                        "title": "Occupancy Rate Changes W/W Monday-Sunday",
                        "x": "day_of_week_x",
                        "y": "available_delta_pct",
                        "color": "zipcode",
                        "labels":{
                            "day_of_week_x": "Day of Week (0=Mon, 6=Sun)",
                            "available_delta_pct": "Occupancy Rate (%)"
                        },
                        "html_filename": "newsletter_features/line_fig_occ_rate_config_generated.html",
                        "png_filename": "newsletter_features/line_fig_occ_rate_config_generated.png"
                    }
                }]
    },
    {  
            "paths": ["joined_viz_table.csv"],
            "plots":[
                {
                    "plot_type": "line",
                    "args": {
                        "title": "Price Changes W/W Monday-Sunday",
                        "x": "day_of_week_x",
                        "y": "total_price_delta_pct",
                        "color": "zipcode",
                        "labels":{
                            "day_of_week_x": "Day of Week (0=Mon, 6=Sun)",
                            "total_price_delta_pct": "Price ($)"
                        },
                        "html_filename": "newsletter_features/line_fig_price_config_generated.html",
                        "png_filename": "newsletter_features/line_fig_price_config_generated.png"
                    }
                }]
    },
"""