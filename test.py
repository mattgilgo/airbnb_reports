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
                        "scope": "usa"
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
                        "scope": "usa"
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
                        "color": "zipcode"
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
                        "color": "zipcode"
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
                {
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
                    }
                }
    ]
}


import pandas as pd
import matplotlib.pyplot as plt

for dataset_config in datasets:
    df = pd.read_parquet(dataset_config['path'])
    for plot_config in dataset_config['plots']:
        if plot_config['plot_type'] == "bar":
            plt.bar(df, **plot_config['args'])
        if plot_config['plot_type'] == "cloroplath":
            plt.bar(df, **plot_config['args'])



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