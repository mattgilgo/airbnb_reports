"""


"""

datasets = \
{
    "datasets": [
        {
            "paths": "price_by_zipcode.parquet",
            "plots": [
                {
                    "plot_type": "bar",
                    "args": {
                        "title": "hello",
                        "xlable": "some units"
                    }
                }
            ]
        },      {
            "paths": "price_by_zipcode_and_dayofweek.parquet",
            "plots": [
                {
                    "plot_type": "choroplath",
                    "args": {
                        "title": "hello",
                        "xlable": "some units",
                        "zips": "33326",
                        "figsize": 55
                    }
                },
                {
                    "plot_type": "bar",
                    "args": {
                        "title": "hello",
                        "xlable": "some units"
                    }
                }
            ]
        },         {
            "paths": "price_by_guest_no.parquet",
            "plots": [
                {
                    "plot_type": "bar",
                    "args": {
                        "title": "hello",
                        "xlable": "some units"
                    }
                }
            ]
        },        {
            "paths": "occupancy_by_zipcode.parquet",
            "plots": [
                {
                    "plot_type": "bar",
                    "args": {
                        "title": "hello",
                        "xlable": "some units"
                    }
                }
            ]
        },      {
            "paths": "occupancy_by_zipcode_and_dayofweek.parquet",
            "plots": [
                {
                    "plot_type": "choroplath",
                    "args": {
                        "title": "hello",
                        "xlable": "some units",
                        "zips": "33326",
                        "figsize": 55
                    }
                },
                {
                    "plot_type": "bar",
                    "args": {
                        "title": "hello",
                        "xlable": "some units"
                    }
                }
            ]
        },         {
            "paths": "occupancy_by_guest_no.parquet",
            "plots": [
                {
                    "plot_type": "bar",
                    "args": {
                        "title": "hello",
                        "xlable": "some units"
                    }
                }
            ]
        },         {
            "paths": ["price_by_guest_no.parquet", "occupancy_by_guest_no.parquet"],
            "plots": [
                {
                    "plot_type": "bar",
                    "args": {
                        "title": "hello",
                        "xlable": "some units"
                    }
                }
            ]
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
