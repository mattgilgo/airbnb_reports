import json
import pandas as pd
from fpdf import FPDF
from datetime import date
import plotly.graph_objects as go
import plotly.express as px
from urllib.request import urlopen
import awswrangler as wr
import smtplib
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders

## What to change when migrating to AWS
# Update paths to parquet tables in aws once created
# Update "day_of_week_x" to whatever variable we have it named
# Potentially add zipcode list as variable to filter data down
# Bonus: decide on new way to plotly occupancy with price

datasets = \
{
    "datasets": [
    {
            "paths": ["dataframe_csvs/joined_viz_table.csv"],
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
                        "html_filename": "html_plots/choropleth_occ_rate_config_generated_cloud_100pctrange.html",
                        "png_filename": "png_plots/choropleth_occ_rate_config_generated_cloud_100pctrange.png"
                    }
                }]
    },
    {
            "paths": ["dataframe_csvs/joined_viz_table.csv"],
            "plots":[
                {
                    "plot_type": "choropleth",
                    "args": {
                        "title": "Avg Nightly Price Changes W/W",
                        "locations":"zipcode", # use with OpenDataDE geojsons
                        #"locations":"fips", # use with plotly geojsons
                        "color": "avg_nightly_price_pct_change",
                        "color_continuous_scale": "RdYlGn",
                        "featureidkey": "properties.ZCTA5CE10",
                        "range_color": [-100,100],
                        "scope": "usa",
                        "html_filename": "html_plots/choropleth_price_config_generated_cloud_100pctrange.html",
                        "png_filename": "png_plots/choropleth_price_config_generated_cloud_100pctrange.png"
                    }
                }]
    },
    {
            "paths": ['dataframe_csvs/joined_viz_table.csv'],
            "plots": [{
                    "plot_type": "Table",
                    "args": {
                        "title": "Recent Occupancy Rate Trends (%) (W/W)",
                        "indexcol": 'zipcode',
                        "agg_method": 'groupby',
                        "colnames": ['occupancy_pct','occupancy_pct_lag_7_day','occ_pct_change'],
                        #"columnwidth": 50, #dont need, table changes dynamically with page in html
                        #"columnorder": [0,1,2,3,4], # dont need
                        "header": {
                            #"height": 40, # dont need
                            "values": [['<b>Zipcode</b>'], ['<b>Availability</b>'], ["<b>Last Week's Availability</b>"], ['<b>Occupancy Rate Change</b>']],
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
                            "values": ["index", "vals.iloc[0]", "vals.iloc[1]", "vals.iloc[2]"],
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
                        "html_filename": "html_plots/occ_rate_trend_weekly_table_config_generated_cloud.html",
                        "png_filename": "png_plots/occ_rate_trend_weekly_table_config_generated_cloud.png"
                    }
                }]
    },
    {
            "paths": ['dataframe_csvs/joined_viz_table.csv'],
            "plots": [{
                    "plot_type": "Table",
                    "args": {
                        "title": "Recent Pricing Trends (%) (W/W)",
                        "indexcol": 'zipcode',
                        "agg_method": 'groupby',
                        "colnames": ['avg_nightly_price_pct_change','avg_cleaning_fee_pct_change','avg_service_fee_pct_change'],
                        #"columnwidth": 50, #dont need, table changes dynamically with page in html
                        #"columnorder": [0,1,2,3,4], # dont need
                        "header": {
                            #"height": 40, # dont need
                            "values": [['<b>Zipcode</b>'], ['<b>Average Nightly Price Change</b>'], ['<b>Cleaning Fee Change</b>'], ['<b>Service Fee Change</b>']],
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
                            "values": ["index", "vals.iloc[0]", "vals.iloc[1]", "vals.iloc[2]", ],
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
                        "html_filename": "html_plots/price_trend_weekly_table_config_generated_cloud.html",
                        "png_filename": "png_plots/price_trend_weekly_table_config_generated_cloud.png"
                    }
                }]
    },
    {
            "paths": ["dataframe_csvs/joined_viz_table.csv"],
            "plots":
                [{
                    "plot_type": "Figure",
                    "args": {
                        "data": {
                            "traces": [
                                {
                                    "plot_type": "Bar",
                                    "title": "Average Nightly Price",
                                    "x": "guest_num",
                                    "y": "avg_nightly_price",
                                    "yaxis": "y",
                                    "offsetgroup": 1,
                                    "dataframe_path_position": 0,
                                    "agg_method": 'groupby'
                                },
                                {
                                    "plot_type": "Bar",
                                    "title": "Occupancy Rate",
                                    "x": "guest_num",
                                    "y": "occupancy_pct",
                                    "yaxis": "y2",
                                    "offsetgroup": 2,
                                    "dataframe_path_position": 0,
                                    "agg_method": 'groupby'
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
                        "html_filename": "html_plots/two_dataset_figure_med_all_prices_occ_by_guests_config_generated_cloud.html",
                        #"png_filename": "two_dataset_figure_med_total_price_occ_by_guests.png", toggle to use when Total Price for y is true
                        "png_filename": "png_plots/two_dataset_figure_med_all_prices_occ_by_guests_config_generated_cloud.png",
                        "barmode": "group"
                        },
                }]
    },
    {
            "paths": ["dataframe_csvs/joined_viz_table.csv"],
            "plots":
                [{
                    "plot_type": "Figure",
                    "args": {
                        "data": {
                            "traces": [
                                {
                                    "plot_type": "Bar",
                                    "title": "Average Nightly Price Change",
                                    "x": "guest_num",
                                    "y": "avg_nightly_price_pct_change",
                                    "yaxis": "y",
                                    "offsetgroup": 1,
                                    "dataframe_path_position": 0,
                                    "agg_method": 'groupby'
                                },
                                {
                                    "plot_type": "Bar",
                                    "title": "Occupancy Rate",
                                    "x": "guest_num",
                                    "y": "occ_pct_change",
                                    "yaxis": "y2",
                                    "offsetgroup": 2,
                                    "dataframe_path_position": 0,
                                    "agg_method": 'groupby'
                                }
                            ]
                        },
                        
                        "layout": {
                                    'xaxis': {'title': '# of Guests'},
                                    'yaxis': {'title': 'Price Change'},
                                    'yaxis2': {'title': 'Occupancy Rate Change', 'overlaying': 'y', 'side': 'right'}
                        },
                        
                        "title": "Median Price and Occupancy by # of Guests",
                        #"location": "", # use for specified zips later
                        #"html_filename": "two_dataset_figure_med_total_price_occ_by_guests.html", toggle to use when Total Price for y is true
                        "html_filename": "html_plots/two_dataset_figure_med_all_price_changes_occ_by_guests_config_generated_cloud.html",
                        #"png_filename": "two_dataset_figure_med_total_price_occ_by_guests.png", toggle to use when Total Price for y is true
                        "png_filename": "png_plots/two_dataset_figure_med_all_price_changes_occ_by_guests_config_generated_cloud.png",
                        "barmode": "group"
                        },
                }]
    }
    ]
}      

# Day of week line plots, add in at later date
"""
    {  
            "paths": ["dataframe_csvs/joined_viz_table.csv"],
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
                        "html_filename": "html_plots/line_fig_occ_rate_config_generated.html",
                        "png_filename": "png_plots/line_fig_occ_rate_config_generated.png"
                    }
                }]
    },
    {  
            "paths": ["dataframe_csvs/joined_viz_table.csv"],
            "plots":[
                {
                    "plot_type": "line",
                    "args": {
                        "title": "Price Changes W/W Monday-Sunday",
                        "x": "day_of_week_x",
                        "y": "avg_nightly_price_delta_pct",
                        "color": "zipcode",
                        "labels":{
                            "day_of_week_x": "Day of Week (0=Mon, 6=Sun)",
                            "avg_nightly_delta_pct": "Price ($)"
                        },
                        "html_filename": "html_plots/line_fig_price_config_generated.html",
                        "png_filename": "png_plots/line_fig_price_config_generated.png"
                    }
                }]
    },
"""


## Newsletter Generation

# Interactive Newsletter generating function from https://stackoverflow.com/questions/59868987/plotly-saving-multiple-plots-into-a-single-html
def combine_plotly_figs_to_html(plotly_figs, html_fname, include_plotlyjs='cdn', 
                                separator=None, auto_open=False):
    letterhead_path = "../airbnb_reports/newsletter_features/letterhead1.png"
    with open(html_fname, 'w') as f:
        f.write("<html>\n")
        f.write('<img src = "' + letterhead_path + '" alt ="cfg">\n')
        f.write("<html>\n")
        f.write(plotly_figs[0].to_html(include_plotlyjs=include_plotlyjs))
        for fig in plotly_figs[1:]:
            if separator:
                f.write(separator)
            f.write(fig.to_html(full_html=False, include_plotlyjs=False))

    if auto_open:
        import pathlib, webbrowser
        uri = pathlib.Path(html_fname).absolute().as_uri()
        webbrowser.open(uri)

# Non-interactive Newsletter for Email

WIDTH = 210
HEIGHT = 297
TEST_DATE = str(date.today())

def create_title(day, pdf, title: str):
  # Unicode is not yet supported in the py3k version; use windows-1252 standard font
  pdf.set_font('Arial', '', 24)  
  pdf.ln(60)
  pdf.write(5, f'{title}')
  pdf.ln(10)
  pdf.set_font('Arial', '', 16)
  pdf.write(4, f'{day}')
  pdf.ln(5)


def full_analytics_report(day=TEST_DATE, filename='reports/full_newsletter_draft_config_generated_cloud.pdf'):

  pdf = FPDF() # A4 (210 by 297 mm)

  # Header and Title Page
  pdf.add_page()
  pdf.image("../airbnb_reports/newsletter_features/letterhead1.png", 0, 0, WIDTH)
  title = 'Airbnb Analytics Report - Full Data Summary'
  create_title(day, pdf, title)
  #pdf.image('../airbnb_reports/newsletter_features/line_fig_occ_rate_config_generated.png', x=25, y=90, w=WIDTH-60, h=100)
  #pdf.image('../airbnb_reports/newsletter_features/line_fig_price_config_generated.png', x=25, y=190, w=WIDTH-60, h=100)

  # Page 2
  pdf.add_page()
  pdf.image('../airbnb_reports/png_plots/choropleth_occ_rate_config_generated_cloud_100pctrange.png', x=0, y=50, w=WIDTH, h=170)

  # Page 3
  # Header and Title Page
  pdf.add_page()
  pdf.image('../airbnb_reports/png_plots/choropleth_price_config_generated_cloud_100pctrange.png', x=0, y=50, w=WIDTH, h=170)

  # Page 4
  pdf.add_page()
  pdf.image('../airbnb_reports/png_plots/occ_rate_trend_weekly_table_config_generated_cloud.png', x=0, y=0, w=WIDTH-20, h=130)
  pdf.image('../airbnb_reports/png_plots/price_trend_weekly_table_config_generated_cloud.png', x=0, y=140, w=WIDTH-20, h=130)

  # Page 5
  pdf.add_page()
  pdf.image('../airbnb_reports/png_plots/two_dataset_figure_med_all_prices_occ_by_guests_config_generated_cloud.png', x=5, y=50, w=WIDTH-5, h=150)
  
  # Page 6
  pdf.add_page()
  pdf.image('../airbnb_reports/png_plots/two_dataset_figure_med_all_price_changes_occ_by_guests_config_generated_cloud.png', x=5, y=50, w=WIDTH-5, h=150)
  
  # Save file
  pdf.output(filename, 'F')

def generate_plots():
    #s3_client = boto3.client('s3')
    #config_object = s3_client.get_object(Bucket=args.bucket, Key=args.key_path)
    #contents = config_object['Body'].read().decode('utf-8')
    
    #import pyarrow.parquet as pq
    #import s3fs
    #s3 = s3fs.S3FileSystem()
    #pandas_dataframe = pq.ParquetDataset('s3://your-bucket/', filesystem=s3).read_pandas().to_pandas()  

    # joined_viz_table df from s3 bucket
    df = wr.s3.read_parquet("s3://airbnb-scraper-bucket-0-1-1/data/beta_data_tables/joined_viz_table/part-00000-a2efad80-8ff7-44f7-b8c9-e7c65949744b-c000.snappy.parquet", dataset=True)

        # Plotly county geojson
    #with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    #    counties = json.load(response)

    # OpenDataDE geojsons for different states
    with urlopen('https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/fl_florida_zip_codes_geo.min.json') as response:
        zipcodes = json.load(response)
    print('geojson loaded')
    """
    with urlopen('https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/nc_north_carolina_zip_codes_geo.min.json') as response:
        nc_zipcodes = json.load(response)

    with urlopen('https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/sc_south_carolina_zip_codes_geo.min.json') as response:
        sc_zipcodes = json.load(response)

    with urlopen('https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/vt_vermont_zip_codes_geo.min.json') as response:
        vt_zipcodes = json.load(response)

    with urlopen('https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/nh_new_hampshire_zip_codes_geo.min.json') as response:
        nh_zipcodes = json.load(response)

    with urlopen('https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/me_maine_zip_codes_geo.min.json') as response:
        me_zipcodes = json.load(response)


    zipcodes.update(nc_zipcodes)
    zipcodes.update(sc_zipcodes)
    zipcodes.update(vt_zipcodes)
    zipcodes.update(nh_zipcodes)
    zipcodes.update(me_zipcodes)
    """
    # Empty list to store generated plots
    figs = []

    # Generate Plots
    #for dataset_config in datasets['datasets']:
    #    dfs = []
    #    df = None
    #    for path in dataset_config["paths"]:
    #        #print(path)
    #        df = pd.read_csv(path)
    #        #df = df[df['zipcode'].isin(desired_zips)]
    #    #    dfs = dfs.append(df)
        #if len(dfs) == 1:
        #    df = dfs[0]
        # indent for-loop back here if needed to run locally with csv
    for plot_config in datasets['datasets']['plots']:
        #print(plot_config)
        if plot_config['plot_type'] == "line":
            print('Creating Line type Plot')
            fig = px.line(df, 
            x=plot_config['args']['x'], 
            y=plot_config['args']['y'], 
            color=plot_config['args']['color'],
            labels=plot_config['args']['labels']
            )
            fig.update_layout(title=plot_config['args']['title'])
            fig.write_html(plot_config['args']['html_filename'])
            fig.write_image(plot_config['args']['png_filename'], engine='kaleido', width=875, height=700)
            figs.append(fig)

        elif plot_config['plot_type'] == "choropleth":
            print('Creating Choropleth type Plot')
            fig = px.choropleth(df,
            geojson=zipcodes,  # use with OpenDataDE geojsons
            #geojson=counties,   # use with plotly geojsons
            locations=plot_config['args']['locations'],
            color=plot_config['args']['color'],
            color_continuous_scale=plot_config['args']['color_continuous_scale'],
            featureidkey=plot_config['args']['featureidkey'],
            range_color=plot_config['args']['range_color'],
            scope=plot_config['args']['scope']
            )
            fig.update_layout(title=plot_config['args']['title'])
            fig.write_html(plot_config['args']['html_filename'])
            fig.write_image(plot_config['args']['png_filename'], engine='kaleido', width=875, height=700)
            figs.append(fig)
        
        elif plot_config['plot_type'] == "Table":
            print('Creating Table type Plot')
            if plot_config['args']['agg_method'] == 'groupby':
                df = df.groupby(plot_config['args']['indexcol'])[plot_config['args']['colnames']].mean().reset_index()
            index = df[plot_config['args']['indexcol']]
            vals = []
            for col_name in plot_config['args']['colnames']:
                vals.append(df[col_name])
            val_col_count = len(vals)
            indvl_vals = [index]
            for col_name in plot_config['args']['colnames']:
                indvl_vals.append(df[col_name])
            indvl_vals_col_count = len(df.columns)
            font_color = 'black'
            if plot_config['args']['cells']['font']['color'] == "conditional_red_or_green":
                if plot_config['args']['cells']['font']['conditional_direction'] == "positive":
                    font_color = ['rgb(40,40,40)'] +  [['rgb(0,125,0)' if v < 0 else 'rgb(255,0,0)' for v in vals[k]] for k in range(val_col_count)]
                elif plot_config['args']['cells']['font']['conditional_direction'] == "negative":
                    font_color = ['rgb(40,40,40)'] +  [['rgb(0,125,0)' if v > 0 else 'rgb(255,0,0)' for v in vals[k]] for k in range(val_col_count)]
            fig = go.Figure(data=go.Table(
                header = plot_config['args']['header'],
                cells = dict(values=indvl_vals,
                            line = plot_config['args']['cells']['line'],
                            align =  [plot_config['args']['cells']['align']]*indvl_vals_col_count,
                            
                            font = dict(family=plot_config['args']['cells']['font']['family'], 
                                        color=font_color
                                        ),
                            format = plot_config['args']['cells']['format'],  #add % sign here
                            #height = plot_config['args']['cells']['height'],
                            #fill = plot_config['args']['cells']['height']['fill']
                            )
                            )
            )
            fig.update_layout(title=plot_config['args']['title'])
            fig.write_html(plot_config['args']['html_filename'])
            fig.write_image(plot_config['args']['png_filename'], engine='kaleido', width=875, height=700)
            figs.append(fig)
        
        elif plot_config['plot_type'] == "Figure":
            print('Creating Figure type Plot')
            #dfs = []
            #df = None
            df_counter = 0
            #for path in dataset_config["paths"]:
                #print(path)
                #df = pd.read_csv(path).reset_index()
            #    df['guest_num'] = df['guest_num'].str.split(' ').str[0]
            #    df['guest_num'] = df['guest_num'].astype('float')
            #    dfs.append(df)
            #if len(dfs) == 1:
            #    df = dfs[0]
            #for plot_config in dataset_config['plots']:
            #    if plot_config['plot_type'] == "Figure":
                    #logic to iterate across multiple paths to use for figure traces
                    # Line tabbed back here for running local
            traces = []
            for trace in plot_config['args']['data']['traces']:
                #print(trace)
                df_for_trace = df
                if trace['agg_method'] == 'groupby':
                        df_for_trace = df.groupby(trace['x'])[trace['y']].mean().reset_index()   
                if trace['plot_type'] == "Bar":
                    plot_trace = go.Bar(
                        name = trace['title'],
                        x = df_for_trace[trace['x']], 
                        y = df_for_trace[trace['y']], 
                        yaxis = trace['yaxis'], 
                        offsetgroup = trace['offsetgroup']
                    )
                    traces.append(plot_trace)
                #print("through trace")
            fig = go.Figure(
                data = traces,
                layout = plot_config['args']['layout']
            )
            fig.update_layout(title_text=plot_config['args']['title'], barmode=plot_config['args']['barmode'])
            fig.write_html(plot_config['args']['html_filename'])
            fig.write_image(plot_config['args']['png_filename'], engine='kaleido', width=875, height=700)
            figs.append(fig)
        else:
            print('Plot type not available in automated script at the moment.')
        print('Plot Complete and Saved.')

    return figs

# Function to Send Email from btd gmail account
def send_mail(send_from, send_to, subject, message, files=[],
              server="localhost", port=587, username='buildthedome@gmail.com', password='pmtahaysafpescdy',
              use_tls=True):
    """Compose and send email with provided info and attachments.

    Args:
        send_from (str): from name
        send_to (list[str]): to name(s)
        subject (str): message title
        message (str): message body
        files (list[str]): list of file paths to be attached to email
        server (str): mail server host name
        port (int): port number
        username (str): server auth username
        password (str): server auth password
        use_tls (bool): use TLS mode
    """
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(message))

    for path in files:
        part = MIMEBase('application', "octet-stream")
        with open(path, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename={}'.format(Path(path).name))
        msg.attach(part)

    smtp = smtplib.SMTP(server, port)
    if use_tls:
        smtp.starttls()
    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()




    

## Main function for generating Newsletter 
if __name__ == "__main__":
    # Generate Desired Plots
    print("Running generate_plots function...")
    figs = generate_plots()
    print("generate_plots complete!")

    # Generate html-based newsletter
    print("Running combine_plotly_figs_to_html function...")
    combine_plotly_figs_to_html(figs, 'reports/interactive_newsletter.html')
    print("combine_plotly_figs_to_html complete!")

    # Generate image-based newsletter
    print("Generating Report...")
    full_analytics_report()
    print('Report Generation complete!')

    # Send newsletter in email from btd account
    print("Sending Email...")
    send_mail('buildthedome@gmail.com', 
    ['george.padavick@gmail.com, justindiemmanuele@gmail.com, mattgilgo@gmail.com'], 
    'Airbnb Newsletter', 
    'Hi there! \r\n\r\nThis report was generated and sent in an email using python. Please see the attached pdf to view the current your customized Airbnb Market Report.\r\n\r\nThank you! :^) ', 
    files=['reports/full_newsletter_draft_config_generated_cloud.pdf'], 
    server="smtp.gmail.com")
    print('Email sent!')
