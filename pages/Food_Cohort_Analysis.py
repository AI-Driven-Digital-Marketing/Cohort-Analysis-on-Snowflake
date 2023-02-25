import streamlit as st
import plotly.graph_objs as go
import pandas as pd
import numpy as np
import datetime as dt
# Snowpark for Python
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType, StringType, StructType, FloatType, StructField, DateType, Variant
from snowflake.snowpark.functions import udf, sum, col,array_construct,month,year,call_udf,lit,count
from snowflake.snowpark.version import VERSION
# Misc
import json
import logging 

# The code below is for the title and logo for this page.
st.set_page_config(page_title= "Cohort Analysis on the Food dataset", page_icon="🍔")

st.image(
    "Food.jpg",
    use_column_width = True,
)

st.title("Cohort Analysis → `Food` dataset")

st.write("")



with st.expander("About this app"):

    st.write(" ")

    st.markdown(
    """

    This dataset comes from the hypothetical Relay Food company. The data spans from June 1, 2009 to September 3, 2010 and is available in CSV format (downloadable here).

    Each row in the dataset contains information about an individual food order:
    - Who bought it
    - How much they paid
    - The pick-up date
    """
    )

    st.write("")

    st.markdown(
        """
    The underlying code groups those purchases into monthly cohorts (with the user's cohort group based on their first order) and calculates the
    `retention rate` so that one can answer the question:
    
    *if I'm making monthly changes to my shop to get people to come back and order more, are those changes working?"*

    These cohorts are then visualized and interpreted through a heatmap [powered by Plotly](https://plotly.com/python/).

    """
    )

    st.write("")

    
with st.expander("Cohort Analysis Architecture"):    
    st.image('src/Architecture.png')
    
# A function that will parse the date Time based cohort:  1 day of month
def get_month(x):
    return dt.datetime(x.year, x.month, 1)

@st.cache_resource
def connect2snowflake():
    # set logger
    logger = logging.getLogger("snowflake.snowpark.session")
    logger.setLevel(logging.ERROR)
    # Create Snowflake Session object
    connection_parameters = json.load(open('connection.json'))
    session = Session.builder.configs(connection_parameters).create()
    session.sql_simplifier_enabled = True

    snowflake_environment = session.sql('select current_user(), current_role(), current_database(), current_schema(), current_version(), current_warehouse()').collect()
    snowpark_version = VERSION
    return session
session = connect2snowflake()



@st.cache_data
def load_data():

    # Load data
    food_df = pd.DataFrame(session.table('FOOD').collect())
    food_df['ORDERDATE'] = pd.to_datetime(food_df['ORDERDATE'])
    #food_df['PICKUPDATE'] = pd.to_datetime(food_df['PICKUPDATE'])
    food_df = food_df.replace(" ",np.NaN)
    food_df = food_df.fillna(food_df.mean(numeric_only=True))
    #food_df.isna().sum()
    food_df['TransactionMonth'] =food_df['ORDERDATE'].apply(lambda x: x.replace(day = 1))
    # Grouping by customer_id and select the InvoiceMonth value
    grouping = food_df.groupby('USERID')['TransactionMonth'] 

    # Assigning a minimum InvoiceMonth value to the dataset
    food_df['CohortMonth'] = grouping.transform('min')
    


    return food_df



food_df = load_data()



@st.cache_data
def get_minmaxCharges():
    return food_df.TOTALCHARGES.max(), food_df.TOTALCHARGES.min()

max_charge, min_charge = get_minmaxCharges()
_, col1, _ = st.columns([0.2, 1, 0.2])

with col1:
    TotalCharges_slider = st.slider(
            "Total Charges (in $)",  0.0,  max_charge-0.01, 
        )    

food_df = food_df[food_df["TOTALCHARGES"] > TotalCharges_slider]
def get_date_int(df, column):
    year = df[column].dt.year
    month = df[column].dt.month
    day = df[column].dt.day
    return year, month, day
# Getting the integers for date parts from the `InvoiceDay` column
transcation_year, transaction_month, _ = get_date_int(food_df, 'TransactionMonth')

# Getting the integers for date parts from the `CohortDay` column
cohort_year, cohort_month, _ = get_date_int(food_df, 'CohortMonth')
#  Get the  difference in years
years_diff = transcation_year - cohort_year

# Calculate difference in months
months_diff = transaction_month - cohort_month
food_df['CohortIndex'] = years_diff * 12 + months_diff  + 1 
# Counting daily active user from each chort
grouping = food_df.groupby(['CohortMonth', 'CohortIndex'])



# Counting number of unique customer Id's falling in each group of CohortMonth and CohortIndex
cohort_data = grouping['USERID'].apply(pd.Series.nunique)
cohort_data = cohort_data.reset_index()


 # Assigning column names to the dataframe created above
cohort_counts = cohort_data.pivot(index='CohortMonth',
                                 columns ='CohortIndex',
                                 values = 'USERID')
cohort_sizes = cohort_counts.iloc[:,0]
retention = cohort_counts.divide(cohort_sizes, axis=0)
retention.index = retention.index.strftime('%Y-%m')


percent_retention = (retention*100).round(3) 

fig = go.Figure()
fig.add_heatmap(
        x=retention.columns,
        y=retention.index,
        z=retention,
        text=percent_retention,
        hoverinfo='text', 
        colorscale="Viridis",
    )

fig.update_layout(title_text="Monthly cohorts showing retention rates", title_x=0.2)
fig.layout.xaxis.title = "Cohort Group"
fig.layout.yaxis.title = "Cohort Period"
fig["layout"]["title"]["font"] = dict(size=25)
fig.layout.width = 750
fig.layout.height = 750
fig.layout.xaxis.tickvals = retention.columns
fig.layout.yaxis.tickvals = retention.index
fig.layout.plot_bgcolor = "#efefef"  # Set the background color to white
fig.layout.margin.b = 100
fig
with st.expander("Show the `Food` dataframe"):
    st.markdown(
        ''' 1. Food-data''')
    st.write(food_df)
    st.markdown(
        ''' 2. Retention''')
    st.write(retention)
