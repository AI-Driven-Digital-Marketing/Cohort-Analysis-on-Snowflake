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
st.set_page_config(page_title="Cohort Analysis on the Food dataset", page_icon="🍔")

st.image(
    "Food.jpg",
    width=400,
)

st.title("Cohort Analysis → `Food` dataset")

st.write("")



with st.expander("About this app"):

    st.write("")

    st.markdown(
        """

    This dataset comes from the hypothetical KPMG.

    Each row in the dataset contains information about an individual bike purchase:

    - Who bought it
    - How much they paid
    - The bike's `brand` and `product line`
    - Its `class` and `size`
    - What day the purchase happened
    - The day the product was first sold
    """
    )

    st.write("")

    st.markdown(
        """
    The underlying code groups those purchases into cohorts and calculates the `retention rate` (split by month) so that one can answer the question:

    *if I'm making monthly changes to my store to get people to come back and buy more bikes, are those changes working?"*

    These cohorts are then visualized and interpreted through a heatmap [powered by Plotly](https://plotly.com/python/).

    """
    )

    st.write("")

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
    food_df.columns = [x.lower() for x in food_df.columns]
    # Write our codes here -- INFO Teams!
    # Process data
#     food_df["orderdate"] = pd.to_datetime(food_df["orderdate"]).dt.date
#     food_df["pickupdate"] = pd.to_datetime(food_df["pickupdate"]).dt.date

    
    return food_df

with st.expander("Show the Data Frame"):
    food_df = load_data()
    st.write(food_df)
    
grouped = df.groupby(["CohortGroup", "OrderPeriod"])


