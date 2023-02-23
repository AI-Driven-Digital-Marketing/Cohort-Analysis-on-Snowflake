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
    # write our codes here -- INFO Teams!

    #Date dtype specification
    food_df= food_df.withColumn("OrderDate", to_date("OrderDateString", "yyyy-MM-dd"))
    
    # Define a window function to partition by CohortGroup and order by OrderPeriod
    cohort_window = Window.partitionBy("CohortGroup").orderBy("OrderPeriod")

    # Group the data by CohortGroup and OrderPeriod, and count the number of distinct users and orders
    cohorts = df.groupBy("CohortGroup", "OrderPeriod") \
                .agg(countDistinct("UserId").alias("TotalUsers"), 
                     countDistinct("OrderId").alias("TotalOrders"), 
                     sum("TotalCharges").alias("TotalRevenue"))

#     # Define a function to calculate the CohortPeriod
#     def cohort_period(df):
#         """
#         Creates a `CohortPeriod` column, which is the Nth period based on the user's first purchase.
#         """
#         return df.withColumn("CohortPeriod", (col("OrderPeriod").cast("long") - col("FirstOrderPeriod")) / 30 + 1)

#     # Join the cohorts dataframe with a new dataframe that contains the first order period for each user
#     cohorts = cohorts.join(
#         df.select("UserId", "OrderPeriod")
#           .groupBy("UserId")
#           .agg(collect_list("OrderPeriod").getItem(0).alias("FirstOrderPeriod")),
#         on="UserId"
#     )

#     # Calculate the CohortPeriod using the cohort_period function
#     cohorts = cohort_period(cohorts)

#     # Rename the columns to be more meaningful
#     cohorts = cohorts.selectExpr("CohortGroup", "OrderPeriod", "TotalUsers as TotalUsers",
#                                  "TotalOrders as TotalOrders", "TotalRevenue as TotalRevenue",
#                                  "CohortPeriod as CohortPeriod")

    cohorts.show()
    
    
    
#     return food_df


# food_df = load_data()
# food_df