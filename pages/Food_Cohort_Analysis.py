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

#     cohorts.rename(columns={"UserId": "TotalUsers", "OrderId": "TotalOrders"}, inplace=True)
#     cohorts.head()  
    
     # Assigning column names to the dataframe created above
    cohort_counts = cohort_data.pivot(index='CohortMonth',
                                     columns ='CohortIndex',
                                     values = 'USERID')
    cohort_sizes = cohort_counts.iloc[:,0]
    retention = cohort_counts.divide(cohort_sizes, axis=0)
    #retention.round(3)*100


    return food_df,retention

with st.expander("DataFrame"):
    food_df,retention = load_data()
    
    st.markdown(
        ''' 1. Food-data''')
    food_df
    st.markdown(
        ''' 2. Retention''')
    retention
    
with st.form("my_form"):
# st.write("Inside the form")
# slider_val = st.slider("Form slider")
# checkbox_val = st.checkbox("Form checkbox")

    cole, col1, cole = st.columns([0.1, 1, 0.1])

with col1:  

    TotalCharges_slider = st.slider(
            "Total Charges (in $)", step=50, min_value=2, max_value=690
        )
        # Every form must have a submit button.

    submitted = st.form_submit_button("Refine results")

    retention.index = retention.index.strftime('%Y-%m')
    


fig = go.Figure()
fig.add_heatmap(
        # x=retention.columns, y=retention.index, z=retention, colorscale="cividis"
        x=retention.columns,
        y=retention.index,
        z=retention,
        # Best
        # colorscale="Reds",
        # colorscale="Sunsetdark",
        #colorscale="Redor"
        colorscale="Viridis"
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
