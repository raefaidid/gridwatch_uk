import streamlit as st
import pandas as pd
import duckdb as ddb
from queries import *
import datetime
from datetime import datetime as dt
import altair as alt

st.set_page_config(layout="wide")

markdown_introduction = """
## Introduction
This dashboard provides a comprehensive analysis of the UK's power grid using historical electricity demand data.
It allows users to explore key trends in energy generation, demand fluctuations, and the contributions of various power sources over time.
The dashboard is designed to offer interactive visualizations and summaries, including year-on-year averages, daily/weekly peaks and troughs, and moving average overlays.

The data is sourced from [Gridwatch](https://www.gridwatch.templar.co.uk/), which tracks real-time power generation in the UK.

Keywords: Power Grid, Time Series Analysis, Energy Mix, Moving Average, UK Energy Consumption

"""

markdown_analysis ="""
## Electricity Demand and Energy Source Trends

The analysis reveals a **downward trend** in **average electricity demand over the years**. Specifically, demand tends to **decrease** between **May** and **July**, with the lowest points occurring just before August. In contrast, electricity demand **increases** between **November** and **March**, reflecting higher consumption during winter months. This seasonality aligns with the usage of heating systems in winter, driving up electricity demand.

We also observed significant shifts in energy output by source. **Coal, nuclear, and gas** are showing a consistent **decline**, while **wind, biomass, and solar energy** are on the **rise**. Notably, wind energy output has grown substantially since 2016. This suggests the UK's increasing awareness of environmental concerns like global warming and a shift toward more sustainable energy sources.

Understanding these trends in electricity demand and energy production is crucial for **planning**. It allows for **better management** of supply across seasons and supports the gradual transition to environmentally friendly energy sources.
"""

def get_report_level_filters():
    
        min_date = st.date_input(label='Enter :orange[minimum] date for analysis', value=datetime.date(2011,1,1),
                             min_value=datetime.date(2011,1,1),
                             max_value=datetime.date(2024,1,1),format="YYYY-MM-DD")
    
        max_date = st.date_input(label='Enter :orange[maximum] date for analysis', value=dt.today(),
                             min_value=datetime.date(2012,1,1),
                             max_value=datetime.date(2024,12,31),format="YYYY-MM-DD")
        
        if min_date > max_date:

         st.warning('Minimum Date should be earlier than maximum Date')
        
        return min_date, max_date
    
    
def preview_tables(conn):
    option = st.selectbox("Choose a table", ('dim_datetime', 'dim_energy_output_and_flow', 'fct_gridwatch'))
    df = check_tbl(conn,option)
    return df


def main(conn):
    
    st.title(':orange[UK Gridwatch Dashboard]')
    
    with st.sidebar:
        st.header(':orange[Chose your reporting period]')
        
        min_date, max_date = get_report_level_filters()
        
        st.write(" :violet[Streamlit version:] " + st.__version__)
        
        st.write(" :violet[Made by:] " + "Raef Aidid")
    
    
    tab1, tab2, tab3, tab4 = st.tabs([':orange[About]','Data Summaries','Interconnectors', 'Data Analysis'])
    
    with tab1:
        
        st.markdown(markdown_introduction)
        
        col1,col2 = st.columns(2,gap="medium" )
        
        with col1:
            st.subheader(':orange[Tables]')
            df = preview_tables(conn)
            st.write(df)
            
        with col2:
            st.subheader(':orange[Data Model]')
            st.image("gridwatch-Page-2.drawio.png", caption = "This is data model of the UK Gridwatch dataset")
    
    with tab2: 
        st.header("Year-on-Year Averages")
        col1,col2 = st.columns(2,gap="medium")
        with col1: 
            st.subheader(f'Average electricity :orange[demand]')
            df = yearly_avg_energy_demand(conn, min_date, max_date)
            st.line_chart(df, x='year', y='demand', y_label = 'Energy Demand', x_label = 'Year')
            
            
        with col2:
            st.subheader("Average :orange[energy output] by source")
            df = yearly_avg_energy_source_contribution(conn, min_date, max_date)
            st.line_chart(df, x='year', y=['coal', 'nuclear', 'ccgt', 'wind', 'pumped', 'hydro', 'biomass', 'oil', 'solar', 'ocgt'], y_label = 'Energy Demand', x_label = 'Year')
            
        st.header("Moving Average for Electricty :orange[Demand]")
        window_size_demand = st.slider(':orange[Choose the rolling window size for demand]',min_value=5,max_value=50,value=28)
        window_size_demand = window_size_demand - 1
        df = time_series_view_demand(conn,window_size=window_size_demand, start_date=min_date, end_date=max_date)
        st.line_chart(df, x='date',y=['da'])
        
        st.header("Moving Average for Energy :orange[Output] by Source")
        window_size_energy_mix = st.slider(':orange[Choose the rolling window size for energy mix]',min_value=5,max_value=50,value=28)
        window_size_energy_mix = window_size_energy_mix - 1
        df = energy_source_contribution(conn, window_size_energy_mix,min_date, max_date)
        st.line_chart(df, x='date', y=['coal', 'nuclear', 'ccgt', 'wind', 'pumped', 'hydro', 'biomass', 'oil', 'solar', 'ocgt'])
        
        st.header("Trend of Electricity :orange[Demand] by Year, Week or Day")
        
        option = st.selectbox(":orange[Choose the date granularity]", ('Daily', 'Weekly', 'Yearly'), index=2)
        
        if option == 'Daily':
            df = daily_demand(conn, min_date, max_date)
            st.line_chart(df, x='date',y='demand')
        elif option == 'Weekly':
            df = weekly_demand(conn, min_date, max_date)
            st.line_chart(df, x='year_week',y='demand')
        elif option == 'Yearly':
            df = yearly_demand(conn, min_date, max_date)
            st.line_chart(df, x='year',y='demand')
        
    with tab3:
        st.header("Import & Export of Power")
        window_size_ict = st.slider(':orange[Choose the rolling window size for interconnectors]',min_value=5,max_value=50,value=28)
        window_size_ict = window_size_ict - 1
        col1,col2,col3 = st.columns(3,gap="medium" )
        with col1:
            st.subheader("French Interconnector")
            df = french_interconnector(conn, window_size= window_size_ict,start_date=min_date, end_date=max_date)
            st.line_chart(df, x= 'date',y='french_ict')
        with col2:
            st.subheader("Dutch Interconnector (BRTINED)")
            df = dutch_interconnector(conn, window_size= window_size_ict,start_date=min_date, end_date=max_date)
            st.line_chart(df, x= 'date',y='dutch_ict')
        with col3:
            st.subheader("Irish Interconnector (Moyle)")
            df = irish_interconnector(conn, window_size= window_size_ict,start_date=min_date, end_date=max_date)
            st.line_chart(df, x= 'date',y='irish_ict')
        
        with col1:
            st.subheader("Irish Interconnector (East-West)")
            df = ew_interconnector(conn, window_size= window_size_ict,start_date=min_date, end_date=max_date)
            st.line_chart(df, x= 'date',y='ew_ict')
        with col2:
            st.subheader("NEMO Interconnector")
            df = nemo_interconnector(conn, window_size= window_size_ict,start_date=min_date, end_date=max_date)
            st.line_chart(df, x= 'date',y='nemo')
        with col3:
            st.subheader("French Interconnector 2")
            df = french_interconnector_two(conn, window_size= window_size_ict,start_date=min_date, end_date=max_date)
            st.line_chart(df, x= 'date',y='french_ict_2')
            
        with col1:
            st.subheader("French Interconnector (INTELEC)")
            df = french_interconnector_intelec(conn, window_size= window_size_ict,start_date=min_date, end_date=max_date)
            st.line_chart(df, x= 'date',y='french_ict_intelec')
        with col2:
            st.subheader("Norway Interconnector")
            df = norway_interconnector(conn, window_size= window_size_ict,start_date=min_date, end_date=max_date)
            st.line_chart(df, x= 'date',y='norway_ict')
        with col3:
            st.subheader("Viking Interconnector")
            df = viking_interconnector(conn, window_size= window_size_ict,start_date=min_date, end_date=max_date)
            st.line_chart(df, x= 'date',y='vkl_ict')
        
        
        
    with tab4:
        st.markdown(markdown_analysis)
            
if __name__ == '__main__':
    with ddb.connect('gridwatch.db', read_only=True) as conn:
        main(conn)
    