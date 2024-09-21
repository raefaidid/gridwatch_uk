import streamlit as st
import pandas as pd
import duckdb as ddb
from queries import *
import datetime
from datetime import datetime as dt

st.set_page_config(layout="wide")

markdown_introduction = """
## Introduction
This dashboard provides a comprehensive analysis of the UK's power grid using historical electricity demand data.
It allows users to explore key trends in energy generation, demand fluctuations, and the contributions of various power sources over time.
The dashboard is designed to offer interactive visualizations and summaries, including year-on-year averages, daily/weekly peaks and troughs, and moving average overlays.

The data is sourced from [Gridwatch](https://www.gridwatch.templar.co.uk/), which tracks real-time power generation in the UK.

Keywords: Power Grid, Time Series Analysis, Energy Mix, Moving Average, UK Energy Consumption

"""


def main(conn):
    
    st.title(':orange[UK Gridwatch Dashboard]')
    
    with st.sidebar:
        st.header(':orange[Chose your reporting period]')
        
        min_date = st.date_input(label='Enter :orange[minimum] date for analysis', value=datetime.date(2011,1,1),
                             min_value=datetime.date(2011,1,1),
                             max_value=datetime.date(2024,1,1),format="YYYY-MM-DD")
    
        max_date = st.date_input(label='Enter :orange[maximum] date for analysis', value=dt.today(),
                             min_value=datetime.date(2012,1,1),
                             max_value=datetime.date(2024,12,31),format="YYYY-MM-DD")
        
        st.write(" :violet[Running on Streamlit version] -- " + st.__version__)
    
    
    if min_date > max_date:

        st.warning('Minimum Date should be earlier than maximum Date')
    
    tab1, tab2, tab3, tab4 = st.tabs([':orange[About]','Data Summaries','Interconnectors', 'Peak & Troughs Demand Analysis'])
    
    with tab1:
        
        st.markdown(markdown_introduction)
        
        col1,col2 = st.columns(2,gap="medium" )
        
        with col1:
            st.subheader(':orange[Tables]')
            option = st.selectbox("Choose a table", ('dim_datetime', 'dim_energy_output_and_flow', 'fct_gridwatch'))
            df = check_tbl(conn,option)
            st.write(df)
            
        with col2:
            st.subheader(':orange[Data Model]')
            st.image("gridwatch-Page-2.drawio.png", caption = "This is data model of the UK Gridwatch dataset")
            
        with st.expander(':orange[Expand to see the data model]'):
            st.image("gridwatch-Page-2.drawio.png")
    
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
            
        st.header("Peaks and Troughs for Electricity :orange[Demand]")
        
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
            
        st.header("Moving Average for :orange[Demand]")
        window_size_demand = st.slider(':orange[Choose the rolling window size for demand]',min_value=5,max_value=50,value=28)
        window_size_demand = window_size_demand - 1
        df = time_series_view_demand(conn,window_size=window_size_demand, start_date=min_date, end_date=max_date)
        st.line_chart(df, x='date',y=['da'])
        
        st.header("Moving Average for :orange[Energy Output] by Source")
        window_size_energy_mix = st.slider(':orange[Choose the rolling window size for energy mix]',min_value=5,max_value=50,value=28)
        window_size_energy_mix = window_size_energy_mix - 1
        df = energy_source_contribution(conn, window_size_energy_mix,min_date, max_date)
        st.line_chart(df, x='date', y=['coal', 'nuclear', 'ccgt', 'wind', 'pumped', 'hydro', 'biomass', 'oil', 'solar', 'ocgt'])
        
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
        col1,col2 = st.columns(2,gap="medium" )
        with col1:
            df = coal_output(conn)
            st.line_chart(df, x='month', y='total_coal', y_label = 'Coal Output', x_label = 'Timestamp')
        with col2:
            df = nuclear_output(conn)
            st.line_chart(df, x='month', y='total_nuclear', y_label = 'Nuclear Output', x_label = 'Timestamp')
            
    
if __name__ == '__main__':
    with ddb.connect('gridwatch.db', read_only=True) as conn:
        main(conn)
    