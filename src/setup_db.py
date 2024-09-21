import duckdb as ddb
import pandas as pd

def basic_data_cleaning(df):
    df.columns = [column.lower().strip() for column in df.columns]
    if 'timestamp' in df.columns: df['timestamp'] = pd.to_datetime(df.timestamp)
    return df


def create_dim_datetime_df(df):
    dim_datetime = df[['timestamp']].drop_duplicates()

    dim_datetime = dim_datetime.assign(
        datetime_id = lambda x: x.index,
        year = lambda x: x.timestamp.dt.year,
        month = lambda x: x.timestamp.dt.month,
        day = lambda x: x.timestamp.dt.day,
        hour = lambda x: x.timestamp.dt.hour,
        day_of_week = lambda x: x.timestamp.dt.day_of_week,
        week = lambda x: x.timestamp.dt.isocalendar().week,
    )

    dim_datetime = dim_datetime[['datetime_id','timestamp', 'year', 'month', 'day', 'hour', 'day_of_week', 'week']]
    
    
    
    return dim_datetime


def create_dim_energy_output_flow_df(df):
    dim_energy_output_and_flow = df[['id','demand', 'frequency', 'coal', 'nuclear', 'ccgt', 'wind', 'pumped', 'hydro', 'biomass', 'oil', 'solar', 'ocgt',
       'french_ict', 'dutch_ict', 'irish_ict', 'ew_ict', 'nemo', 'other',
       'north_south', 'scotland_england', 'ifa2', 'intelec_ict', 'nsl',
       'vkl_ict']]\
        .drop_duplicates()
        
    dim_energy_output_and_flow = dim_energy_output_and_flow.rename(columns={
        'id': 'energy_id',
        'ifa2' : 'french_ict_2',
        'intelec_ict' : 'french_ict_intelec',
        'nsl' : 'norway_ict', 
    })
    
    

    return dim_energy_output_and_flow


def create_fct_gridwatch_df(df, dim_energy_output_and_flow, dim_datetime):
    fct_gridwatch = df.reset_index().rename(columns={'index':'fact_id'}).loc[:,['fact_id','id','timestamp']]

    fct_gridwatch = fct_gridwatch\
        .merge(dim_energy_output_and_flow[['energy_id']],left_on='id', right_on='energy_id')\
        .merge(dim_datetime[['datetime_id','timestamp']],on='timestamp')\
        .drop(columns=['id','timestamp'])

    fct_gridwatch = fct_gridwatch[['fact_id', 'datetime_id', 'energy_id']]
    
    

    return fct_gridwatch


def export_all_tbl(dim_datetime, dim_energy_output_and_flow, fct_gridwatch):
    dim_datetime.to_parquet('../data/dim_datetime.gzip',compression='gzip')
    dim_energy_output_and_flow.to_parquet('../data/dim_energy_output_and_flow.gzip',compression='gzip')
    fct_gridwatch.to_parquet('../data/fct_gridwatch.gzip',compression='gzip')


def run_etl_pipeline():
    df = pd.read_csv('data/gridwatch_110514_240914.csv')
    df = basic_data_cleaning(df)
    dim_datetime = create_dim_datetime_df(df)
    dim_energy_output_and_flow = create_dim_energy_output_flow_df(df)
    fct_gridwatch = create_fct_gridwatch_df(df, dim_energy_output_and_flow=dim_energy_output_and_flow, dim_datetime=dim_datetime )
    export_all_tbl(dim_datetime=dim_datetime, dim_energy_output_and_flow=dim_energy_output_and_flow, fct_gridwatch=fct_gridwatch)


def create_schema(conn):
    create_schema_query ="""
    CREATE SCHEMA warehouse;
    """
    conn.execute(create_schema_query)


def create_tables_in_schema(conn):
    
    create_fct_table_query ="""
    CREATE OR REPLACE TABLE warehouse.fct_gridwatch AS
    SELECT * FROM read_parquet('data/fct_gridwatch.gzip');
    """

    create_dim_datetime_query ="""
    CREATE OR REPLACE TABLE warehouse.dim_datetime AS
    SELECT * FROM read_parquet('data/dim_datetime.gzip');
    """

    create_dim_energy_output_and_flow_query ="""
    CREATE OR REPLACE TABLE warehouse.dim_energy_output_and_flow AS
    SELECT * FROM read_parquet('data/dim_energy_output_and_flow.gzip');
    """

    conn.execute(create_fct_table_query)

    conn.execute(create_dim_datetime_query)

    conn.execute(create_dim_energy_output_and_flow_query)

def main():
    run_etl_pipeline()
    conn = ddb.connect('gridwatch.db') #open connection if db exists or create db
    create_schema(conn)
    create_tables_in_schema(conn)
    conn.close() #close connection
    
main()