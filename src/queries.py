import duckdb as ddb

def check_db(conn):
    df = conn.sql('SHOW ALL TABLES').fetchdf()
    return df


def check_tbl(conn, table_name):
    df = conn.sql(f'DESCRIBE warehouse.{table_name}').fetchdf()
    return df


def check_fct_tbl(conn):
    df = conn.sql('SELECT * FROM warehouse.fct_gridwatch').fetchdf()
    return df


def time_series_view_demand(conn, window_size, start_date, end_date):
    
    query = f"""
    
    with base_tbl as (
    SELECT 
        dt.timestamp::date as date,
        SUM(eof.demand) as d
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id 
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        date,
        avg(d) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as da
    FROM base_tbl
    """
    
    df = conn.sql(query).fetchdf()
    return df


def five_days_rolling_average_demand_by_year(conn,year, window_size):
    
    query = f"""
    
    with base_tbl as (
    SELECT 
        dt.timestamp::date as date,
        SUM(eof.demand) as d
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id    
    WHERE dt.year ={year}
    GROUP BY 1
    )
    SELECT
        date,
        d,
        avg(d) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as da
    FROM base_tbl
    """
    
    df = conn.sql(query).fetchdf()
    return df


def yearly_avg_energy_source_contribution(conn, start_date, end_date):
    
    query = f"""
    SELECT 
        CAST(dt.year AS STRING) as year,
        AVG(eof.coal) coal,
        AVG(eof.nuclear) nuclear,
        AVG(eof.ccgt) ccgt,
        AVG(eof.wind) wind,
        AVG(eof.pumped) pumped,
        AVG(eof.hydro) hydro,
        AVG(eof.biomass) biomass,
        AVG(eof.oil) oil,
        AVG(eof.solar) solar,
        AVG(eof.ocgt) ocgt
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1;
    """

    df = conn.sql(query).fetchdf()
    return df


def yearly_avg_energy_demand(conn, start_date, end_date):
    
    query = f"""
    SELECT 
        CAST(dt.year AS STRING) as year,
        AVG(eof.demand) demand
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1;
    """

    df = conn.sql(query).fetchdf()
    return df


def daily_demand(conn,start_date, end_date):
    
    query = f"""
    WITH base_tbl AS (
    SELECT 
        dt.timestamp::date as date,
        SUM(eof.demand) as demand
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id 
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        date,
        AVG(demand) OVER(ORDER BY date ROWS BETWEEN {27} PRECEDING AND CURRENT ROW) as demand
    FROM base_tbl
    ORDER BY 1
    """
    
    df = conn.sql(query).fetchdf()
    return df


def weekly_demand(conn,start_date, end_date):
    
    query = f"""
    with base_tbl as (
    SELECT 
        CONCAT(YEAR(dt.timestamp::date),'-',WEEK(dt.timestamp::date)) as year_week,
        SUM(eof.demand) as demand
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id 
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        year_week,
        AVG(demand) OVER(ORDER BY year_week ROWS BETWEEN {3} PRECEDING AND CURRENT ROW) as demand
    FROM base_tbl
    ORDER BY 1
    """
    
    df = conn.sql(query).fetchdf()
    return df


def yearly_demand(conn,start_date, end_date):
    
    query = f"""
    with base_tbl as (
    SELECT 
        dt.timestamp::date as date,
        SUM(eof.demand) as demand
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id 
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        CONCAT(YEAR(date)) as year,
        SUM(demand) demand
    FROM base_tbl
    GROUP BY 1
    ORDER BY 1
    """
    
    df = conn.sql(query).fetchdf()
    return df


def energy_source_contribution(conn, window_size,start_date, end_date):
    
    query = f"""
    with base_tbl AS (
    SELECT 
        dt.timestamp::date as date,
        SUM(eof.coal) coal,
        SUM(eof.nuclear) nuclear,
        SUM(eof.ccgt) ccgt,
        SUM(eof.wind) wind,
        SUM(eof.pumped) pumped,
        SUM(eof.hydro) hydro,
        SUM(eof.biomass) biomass,
        SUM(eof.oil) oil,
        SUM(eof.solar) solar,
        SUM(eof.ocgt) ocgt
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        date,
        AVG(coal) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as coal,
        AVG(nuclear) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as nuclear,
        AVG(ccgt) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as ccgt,
        AVG(wind) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as wind,
        AVG(pumped) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as pumped,
        AVG(hydro) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as hydro,
        AVG(biomass) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as biomass,
        AVG(oil) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as oil,
        AVG(solar) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as solar,
        AVG(ocgt) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as ocgt
    FROM base_tbl    
    """

    df = conn.sql(query).fetchdf()
    return df


def nuclear_output(conn,year=2012):
    
    query = f"""
    SELECT 
        dt.month,
        SUM(eof.nuclear) total_nuclear
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id
    WHERE dt.year = {year}
    GROUP BY 1;
    """

    df = conn.sql(query).fetchdf()
    return df


def french_interconnector(conn,window_size,start_date, end_date):
    
    query = f"""
    WITH base_tbl AS (
    SELECT 
        dt.timestamp::date date,
        SUM(eof.french_ict) french_ict
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        date,
        AVG(french_ict) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as french_ict,
    FROM base_tbl
    """
    
    df = conn.sql(query).fetchdf()
    return df

    
def dutch_interconnector(conn,window_size,start_date, end_date):
    
    query = f"""
    WITH base_tbl AS (
    SELECT 
        dt.timestamp::date date,
        SUM(eof.dutch_ict) dutch_ict
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        date,
        AVG(dutch_ict) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as dutch_ict,
    FROM base_tbl
    """

    df = conn.sql(query).fetchdf()
    return df


def irish_interconnector(conn,window_size,start_date, end_date):
    
    query = f"""
    WITH base_tbl AS (
    SELECT 
        dt.timestamp::date date,
        SUM(eof.irish_ict) irish_ict
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        date,
        AVG(irish_ict) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as irish_ict,
    FROM base_tbl
    """

    df = conn.sql(query).fetchdf()
    return df


def ew_interconnector(conn,window_size,start_date, end_date):
    
    query = f"""
    WITH base_tbl AS (
    SELECT 
        dt.timestamp::date date,
        SUM(eof.ew_ict) ew_ict
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        date,
        AVG(ew_ict) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as ew_ict,
    FROM base_tbl
    """

    df = conn.sql(query).fetchdf()
    return df


def nemo_interconnector(conn,window_size,start_date, end_date):
    
    query = f"""
    WITH base_tbl AS (
    SELECT 
        dt.timestamp::date date,
        SUM(eof.nemo) nemo
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        date,
        AVG(nemo) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as nemo,
    FROM base_tbl
    """

    df = conn.sql(query).fetchdf()
    return df


def french_interconnector_two(conn,window_size,start_date, end_date):
    
    query = f"""
    WITH base_tbl AS (
    SELECT 
        dt.timestamp::date date,
        SUM(eof.french_ict_2) french_ict_2
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        date,
        AVG(french_ict_2) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as french_ict_2,
    FROM base_tbl
    """

    df = conn.sql(query).fetchdf()
    return df


def french_interconnector_intelec(conn,window_size,start_date, end_date):
    
    query = f"""
    WITH base_tbl AS (
    SELECT 
        dt.timestamp::date date,
        SUM(eof.french_ict_intelec) french_ict_intelec
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        date,
        AVG(french_ict_intelec) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as french_ict_intelec,
    FROM base_tbl
    """

    df = conn.sql(query).fetchdf()
    return df


def norway_interconnector(conn,window_size,start_date, end_date):
    
    query = f"""
    WITH base_tbl AS (
    SELECT 
        dt.timestamp::date date,
        SUM(eof.norway_ict) norway_ict
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        date,
        AVG(norway_ict) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as norway_ict,
    FROM base_tbl
    """

    df = conn.sql(query).fetchdf()
    return df


def viking_interconnector(conn,window_size,start_date, end_date):
    
    query = f"""
    WITH base_tbl AS (
    SELECT 
        dt.timestamp::date date,
        SUM(eof.vkl_ict) vkl_ict
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        date,
        AVG(vkl_ict) OVER(ORDER BY date ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) as vkl_ict,
    FROM base_tbl
    """

    df = conn.sql(query).fetchdf()
    return df


def daily_min_max_demand(conn,start_date, end_date):
    
    query = f"""
    SELECT 
        dt.timestamp::date as date,
        MAX(eof.demand) as max_demand,
        MIN(eof.demand) as min_demand
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id 
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    """
    
    df = conn.sql(query).fetchdf()
    return df


def weekly_min_max_demand(conn,start_date, end_date):
    
    query = f"""
    with base_tbl as (
    SELECT 
        dt.timestamp::date as date,
        MAX(eof.demand) as max_demand,
        MIN(eof.demand) as min_demand
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id 
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        CONCAT(YEAR(date),'-',WEEK(date)) as year_week,
        SUM(max_demand) as max_demand,
        SUM(min_demand) as min_demand
    FROM base_tbl
    GROUP BY 1
    ORDER BY 1
    """
    
    df = conn.sql(query).fetchdf()
    return df


def yearly_min_max_demand(conn,start_date, end_date):
    
    query = f"""
    with base_tbl as (
    SELECT 
        dt.timestamp::date as date,
        MAX(eof.demand) as max_demand,
        MIN(eof.demand) as min_demand
    FROM warehouse.fct_gridwatch g
    LEFT JOIN warehouse.dim_datetime dt ON dt.datetime_id = g.datetime_id
    LEFT JOIN warehouse.dim_energy_output_and_flow eof ON eof.energy_id = g.energy_id 
    WHERE dt.timestamp::date BETWEEN '{start_date}::date' AND '{end_date}::date'
    GROUP BY 1
    )
    SELECT
        CONCAT(YEAR(date)) as year,
        SUM(max_demand) as max_demand,
        SUM(min_demand) as min_demand
    FROM base_tbl
    GROUP BY 1
    ORDER BY 1
    """
    
    df = conn.sql(query).fetchdf()
    return df

if __name__ == '__main__':
    pass

