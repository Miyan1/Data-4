import pandas as pd
import pyodbc

from dwh_tools import establish_connection, close_connection
import openmeteo_requests
import requests_cache
from retry_requests import retry


API_URL = "https://archive-api.open-meteo.com/v1/archive"


def create_rain_dim(cursor_dwh):
    cursor_dwh.execute('''
       IF OBJECT_ID(N'Rain_Dim', N'U') IS NULL 
       CREATE TABLE Rain_Dim (
       id INT PRIMARY KEY IDENTITY(1,1),
       weather VARCHAR(16) 
       )
       ''') 

    cursor_dwh.execute('''
    SET IDENTITY_INSERT Rain_Dim ON; IF (NOT EXISTS(SELECT 1 FROM Rain_Dim))
    INSERT INTO Rain_Dim (id, weather) VALUES 
    (0, 'Rain'), (1, 'No rain'), (2, 'Unknown')
    ''')

    cursor_dwh.commit() 


def get_top_10_most_popular_cities(op_cursor):
    op_cursor.execute('''  
    SELECT MIN(tl.log_time), MAX(tl.log_time), c.city_id, c.latitude, c.longitude
    FROM city "c"
    JOIN treasure t ON c.city_id = t.city_city_id
    JOIN treasure_log tl ON t.id = tl.treasure_id
    WHERE c.city_id IN (SELECT TOP 10 c.city_id
        FROM city c
        JOIN treasure t ON c.city_id = t.city_city_id
        JOIN treasure_log tl ON t.id = tl.treasure_id
        GROUP BY c.city_id
        ORDER BY COUNT(tl.log_time) DESC)
    GROUP BY c.city_id, c.latitude, c.longitude;
    ''')

    rows = op_cursor.fetchall()
    rows = [list(row) for row in rows]

    city_df = pd.DataFrame(rows, columns=['min_log_time', 'max_log_time', 'city_id', 'lat', 'lon'])
    return city_df


def hourly_data(start_date, end_date, city_id, lat, lon): 
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d')
    end_date = pd.to_datetime(end_date).strftime('%Y-%m-%d')

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["rain"]
    }
    response = openmeteo.weather_api(API_URL, params=params)[0] 

    hourly = response.Hourly()
    hourly_rain = hourly.Variables(0).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ), "rain": hourly_rain, "city_id": city_id}

    hourly_dataframe = pd.DataFrame(data=hourly_data)

    return hourly_dataframe


def create_weather_table(cursor_op):
    cursor_op.execute('''
               IF OBJECT_ID(N'weather_history', N'U') IS NULL 
               CREATE TABLE weather_history (
               id INT PRIMARY KEY IDENTITY(1,1),
               city_id binary(16), 
               log_time datetime,
               amount_of_rain DECIMAL(5,2)
               )
               ''')

    cursor_op.commit() 


def fill_op_weather_table(weather_data, cursor_op):
    insert_query = """
        INSERT INTO weather_history (city_id, log_time, amount_of_rain)
        VALUES (CONVERT(binary(16), ?), ?, ?)
        """
    for index, row in weather_data.iterrows(): 
        cursor_op.execute(insert_query, (
            row['city_id'],
            row['date'],
            row['rain'], 
        ))
        cursor_op.commit() 


def main():
    try:
        conn_op = establish_connection(database='catchem')
        cursor_op = conn_op.cursor()
        conn_dwh = establish_connection(database='catchem_dwh')
        cursor_dwh = conn_dwh.cursor()

        print("Creating weather table")
        create_weather_table(cursor_op)
        print("Creating weather dim")
        create_rain_dim(cursor_dwh)
        print("Filter logs to 10 most popular cities")
        city_df = get_top_10_most_popular_cities(cursor_op)
        for i, df in city_df.iterrows():
            print("Process logs from next city")
            print(df)
            rain_df = hourly_data(df['min_log_time'], df['max_log_time'], df['city_id'], df['lat'], df['lon'])
            fill_op_weather_table(rain_df, cursor_op)

    except Exception as e:
        print("Error:", e)

    finally:
        close_connection(conn_op, cursor_op)
        close_connection(conn_dwh, cursor_dwh)

if __name__ == "__main__":
    main()
