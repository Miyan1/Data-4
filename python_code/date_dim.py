import pandas as pd
import pyodbc
import dwh_tools as dwh
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER

def fetch_min_log_date(cursor_op): 
    cursor_op.execute(f'SELECT MIN(session_start) FROM {DATABASE_OP}.dbo.treasure_log')
    return cursor_op.fetchone()[0]

def fill_table_date_dim(cursor_dwh, start_date, end_date='2040-01-01', table_name='Date_Dim'): 
    insert_query = f"""
    INSERT INTO {DATABASE_DWH}.dbo.{table_name} ([Date], [DayofTheMonth], [Month], [Year], [DayofTheWeek], [Week], [Season])
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    current_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
 
    while current_date <= end_date:
        day_of_month = current_date.day
        month = current_date.month
        year = current_date.year
        week = current_date.week
        day_of_week = current_date.dayofweek
        day_of_year = current_date.timetuple().tm_yday
        season = month%12 // 3 + 1

        # Execute the INSERT query
        cursor_dwh.execute(insert_query, (
            current_date, day_of_month, month, year, day_of_week, week, season
        ))

        # Commit the transaction
        cursor_dwh.commit()
        current_date += pd.Timedelta(days=1) 

def main():
    try:
        # Define connection parameters
        # Connect to the 'tutorial_op' database
        conn_op = dwh.establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
        cursor_op = conn_op.cursor()

        # Connect to the 'tutorial_dwh' database
        conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)
        cursor_dwh = conn_dwh.cursor()

        # Fetch minimum order date
        start_date = fetch_min_log_date(cursor_op)
        print(start_date)

        # Fill the 'dimDay' table
        fill_table_date_dim(cursor_dwh, start_date)

        # Close the connections
        cursor_op.close()
        conn_op.close()
        cursor_dwh.close()
        conn_dwh.close()

    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")

if __name__ == "__main__":
    main()
