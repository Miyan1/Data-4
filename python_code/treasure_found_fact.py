from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER
import dwh_tools as dwh
import pyodbc


def create_treasure_found_fact_table(cursor_dwh):
    cursor_dwh.execute('''
    IF OBJECT_ID(N'TreasureFound_Fact', N'U') IS NULL 
    CREATE TABLE TreasureFound_Fact (
    id INT PRIMARY KEY IDENTITY(1,1),
    TreasureID INT NOT NULL,
    WeatherID INT NOT NULL,
    DateID INT NOT NULL,
    UserSK INT NOT NULL,
    SearchTime DECIMAL(5, 2) NOT NULL
    )
    ''')

    cursor_dwh.commit() 


def insert_treasure_found_facts(cursor):
    cursor.execute('''
    INSERT INTO catchem_dwh.dbo.TreasureFound_Fact (TreasureID, WeatherID, DateID, UserSK, SearchTime) 
    SELECT 
        td.id,
        CASE 
            WHEN wh.amount_of_rain >= 0.1 THEN 0
            WHEN wh.amount_of_rain < 0.1 THEN 1
            ELSE 2
        END,
        dd.id,
        ud.UserSK,
        DATEDIFF(ss, tl.log_time, tl.session_start)
    FROM catchem.dbo.treasure_log tl
    JOIN catchem_dwh.dbo.Date_Dim dd ON dd.date = convert(date, tl.log_time)
    JOIN catchem_dwh.dbo.UserDim ud ON ud.id = tl.hunter_id
    JOIN catchem_dwh.dbo.TreasureDim td ON td.id = tl.treasure_id
    LEFT OUTER JOIN catchem.dbo.weather_history wh ON wh.log_time = dateadd(hour, datediff(hour, 0, tl.log_time), 0)
    WHERE tl.log_type = 2;
    ''')
    cursor.commit()


def main():
    try:
        # Establish connections
        conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)
        
        with conn_dwh.cursor() as cursor_dwh:
            # Creature TreasureFoundFact table if it doesn't exist
            create_treasure_found_fact_table(cursor_dwh)

            # Insert treasure found facts 
            insert_treasure_found_facts(cursor_dwh)

        # Close connections
        conn_dwh.close()
    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")


if __name__ == "__main__":
    main()
