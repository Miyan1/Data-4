from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER
import dwh_tools as dwh
import pyodbc


def create_treasure_found_fact_table(cursor_dwh):
    cursor_dwh.execute('''
    IF OBJECT_ID(N'TreasureFound_Fact', N'U') IS NULL 
    CREATE TABLE TreasureFound_Fact (
    FactID INT PRIMARY KEY IDENTITY(1,1),
    TreasureID INT NOT NULL,
    WeatherID INT NOT NULL,
    DateSK INT NOT NULL,
    UserSK INT NOT NULL,
    SearchTime DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (TreasureID) REFERENCES TreasureTypeDim(id),
    FOREIGN KEY (WeatherID) REFERENCES Rain_Dim(id),
    FOREIGN KEY (DateSK) REFERENCES Date_Dim(DateSK),
    FOREIGN KEY (UserSK) REFERENCES UserDim(UserSK)
    )
    ''')

    cursor_dwh.commit()


def insert_treasure_found_facts(cursor_dwh):
    cursor_dwh.execute('''
    INSERT INTO TreasureFound_Fact (TreasureID, WeatherID, DateSK, UserSK, SearchTime) 
    SELECT 
        (SELECT id FROM TreasureTypeDim WHERE difficulty = t.difficulty AND terrain = t.terrain AND size = s.container_size AND visibility = s.visibility),
        CASE 
            WHEN wh.amount_of_rain >= 0.1 THEN 0
            WHEN wh.amount_of_rain < 0.1 THEN 1
            ELSE 2
        END,
        dd.DateSK,
        ud.UserSK,
        DATEDIFF(ss, tl.session_start, tl.log_time)
    FROM catchem.dbo.treasure_log tl
    JOIN Date_Dim dd ON dd.date = CONVERT(date, tl.log_time)
    JOIN UserDim ud ON ud.UserID = tl.hunter_id
    JOIN catchem.dbo.treasure t ON t.id = tl.treasure_id
    JOIN catchem.dbo.treasure_stages ts ON ts.treasure_id = t.id
    JOIN catchem.dbo.stage s ON s.id = ts.stages_id
    LEFT OUTER JOIN catchem.dbo.weather_history wh ON wh.log_time = DATEADD(hour, DATEDIFF(hour, 0, tl.log_time), 0)
    WHERE tl.log_type = 2 AND s.id = (SELECT TOP 1 id FROM catchem.dbo.treasure_stages ts2 JOIN catchem.dbo.stage s2 ON s2.id = ts2.stages_id WHERE ts2.treasure_id = t.id ORDER BY s2.sequence_number desc); 
    ''')
    cursor_dwh.commit()


def main():
    try:
        # Establish connections
        conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)

        with conn_dwh.cursor() as cursor_dwh:
            # Create TreasureFoundFact table if it doesn't exist
            create_treasure_found_fact_table(cursor_dwh)

            # Insert treasure found facts
            insert_treasure_found_facts(cursor_dwh)

        # Close connections
        conn_dwh.close()
    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")


if __name__ == "__main__":
    main()

