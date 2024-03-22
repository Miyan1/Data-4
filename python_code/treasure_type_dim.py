from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER
import dwh_tools as dwh
import pyodbc


def create_treasure_type_dim(cursor_dwh):
    cursor_dwh.execute('''
    IF OBJECT_ID(N'TreasureTypeDim', N'U') IS NULL 
    CREATE TABLE TreasureTypeDim (
    id INT PRIMARY KEY IDENTITY(1,1),
    difficulty INT, 
    terrain INT,
    size INT,
    visibility INT
    )
    ''')

    cursor_dwh.commit() 


def insert_treasure_types(cursor):
    cursor.execute('''
    INSERT INTO TreasureTypeDim (difficulty, terrain, size, visibility)
SELECT
    d.difficulty,
    t.terrain,
    s.size,
    v.visibility
FROM
    (VALUES (0), (1), (2), (3), (4)) AS d(difficulty)
    CROSS JOIN (VALUES (0), (1), (2), (3), (4)) AS t(terrain)
    CROSS JOIN (VALUES (0), (1), (2), (3)) AS s(size)
    CROSS JOIN (VALUES (0), (1), (2)) AS v(visibility);

    ''')
    cursor.commit()


def main():
    try:
        # Establish connections
        conn_op = dwh.establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
        conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)
        
        with conn_dwh.cursor() as cursor_dwh:
            # Creature TreasureTypeDim table if it doesn't exist
            create_treasure_type_dim(cursor_dwh)

            # Insert treasure types 
            insert_treasure_types(cursor_dwh)

        # Close connections
        conn_op.close()
        conn_dwh.close()
    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")


if __name__ == "__main__":
    main()

