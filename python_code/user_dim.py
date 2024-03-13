import pandas as pd
import pyodbc
import dwh_tools as dwh
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER


def extract_user_data(cursor_op, row_limit=10):
    query = f"""
    SELECT TOP {row_limit} 
    u.id, 
    u.first_name, 
    u.last_name, 
    u.mail, 
    u.number, 
    u.street, 
    c.city_name,
    c.country_code
    FROM dbo.user_table u
    JOIN dbo.city c ON u.city_city_id = c.city_id
    JOIN dbo.country ctr ON c.country_code = ctr.code
"""
    cursor_op.execute(query)
    columns = [column[0] for column in cursor_op.description]
    data = cursor_op.fetchall()

    data_list = [list(row) for row in data]

    return pd.DataFrame(data_list, columns=columns)


def transform_user_data(user_data, cursor_op):
    # check if 'country_code' is df
    if 'country_code' in user_data.columns:
        # Fetch country names for each country code
        for index, row in user_data.iterrows():
            cursor_op.execute("SELECT name FROM dbo.country WHERE code = ?", row['country_code'])
            country_result = cursor_op.fetchone()
            country_name = country_result[0] if country_result else None
            # adding country to the df
            user_data.at[index, 'Country'] = country_name

            # Fetch the count of 'Found' logs placed for each user
            user_id = row['id']
            cursor_op.execute(
                "SELECT COUNT(*) FROM dbo.treasure_log WHERE hunter_id = ? AND CAST(log_type AS VARCHAR) = 'found'",
                user_id)
            found_logs_count = cursor_op.fetchone()[0]

            # Derive the ExperienceLevel based on the found_logs_count
            if found_logs_count == 0:
                experience_level = 'Starter'
            elif found_logs_count < 4:
                experience_level = 'Amateur'
            elif 4 <= found_logs_count <= 10:
                experience_level = 'Professional'
            else:
                experience_level = 'Pirate'

            # add 'ExperienceLevel' column to df
            user_data.at[index, 'ExperienceLevel'] = experience_level
    else:
        # debugging
        print("country_code not in df")
    return user_data


def load_user_dim(cursor_dwh, user_data, table_name='UserDim'):
    for index, row in user_data.iterrows():
        print(f"Inserting row: {row}")  # Debugging line to see the row data

        try:
            insert_query = f"""
            INSERT INTO {DATABASE_DWH}.dbo.{table_name} (UserID, FirstName, LastName, City, Country, ExperienceLevel, IsDedicator, ValidFrom, IsCurrent)
            VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE(), 1)
            """
            cursor_dwh.execute(insert_query, row['id'], row['first_name'], row['last_name'], row['city_name'],
                               row['Country'],
                               row['ExperienceLevel'], row['IsDedicator'])

            # Commit the transaction
            cursor_dwh.commit()
        except pyodbc.Error as e:
            print(f"Error inserting row: {e}")
            break




def main():
    try:
        # Extract
        conn_op = dwh.establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
        cursor_op = conn_op.cursor()
        user_data = extract_user_data(cursor_op, row_limit=5)

        # Transform
        transformed_user_data = transform_user_data(user_data, cursor_op)

        # Load
        conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)
        cursor_dwh = conn_dwh.cursor()
        load_user_dim(cursor_dwh, transformed_user_data)

        # Close the connections
        cursor_op.close()
        conn_op.close()
        cursor_dwh.close()
        conn_dwh.close()
    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")


if __name__ == "__main__":
    main()



