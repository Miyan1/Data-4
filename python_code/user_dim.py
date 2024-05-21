import pandas as pd
import pyodbc
import dwh_tools as dwh
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER


def create_user_dim_table(cursor_dwh):
    cursor_dwh.execute('''
    IF OBJECT_ID(N'[dbo].[UserDim]', N'U') IS NULL 
    CREATE TABLE [dbo].[UserDim] (
    [UserSK] INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    [UserID] BINARY(16) NOT NULL,
    [FirstName] NVARCHAR(255) NULL,
    [LastName] NVARCHAR(255) NULL,
    [City] NVARCHAR(255) NULL,
    [Country] NVARCHAR(255) NULL,
    [ExperienceLevel] NVARCHAR(50) NULL,
    [IsDedicator] BIT NULL,
    [ValidFrom] DATETIME NOT NULL,
    [EndDate] DATETIME NULL,
    [IsActive] BIT NOT NULL
    )
    ''')
    cursor_dwh.commit()


def extract_user_data(cursor_op):
    query = """
    SELECT
    u.id, 
    u.first_name, 
    u.last_name, 
    c.city_name,
    ctr.name AS Country, 
    (SELECT COUNT(*) FROM dbo.treasure WHERE owner_id = u.id) AS CacheCount
    FROM dbo.user_table u
    JOIN dbo.city c ON u.city_city_id = c.city_id
    JOIN dbo.country ctr ON c.country_code = ctr.code
    WHERE u.first_Name= 'Celine'
    AND u.last_name= 'Kemmer'
    """
    cursor_op.execute(query)
    columns = [column[0] for column in cursor_op.description]
    data = cursor_op.fetchall()
    data_list = [list(row) for row in data]

    # Convert the fetched data to a DataFrame
    user_data = pd.DataFrame(data_list, columns=columns)

    # Print the extracted data
    print(user_data)

    # Calculate if user is a dedicator
    user_data['IsDedicator'] = user_data['CacheCount'].apply(lambda x: int(x > 0))

    # Drop the CacheCount column as it is no longer needed
    user_data.drop('CacheCount', axis=1, inplace=True)

    return user_data


def transform_user_data(user_data, cursor_op):
    found_log_type = 2

    def calculate_experience_level(found_count):
        if found_count == 0:
            return 'Starter'
        elif found_count < 4:
            return 'Amateur'
        elif 4 <= found_count <= 10:
            return 'Professional'
        else:
            return 'Pirate'

    experience_changes = []

    for index, row in user_data.iterrows():
        print(f"[{index}] Transform next user data")
        user_id = row['id']
        first_name = row['first_name']
        last_name = row['last_name']
        city = row['city_name']
        country = row['Country']
        is_dedicator = row['IsDedicator']

        cursor_op.execute("""
            SELECT log_time FROM dbo.treasure_log
            WHERE hunter_id = ? AND log_type = ?
            ORDER BY log_time ASC
        """, user_id, found_log_type)
        log_history = cursor_op.fetchall()

        # Sort by log_time
        log_history = sorted(log_history, key=lambda x: x[0])

        previous_log_date = None
        previous_experience_level = None
        found_count = 0

        for log_entry in log_history:
            found_count += 1
            current_experience_level = calculate_experience_level(found_count)

            # Calculate the EndDate for the previous experience level
            if previous_log_date is not None:
                end_date = previous_log_date - pd.Timedelta(milliseconds=1)
                experience_changes[-1]['EndDate'] = end_date  # Set 'EndDate' for the previous entry

            experience_changes.append({
                'UserID': user_id,
                'FirstName': first_name,
                'LastName': last_name,
                'City': city,
                'Country': country,
                'ExperienceLevel': current_experience_level,
                'IsDedicator': int(is_dedicator),
                'ValidFrom': log_entry[0],
                'EndDate': None
            })

            # Update experience level and log date for the next iteration
            previous_experience_level = current_experience_level
            previous_log_date = log_entry[0]

        # After the loop, set the 'EndDate' of the last experience level to None or a specific cut-off date
        if previous_experience_level:
            experience_changes[-1]['EndDate'] = None

    # Convert the list of dictionaries to a DataFrame
    experience_changes = pd.DataFrame(experience_changes)
    return experience_changes



def load_user_dim(cursor_dwh, experience_changes_df, table_name='UserDim'):
    for index, change in experience_changes_df.iterrows():
        print(f"[{index}] Change experience for next user: {change}")
        user_id = change['UserID']
        new_experience_level = change['ExperienceLevel']
        valid_from = change['ValidFrom']
        city = change['City']
        country = change['Country']

        try:
            # Check for the current latest experience level for the user
            cursor_dwh.execute(f"""
                SELECT UserSK, ExperienceLevel, ValidFrom, City, Country
                FROM {table_name} 
                WHERE UserID = ? AND IsActive = 1
                ORDER BY ValidFrom DESC
            """, (user_id,))
            result = cursor_dwh.fetchone()

            # If there's no existing record or the experience level or address has changed, insert a new record
            if not result or (result and (result[1] != new_experience_level or result[3] != city or result[4] != country)):
                # Check for duplicate records based on all attributes except ValidFrom
                cursor_dwh.execute(f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE UserID = ? AND FirstName = ? AND LastName = ? AND City = ? AND Country = ? AND ExperienceLevel = ? AND IsDedicator = ? 
                """, (
                    user_id,
                    change['FirstName'],
                    change['LastName'],
                    city,
                    country,
                    new_experience_level,
                    change['IsDedicator']
                ))
                duplicate_count = cursor_dwh.fetchone()[0]

                # If no duplicate records found, insert the new record
                if duplicate_count == 0:
                    # End-date the previous record if it exists
                    if result:
                        previous_user_sk = result[0]
                        # Calculate the EndDate for the previous record
                        end_date = valid_from - pd.Timedelta(milliseconds=1)
                        cursor_dwh.execute(f"""
                            UPDATE {table_name} 
                            SET EndDate = ?, IsActive = 0
                            WHERE UserSK = ?
                        """, (end_date, previous_user_sk))

                    # Insert the new record
                    cursor_dwh.execute(f"""
                        INSERT INTO {table_name} (
                            UserID, FirstName, LastName, City, Country, ExperienceLevel, IsDedicator, ValidFrom, EndDate, IsActive
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 1)
                    """, (
                        user_id,
                        change['FirstName'],
                        change['LastName'],
                        city,
                        country,
                        new_experience_level,
                        change['IsDedicator'],
                        valid_from
                    ))

                    print(f"[{index}] Successfully inserted new record for user {user_id}")

            cursor_dwh.commit()
            print(f"[{index}] Commit successful for user {user_id}")

        except Exception as e:
            print(f"Error updating/inserting user {user_id}: {e}")
            cursor_dwh.rollback()

    # Handle the case where city or country is updated without experience level change
    for index, change in experience_changes_df.iterrows():
        user_id = change['UserID']
        city = change['City']
        country = change['Country']

        try:
            # Update city and country if the record is active
            cursor_dwh.execute(f"""
                UPDATE {table_name}
                SET City = ?, Country = ?
                WHERE UserID = ? AND IsActive = 1
            """, (city, country, user_id))
            cursor_dwh.commit()
            print(f"[{index}] Updated city and country for user {user_id}")

        except Exception as e:
            print(f"Error updating city and country for user {user_id}: {e}")
            cursor_dwh.rollback()




def main():
    try:
        # Establish connection to DWH
        conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)
        cursor_dwh = conn_dwh.cursor()

        # Create UserDim table if it doesn't exist
        create_user_dim_table(cursor_dwh)

        # Extract
        conn_op = dwh.establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
        cursor_op = conn_op.cursor()
        user_data = extract_user_data(cursor_op)

        # Transform
        experience_changes_df = transform_user_data(user_data, cursor_op)

        # Load
        load_user_dim(cursor_dwh, experience_changes_df, 'UserDim')

        # Close the connections
        cursor_op.close()
        conn_op.close()
        cursor_dwh.close()
        conn_dwh.close()
    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")

if __name__ == "__main__":
    main()







