import pandas as pd
import pyodbc
import dwh_tools as dwh
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, DRIVER


def extract_user_data(cursor_op):
    query = """
    SELECT TOP 200
    u.id, 
    u.first_name, 
    u.last_name, 
    c.city_name,
    ctr.name AS Country, 
    (SELECT COUNT(*) FROM dbo.treasure WHERE owner_id = u.id) AS CacheCount
FROM dbo.user_table u
JOIN dbo.city c ON u.city_city_id = c.city_id
JOIN dbo.country ctr ON c.country_code = ctr.code
    """
    cursor_op.execute(query)
    columns = [column[0] for column in cursor_op.description]
    data = cursor_op.fetchall()
    data_list = [list(row) for row in data]

    # Convert the fetched data to a DataFrame
    user_data = pd.DataFrame(data_list, columns=columns)

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
        user_id = row['id']
        first_name = row['first_name']
        last_name = row['last_name']
        city = row['city_name']
        country = row['Country']

        # Determining dedicator
        cursor_op.execute("SELECT COUNT(*) FROM dbo.Treasure WHERE owner_id = ?", user_id)
        treasures_count = cursor_op.fetchone()[0]
        is_dedicator = treasures_count > 0

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
    experience_changes_df = pd.DataFrame(experience_changes)
    return experience_changes_df


def load_user_dim(cursor_dwh, experience_changes_df, table_name='UserDim'):
    for index, change in experience_changes_df.iterrows():
        user_id = change['UserID']
        new_experience_level = change['ExperienceLevel']
        valid_from = change['ValidFrom']

        # Check for the current latest experience level for the user
        cursor_dwh.execute(f"""
            SELECT UserSK, ExperienceLevel, ValidFrom
            FROM {table_name} 
            WHERE UserID = ? AND IsActive = 1
            ORDER BY ValidFrom DESC
        """, (user_id,))
        result = cursor_dwh.fetchone()

        # If there's no existing record or the experience level has changed, insert a new record
        if not result or (result and result[1] != new_experience_level):
            # End-date the previous record if it exists
            if result:
                previous_user_sk = result[0]
                # Calculate the EndDate for the previous record
                end_date = valid_from
                cursor_dwh.execute(f"""
                    UPDATE {table_name} 
                    SET EndDate = ?, IsActive = 0
                    WHERE UserSK = ?
                """, (end_date, previous_user_sk))

            cursor_dwh.execute(f"""
                INSERT INTO {table_name} (
                    UserID, FirstName, LastName, City, Country, ExperienceLevel, IsDedicator, ValidFrom, EndDate, IsActive
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 1)
            """, (
                user_id,
                change['FirstName'],
                change['LastName'],
                change['City'],
                change['Country'],
                change['ExperienceLevel'],
                change['IsDedicator'],
                change['ValidFrom']
            ))

        cursor_dwh.commit()


def main():
    try:
        # Extract
        conn_op = dwh.establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, DRIVER)
        cursor_op = conn_op.cursor()
        user_data = extract_user_data(cursor_op)

        # Transform
        experience_changes_df = transform_user_data(user_data, cursor_op)

        # Load
        conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, DRIVER)
        cursor_dwh = conn_dwh.cursor()
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





