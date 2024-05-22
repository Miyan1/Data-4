# Exam Queries

## XML Transformations Query

```SQL
-- COMPARE COUNTRY WITH COUNTRY2
SELECT
    c1.name,
    c2.name
FROM
    country c1
INNER JOIN
    country2 c2 ON c1.code = c2.code
                AND c1.code3 = c2.code3
                AND c1.name = c2.name;

-- COMPARE CITY WITH CITY2
SELECT
    c1.city_name AS city_name_only_city,
    c2.city_name AS city_name_only_city2,
    CASE
        WHEN c2.city_name IS NULL THEN NULL
        ELSE c1.city_name
    END AS identical_city_name
FROM city c1
LEFT JOIN city2 c2 ON c1.city_id = c2.city_id
                    AND c1.city_name = c2.city_name
                    AND c1.latitude = c2.latitude
                    AND c1.longitude = c2.longitude
                    AND c1.postal_code = c2.postal_code;
```

## SCD Query Test for User Dimension

**Original city_id for Celine Kemmer=** 0xAD046FCE93414921B6120478ED86ED0A (Kuala Lumpur, Malasya)

**city_id test=** 0xC394696E54704B06ACF9B1CEFBE0FD8D   (Aisawa, Japan)

- Run query in the original db to change city for the user:
```SQL
UPDATE [catchem].[dbo].[user_table]
SET [city_city_id] = CONVERT(binary(16), '0xC394696E54704B06ACF9B1CEFBE0FD8D', 1)
WHERE [first_name] = 'Celine'
AND [last_name] = 'Kemmer';
```

- Run the Python Script again with a filter in the query for fast results: 
```SQL
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
```

A new row will be added with the user new city and country, but still keep a history of the user's old address (city and country). If you run the script again without changing in the source db, nothing will be updated as duplicate rows aren't allowed. 

The row Valid_From hasn't been implemented to track manual changes, but only to retrieve log_time from treasure_log, so the date will be the same if you don't update the log in the source db.

## mongodb

### Initialize the cluster

```
docker-compose up -d
# wait for a minute
./init_sharding.sh
```

### Import the data

```
python3 import.py
```

### Check sharding status

```
docker exec -it mongos1 bash -c "echo 'db.printShardingStatus()' | mongo --quiet catchem"
```

### Get the count of objects in the table

```
docker exec -it mongos1 bash -c "echo 'db.treasure_stages.count()' | mongo --quiet catchem"
```

### Query for records from Aland Islands (AX), for city Mariehamn
```
docker exec -it mongos1 bash -c "echo 'db[\"treasure_stages\"].find({ country_code: \"AX\", city_name: \"Mariehamn\" })' | mongo --quiet catchem"
```