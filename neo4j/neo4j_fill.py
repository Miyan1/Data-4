import csv
from neo4j import GraphDatabase

def clear_database(driver):
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

def create_constraints(driver):
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (h:Hunter) REQUIRE h.hunterID IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Treasure) REQUIRE t.TreasureID IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:City) REQUIRE c.CityName IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (co:Country) REQUIRE co.CountryName IS UNIQUE"
    ]
    with driver.session() as session:
        for constraint in constraints:
            session.run(constraint)

def create_graph_from_csv(file_path, batch_size=1000):
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "pamela12"))

    query = (
        "MERGE (h:Hunter {hunterID: $hunter_id}) "
        "ON CREATE SET h.fullName = $hunter_name "
        "MERGE (t:Treasure {TreasureID: $treasure_id}) "
        "MERGE (c:City {CityName: $city_name}) "
        "MERGE (co:Country {CountryName: $country_name}) "
        "MERGE (h)-[:SEEKS]->(t) "
        "MERGE (t)-[:LOCATED_IN]->(c) "
        "MERGE (c)-[:IN_COUNTRY]->(co)"
    )

    def execute_batch(session, batch):
        for row in batch:
            if len(row) == 6:  # Ensure the row has exactly six columns
                hunter_id, hunter_name, log_id, treasure_id, city_name, country_name = row
                session.run(query, {
                    "hunter_id": hunter_id,
                    "hunter_name": hunter_name,
                    "treasure_id": treasure_id,
                    "city_name": city_name,
                    "country_name": country_name
                })

    with driver.session() as session:
        with open(file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            batch = []
            row_count = 0
            for row in reader:
                batch.append(row)
                if len(batch) >= batch_size:
                    execute_batch(session, batch)
                    batch = []
                    row_count += batch_size
                    print(f"Processed {row_count} rows...")
            if batch:
                execute_batch(session, batch)
                row_count += len(batch)
                print(f"Processed {row_count} rows...")

    driver.close()
    print(f"Finished processing {row_count} rows.")

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "pamela12"))

# clear_database(driver)

create_constraints(driver)

create_graph_from_csv('joined_tables.csv', batch_size=1000)

driver.close()



