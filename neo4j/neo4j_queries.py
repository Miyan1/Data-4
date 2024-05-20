from neo4j import GraphDatabase

def find_linked_city(driver, city_name):
    query = (
        "MATCH (:City {CityName: $CityName})<-[:LOCATED_IN]-(:Treasure)<-[:SEEKS]-(hunter:Hunter)-[:SEEKS]->(other_treasure:Treasure)-[:LOCATED_IN]->(other_city:City) "
        "WHERE other_city.CityName <> $CityName "
        "RETURN other_city.CityName AS OtherCity, COUNT(DISTINCT hunter) AS SharedHunters "
        "ORDER BY SharedHunters DESC "
        "LIMIT 1"
    )
    with driver.session() as session:
        result = session.run(query, {"CityName": city_name})
        if result.peek() is None:
            print(f"No linked city found for {city_name}.")
        else:
            for record in result:
                print(f"City {city_name} is linked to city {record['OtherCity']} with {record['SharedHunters']} common hunters.")

def find_fellow_hunters(driver, hunter_name):
    query = (
        "MATCH (h:Hunter {fullName: $HunterName})-[:SEEKS]->(self_treasure:Treasure)<-[:SEEKS]-(fellow:Hunter) "
        "WHERE h <> fellow "
        "WITH fellow, COUNT(DISTINCT self_treasure) AS shared_treasures "
        "ORDER BY shared_treasures DESC "
        "RETURN fellow.fullName AS FellowHunter, shared_treasures AS SharedTreasures"
    )
    with driver.session() as session:
        result = session.run(query, {"HunterName": hunter_name})
        if result.peek() is None:
            print(f"No fellow hunters found for {hunter_name}.")
        else:
            for record in result:
                print(f"Hunter {hunter_name} has {record['SharedTreasures']} common treasures with hunter {record['FellowHunter']}.")

# Usage examples
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "pamela12"))

find_linked_city(driver, "Werder (Havel)")
find_fellow_hunters(driver, "Mariam Frisch")

driver.close()
