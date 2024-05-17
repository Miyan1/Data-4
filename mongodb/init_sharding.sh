#!/usr/bin/env bash

docker exec -it mongors1n1 bash -c "echo 'rs.initiate({_id : \"mongors1\", members: [{ _id : 0, host : \"172.25.0.7\" }]})' | mongo"
docker exec -it mongors2n1 bash -c "echo 'rs.initiate({_id : \"mongors2\", members: [{ _id : 0, host : \"172.25.0.5\" }]})' | mongo"
docker exec -it mongors3n1 bash -c "echo 'rs.initiate({_id : \"mongors3\", members: [{ _id : 0, host : \"172.25.0.3\" }]})' | mongo"

docker exec -it mongocfg1 bash -c "echo 'rs.initiate({_id: \"mongors1conf\", configsvr: true, members: [{ _id : 0, host : \"172.25.0.2\" }, { _id : 1, host : \"172.25.0.4\" }, { _id : 2, host : \"172.25.0.6\" }]})' | mongo"

docker exec -it mongos1 bash -c "echo 'sh.addShard(\"mongors1/172.25.0.7\")' | mongo catchem"
docker exec -it mongos1 bash -c "echo 'sh.addShard(\"mongors2/172.25.0.5\")' | mongo catchem"
docker exec -it mongos1 bash -c "echo 'sh.addShard(\"mongors3/172.25.0.3\")' | mongo catchem"
docker exec -it mongos1 bash -c "echo 'sh.status()' | mongo catchem"

docker exec -it mongos1 bash -c "echo 'sh.enableSharding(\"catchem\")' | mongo catchem"

docker exec -it mongos1 bash -c "echo 'db.settings.updateOne({_id:\"chunksize\"}, {\$set: {_id:\"chunksize\",value:1}}, {upsert: true})' | mongo catchem"

docker exec -it mongors1n1 bash -c "echo 'db.createCollection(\"treasure_stages\")' | mongo catchem"

docker exec -it mongos1 bash -c "echo 'sh.shardCollection(\"catchem.treasure_stages\", {\"longitude\" : 1, \"latitude\": 1})' | mongo catchem"
