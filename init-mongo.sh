#!/bin/bash

echo "Initializing Config Server..."
mongosh --host mongo-config:27019 --eval 'rs.initiate({_id: "configRS", configsvr: true, members: [{_id: 0, host: "mongo-config:27019"}]})'

echo "Initializing Shard..."
mongosh --host mongo-shard:27018 --eval 'rs.initiate({_id: "shard1RS", members: [{_id: 0, host: "mongo-shard:27018"}]})'

echo "Waiting for initialization..."
sleep 15

echo "Adding Shard to Router..."
mongosh --host mongo-router:27017 --eval 'sh.addShard("shard1RS/mongo-shard:27018")'

echo "Sharding initialization completed"
