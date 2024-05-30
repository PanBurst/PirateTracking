@echo off

echo "Initializing config servers"
docker exec config-svr-1 mongosh --eval "rs.initiate({_id: 'config-svr-replica-set', configsvr: true, members: [{ _id: 0, host: 'config-svr-1:27017' }, { _id: 1, host: 'config-svr-2:27017' }, { _id: 2, host: 'config-svr-3:27017' }]});"

echo "Initializing shard 1"
docker exec shard-1-node-a mongosh --eval "rs.initiate({_id: 'shard-1-replica-set', members: [{ _id: 0, host: 'shard-1-node-a:27017' }, { _id: 1, host: 'shard-1-node-b:27017' }, { _id: 2, host: 'shard-1-node-c:27017' }]});"

echo "Initializing shard 2"
docker exec shard-2-node-a mongosh --eval "rs.initiate({_id: 'shard-2-replica-set', members: [{ _id: 0, host: 'shard-2-node-a:27017' }, { _id: 1, host: 'shard-2-node-b:27017' }, { _id: 2, host: 'shard-2-node-c:27017' }]});"

echo "Waiting for the cluster to initialize"
timeout /t 30 /nobreak

echo "Adding shards to the router"
docker exec router mongosh --eval "sh.addShard('shard-1-replica-set/shard-1-node-a:27017'); sh.addShard('shard-2-replica-set/shard-2-node-a:27017');"

echo "Create DB and enable sharding on the collection"
docker exec router mongosh sea --eval "sh.enableSharding('sea'); db.createCollection('vessels'); sh.shardCollection('sea.vessels', { MMSI: 1 }); db.createCollection('filtered_vessels'); sh.shardCollection('sea.filtered_vessels', { MMSI: 1 }); sh.status();"