@echo off

REM Initialize Config Servers
docker exec config-svr-1 mongo --eval "rs.initiate({_id: 'config-svr-replica-set', configsvr: true, members: [{ _id: 0, host: 'config-svr-1:27017' }, { _id: 1, host: 'config-svr-2:27017' }]}); rs.status();"

REM Initialize Shard 1
docker exec shard-1-node-a mongo --eval "rs.initiate({_id: 'shard-1-replica-set', members: [{ _id: 0, host: 'shard-1-node-a:27017' }, { _id: 1, host: 'shard-1-node-b:27017' }]}); rs.status();"

REM Initialize Shard 2
docker exec shard-2-node-a mongo --eval "rs.initiate({_id: 'shard-2-replica-set', members: [{ _id: 0, host: 'shard-2-node-a:27017' }, { _id: 1, host: 'shard-2-node-b:27017' }]}); rs.status();"

REM Add Shards to the Router
docker exec router mongo --eval "sh.addShard('shard-1-replica-set/shard-1-node-a:27017'); sh.addShard('shard-2-replica-set/shard-2-node-a:27017'); sh.status();"

echo MongoDB sharded cluster initialization complete.
