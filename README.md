# Governor: A Template for PostgreSQL HA with etcd

This is a fork of https://github.com/compose/governor

for the purpose of creating a Docker container running governor, configurable via ENV variables, with etcd on an external host / container.

Work In Progress - This is currently BETA TESTING

USAGE:

I set up a docker swarm, with two nodes:

 - swarm1: 192.168.42.41
 - swarm2: 192.168.42.42

Let's run a single etcd node for now:

docker -H tcp://0.0.0.0:2375 run -d --name etcd1 -e constraint:node==swarm1 --net host coreos/etcd -addr 192.168.42.41:4001 -peer-addr 192.168.42.41:4002

Now let's run up two governor nodes:

docker -H tcp://0.0.0.0:2375 run -d --name pg1 --net host -e constraint:node==swarm1 -e GOVERNOR_ETCD_HOST=192.168.42.41:4001 -e GOVERNOR_POSTGRESQL_NAME=postgresql1 -e GOVERNOR_POSTGRESQL_LISTEN=192.168.42.41:5432 -e GOVERNOR_POSTGRESQL_DATA_DIR=/data/postgres -e GOVERNOR_POSTGRESQL_REPLICATION_NETWORK=192.168.42.1/24 miketonks/governor

docker -H tcp://0.0.0.0:2375 run -d --name pg2 --net host -e constraint:node==swarm2 -e GOVERNOR_ETCD_HOST=192.168.42.41:4001 -e GOVERNOR_POSTGRESQL_NAME=postgresql2 -e GOVERNOR_POSTGRESQL_LISTEN=192.168.42.42:5432 -e GOVERNOR_POSTGRESQL_DATA_DIR=/data/postgres -e GOVERNOR_POSTGRESQL_REPLICATION_NETWORK=192.168.42.1/24 miketonks/governor

After a short while:

$docker logs pg1

2015-07-10 16:10:32,404 INFO: Governor Starting up: Starting Postgres <br />
2015-07-10 16:10:34,460 INFO: Governor Running: Starting Running Loop <br />
2015-07-10 16:10:39,474 INFO: Lock owner: postgresql1; I am postgresql1 <br />
2015-07-10 16:10:39,476 INFO: Governor Running: no action.  i am the leader with the lock <br />
2015-07-10 16:10:39,476 INFO: Governor Running: I am the Leader <br />
2015-07-10 16:10:39,477 INFO: Governor Running: Create Replication Slot: postgresql2 <br />
2015-07-10 16:10:49,495 INFO: Lock owner: postgresql1; I am postgresql1 <br />
2015-07-10 16:10:49,497 INFO: Governor Running: no action.  i am the leader with the lock <br />
2015-07-10 16:10:49,497 INFO: Governor Running: I am the Leader <br />

$docker logs pg2

2015-07-10 16:10:32,404 INFO: Governor Starting up: Starting Postgres <br />
2015-07-10 16:10:32,416 INFO: Governor Running: Starting Running Loop <br />
FATAL:  the database system is starting up <br />
LOG:  started streaming WAL from primary at 0/3000000 on timeline 1 <br />
LOG:  redo starts at 0/3000028 <br />
LOG:  consistent recovery state reached at 0/30000F0 <br />
LOG:  database system is ready to accept read only connections <br />
2015-07-10 16:10:52,461 INFO: Lock owner: postgresql1; I am postgresql2 <br />
2015-07-10 16:10:52,461 INFO: does not have lock <br />
2015-07-10 16:10:52,465 INFO: Governor Running: no action.  i am a secondary and i am following a leader <br />
2015-07-10 16:11:02,483 INFO: Lock owner: postgresql1; I am postgresql2 <br />
2015-07-10 16:11:02,483 INFO: does not have lock <br />
2015-07-10 16:11:02,487 INFO: Governor Running: no action.  i am a secondary and i am following a leader <br />

Now kill the pg1 node and you will see, after a short while, that pg2 automatically reconfigures and takes over as primary
