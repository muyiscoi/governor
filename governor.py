#!/usr/bin/env python

import sys, os, yaml, time, urllib2, atexit, signal
import logging

from helpers.keystore import Etcd
from helpers.postgresql import Postgresql
from helpers.ha import Ha
from helpers.ascii import splash, showtime

LOG_LEVEL = logging.DEBUG if os.getenv('DEBUG', None) else logging.INFO

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=LOG_LEVEL)

# load passed config file, or default
config_file = 'postgres0.yml'
if len(sys.argv) > 1:
    config_file = sys.argv[1]

with open(config_file, "r") as f:
    config = yaml.load(f.read())

# allow config setting from env, for docker
if os.getenv('GOVERNOR_ETCD_HOST'):
    config['etcd']['host'] = os.getenv('GOVERNOR_ETCD_HOST')

if os.getenv('GOVERNOR_POSTGRESQL_NAME'):
    config['postgresql']['name'] = os.getenv('GOVERNOR_POSTGRESQL_NAME')

if os.getenv('GOVERNOR_POSTGRESQL_CONNECT'):
    config['postgresql']['connect'] = os.getenv('GOVERNOR_POSTGRESQL_CONNECT')

if os.getenv('GOVERNOR_POSTGRESQL_LISTEN'):
    config['postgresql']['listen'] = os.getenv('GOVERNOR_POSTGRESQL_LISTEN')

if os.getenv('GOVERNOR_POSTGRESQL_READ_ONLY_PORT'):
    config['postgresql']['read_only_port'] = os.getenv('GOVERNOR_POSTGRESQL_READ_ONLY_PORT')

if os.getenv('GOVERNOR_POSTGRESQL_DATA_DIR'):
    config['postgresql']['data_dir'] = os.getenv('GOVERNOR_POSTGRESQL_DATA_DIR')

if os.getenv('GOVERNOR_POSTGRESQL_REPLICATION_NETWORK'):
    config['postgresql']['replication']['network'] = os.getenv('GOVERNOR_POSTGRESQL_REPLICATION_NETWORK')

etcd = Etcd(config["etcd"])
postgresql = Postgresql(config["postgresql"])
ha = Ha(postgresql, etcd)


# leave things clean when shutting down, if possible
def shutdown(signal, frame):
    logging.info("Governor Shutting Down: Received Shutdown Signal")
    try:
        if ha.has_lock():
            logging.info("Governor Shutting Down: Abdicating Leadership")
            etcd.abdicate(postgresql.name)

        logging.info("Governor Shutting Down: Removing Membership")
        etcd.delete_member(postgresql.name)
    except:
        pass

    logging.info("Governor Shutting Down: Stopping Postgres")
    postgresql.stop()
    sys.exit(0)

# atexit.register(shutdown)
signal.signal(signal.SIGTERM, shutdown)

# wait for etcd to be available
splash()
logging.info("Governor Starting up: Connect to Etcd")
etcd_ready = False
while not etcd_ready:
    try:
        etcd.touch_member(postgresql.name, postgresql.advertised_connection_string)
        etcd_ready = True
    except urllib2.URLError:
        logging.info("waiting on etcd")
        time.sleep(5)

# is data directory empty?
if postgresql.data_directory_empty():
    logging.info("Governor Starting up: Empty Data Dir")
    # racing to initialize
    if etcd.race("/initialize", postgresql.name):
        logging.info("Governor Starting up: Initialisation Race ... WON!!!")
        logging.info("Governor Starting up: Initialise Postgres")
        postgresql.initialize()
        logging.info("Governor Starting up: Initialise Complete")
        etcd.take_leader(postgresql.name)
        logging.info("Governor Starting up: Starting Postgres")
        postgresql.start(master=True)
    else:
        logging.info("Governor Starting up: Initialisation Race ... LOST")
        time.sleep(20)
        logging.info("Governor Starting up: Sync Postgres from Leader")
        synced_from_leader = False
        while not synced_from_leader:
            leader = etcd.current_leader()
            if not leader:
                time.sleep(5)
                continue
            if postgresql.sync_from_leader(leader):
                logging.info("Governor Starting up: Sync Completed")
                postgresql.write_recovery_conf(leader)
                logging.info("Governor Starting up: Starting Postgres")
                postgresql.start(master=False)
                synced_from_leader = True
            else:
                time.sleep(5)
else:
    logging.info("Governor Starting up: Existing Data Dir")
    postgresql.copy_pg_hba()
    postgresql.follow_no_leader()
    logging.info("Governor Starting up: Starting Postgres")
    postgresql.start(master=False)

showtime()
logging.info("Governor Running: Starting Running Loop")
while True:
    try:
        logging.info("Governor Running: %s" % ha.run_cycle())

        # create replication slots
        if postgresql.is_leader():
            logging.debug("Governor Running: I am the Leader")

            for member in etcd.members():
                member = member['hostname']
                if member != postgresql.name:
                    postgresql.create_replication_slot(member)

        etcd.touch_member(postgresql.name, postgresql.advertised_connection_string)

    except SystemExit as e:
        logging.info("Governor Shutting Down: Exiting Running Loop")
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)

    finally:
        time.sleep(config["loop_wait"])

