#!/bin/bash

set -e

GOVERNOR_POSTGRESQL_LISTEN=`hostname -I`:5432 exec gosu postgres /governor/governor.py

exec "$@"
