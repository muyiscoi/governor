FROM postgres:9.4

RUN apt-get update && \
  apt-get install -y python-psycopg2 python-yaml

RUN mkdir -p /governor/helpers
ADD governor.py /governor/governor.py
ADD helpers /governor/helpers
ADD postgres0.yml /governor/

CMD mkdir -p /data/postgres && \
  chown postgres /data/postgres && \
  chown postgres /governor

WORKDIR /governor

CMD gosu postgres /governor/governor.py


