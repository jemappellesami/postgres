PATH=/usr/local/pgsql/bin:$PATH
pg_ctl -D /usr/local/pgsql/data stop
pg_ctl -D /usr/local/pgsql/data start
