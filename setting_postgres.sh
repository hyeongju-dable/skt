#!/bin/bash -
bash /etc/init.d/postgresql start
su postgres -c "psql -c \"ALTER USER postgres WITH PASSWORD 'postpassword'\""
