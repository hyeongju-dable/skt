#!/bin/bash -
su postgres -c "psql -c \"ALTER USER postgres WITH PASSWORD 'postpassword'\""
