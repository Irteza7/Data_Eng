#!/bin/sh

#extract 

cut -d"#" -f1-4 web-server-access-log.txt > extracted-data_tar.txt

#transform 

tr "#" ","  < extracted-data_tar.txt > transformed-data_tar.csv

#load

echo "\c template1; \COPY access_log FROM '/home/project/transformed-data_tar.csv' DELIMITERS ',' CSV HEADER;" | psql --username=postgres --host=localhost