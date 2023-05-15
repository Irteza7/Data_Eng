#!/bin/sh

dbname=$(mysql -e "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '$1'" | grep $1)

if [ ! -d $2 ]; then
    mkdir $2
fi

if [ $1 == $dbname]; then
    sqlfile=$2/$1-$(date +%d-%m-%Y).sql
    if mysqldump $1 > $sqlfile; then
    echo 'SQL dump created!'
    else 
        echo 'Error creating backup'
    fi
else
    echo "Database doesn't exist"
    