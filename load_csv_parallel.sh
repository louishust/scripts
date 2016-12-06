#!/bin/bash

if [ $# -ne 8 ]
then
  echo "Usage: $0 mysql-user mysql-password mysql-host mysql-port mysql-database mysql-table threads csv-file"
  exit -1
fi

MYSQL_USER=$1
MYSQL_PWD=$2
MYSQL_HOST=$3
MYSQL_PORT=$4
MYSQL_DB=$5
MYSQL_TABLE=$6
THREADS=$7
CSV_FILE=$8
FAIL=0
YELLOW='\033[0;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
SPLIT_LINES=5000
CHUNK="chunk"

start=$SECONDS

echo -e "${YELLOW}Starting load data from $CSV_FILE into table $MYSQL_DB.$MYSQL_TABLE"

lines=`wc -l $CSV_FILE | awk {'print $1'}`
chunksize=$((lines/THREADS + 4))

split -a2 -l $chunksize $CSV_FILE mysplit


i=1
for file in `find -name "mysplit*"`
do
  realfile=`readlink -f $file`
  tmpfile=$CHUNK$i
  i=$((i+1))
  pt-fifo-split --force $realfile --fifo $tmpfile --lines=$SPLIT_LINES &
done

sleep 1;


myload() {
  i=$1
  file=$CHUNK$i
  realfile=`readlink -f $file`
  while [ -e $realfile ]; do
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PWD -P$MYSQL_PORT $MYSQL_DB -A -e "LOAD DATA INFILE '$realfile' INTO TABLE $MYSQL_TABLE"
  done
}


i=1
for file in `find -name "mysplit*"`
do
  myload $i &
  i=$((i+1))
done

for job in `jobs -p`
do
  wait $job || let "FAIL+=1"
done

duration=$(( SECONDS - start ))
for file in `find -name "mysplit*"`
do
  realfile=`readlink -f $file`
  rm $realfile
done

if [ "$FAIL" == "0" ];
then
  echo -e "${GREEN}LOAD DATA FROM $CSV_FILE INTO $MYSQL_DB.$MYSQL_TABLE Finished!"
  echo -e "${GREEN}Time Cost ($duration) seconds"
else
  echo -e "${RED}LOAD DATA FAIL($FAIL)! "
fi
