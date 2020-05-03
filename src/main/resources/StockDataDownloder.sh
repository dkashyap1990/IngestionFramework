#!/bin/bash

if [[ $# == 0 ]]; then
   echo "Provide timeframe - eg: yyyymmdd"
   exit 1
fi


regex=$1

wget http://data.gdeltproject.org/events/md5sums
for file in `cat md5sums | cut -d' ' -f3 | grep '^'${regex}''` ; do wget http://data.gdeltproject.org/events/$file ; done
md5sum -c md5sums 2>&1 | grep '^'${regex}''
unzip ''${regex}'*.zip'
rm ${regex}*.zip