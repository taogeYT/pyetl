#!/bin/sh
today=`date +%Y%m%d`
echo $today
cur_dir=$(dirname $0)
cd $cur_dir/../..
home=`pwd`
echo $home
export PYTHONPATH=$home
source /etc/profile

python3 ./etl/task/work.py