#!/bin/bash

MYSQL_HOST="localhost"
MYSQL_USER="root"
MYSQL_PASS="root890*()"
MYSQL_DB="trade_history"

for i in `seq 0 99`
do
    echo "clear table balance_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "delete from balance_history_$i;" 
done

for i in `seq 0 99`
do
    echo "clear table user_deal_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "delete from user_deal_history_$i;" 
done

for i in `seq 0 99`
do
    echo "clear table order_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "delete from order_history_$i;"
done

for i in `seq 0 99`
do
    echo "clear table order_detail_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "delete from order_detail_$i;"
done

for i in `seq 0 99`
do
    echo "clear table deal_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "delete from deal_history_$i;"
done
