#!/bin/bash

MYSQL_HOST="localhost"
MYSQL_USER="root"
MYSQL_PASS="root890*()"
MYSQL_DB_HISTORY="trade_history"
MYSQL_DB_LOG="trade_log"

for i in `seq 0 99`
do
    echo "alter table user_deal_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB_HISTORY -e "ALTER TABLE user_deal_history_$i ADD token VARCHAR(30) NOT NULL, ADD token_rate DECIMAL(30,8) NOT NULL, ADD asset_rate DECIMAL(30,8) NOT NULL, ADD discount DECIMAL(30,4) NOT NULL, ADD deal_token DECIMAL(30,16) NOT NULL;"
done

for i in `seq 0 99`
do
    echo "alter table order_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB_HISTORY -e "ALTER TABLE order_history_$i ADD token VARCHAR(30) NOT NULL, ADD token_rate DECIMAL(30,8) NOT NULL, ADD asset_rate DECIMAL(30,8) NOT NULL, ADD discount DECIMAL(30,4) NOT NULL, ADD deal_token DECIMAL(30,16) NOT NULL;" 
done

for i in `seq 0 99`
do
    echo "alter table order_detail_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB_HISTORY -e "ALTER TABLE order_detail_$i ADD token VARCHAR(30) NOT NULL, ADD token_rate DECIMAL(30,8) NOT NULL, ADD asset_rate DECIMAL(30,8) NOT NULL, ADD discount DECIMAL(30,4) NOT NULL, ADD deal_token DECIMAL(30,16) NOT NULL;"
done

for i in `seq 0 99`
do
    echo "alter table deal_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB_HISTORY -e "ALTER TABLE deal_history_$i ADD token VARCHAR(30) NOT NULL, ADD token_rate DECIMAL(30,8) NOT NULL, ADD asset_rate DECIMAL(30,8) NOT NULL, ADD discount DECIMAL(30,4) NOT NULL, ADD deal_token DECIMAL(30,16) NOT NULL;"
done

echo "alter table alter_slice_order_example"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB_LOG -e "ALTER TABLE slice_order_example ADD token VARCHAR(30) NOT NULL, ADD discount DECIMAL(30,4) NOT NULL, ADD token_rate DECIMAL(30,8) NOT NULL, ADD asset_rate DECIMAL(30,8) NOT NULL, ADD deal_token DECIMAL(30,16) NOT NULL;"
