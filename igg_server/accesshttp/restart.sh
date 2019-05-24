#!/bin/bash

killall -s SIGQUIT igg_http
sleep 1
./igg_http config.json
