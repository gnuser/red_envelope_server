#!/bin/bash

killall -s SIGQUIT el_http.exe 
sleep 1
./el_http.exe config.json
