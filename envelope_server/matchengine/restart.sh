#!/bin/bash

killall -s SIGQUIT envelope.exe
sleep 1
./envelope.exe config.json
