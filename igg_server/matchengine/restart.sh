#!/bin/bash

killall -s SIGQUIT igg_mhg
sleep 1
./igg_mhg config.json
