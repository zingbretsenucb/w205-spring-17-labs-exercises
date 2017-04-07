#!/bin/bash

mount -t ext4 /dev/xvdf /data
/data/start_postgres.sh
