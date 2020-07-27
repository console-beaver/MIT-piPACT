#!/bin/bash
cd /home/pi
if [ -f /home/pi/pi_pact_scan.csv ]; then 
	mv /home/pi/pi_pact_scan.csv /home/pi/pi_pact_scan.csv.`date '+%Y%m%d%H%M%S'` 
fi   
taskset -c 3 python_pact pi_pact.py --config_yml pi_pact_config.yml --tx_power -60 &>/dev/null


