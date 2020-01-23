#!/bin/bash

(cd kafka_2.12-2.4.0 && bin/connect-standalone.sh connect_config/connect-standalone.properties connect_config/elasticsearch-connect.properties) 
