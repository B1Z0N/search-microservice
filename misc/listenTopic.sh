#!/bin/bash

kafka_2.12-2.4.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$1"
