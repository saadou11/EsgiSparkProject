#!/bin/bash

$SPARK_HOME/bin/spark-submit \
--master local[*] \
--properties-file application.properties \
--files $1/transaction.csv,$1/clients.csv,$1/items.csv \
--class Main \
target/SparkProject-1.2.jar
