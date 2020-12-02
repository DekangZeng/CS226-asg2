#!/bin/bash
mvn clean compile package
spark-submit --master local --class com.testing.SparkTest  /target/SPARK-1.0-SNAPSHOT.jar $1 
