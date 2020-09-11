#!/bin/bash
mvn -f ./createIndex/pom.xml clean
mvn -f ./createIndex/pom.xml package
mv createIndex/target/createIndex-1.0-SNAPSHOT.jar IndexBuilder.jar
mvn -f ./createIndex/pom.xml clean
