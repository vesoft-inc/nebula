#!/bin/bash

mvn clean compile package -DskipTests
mvn test
