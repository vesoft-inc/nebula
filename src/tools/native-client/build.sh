#!/bin/bash

mvn clean package -X -DNEBULA_BUILD_VERSION=$1 -DBUILD_TARGET=$2 -f $3/pom.xml
