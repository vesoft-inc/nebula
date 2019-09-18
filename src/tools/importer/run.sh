# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

mvn compile exec:java \
    -Dexec.mainClass="com.vesoft.nebula.importer.Importer" \
    -Dexec.args="$1" \
    -Dexec.classpathScope=compile
