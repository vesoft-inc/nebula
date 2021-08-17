# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

install(
    FILES
        gflags.json
        date_time_zonespec.csv
    PERMISSIONS
        OWNER_WRITE OWNER_READ
        GROUP_READ
        WORLD_READ
    DESTINATION
        share/resources
    COMPONENT
        common
)
