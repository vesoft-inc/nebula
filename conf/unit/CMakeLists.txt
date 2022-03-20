# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

if(NOT ENABLE_STANDALONE_VERSION)

install(
  FILES
    nebula-graphd.service
  PERMISSIONS
    OWNER_WRITE OWNER_READ
    GROUP_READ
    WORLD_READ
  DESTINATION
    etc/unit
  COMPONENT
    graph
)

install(
  FILES
    nebula-metad.service
  PERMISSIONS
    OWNER_WRITE OWNER_READ
    GROUP_READ
    WORLD_READ
  DESTINATION
    etc/unit
  COMPONENT
    meta
)

install(
  FILES
    nebula-storaged.service
  PERMISSIONS
    OWNER_WRITE OWNER_READ
    GROUP_READ
    WORLD_READ
  DESTINATION
    etc/unit
  COMPONENT
    storage
)

install(
  FILES
    nebula.service
  PERMISSIONS
    OWNER_WRITE OWNER_READ
    GROUP_READ
    WORLD_READ
  DESTINATION
    etc/unit
  COMPONENT
    graph
)

else()

install(
  FILES
    nebula.service
  PERMISSIONS
    OWNER_WRITE OWNER_READ
    GROUP_READ
    WORLD_READ
  DESTINATION
    etc/unit
  COMPONENT
    graph
)

endif()
