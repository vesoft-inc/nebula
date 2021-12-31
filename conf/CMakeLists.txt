# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

# These configuration files are for reference to generate your own customized ones.
# Thus, they are installed as read-only, even for the owner.

if(NOT ENABLE_STANDALONE_VERSION)

install(
  FILES
    nebula-graphd.conf.default
    nebula-graphd.conf.production
  PERMISSIONS
    OWNER_READ
    GROUP_READ
    WORLD_READ
  DESTINATION
    etc
  COMPONENT
    graph
  )

install(
  FILES
    nebula-metad.conf.default
    nebula-metad.conf.production
  PERMISSIONS
    OWNER_READ
    GROUP_READ
    WORLD_READ
  DESTINATION
    etc
  COMPONENT
    meta
  )

install(
  FILES
    nebula-storaged.conf.default
    nebula-storaged.conf.production
    nebula-storaged-listener.conf.production
  PERMISSIONS
    OWNER_READ
    GROUP_READ
    WORLD_READ
  DESTINATION
    etc
  COMPONENT
    storage
  )

else()

install(
  FILES
    nebula-standalone.conf.default
  PERMISSIONS
    OWNER_READ
    GROUP_READ
    WORLD_READ
  DESTINATION
    etc
  COMPONENT
    graph
  )

endif()
