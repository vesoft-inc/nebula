# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
  scheduler_obj
  OBJECT
  Scheduler.cpp
  AsyncMsgNotifyBasedScheduler.cpp
  PushBasedScheduler.cpp
  )
