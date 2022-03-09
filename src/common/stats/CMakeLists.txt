# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    stats_obj
    OBJECT
    StatsManager.cpp
)
nebula_add_library(
    metrics_obj
    OBJECT
    Metrics.cpp
    Histogram.cpp
    Counter.cpp
    Gauge.cpp
)
nebula_add_subdirectory(test)
