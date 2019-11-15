# - Try to find Prometheus includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Prometheus)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Prometheus_FOUND            System has prometheus, include and lib dirs found.
#  Prometheus_INCLUDE_DIR      The prometheus includes directories.
#  Prometheus_LIBRARY          The prometheus library.

find_path(Prometheus_INCLUDE_DIR NAMES exposer.h)
find_library(Prometheus_LIBRARY NAMES prometheus-cpp-core.a prometheus-cpp-pull.a)

if(Prometheus_INCLUDE_DIR AND Prometheus_LIBRARY)
    set(Prometheus_FOUND TRUE)
    mark_as_advanced(
        Prometheus_INCLUDE_DIR
        Prometheus_LIBRARY
    )
endif()

if(NOT Prometheus_FOUND)
    message(FATAL_ERROR "Prometheus doesn't exist")
endif()


