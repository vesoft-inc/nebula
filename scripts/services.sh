#! /bin/bash

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

SCRIPT_PATH=$(cd "$(dirname "$0")";pwd)

if [ $# != 1 ]; then
  echo "USAGE: $0 <start|stop|restart|status|kill>"
  exit 1;
fi

if [ -z $NEBULA_HOME ]; then
  echo "NEBULA_HOME is not setting, using /usr/local as default value"
  NEBULA_HOME=/usr/local
fi

echo "Processing Meta Service ..."

if [ ! -f ${SCRIPT_PATH}"/meta.hosts" ]; then
  echo "${SCRIPT_PATH}/metas not exist"
  exit 0
fi

for host in `cat ${SCRIPT_PATH}/meta.hosts`
do
  echo "start $host"
  if [ $1 = "status" ]; then
    ssh $host "${NEBULA_HOME}/scripts/nebula.service -c ${NEBULA_HOME}/etc/nebula-metad.conf $1 metad"
  else
    ssh $host "${NEBULA_HOME}/scripts/nebula.service -c ${NEBULA_HOME}/etc/nebula-metad.conf $1 metad > /dev/null 2>&1 &"
  fi
done

echo "Processing Storage Service ..."

if [ ! -f ${SCRIPT_PATH}"/storage.hosts" ]; then
  echo "${SCRIPT_PATH}/storage.hosts not exist"
  exit 0
fi

for host in `cat ${SCRIPT_PATH}/storage.hosts`
do
  echo "start $host"
  if [ $1 = "status" ]; then
    ssh $host "${NEBULA_HOME}/scripts/nebula.service -c ${NEBULA_HOME}/etc/nebula-storaged.conf $1 storaged"
  else
    ssh $host "${NEBULA_HOME}/scripts/nebula.service -c ${NEBULA_HOME}/etc/nebula-storaged.conf $1 storaged > /dev/null 2>&1 &"
  fi
done

echo "Processing Graph Service ..."

if [ ! -f ${SCRIPT_PATH}"/graph.hosts" ]; then
  echo "${SCRIPT_PATH}/graph.hosts not exist"
  exit 0
fi

for host in `cat ${SCRIPT_PATH}/graph.hosts`
do
  echo "start $host"
  if [ $1 = "status" ]; then
    ssh $host "${NEBULA_HOME}/scripts/nebula.service -c ${NEBULA_HOME}/etc/nebula-graphd.conf $1 graphd"
  else
    ssh $host "${NEBULA_HOME}/scripts/nebula.service -c ${NEBULA_HOME}/etc/nebula-graphd.conf $1 graphd > /dev/null 2>&1 &"
  fi
done
