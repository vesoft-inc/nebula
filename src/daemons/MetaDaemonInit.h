/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef DAEMONS_METADAEMONINIT_H
#define DAEMONS_METADAEMONINIT_H

#include <memory>

#include "common/base/Status.h"
#include "common/hdfs/HdfsCommandHelper.h"
#include "interface/gen-cpp2/common_types.h"
#include "kvstore/KVStore.h"
#include "webservice/WebService.h"

nebula::ClusterID& metaClusterId();

std::unique_ptr<nebula::kvstore::KVStore> initKV(std::vector<nebula::HostAddr> peers,
                                                 nebula::HostAddr localhost);

nebula::Status initWebService(nebula::WebService* svc, nebula::kvstore::KVStore* kvstore);

nebula::cpp2::ErrorCode initGodUser(nebula::kvstore::KVStore* kvstore,
                                    const nebula::HostAddr& localhost);
#endif
