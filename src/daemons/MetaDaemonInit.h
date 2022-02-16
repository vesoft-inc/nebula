/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef DAEMONS_METADAEMONINIT_H
#define DAEMONS_METADAEMONINIT_H

#include <memory>  // for unique_ptr
#include <vector>  // for vector

#include "common/base/Status.h"  // for Status
#include "common/hdfs/HdfsCommandHelper.h"
#include "common/thrift/ThriftTypes.h"  // for ClusterID
#include "kvstore/KVStore.h"
#include "webservice/WebService.h"

namespace nebula {
class WebService;
namespace hdfs {
class HdfsCommandHelper;
}  // namespace hdfs
namespace kvstore {
class KVStore;
}  // namespace kvstore
namespace thread {
class GenericThreadPool;
}  // namespace thread
struct HostAddr;

class WebService;
namespace hdfs {
class HdfsCommandHelper;
}  // namespace hdfs
namespace kvstore {
class KVStore;
}  // namespace kvstore
namespace thread {
class GenericThreadPool;
}  // namespace thread
struct HostAddr;
}  // namespace nebula

nebula::ClusterID& metaClusterId();

std::unique_ptr<nebula::kvstore::KVStore> initKV(std::vector<nebula::HostAddr> peers,
                                                 nebula::HostAddr localhost);

nebula::Status initWebService(nebula::WebService* svc,
                              nebula::kvstore::KVStore* kvstore,
                              nebula::hdfs::HdfsCommandHelper* helper,
                              nebula::thread::GenericThreadPool* pool);
#endif
