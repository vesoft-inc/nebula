/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef NEBULA_GRAPH_ROCKSDBCONFIGOPTIONS_H
#define NEBULA_GRAPH_ROCKSDBCONFIGOPTIONS_H

#include <gtest/gtest_prod.h>
#include "base/Base.h"
#include "rocksdb/db.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/slice_transform.h"
#include "kvstore/Common.h"

namespace nebula {
namespace kvstore {
class RocksdbConfigOptions final {
    FRIEND_TEST(RocksdbEngineOptionsTest, createOptionsTest);

public:
    static rocksdb::Status initRocksdbOptions(rocksdb::Options &baseOpts);
};
}  // namespace kvstore
}  // namespace nebula
#endif  // NEBULA_GRAPH_ROCKSDBCONFIGOPTIONS_H
