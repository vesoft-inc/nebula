/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_METASERVICEHANDLER_H_
#define META_METASERVICEHANDLER_H_

#include "base/Base.h"
#include "interface/gen-cpp2/MetaService.h"
#include "kvstore/include/KVStore.h"
#include <folly/RWSpinLock.h>

namespace nebula {
namespace meta {

class MetaServiceHandler final : public cpp2::MetaServiceSvIf {
public:
    explicit MetaServiceHandler(kvstore::KVStore* kv) : kvstore_(kv) {}

    folly::Future<cpp2::ExecResponse>
    future_createNode(const cpp2::CreateNodeRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_setNode(const cpp2::SetNodeRequest& req) override;

    folly::Future<cpp2::GetNodeResponse>
    future_getNode(const cpp2::GetNodeRequest& req) override;

    folly::Future<cpp2::ListChildrenResponse>
    future_listChildren(const cpp2::ListChildrenRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_removeNode(const cpp2::RemoveNodeRequest& req) override;

private:
    kvstore::KVStore* kvstore_ = nullptr;
    folly::RWSpinLock lock_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METASERVICEHANDLER_H_
