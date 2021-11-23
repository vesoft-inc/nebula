/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_METASTREAMSERVICE_H_
#define META_METASTREAMSERVICE_H_

#include "common/base/Base.h"
#include "interface/gen-cpp2/MetaStreamService.h"
#include "meta/MetaCache.h"

namespace nebula {
namespace meta {

class MetaStreamService final : public cpp2::MetaStreamServiceSvIf {
public:
  explicit MetaStreamService(MetaCache* cache) : cache_(cache) {}

  ::apache::thrift::ServerStream<cpp2::MetaData> readMetaData(int64_t lastRevision)
      override;

private:
  MetaCache* cache_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METASTREAMSERVICE_H_

