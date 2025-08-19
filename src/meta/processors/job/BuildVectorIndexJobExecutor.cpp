/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/BuildVectorIndexJobExecutor.h"

#include "common/base/Base.h"
#include "common/time/WallClock.h"
#include "common/utils/MetaKeyUtils.h"
#include "interface/gen-cpp2/storage_types.h"
#include "meta/processors/Common.h"

namespace nebula {
namespace meta {

nebula::cpp2::ErrorCode BuildVectorIndexJobExecutor::check() {
  // Parameters validation: [index_name, is_tag, prop_name, tag nums, tag_name_list, ann_type,
  // ann_params...]
  indexName_ = paras_[0];
  auto tagsNum = std::stoi(paras_[3]);
  annType_ = paras_[4 + tagsNum];
  // Validate ANN type
  if (annType_ != "HNSW" && annType_ != "IVF") {
    LOG(ERROR) << "Unsupported ANN type: " << annType_;
    return nebula::cpp2::ErrorCode::E_INVALID_PARM;
  }

  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode BuildVectorIndexJobExecutor::prepare() {
  // The value of paras_ are index name
  auto spaceRet = spaceExist();
  if (spaceRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Can't find the space, spaceId " << space_;
    return spaceRet;
  }

  std::string indexValue;
  IndexID indexId = -1;

  auto indexKey = MetaKeyUtils::indexIndexKey(space_, indexName_);
  auto retCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &indexValue);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Get indexKey error indexName: " << indexName_
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  indexId = *reinterpret_cast<const IndexID*>(indexValue.c_str());
  taskParameters_.emplace_back(folly::to<std::string>(indexId));
  taskParameters_.insert(taskParameters_.end(), paras_.begin() + 1, paras_.end());
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
