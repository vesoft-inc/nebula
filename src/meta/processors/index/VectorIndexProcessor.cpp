/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/index/VectorIndexProcessor.h"

#include "common/base/CommonMacro.h"
#include "common/plugin/fulltext/elasticsearch/ESAdapter.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace meta {

void CreateVectorIndexProcessor::process(const cpp2::CreateVectorIndexReq& req) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  const auto& index = req.get_index();
  const std::string& name = req.get_vector_index_name();
  CHECK_SPACE_ID_AND_RETURN(index.get_space_id());

  // Check tag or edge exist like FTIndex does
  auto isEdge = index.get_depend_schema().getType() == nebula::cpp2::SchemaID::Type::edge_type;
  auto schemaPrefix = isEdge ? MetaKeyUtils::schemaEdgePrefix(
                                   index.get_space_id(), index.get_depend_schema().get_edge_type())
                             : MetaKeyUtils::schemaTagPrefix(
                                   index.get_space_id(), index.get_depend_schema().get_tag_id());

  auto retPre = doPrefix(schemaPrefix);
  if (!nebula::ok(retPre)) {
    auto retCode = nebula::error(retPre);
    LOG(INFO) << "Tag or Edge Prefix failed, " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto iter = nebula::value(retPre).get();
  if (!iter->valid()) {
    LOG(INFO) << "Edge or Tag could not be found";
    auto eCode = isEdge ? nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND
                        : nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
    handleErrorCode(eCode);
    onFinished();
    return;
  }

  // Verify the field exists and is a vector
  auto schema = MetaKeyUtils::parseSchema(iter->val());
  auto columns = schema.get_columns();
  auto targetCol = std::find_if(columns.begin(), columns.end(), [&index](const auto& c) {
    return index.get_field() == c.get_name();
  });
  if (targetCol == columns.end()) {
    LOG(INFO) << "Column not found: " << index.get_field();
    auto eCode = isEdge ? nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND
                        : nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
    handleErrorCode(eCode);
    onFinished();
    return;
  }

  // Check vector index exist
  auto vectorPrefix = MetaKeyUtils::vectorIndexPrefix();
  auto ret = doPrefix(vectorPrefix);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(INFO) << "Vector Index Prefix failed, " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto it = nebula::value(ret).get();
  while (it->valid()) {
    auto indexName = MetaKeyUtils::parseVectorIndexName(it->key());
    auto indexItem = MetaKeyUtils::parseVectorIndex(it->val());
    if (indexName == name) {
      LOG(INFO) << "Vector index exists: " << indexName;
      handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
      onFinished();
      return;
    }
    it->next();
  }

  // Store the index
  std::vector<kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::vectorIndexKey(name), MetaKeyUtils::vectorIndexVal(index));
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto result = doSyncPut(std::move(data));
  handleErrorCode(result);
  onFinished();
}

void DropVectorIndexProcessor::process(const cpp2::DropVectorIndexReq& req) {
  CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());

  auto indexKey = MetaKeyUtils::vectorIndexKey(req.get_vector_index_name());
  auto ret = doGet(indexKey);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      LOG(INFO) << "Drop vector index failed, Vector index not exists.";
      handleErrorCode(nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND);
      onFinished();
      return;
    }
    LOG(INFO) << "Drop vector index failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto batchHolder = std::make_unique<kvstore::BatchHolder>();
  batchHolder->remove(std::move(indexKey));
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(batchHolder.get(), timeInMilliSec);
  auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
  doBatchOperation(std::move(batch));
}

void ListVectorIndexesProcessor::process(const cpp2::ListVectorIndexesReq&) {
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  const auto& prefix = MetaKeyUtils::vectorIndexPrefix();
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "List vector indexes failed, error: "
              << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(iterRet).get();
  std::unordered_map<std::string, cpp2::VectorIndex> indexes;
  while (iter->valid()) {
    auto name = MetaKeyUtils::parseVectorIndexName(iter->key());
    auto index = MetaKeyUtils::parseVectorIndex(iter->val());
    indexes.emplace(std::move(name), std::move(index));
    iter->next();
  }

  resp_.indexes_ref() = std::move(indexes);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
