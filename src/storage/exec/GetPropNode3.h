/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_EXEC_GETPROPNODE3_H_
#define STORAGE_EXEC_GETPROPNODE3_H_

#include <vector>

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "common/meta/SchemaProviderIf.h"
#include "common/thrift/ThriftTypes.h"
#include "storage/CommonUtils.h"
#include "storage/context/StorageExpressionContext.h"
#include "storage/exec/RelNode.h"
namespace nebula::storage {

class TagScan final : public IterateNode<std::vector<VertexID>> {
 public:
  using RelNode::doExecute;
  TagScan(RuntimeContext* context,
          TagContext* ctx,
          TagID tagId,
          const std::vector<PropContext>* props);
  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const std::vector<VertexID>& vId) override;
  nebula::cpp2::ErrorCode collectTagPropsIfValid(NullHandler nullHandler, PropHandler valueHandler);
  bool valid() const override;
  void next() override;
  folly::StringPiece key() const override;
  folly::StringPiece val() const override;
  RowReader* reader() const override;
  const std::string& getTagName() const;
  TagID tagId() const;

 private:
  RuntimeContext* context_;
  TagContext* tagContext_;
  TagID tagId_;
  const std::vector<PropContext>* props_;
  const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
  std::optional<std::pair<std::string, int64_t>> ttl_;
  std::string tagName_;

  mutable std::string key_;
  mutable std::string value_;
  mutable RowReaderWrapper reader_;

  size_t next_ = 0;
  std::vector<std::string> keys_;
  std::vector<std::string> values_;
  std::vector<nebula::Status> statuses_;
  mutable bool valid_ = false;
};

class GetTagPropNode3 : public QueryNode<std::vector<VertexID>> {
 public:
  using RelNode<std::vector<VertexID>>::doExecute;
  GetTagPropNode3(RuntimeContext* context,
                  std::vector<TagScan*> tagScans,
                  nebula::DataSet* resultDataSet,
                  Expression* filter,
                  std::size_t limit);
  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const std::vector<VertexID>& vIds) override;

 private:
  RuntimeContext* context_;
  std::vector<TagScan*> tagScans_;
  nebula::DataSet* resultDataSet_;
  std::unique_ptr<StorageExpressionContext> expCtx_{nullptr};
  Expression* filter_{nullptr};
  const std::size_t limit_{std::numeric_limits<std::size_t>::max()};
};

class EdgeScan : public IterateNode<std::vector<cpp2::EdgeKey>> {
 public:
  using RelNode::doExecute;
  EdgeScan(RuntimeContext* context,
           EdgeContext* edgeContext,
           EdgeType edgeType,
           const std::vector<PropContext>* props);
  const std::string& getEdgeName() const;
  EdgeType edgeType() const;
  nebula::cpp2::ErrorCode collectEdgePropsIfValid(NullHandler nullhandler,
                                                  PropHandler valueHandler);

  bool valid() const override;
  void next() override;
  folly::StringPiece key() const override;
  folly::StringPiece val() const override;
  RowReader* reader() const override;
  nebula::cpp2::ErrorCode doExecute(PartitionID partId,
                                    const std::vector<cpp2::EdgeKey>& edgeKeys) override;

 private:
  RuntimeContext* context_;
  EdgeContext* edgeContext_;
  EdgeType edgeType_;
  const std::vector<PropContext>* props_;

  const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
  std::optional<std::pair<std::string, int64_t>> ttl_;
  std::string edgeName_;

  mutable RowReaderWrapper reader_;
  mutable std::string key_;
  mutable std::string val_;

  std::vector<std::string> keys_;
  std::vector<std::string> values_;
  std::vector<nebula::Status> statuses_;
  size_t next_ = 0;
  mutable bool valid_ = false;
};

class GetEdgePropNode3 : public QueryNode<std::vector<cpp2::EdgeKey>> {
 public:
  using RelNode::doExecute;
  GetEdgePropNode3(RuntimeContext* context,
                   std::vector<EdgeScan*> edgeScans,
                   nebula::DataSet* resultDataSet,
                   Expression* filter,
                   std::size_t limit);
  nebula::cpp2::ErrorCode doExecute(PartitionID partId,

                                    const std::vector<cpp2::EdgeKey>& edgeKeys) override;

 private:
  RuntimeContext* context_;
  std::vector<EdgeScan*> edgeScans_;
  nebula::DataSet* resultDataSet_;
  std::unique_ptr<StorageExpressionContext> expCtx_{nullptr};
  Expression* filter_{nullptr};
  const std::size_t limit_{std::numeric_limits<std::size_t>::max()};
};

}  // namespace nebula::storage

#endif
