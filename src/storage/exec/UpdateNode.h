/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_UPDATENODE_H_
#define STORAGE_EXEC_UPDATENODE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "storage/context/StorageExpressionContext.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/FilterNode.h"
#include "storage/StorageFlags.h"
#include "kvstore/LogEncoder.h"
#include "utils/OperationKeyUtils.h"

namespace nebula {
namespace storage {

template<typename T>
class UpdateNode  : public RelNode<T> {
public:
    UpdateNode(RunTimeContext* context,
               std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes,
               std::vector<storage::cpp2::UpdatedProp>& updatedProps,
               FilterNode<T>* filterNode,
               bool insertable,
               std::vector<std::pair<std::string, std::unordered_set<std::string>>> depPropMap,
               StorageExpressionContext* expCtx,
               bool isEdge)
        : context_(context)
        , indexes_(indexes)
        , updatedProps_(updatedProps)
        , filterNode_(filterNode)
        , insertable_(insertable)
        , depPropMap_(depPropMap)
        , expCtx_(expCtx)
        , isEdge_(isEdge) {}

    nebula::cpp2::ErrorCode checkField(const meta::SchemaProviderIf::Field* field) {
        if (!field) {
            VLOG(1) << "Fail to read prop";
            if (isEdge_) {
                return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
            }
            return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    nebula::cpp2::ErrorCode
    getDefaultOrNullValue(const meta::SchemaProviderIf::Field* field,
                          const std::string& name) {
        if (field->hasDefault()) {
            auto expr = field->defaultValue()->clone();
            props_[field->name()] = Expression::eval(expr, *expCtx_);
        } else if (field->nullable()) {
            props_[name] = Value::kNullValue;
        } else {
            return nebula::cpp2::ErrorCode::E_INVALID_FIELD_VALUE;
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    // Used for upsert tag/edge
    nebula::cpp2::ErrorCode checkPropsAndGetDefaultValue() {
        // Store checked props
        // For example:
        // set a = 1, b = a + 1, c = 2,             `a` does not require default value and nullable
        // set a = 1, b = c + 1, c = 2,             `c` requires default value and nullable
        // set a = 1, b = (a + 1) + 1, c = 2,       support recursion multiple times
        // set a = 1, c = 2, b = (a + 1) + (c + 1)  support multiple properties
        std::unordered_set<std::string> checkedProp;
        // check depPropMap_ in set clause
        // this props must have default value or nullable, or set int UpdatedProp_
        for (auto& prop : depPropMap_) {
            for (auto& p :  prop.second) {
                auto it = checkedProp.find(p);
                if (it == checkedProp.end()) {
                    auto field = schema_->field(p);
                    auto ret = checkField(field);
                    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                        return ret;
                    }
                    ret = getDefaultOrNullValue(field, p);
                    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                        return ret;
                    }
                    checkedProp.emplace(p);
                }
            }

            // set field not need default value or nullable
            auto field = schema_->field(prop.first);
            auto ret = checkField(field);
            if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
            checkedProp.emplace(prop.first);
        }

        // props not in set clause must have default value or nullable
        auto fieldIter = schema_->begin();
        while (fieldIter) {
            auto propName = fieldIter->name();
            auto propIter = checkedProp.find(propName);
            if (propIter == checkedProp.end()) {
                auto ret = getDefaultOrNullValue(&(*fieldIter), propName);
                if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            ++fieldIter;
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

protected:
    // ============================ input =====================================================
    RunTimeContext                                                         *context_;
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>             indexes_;
    // update <prop name, new value expression>
    std::vector<storage::cpp2::UpdatedProp>                                 updatedProps_;
    FilterNode<T>                                                          *filterNode_;
    // Whether to allow insert
    bool                                                                    insertable_{false};

    std::string                                                             key_;
    RowReader                                                              *reader_{nullptr};

    const meta::NebulaSchemaProvider                                       *schema_{nullptr};

    // use to save old row value
    std::string                                                             val_;
    std::unique_ptr<RowWriterV2>                                            rowWriter_;
    // prop -> value
    std::unordered_map<std::string, Value>                                  props_;
    std::atomic<nebula::cpp2::ErrorCode>                                    exeResult_;

    // updatedProps_ dependent props in value expression
    std::vector<std::pair<std::string, std::unordered_set<std::string>>>    depPropMap_;

    StorageExpressionContext                                               *expCtx_;
    bool                                                                    isEdge_{false};
};

// Only use for update vertex
// Update records, write to kvstore
class UpdateTagNode : public UpdateNode<VertexID> {
public:
    using RelNode<VertexID>::execute;

    UpdateTagNode(RunTimeContext* context,
                  std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes,
                  std::vector<storage::cpp2::UpdatedProp>& updatedProps,
                  FilterNode<VertexID>* filterNode,
                  bool insertable,
                  std::vector<std::pair<std::string, std::unordered_set<std::string>>> depPropMap,
                  StorageExpressionContext* expCtx,
                  TagContext* tagContext)
        : UpdateNode<VertexID>(context, indexes, updatedProps,
                               filterNode, insertable, depPropMap, expCtx, false)
        , tagContext_(tagContext) {
            tagId_ = context_->tagId_;
        }

    nebula::cpp2::ErrorCode execute(PartitionID partId, const VertexID& vId) override {
        CHECK_NOTNULL(context_->env()->kvstore_);
        IndexCountWrapper wrapper(context_->env());

        // Update is read-modify-write, which is an atomic operation.
        std::vector<VMLI> dummyLock = {std::make_tuple(context_->spaceId(),
                                                       partId, tagId_, vId)};
        nebula::MemoryLockGuard<VMLI> lg(context_->env()->verticesML_.get(),
                                         std::move(dummyLock));
        if (!lg) {
            auto conflict = lg.conflictKey();
            LOG(ERROR) << "vertex conflict "
                       << std::get<0>(conflict) << ":"
                       << std::get<1>(conflict) << ":"
                       << std::get<2>(conflict) << ":"
                       << std::get<3>(conflict);
            return nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
        }

        auto ret = RelNode::execute(partId, vId);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        if (this->context_->resultStat_ == ResultStatus::ILLEGAL_DATA) {
            return nebula::cpp2::ErrorCode::E_INVALID_DATA;
        } else if (this->context_->resultStat_ ==  ResultStatus::FILTER_OUT) {
            return nebula::cpp2::ErrorCode::E_FILTER_OUT;
        }

        if (filterNode_->valid()) {
            this->reader_ = filterNode_->reader();
        }
        // reset StorageExpressionContext reader_, because it contains old value
        this->expCtx_->reset();

        if (!this->reader_ && this->insertable_) {
            ret = this->insertTagProps(partId, vId);
        } else if (this->reader_) {
            this->key_ = filterNode_->key().str();
            ret = this->collTagProp(vId);
        } else {
            ret = nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND;
        }

        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        auto batch = this->updateAndWriteBack(partId, vId);
        if (batch == folly::none) {
            return nebula::cpp2::ErrorCode::E_INVALID_DATA;
        }

        folly::Baton<true, std::atomic> baton;
        auto callback = [&ret, &baton] (nebula::cpp2::ErrorCode code) {
            ret = code;
            baton.post();
        };
        context_->env()->kvstore_->asyncAppendBatch(
            context_->spaceId(), partId, std::move(batch).value(), callback);
        baton.wait();
        return ret;
    }

    nebula::cpp2::ErrorCode getLatestTagSchemaAndName() {
        auto schemaIter = tagContext_->schemas_.find(tagId_);
        if (schemaIter == tagContext_->schemas_.end() ||
            schemaIter->second.empty()) {
            return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }
        schema_ = schemaIter->second.back().get();
        if (!schema_) {
            LOG(ERROR) << "Get nullptr schema";
            return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }

        auto iter = tagContext_->tagNames_.find(tagId_);
        if (iter == tagContext_->tagNames_.end()) {
            VLOG(1) << "Can't find spaceId " << context_->spaceId()
                    << " tagId " << tagId_;
            return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }
        tagName_ = iter->second;
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    // Insert props row,
    // For insert, condition is always true,
    // Props must have default value or nullable, or set in UpdatedProp_
    nebula::cpp2::ErrorCode insertTagProps(PartitionID partId, const VertexID& vId) {
        context_->insert_ = true;
        auto ret = getLatestTagSchemaAndName();
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }
        ret = checkPropsAndGetDefaultValue();
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        expCtx_->setTagProp(tagName_, kVid, vId);
        expCtx_->setTagProp(tagName_, kTag, tagId_);
        for (auto &p : props_) {
            expCtx_->setTagProp(tagName_, p.first, p.second);
        }

        key_ = NebulaKeyUtils::vertexKey(context_->vIdLen(), partId, vId, tagId_);
        rowWriter_ = std::make_unique<RowWriterV2>(schema_);

        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    // collect tag prop
    nebula::cpp2::ErrorCode collTagProp(const VertexID& vId) {
        auto ret = getLatestTagSchemaAndName();
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        for (auto index = 0UL; index < schema_->getNumFields(); index++) {
            auto propName = std::string(schema_->getFieldName(index));
            VLOG(1) << "Collect prop " << propName << ", type " << tagId_;

            // read prop value, If the RowReader contains this field,
            // read from the rowreader, otherwise read the default value
            // or null value from the latest schema
            auto retVal = QueryUtils::readValue(reader_, propName, schema_);
            if (!retVal.ok()) {
                VLOG(1) << "Bad value for tag: " << tagId_
                        << ", prop " << propName;
                return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
            }
            props_[propName] = std::move(retVal.value());
        }

        expCtx_->setTagProp(tagName_, kVid, vId);
        expCtx_->setTagProp(tagName_, kTag, tagId_);
        for (auto &p : props_) {
            expCtx_->setTagProp(tagName_, p.first, p.second);
        }

        // After alter tag, the schema get from meta and the schema in RowReader
        // may be inconsistent, so the following method cannot be used
        // this->rowWriter_ = std::make_unique<RowWriterV2>(schema.get(), reader->getData());
        rowWriter_ = std::make_unique<RowWriterV2>(schema_);
        val_ = reader_->getData();
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    folly::Optional<std::string> updateAndWriteBack(const PartitionID partId, const VertexID vId) {
        ObjectPool pool;
        for (auto& updateProp : updatedProps_) {
            auto propName = updateProp.get_name();
            auto updateExp = Expression::decode(&pool, updateProp.get_value());
            if (!updateExp) {
                LOG(ERROR) << "Update expression decode failed " << updateProp.get_value();
                return folly::none;
            }
            auto updateVal = updateExp->eval(*expCtx_);
            // update prop value to props_
            props_[propName] = updateVal;
            // update expression context
            expCtx_->setTagProp(tagName_, propName, std::move(updateVal));
        }

        for (auto& e : props_) {
            auto wRet = rowWriter_->setValue(e.first, e.second);
            if (wRet != WriteResult::SUCCEEDED) {
                LOG(ERROR) << "Add field faild ";
                return folly::none;
            }
        }

        std::unique_ptr<kvstore::BatchHolder> batchHolder
            = std::make_unique<kvstore::BatchHolder>();

        auto wRet = rowWriter_->finish();
        if (wRet != WriteResult::SUCCEEDED) {
            LOG(ERROR) << "Add field faild ";
            return folly::none;
        }

        auto nVal = rowWriter_->moveEncodedStr();

        // update index if exists
        // Note: when insert_ is true, either there is no origin data or TTL expired
        // when there is no origin data, there is no the old index.
        // when TTL exists, there is no index.
        // when insert_ is true, not old index, val_ is empty.
        if (!indexes_.empty()) {
            RowReaderWrapper nReader;
            for (auto& index : indexes_) {
                if (tagId_ == index->get_schema_id().get_tag_id()) {
                    // step 1, delete old version index if exists.
                    if (!val_.empty()) {
                        if (!reader_) {
                            LOG(ERROR) << "Bad format row";
                            return folly::none;
                        }
                        auto oi = indexKey(partId, vId, reader_, index);
                        if (!oi.empty()) {
                            auto iState = context_->env()->getIndexState(context_->spaceId(),
                                                                            partId);
                            if (context_->env()->checkRebuilding(iState)) {
                                auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                                batchHolder->put(std::move(deleteOpKey), std::move(oi));
                            } else if (context_->env()->checkIndexLocked(iState)) {
                                LOG(ERROR) << "The index has been locked: "
                                           << index->get_index_name();
                                return folly::none;
                            } else {
                                batchHolder->remove(std::move(oi));
                            }
                        }
                    }

                    // step 2, insert new vertex index
                    if (!nReader) {
                        nReader = RowReaderWrapper::getTagPropReader(
                            context_->env()->schemaMan_, context_->spaceId(), tagId_, nVal);
                    }
                    if (!nReader) {
                        LOG(ERROR) << "Bad format row";
                        return folly::none;
                    }
                    auto ni = indexKey(partId, vId, nReader.get(), index);
                    if (!ni.empty()) {
                        auto v = CommonUtils::ttlValue(schema_, nReader.get());
                        auto niv = v.ok()  ? IndexKeyUtils::indexVal(std::move(v).value()) : "";
                        auto indexState = context_->env()->getIndexState(context_->spaceId(),
                                                                            partId);
                        if (context_->env()->checkRebuilding(indexState)) {
                            auto modifyKey = OperationKeyUtils::modifyOperationKey(partId,
                                                                                   std::move(ni));
                            batchHolder->put(std::move(modifyKey), std::move(niv));
                        } else if (context_->env()->checkIndexLocked(indexState)) {
                            LOG(ERROR) << "The index has been locked: " << index->get_index_name();
                            return folly::none;
                        } else {
                            batchHolder->put(std::move(ni), std::move(niv));
                        }
                    }
                }
            }
        }
        // step 3, insert new vertex data
        batchHolder->put(std::move(key_), std::move(nVal));
        return encodeBatchValue(batchHolder->getBatch());
    }

    std::string indexKey(PartitionID partId,
                         const VertexID& vId,
                         RowReader* reader,
                         std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
        auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields());
        if (!values.ok()) {
            return "";
        }
        return IndexKeyUtils::vertexIndexKey(context_->vIdLen(), partId, index->get_index_id(),
                                             vId, std::move(values).value());
    }

private:
    TagContext                 *tagContext_;
    TagID                       tagId_;
    std::string                 tagName_;
};

// Only use for update edge
// Update records, write to kvstore
class UpdateEdgeNode : public UpdateNode<cpp2::EdgeKey> {
public:
    using RelNode<cpp2::EdgeKey>::execute;

    UpdateEdgeNode(RunTimeContext* context,
                   std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes,
                   std::vector<storage::cpp2::UpdatedProp>& updatedProps,
                   FilterNode<cpp2::EdgeKey>* filterNode,
                   bool insertable,
                   std::vector<std::pair<std::string, std::unordered_set<std::string>>> depPropMap,
                   StorageExpressionContext* expCtx,
                   EdgeContext* edgeContext)
        : UpdateNode<cpp2::EdgeKey>(context,
                                    indexes,
                                    updatedProps,
                                    filterNode,
                                    insertable,
                                    depPropMap,
                                    expCtx,
                                    true),
          edgeContext_(edgeContext) {
        edgeType_ = context_->edgeType_;
    }

    nebula::cpp2::ErrorCode
    execute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
        CHECK_NOTNULL(context_->env()->kvstore_);
        auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
        IndexCountWrapper wrapper(context_->env());

        // Update is read-modify-write, which is an atomic operation.
        std::vector<EMLI> dummyLock = {
            std::make_tuple(context_->spaceId(),
                            partId,
                            edgeKey.get_src().getStr(),
                            edgeKey.get_edge_type(),
                            edgeKey.get_ranking(),
                            edgeKey.get_dst().getStr())
        };
        nebula::MemoryLockGuard<EMLI> lg(context_->env()->edgesML_.get(),
                                         std::move(dummyLock));
        if (!lg) {
            auto conflict = lg.conflictKey();
            LOG(ERROR) << "edge conflict "
                       << std::get<0>(conflict) << ":"
                       << std::get<1>(conflict) << ":"
                       << std::get<2>(conflict) << ":"
                       << std::get<3>(conflict) << ":"
                       << std::get<4>(conflict) << ":"
                       << std::get<5>(conflict);
            return nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
        }

        auto op = [&partId, &edgeKey, this]() -> folly::Optional<std::string> {
            this->exeResult_ = RelNode::execute(partId, edgeKey);
            if (this->exeResult_ == nebula::cpp2::ErrorCode::SUCCEEDED) {
                if (*edgeKey.edge_type_ref() != this->edgeType_) {
                    this->exeResult_ = nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND;
                    return folly::none;
                }
                if (this->context_->resultStat_ == ResultStatus::ILLEGAL_DATA) {
                    this->exeResult_ = nebula::cpp2::ErrorCode::E_INVALID_DATA;
                    return folly::none;
                } else if (this->context_->resultStat_ == ResultStatus::FILTER_OUT) {
                    this->exeResult_ = nebula::cpp2::ErrorCode::E_FILTER_OUT;
                    return folly::none;
                }

                if (filterNode_->valid()) {
                    this->reader_ = filterNode_->reader();
                }
                // reset StorageExpressionContext reader_ to clean old value in context
                this->expCtx_->reset();

                if (!this->reader_ && this->insertable_) {
                    this->exeResult_ = this->insertEdgeProps(partId, edgeKey);
                } else if (this->reader_) {
                    this->key_ = filterNode_->key().str();
                    this->exeResult_ = this->collEdgeProp(edgeKey);
                } else {
                    this->exeResult_ = nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND;
                }

                if (this->exeResult_ != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    return folly::none;
                }
                auto batch = this->updateAndWriteBack(partId, edgeKey);
                if (batch == folly::none) {
                    // There is an error in updateAndWriteBack
                    this->exeResult_ = nebula::cpp2::ErrorCode::E_INVALID_DATA;
                }
                return batch;
            } else {
                // If filter out, StorageExpressionContext is set in filterNode
                return folly::none;
            }
        };

        if (context_->env()->txnMan_ &&
            context_->env()->txnMan_->enableToss(context_->spaceId())) {
            LOG(INFO) << "before update edge atomic" << TransactionUtils::dumpKey(edgeKey);
            auto f = context_->env()->txnMan_->updateEdgeAtomic(
                context_->vIdLen(), context_->spaceId(), partId, edgeKey, std::move(op));
            f.wait();

            if (f.valid()) {
                ret = f.value();
            } else {
                ret = nebula::cpp2::ErrorCode::E_UNKNOWN;
            }
        } else {
            auto batch = op();
            if (batch == folly::none) {
                return this->exeResult_;
            }

            folly::Baton<true, std::atomic> baton;
            auto callback = [&ret, &baton] (nebula::cpp2::ErrorCode code) {
                ret = code;
                baton.post();
            };

            context_->env()->kvstore_->asyncAppendBatch(
                context_->spaceId(), partId, std::move(batch).value(), callback);
            baton.wait();
        }
        return ret;
    }

    nebula::cpp2::ErrorCode getLatestEdgeSchemaAndName() {
        auto schemaIter = edgeContext_->schemas_.find(std::abs(edgeType_));
        if (schemaIter == edgeContext_->schemas_.end() ||
            schemaIter->second.empty()) {
            return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        schema_ = schemaIter->second.back().get();
        if (!schema_) {
            LOG(ERROR) << "Get nullptr schema";
            return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }

        auto iter = edgeContext_->edgeNames_.find(edgeType_);
        if (iter == edgeContext_->edgeNames_.end()) {
            VLOG(1) << "Can't find spaceId " << context_->spaceId()
                    << " edgeType " << edgeType_;
            return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        edgeName_ = iter->second;
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    // Insert props row,
    // Operator props must have default value or nullable, or set in UpdatedProp_
    nebula::cpp2::ErrorCode
    insertEdgeProps(const PartitionID partId, const cpp2::EdgeKey& edgeKey) {
        context_->insert_ = true;
        auto ret = getLatestEdgeSchemaAndName();
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        ret = checkPropsAndGetDefaultValue();
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        // build expression context
        // add kSrc, kType, kRank, kDst
        expCtx_->setEdgeProp(edgeName_, kSrc, edgeKey.get_src());
        expCtx_->setEdgeProp(edgeName_, kDst, edgeKey.get_dst());
        expCtx_->setEdgeProp(edgeName_, kRank, edgeKey.get_ranking());
        expCtx_->setEdgeProp(edgeName_, kType, edgeKey.get_edge_type());

        for (auto &e : props_) {
            expCtx_->setEdgeProp(edgeName_, e.first, e.second);
        }

        key_ = NebulaKeyUtils::edgeKey(context_->vIdLen(),
                                       partId,
                                       edgeKey.get_src().getStr(),
                                       edgeKey.get_edge_type(),
                                       edgeKey.get_ranking(),
                                       edgeKey.get_dst().getStr());
        rowWriter_ = std::make_unique<RowWriterV2>(schema_);

        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    // Collect edge prop
    nebula::cpp2::ErrorCode collEdgeProp(const cpp2::EdgeKey& edgeKey) {
        auto ret = getLatestEdgeSchemaAndName();
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        for (auto index = 0UL; index < schema_->getNumFields(); index++) {
            auto propName = std::string(schema_->getFieldName(index));
            VLOG(1) << "Collect prop " << propName << ", edgeType " << edgeType_;

            // Read prop value, If the RowReader contains this field,
            // read from the rowreader, otherwise read the default value
            // or null value from the latest schema
            auto retVal = QueryUtils::readValue(reader_, propName, schema_);
            if (!retVal.ok()) {
                VLOG(1) << "Bad value for edge: " << edgeType_
                        << ", prop " << propName;
                return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
            }
            props_[propName] = std::move(retVal.value());
        }

        // build expression context
        // add _src, _type, _rank, _dst
        expCtx_->setEdgeProp(edgeName_, kSrc, edgeKey.get_src());
        expCtx_->setEdgeProp(edgeName_, kDst, edgeKey.get_dst());
        expCtx_->setEdgeProp(edgeName_, kRank, edgeKey.get_ranking());
        expCtx_->setEdgeProp(edgeName_, kType, edgeKey.get_edge_type());

        for (auto &e : props_) {
            expCtx_->setEdgeProp(edgeName_, e.first, e.second);
        }

        // After alter edge, the schema get from meta and the schema in RowReader
        // may be inconsistent, so the following method cannot be used
        // this->rowWriter_ = std::make_unique<RowWriterV2>(schema.get(), reader->getData());
        rowWriter_ = std::make_unique<RowWriterV2>(schema_);
        val_ = reader_->getData();
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    folly::Optional<std::string>
    updateAndWriteBack(const PartitionID partId, const cpp2::EdgeKey& edgeKey) {
        ObjectPool pool;
        for (auto& updateProp : updatedProps_) {
            auto propName = updateProp.get_name();
            auto updateExp = Expression::decode(&pool, updateProp.get_value());
            if (!updateExp) {
                return folly::none;
            }
            auto updateVal = updateExp->eval(*expCtx_);
            // update prop value to updateContext_
            props_[propName] = updateVal;
            // update expression context
            expCtx_->setEdgeProp(edgeName_, propName, std::move(updateVal));
        }

        for (auto& e : props_) {
            auto wRet = rowWriter_->setValue(e.first, e.second);
            if (wRet != WriteResult::SUCCEEDED) {
                VLOG(1) << "Add field faild ";
                return folly::none;
            }
        }

        std::unique_ptr<kvstore::BatchHolder> batchHolder
            = std::make_unique<kvstore::BatchHolder>();

        auto wRet = rowWriter_->finish();
        if (wRet != WriteResult::SUCCEEDED) {
            VLOG(1) << "Add field faild ";
            return folly::none;
        }

        auto nVal = rowWriter_->moveEncodedStr();
        // update index if exists
        // Note: when insert_ is true, either there is no origin data or TTL expired
        // when there is no origin data, there is no the old index.
        // when TTL exists, there is no index.
        // when insert_ is true, not old index, val_ is empty.
        if (!indexes_.empty()) {
            RowReaderWrapper nReader;
            for (auto& index : indexes_) {
                if (edgeType_ == index->get_schema_id().get_edge_type()) {
                    // step 1, delete old version index if exists.
                    if (!val_.empty()) {
                        if (!reader_) {
                            LOG(ERROR) << "Bad format row";
                            return folly::none;
                        }
                        auto oi = indexKey(partId, reader_, edgeKey, index);
                        if (!oi.empty()) {
                            auto iState = context_->env()->getIndexState(context_->spaceId(),
                                                                            partId);
                            if (context_->env()->checkRebuilding(iState)) {
                                auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                                batchHolder->put(std::move(deleteOpKey), std::move(oi));
                            } else if (context_->env()->checkIndexLocked(iState)) {
                                LOG(ERROR) << "The index has been locked: "
                                           << index->get_index_name();
                                return folly::none;
                            } else {
                                batchHolder->remove(std::move(oi));
                            }
                        }
                    }

                    // step 2, insert new edge index
                    if (!nReader) {
                        nReader = RowReaderWrapper::getEdgePropReader(
                            context_->env()->schemaMan_, context_->spaceId(),
                            std::abs(edgeType_), nVal);
                    }
                    if (!nReader) {
                        LOG(ERROR) << "Bad format row";
                        return folly::none;
                    }
                    auto nik = indexKey(partId, nReader.get(), edgeKey, index);
                    if (!nik.empty()) {
                        auto v = CommonUtils::ttlValue(schema_, nReader.get());
                        auto niv = v.ok()  ? IndexKeyUtils::indexVal(std::move(v).value()) : "";
                        auto indexState = context_->env()->getIndexState(context_->spaceId(),
                                                                            partId);
                        if (context_->env()->checkRebuilding(indexState)) {
                            auto modifyKey = OperationKeyUtils::modifyOperationKey(partId,
                                                                                   std::move(nik));
                            batchHolder->put(std::move(modifyKey), std::move(niv));
                        } else if (context_->env()->checkIndexLocked(indexState)) {
                            LOG(ERROR) << "The index has been locked: " << index->get_index_name();
                            return folly::none;
                        } else {
                            batchHolder->put(std::move(nik), std::move(niv));
                        }
                    }
                }
            }
        }
        // step 3, insert new edge data
        batchHolder->put(std::move(key_), std::move(nVal));
        return encodeBatchValue(batchHolder->getBatch());
    }

    std::string indexKey(PartitionID partId,
                         RowReader* reader,
                         const cpp2::EdgeKey& edgeKey,
                         std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
        auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields());
        if (!values.ok()) {
            return "";
        }
        return IndexKeyUtils::edgeIndexKey(context_->vIdLen(),
                                           partId,
                                           index->get_index_id(),
                                           edgeKey.get_src().getStr(),
                                           edgeKey.get_ranking(),
                                           edgeKey.get_dst().getStr(),
                                           std::move(values).value());
    }

private:
    EdgeContext                            *edgeContext_;
    EdgeType                                edgeType_;
    std::string                             edgeName_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
