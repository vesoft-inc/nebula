/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_STORAGEITERATOR_H_
#define STORAGE_EXEC_STORAGEITERATOR_H_

#include "common/base/Base.h"
#include "kvstore/KVIterator.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

class StorageIterator {
public:
    virtual ~StorageIterator()  = default;

    virtual bool valid() const = 0;

    // next will skip invalid data, until it points to a valid data or it is invalid
    virtual void next() = 0;

    virtual folly::StringPiece key() const = 0;

    virtual folly::StringPiece val() const = 0;
};

class TagIterator : public StorageIterator {
public:
    virtual VertexID vId() const = 0;

    virtual TagID tagId() const = 0;
};

class SingleTagIterator : public TagIterator {
public:
    SingleTagIterator(std::unique_ptr<kvstore::KVIterator> iter,
                      TagID tagId,
                      size_t vIdLen)
        : iter_(std::move(iter))
        , tagId_(tagId)
        , vIdLen_(vIdLen) {}

    bool valid() const override {
        return !!iter_ && iter_->valid();
    }

    void next() override {
        iter_->next();
    }

    folly::StringPiece key() const override {
        return iter_->key();
    }

    folly::StringPiece val() const override {
        return iter_->val();
    }

    VertexID vId() const override {
        return NebulaKeyUtils::getVertexId(vIdLen_, iter_->key()).str();
    }

    TagID tagId() const override {
        return NebulaKeyUtils::getTagId(vIdLen_, iter_->key());
    }

protected:
    std::unique_ptr<kvstore::KVIterator> iter_;
    TagID tagId_;
    size_t vIdLen_;
};

class EdgeIterator : public StorageIterator {
public:
    virtual VertexIDSlice srcId() const = 0;

    virtual EdgeType edgeType() const = 0;

    virtual EdgeRanking edgeRank() const = 0;

    virtual VertexIDSlice dstId() const = 0;

    virtual RowReader* reader() const = 0;
};

// Iterator of single specified type
class SingleEdgeIterator : public EdgeIterator {
public:
    SingleEdgeIterator(
            std::unique_ptr<kvstore::KVIterator> iter,
            EdgeType edgeType,
            size_t vIdLen,
            const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas,
            const folly::Optional<std::pair<std::string, int64_t>>* ttl)
        : iter_(std::move(iter))
        , edgeType_(edgeType)
        , vIdLen_(vIdLen)
        , schemas_(schemas)
        , ttl_(ttl) {
        CHECK(!!iter_);
        while (iter_->valid() && !check()) {
            iter_->next();
        }
    }

    bool valid() const override {
        return iter_->valid();
    }

    void next() override {
        do {
            iter_->next();
        } while (iter_->valid() && !check());
    }

    folly::StringPiece key() const override {
        return iter_->key();
    }

    folly::StringPiece val() const override {
        return iter_->val();
    }

    RowReader* reader() const override {
        return reader_.get();
    }

    VertexIDSlice srcId() const override {
        return NebulaKeyUtils::getSrcId(vIdLen_, iter_->key());
    }

    EdgeType edgeType() const override {
        return NebulaKeyUtils::getEdgeType(vIdLen_, iter_->key());
    }

    EdgeRanking edgeRank() const override {
        return NebulaKeyUtils::getRank(vIdLen_, iter_->key());
    }

    VertexIDSlice dstId() const override {
        return NebulaKeyUtils::getDstId(vIdLen_, iter_->key());
    }

protected:
    // return true when the value iter to a valid edge value
    bool check() {
        auto key = iter_->key();
        auto rank = NebulaKeyUtils::getRank(vIdLen_, key);
        auto dstId = NebulaKeyUtils::getDstId(vIdLen_, key);
        if (!firstLoop_ && rank == lastRank_ && lastDstId_ == dstId) {
            // pass old version data of same edge
            return false;
        }

        auto val = iter_->val();
        if (!reader_) {
            reader_ = RowReader::getRowReader(*schemas_, val);
            if (!reader_) {
                return false;
            }
        } else if (!reader_->reset(*schemas_, val)) {
            reader_.reset();
            return false;
        }

        firstLoop_ = false;
        lastRank_ = rank;
        lastDstId_ = dstId.str();

        if (ttl_->hasValue()) {
            auto ttlValue = ttl_->value();
            if (CommonUtils::checkDataExpiredForTTL(schemas_->back().get(), reader_.get(),
                                                    ttlValue.first, ttlValue.second)) {
                return false;
            }
        }

        return true;
    }

    std::unique_ptr<kvstore::KVIterator> iter_;
    EdgeType edgeType_;
    size_t vIdLen_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
    const folly::Optional<std::pair<std::string, int64_t>>* ttl_ = nullptr;

    std::unique_ptr<RowReader> reader_;
    EdgeRanking lastRank_ = 0;
    VertexID lastDstId_ = "";
    bool firstLoop_ = true;
};

// Iterator of multiple SingleEdgeIterator, it will iterate over edges of different types
class MultiEdgeIterator : public EdgeIterator {
public:
    explicit MultiEdgeIterator(std::vector<EdgeIterator*> iters)
        : iters_(std::move(iters)) {
        if (!iters_.empty()) {
            moveToNextValidIterator();
        }
    }

    bool valid() const override {
        return curIter_ < iters_.size();
    }

    void next() override {
        iters_[curIter_]->next();
        if (!iters_[curIter_]->valid()) {
            moveToNextValidIterator();
        }
    }

    folly::StringPiece key() const override {
        return iters_[curIter_]->key();
    }

    folly::StringPiece val() const override {
        return iters_[curIter_]->val();
    }

    RowReader* reader() const override {
        return iters_[curIter_]->reader();
    }

    VertexIDSlice srcId() const override {
        return iters_[curIter_]->srcId();
    }

    EdgeType edgeType() const override {
        return iters_[curIter_]->edgeType();
    }

    EdgeRanking edgeRank() const override {
        return iters_[curIter_]->edgeRank();
    }

    VertexIDSlice dstId() const override {
        return iters_[curIter_]->dstId();
    }

    // return the index of multiple iterators
    size_t getIdx() const {
        return curIter_;
    }

private:
    void moveToNextValidIterator() {
        while (curIter_ < iters_.size()) {
            if (iters_[curIter_] && iters_[curIter_]->valid()) {
                return;
            }
            ++curIter_;
        }
    }

private:
    std::vector<EdgeIterator*> iters_;
    size_t curIter_ = 0;
};

// Iterator of all edges type of a specified vertex, can specify edge direction
class AllEdgeIterator : public EdgeIterator {
public:
    AllEdgeIterator(EdgeContext* ctx,
                    std::unique_ptr<kvstore::KVIterator> iter,
                    size_t vIdLen,
                    const cpp2::EdgeDirection& dir)
        : edgeContext_(ctx)
        , iter_(std::move(iter))
        , vIdLen_(vIdLen)
        , dir_(dir) {
        CHECK(!!iter_);
        while (iter_->valid() && !check()) {
            iter_->next();
        }
    }

    bool valid() const override {
        return iter_->valid();
    }

    void next() override {
        do {
            iter_->next();
        } while (iter_->valid() && !check());
    }

    folly::StringPiece key() const override {
        return iter_->key();
    }

    folly::StringPiece val() const override {
        return iter_->val();
    }

    RowReader* reader() const override {
        return reader_.get();
    }

    VertexIDSlice srcId() const override {
        return NebulaKeyUtils::getSrcId(vIdLen_, iter_->key());
    }

    EdgeType edgeType() const override {
        return NebulaKeyUtils::getEdgeType(vIdLen_, iter_->key());
    }

    EdgeRanking edgeRank() const override {
        return NebulaKeyUtils::getRank(vIdLen_, iter_->key());
    }

    VertexIDSlice dstId() const override {
        return NebulaKeyUtils::getDstId(vIdLen_, iter_->key());
    }

private:
    // return true when the value iter to a valid edge value
    bool check() {
        auto key = iter_->key();
        if (!NebulaKeyUtils::isEdge(vIdLen_, key)) {
            return false;
        }

        auto type = NebulaKeyUtils::getEdgeType(vIdLen_, key);
        if (dir_ == cpp2::EdgeDirection::IN_EDGE && type > 0) {
            return false;
        } else if (dir_ == cpp2::EdgeDirection::OUT_EDGE && type < 0) {
            return false;
        }

        if (type != edgeType_) {
            auto idxIter = edgeContext_->indexMap_.find(type);
            if (idxIter == edgeContext_->indexMap_.end()) {
                return false;
            }
            auto schemaIter = edgeContext_->schemas_.find(std::abs(type));
            if (schemaIter == edgeContext_->schemas_.end() ||
                schemaIter->second.empty()) {
                return false;
            }
            edgeType_ = type;
            schemas_ = &(schemaIter->second);
            ttl_ = QueryUtils::getEdgeTTLInfo(edgeContext_, edgeType_);
        }

        auto rank = NebulaKeyUtils::getRank(vIdLen_, key);
        auto dstId = NebulaKeyUtils::getDstId(vIdLen_, key);
        // pass old version data of same edge
        if (type == edgeType_ && rank == lastRank_ && lastDstId_ == dstId) {
            return false;
        }

        auto val = iter_->val();
        if (!reader_) {
            reader_ = RowReader::getRowReader(*schemas_, val);
            if (!reader_) {
                return false;
            }
        } else if (!reader_->reset(*schemas_, val)) {
            return false;
        }

        lastRank_ = rank;
        lastDstId_ = dstId.str();

        if (ttl_.hasValue()) {
            auto ttlValue = ttl_.value();
            if (CommonUtils::checkDataExpiredForTTL(schemas_->back().get(), reader_.get(),
                                                    ttlValue.first, ttlValue.second)) {
                return false;
            }
        }

        return true;
    }

private:
    EdgeContext* edgeContext_;
    std::unique_ptr<kvstore::KVIterator> iter_;
    size_t vIdLen_;
    cpp2::EdgeDirection dir_ = cpp2::EdgeDirection::BOTH;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
    folly::Optional<std::pair<std::string, int64_t>> ttl_ = nullptr;

    std::unique_ptr<RowReader> reader_;
    EdgeType edgeType_ = 0;
    EdgeRanking lastRank_ = 0;
    VertexID lastDstId_ = "";
};


}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_STORAGEITERATOR_H_
