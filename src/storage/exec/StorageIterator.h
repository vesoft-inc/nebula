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
#include "storage/StorageFlags.h"

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

    virtual RowReader* reader() const = 0;
};

// Iterator of single specified type
class SingleEdgeIterator : public StorageIterator {
public:
    SingleEdgeIterator() = default;
    SingleEdgeIterator(
            RunTimeContext* context,
            std::unique_ptr<kvstore::KVIterator> iter,
            EdgeType edgeType,
            const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas,
            const folly::Optional<std::pair<std::string, int64_t>>* ttl)
        : context_(context)
        , iter_(std::move(iter))
        , edgeType_(edgeType)
        , schemas_(schemas) {
        CHECK(!!iter_);
        if (ttl->hasValue()) {
            hasTtl_ = true;
            ttlCol_ = ttl->value().first;
            ttlDuration_ = ttl->value().second;
        }
        while (iter_->valid() && !check()) {
            iter_->next();
        }
    }

    bool valid() const override {
        return reader_ != nullptr;
    }

    void next() override {
        do {
            iter_->next();
            if (!iter_->valid()) {
                reader_.reset();
                break;
            }
        } while (!check());
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

    EdgeType edgeType() const {
        return edgeType_;
    }

protected:
    // return true when the value iter to a valid edge value
    bool check() {
        reader_.reset(*schemas_, iter_->val());
        if (!reader_) {
            context_->resultStat_ = ResultStatus::ILLEGAL_DATA;
            return false;
        }

        if (hasTtl_ && CommonUtils::checkDataExpiredForTTL(schemas_->back().get(), reader_.get(),
                                                           ttlCol_, ttlDuration_)) {
            reader_.reset();
            return false;
        }

        return true;
    }

    RunTimeContext                                                       *context_;
    std::unique_ptr<kvstore::KVIterator>                                  iter_;
    EdgeType                                                              edgeType_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>> *schemas_ = nullptr;
    bool                                                                  hasTtl_ = false;
    std::string                                                           ttlCol_;
    int64_t                                                               ttlDuration_;

    RowReaderWrapper                                                      reader_;
    EdgeRanking                                                           lastRank_ = 0;
    VertexID                                                              lastDstId_ = "";
};

// Iterator of multiple SingleEdgeIterator, it will iterate over edges of different types
class MultiEdgeIterator : public StorageIterator {
public:
    // will move to a valid SingleEdgeIterator if there is one
    explicit MultiEdgeIterator(std::vector<SingleEdgeIterator*> iters)
        : iters_(std::move(iters)) {
        moveToNextValidIterator();
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

    EdgeType edgeType() const {
        return iters_[curIter_]->edgeType();
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
    std::vector<SingleEdgeIterator*> iters_;
    size_t curIter_ = 0;
};

class IndexIterator : public StorageIterator {
public:
    explicit IndexIterator(std::unique_ptr<kvstore::KVIterator> iter)
    : iter_(std::move(iter)) {}

    virtual IndexID indexId() const = 0;

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

protected:
    std::unique_ptr<kvstore::KVIterator> iter_;
};

class VertexIndexIterator : public IndexIterator {
public:
    VertexIndexIterator(std::unique_ptr<kvstore::KVIterator> iter, size_t vIdLen)
        : IndexIterator(std::move(iter))
        , vIdLen_(vIdLen) {}

    RowReader* reader() const override {
        return nullptr;
    }

    IndexID indexId() const override {
        return IndexKeyUtils::getIndexId(iter_->key());
    }

    VertexID vId() const {
        return IndexKeyUtils::getIndexVertexID(vIdLen_, iter_->key()).str();
    }

    folly::StringPiece val() const override {
        return iter_->val();
    }

protected:
    size_t vIdLen_;
};

class EdgeIndexIterator : public IndexIterator {
public:
    EdgeIndexIterator(std::unique_ptr<kvstore::KVIterator> iter, size_t vIdLen)
        : IndexIterator(std::move(iter))
        , vIdLen_(vIdLen) {}

    RowReader* reader() const override {
        return nullptr;
    }

    IndexID indexId() const override {
        return IndexKeyUtils::getIndexId(iter_->key());
    }

    VertexID srcId() const {
        return IndexKeyUtils::getIndexSrcId(vIdLen_, iter_->key()).str();
    }

    VertexID dstId() const {
        return IndexKeyUtils::getIndexDstId(vIdLen_, iter_->key()).str();
    }

    EdgeRanking ranking() const {
        return IndexKeyUtils::getIndexRank(vIdLen_, iter_->key());
    }

    folly::StringPiece val() const override {
        return iter_->val();
    }

protected:
    size_t vIdLen_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_STORAGEITERATOR_H_
