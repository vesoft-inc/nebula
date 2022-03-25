/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_EXEC_STORAGEITERATOR_H_
#define STORAGE_EXEC_STORAGEITERATOR_H_

#include "codec/RowReaderWrapper.h"
#include "common/base/Base.h"
#include "kvstore/KVIterator.h"
#include "storage/CommonUtils.h"
#include "storage/StorageFlags.h"
namespace nebula {
namespace storage {

/**
 * @brief Storage iterate Interface
 *
 */
class StorageIterator {
 public:
  virtual ~StorageIterator() = default;

  /**
   * @brief check if there is any data
   *
   * @return true if there is still data to be accessed
   * @return false if there is no data to be accessed
   */
  virtual bool valid() const = 0;

  /**
   * @brief next will skip invalid data, until it points to a valid data or it is invalid
   *
   */
  virtual void next() = 0;

  /**
   * @brief Return the key of current data.
   *
   * All data is iterated in key-value format.
   *
   * @return folly::StringPiece
   */
  virtual folly::StringPiece key() const = 0;

  /**
   * @brief Return the value of current data.
   *
   */
  virtual folly::StringPiece val() const = 0;

  /**
   * @brief Returns the encapsulation of key-value data
   *
   * @see RowReader
   */
  virtual RowReader* reader() const = 0;
};

/**
 * @brief Iterator of single specified type
 *
 */
class SingleEdgeIterator : public StorageIterator {
 public:
  SingleEdgeIterator() = default;
  /**
   * @brief Construct a new Single Edge Iterator object
   *
   * @param context
   * @param iter Kvstore's iterator.
   * @param edgeType EdgeType to be read.
   * @param schemas EdgeType's all version schemas.
   * @param ttl
   */
  SingleEdgeIterator(RuntimeContext* context,
                     std::unique_ptr<kvstore::KVIterator> iter,
                     EdgeType edgeType,
                     const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas,
                     const std::optional<std::pair<std::string, int64_t>>* ttl)
      : context_(context), iter_(std::move(iter)), edgeType_(edgeType), schemas_(schemas) {
    CHECK(!!iter_);
    if (ttl->has_value()) {
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
  /**
   * @brief return true when the value iter to a valid edge value
   */
  bool check() {
    reader_.reset(*schemas_, iter_->val());
    if (!reader_) {
      context_->resultStat_ = ResultStatus::ILLEGAL_DATA;
      return false;
    }

    if (hasTtl_ && CommonUtils::checkDataExpiredForTTL(
                       schemas_->back().get(), reader_.get(), ttlCol_, ttlDuration_)) {
      reader_.reset();
      return false;
    }

    return true;
  }

  RuntimeContext* context_;
  std::unique_ptr<kvstore::KVIterator> iter_;
  EdgeType edgeType_;
  const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
  bool hasTtl_ = false;
  std::string ttlCol_;
  int64_t ttlDuration_;

  RowReaderWrapper reader_;
  EdgeRanking lastRank_ = 0;
  VertexID lastDstId_ = "";
};

/**
 * @brief Iterator of multiple SingleEdgeIterator, it will iterate over edges of different types
 */
class MultiEdgeIterator : public StorageIterator {
 public:
  /**
   * @brief will move to a valid SingleEdgeIterator if there is one
   */
  explicit MultiEdgeIterator(std::vector<SingleEdgeIterator*> iters) : iters_(std::move(iters)) {
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

  /**
   * @brief return the index of multiple iterators
   */
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
}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_STORAGEITERATOR_H_
