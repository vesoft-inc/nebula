/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_LOGENCODER_H_
#define KVSTORE_LOGENCODER_H_
#include <boost/core/noncopyable.hpp>

#include "common/cpp/helpers.h"
#include "kvstore/Common.h"

namespace nebula {
namespace kvstore {

enum LogType : char {
  OP_PUT = 0x01,
  OP_MULTI_PUT = 0x02,
  OP_REMOVE = 0x03,
  OP_MULTI_REMOVE = 0x04,
  OP_REMOVE_RANGE = 0x06,
  OP_ADD_LEARNER = 0x07,
  OP_TRANS_LEADER = 0x08,
  OP_ADD_PEER = 0x09,
  OP_REMOVE_PEER = 0x10,
  OP_BATCH_WRITE = 0x11,
};

enum BatchLogType : char {
  OP_BATCH_PUT = 0x01,
  OP_BATCH_REMOVE = 0x02,
  OP_BATCH_REMOVE_RANGE = 0x03,
};

/**
 * @brief Encode key value into a string
 *
 * @param key
 * @param val
 * @return std::string Encoded string
 */
std::string encodeKV(const folly::StringPiece& key, const folly::StringPiece& val);

/**
 * @brief Decode key/value from string
 *
 * @param data Encoded string
 * @return std::pair<folly::StringPiece, folly::StringPiece> Decoded key/value
 */
std::pair<folly::StringPiece, folly::StringPiece> decodeKV(const std::string& data);

/**
 * @brief Encode single value into a wal log
 *
 * @param type Log type
 * @param val Log message, usually is a output of 'encodeKV'
 * @return std::string Encoded wal
 */
std::string encodeSingleValue(LogType type, folly::StringPiece val);

/**
 * @brief Decode single value from wal log
 *
 * @param encoded Encoded wal log
 * @return folly::StringPiece Decoded value
 */
folly::StringPiece decodeSingleValue(folly::StringPiece encoded);

/**
 * @brief Encode multiple value into a wal log
 *
 * @param type Log type
 * @param values Log messages
 * @return std::string Encoded wal
 */
std::string encodeMultiValues(LogType type, const std::vector<std::string>& values);

/**
 * @brief Encode multiple key/value into a wal log
 *
 * @param type Log type
 * @param kvs Log messages
 * @return std::string Encoded wal
 */
std::string encodeMultiValues(LogType type, const std::vector<KV>& kvs);

/**
 * @brief Overload version of encodeMultiValues
 */
std::string encodeMultiValues(LogType type, folly::StringPiece v1, folly::StringPiece v2);

/**
 * @brief Decode multiple values from encoded wal log
 *
 * @param encoded Encoded wal log
 * @return std::vector<folly::StringPiece> Decoded values
 */
std::vector<folly::StringPiece> decodeMultiValues(folly::StringPiece encoded);

/**
 * @brief Encode a log batch
 *
 * @param batch
 * @return std::string Encoded wal
 */
std::string encodeBatchValue(
    const std::vector<std::tuple<BatchLogType, std::string, std::string>>& batch);

/**
 * @brief Decode into log batchs
 *
 * @param encoded Encoded wal
 * @return std::vector<std::pair<BatchLogType, std::pair<folly::StringPiece, folly::StringPiece>>>
 * Log batch
 */
std::vector<std::pair<BatchLogType, std::pair<folly::StringPiece, folly::StringPiece>>>
decodeBatchValue(folly::StringPiece encoded);

/**
 * @brief Encode a host into wal log
 *
 * @param type Log type
 * @param learner Host address
 * @return std::string Encoded wal
 */
std::string encodeHost(LogType type, const HostAddr& learner);

/**
 * @brief Decode a host from wal log
 *
 * @param type Log type
 * @param encoded Encoded wal
 * @return HostAddr Decoded host address
 */
HostAddr decodeHost(LogType type, const folly::StringPiece& encoded);

/**
 * @brief Get the timestamp from wal
 *
 * @param log WAL log
 * @return int64_t timestamp in utc
 */
int64_t getTimestamp(const folly::StringPiece& log);

/**
 * @brief A wrapper class of batchs of log, support put/remove/removeRange
 */
class BatchHolder : public boost::noncopyable, public nebula::cpp::NonMovable {
 public:
  BatchHolder() = default;
  ~BatchHolder() = default;

  /**
   * @brief Add a put operation to batch
   *
   * @param key Key to put
   * @param val Value to put
   */
  void put(std::string&& key, std::string&& val) {
    size_ += key.size() + val.size();
    auto op = std::make_tuple(
        BatchLogType::OP_BATCH_PUT, std::forward<std::string>(key), std::forward<std::string>(val));
    batch_.emplace_back(std::move(op));
  }

  /**
   * @brief Add a remove operation to batch
   *
   * @param key Key to remove
   */
  void remove(std::string&& key) {
    size_ += key.size();
    auto op = std::make_tuple(BatchLogType::OP_BATCH_REMOVE, std::forward<std::string>(key), "");
    batch_.emplace_back(std::move(op));
  }

  /**
   * @brief Add a remove range operation to batch, [start, end)
   *
   * @param begin Start key to remove
   * @param end End key to remove
   */
  void rangeRemove(std::string&& begin, std::string&& end) {
    size_ += begin.size() + end.size();
    auto op = std::make_tuple(BatchLogType::OP_BATCH_REMOVE_RANGE,
                              std::forward<std::string>(begin),
                              std::forward<std::string>(end));
    batch_.emplace_back(std::move(op));
  }

  /**
   * @brief reserve spaces for batch
   */
  void reserve(int32_t size) {
    batch_.reserve(size);
  }

  /**
   * @brief Get the batch object
   *
   * @return const std::vector<std::tuple<BatchLogType, std::string, std::string>>&
   */
  const std::vector<std::tuple<BatchLogType, std::string, std::string>>& getBatch() const {
    return batch_;
  }

  /**
   * @brief size of key in operation of the batch, in bytes
   */
  size_t size() {
    return size_;
  }

 private:
  std::vector<std::tuple<BatchLogType, std::string, std::string>> batch_;
  size_t size_{0};
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_LOGENCODER_H_
