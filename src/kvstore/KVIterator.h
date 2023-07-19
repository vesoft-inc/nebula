/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_KVITERATOR_H_
#define KVSTORE_KVITERATOR_H_

#include "common/base/Base.h"

namespace nebula {
namespace kvstore {

class KVIterator {
 public:
  virtual ~KVIterator() = default;

  /**
   * @brief Return whether iterator has more key/value
   */
  virtual bool valid() const = 0;

  /**
   * @brief Move to next key/value, undefined behaviour when valid is false
   */
  virtual void next() = 0;

  /**
   * @brief Move to previous key/value
   */
  virtual void prev() = 0;

  /**
   * @brief Return the key of iterator points to
   *
   * @return folly::StringPiece Key
   */
  virtual folly::StringPiece key() const = 0;

  /**
   * @brief Return the value of iterator points to
   *
   * @return folly::StringPiece Value
   */
  virtual folly::StringPiece val() const = 0;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_KVITERATOR_H_
