/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_INDEXSCANNODE_H
#define STORAGE_EXEC_INDEXSCANNODE_H
#include <folly/Range.h>             // for StringPiece, Range
#include <folly/container/F14Set.h>  // for F14FastSet
#include <gtest/gtest_prod.h>        // for FRIEND_TEST
#include <sys/types.h>               // for size_t, u_short

#include <algorithm>   // for min
#include <bitset>      // for bitset
#include <cstdint>     // for int64_t
#include <cstring>     // for size_t, memcmp
#include <functional>  // for function
#include <memory>      // for unique_ptr, shared_ptr
#include <string>      // for string, basic_string
#include <tuple>       // for tuple
#include <utility>     // for move, pair
#include <vector>      // for vector

#include "common/base/Base.h"
#include "common/base/Logging.h"        // for CHECK_GE, CHECK_LE
#include "common/datatypes/DataSet.h"   // for Row
#include "common/datatypes/Value.h"     // for Value
#include "common/thrift/ThriftTypes.h"  // for PartitionID
#include "common/utils/IndexKeyUtils.h"
#include "common/utils/Types.h"                // for IndexID
#include "interface/gen-cpp2/common_types.h"   // for ErrorCode
#include "interface/gen-cpp2/meta_types.h"     // for IndexItem (ptr only)
#include "interface/gen-cpp2/storage_types.h"  // for IndexColumnHint
#include "kvstore/KVIterator.h"                // for KVIterator
#include "storage/CommonUtils.h"
#include "storage/exec/IndexNode.h"  // for Map, IndexNode, Set

namespace nebula {
namespace meta {
class NebulaSchemaProvider;
class SchemaProviderIf;
}  // namespace meta
namespace storage {
struct RuntimeContext;
}  // namespace storage

namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore
namespace meta {
class NebulaSchemaProvider;
class SchemaProviderIf;
}  // namespace meta

namespace storage {

/**
 *
 * IndexScanNode
 *
 * reference: IndexNode, IndexVertexScanNode, IndexEdgeScanNode
 *
 * `IndexScanNode` is the base class of the node which need to access disk. It has two derive
 * class `IndexVertexScanNode` and `IndexEdgeScanNode`
 *
 *                   ┌───────────┐
 *                   │ IndexNode │
 *                   └─────┬─────┘
 *                         │
 *                 ┌───────┴───────┐
 *                 │ IndexScanNode │
 *                 └───────┬───────┘
 *             ┌───────────┴────────────┐
 *  ┌──────────┴──────────┐ ┌───────────┴─────────┐
 *  │ IndexVertexScanNode │ │  IndexEdgeScanNode  │
 *  └─────────────────────┘ └─────────────────────┘
 *
 * `IndexScanNode` will access index data, and then access base data if necessary.
 *
 *  Member:
 * `indexId_`               : index_ in this Node to access
 * `partId_`                : part to access.It will be modify while `doExecute`
 * `index_`                 : index definition
 * `indexNullable_`         : if index contain nullable field or not
 * `columnHints_`           :
 * `path_`                  :
 * `iter_`                  : current kvstore iterator.It while be reset `doExecute` and iterated
 *                            during `doNext`
 * `kvstore_`               : server kvstore
 * `requiredColumns_`       : row format that `doNext` needs to return
 * `requiredAndHintColumns_`: columns that `decodeFromBase` needs to decode
 * `ttlProps`               : ttl properties `needAccessBase_`         : if need
 * `fatalOnBaseNotFound_`   : for debug
 *
 * Function:
 * `decodePropFromIndex`    : decode properties from Index key.It will be called by
 *                            `decodeFromIndex`
 * `decodeFromIndex`        : decode all column in `requiredColumns_` by index
 *                            key-value.
 * `getBaseData`            : get key-value of base data `decodeFromBase`         : get
 *                            all values that `requiredAndHintColumns_` required
 * `checkTTL`               : check data is
 * expired or not
 * -------------------------------------------------------------
 *
 * Path
 *
 * `Path` is the most important part of `IndexScanNode`. By analyzing `ColumnHint`, it obtains
 * the mode(Prefix or Range) and range(key of Prefix or [start,end) of Range) of keys that
 * `IndexScanNode` need to query in kvstore.
 *
 * `Path` not only generate the key to access, but also `qualified` whether the key complies with
 * the columnhint constraint or not.For example, if there is a truncated string index, we cannot
 * simply compare bytes to determine whether the current key complies with the columnhints
 * constraint, the result of `qualified(bytes)` should be `UNCERTAIN` and `IndexScanNode` will
 * access base data then `Path` reconfirm `ColumnHint` constraint by `qualified(RowData)`. In
 * addition to the above examples, there are other cases to deal with.`Path` and it's derive class
 * will dynamic different strategy by `ColumnHint`,`IndexItem`,and `Schema`.All strategy will be
 * added to `QFList_`(QualifiedFunctionList) during `buildKey`, and executed during `qualified`.
 *
 * `Path` will be reset when `IndexScanNode` execute on a new part.
 *
 * It should be noted that the range generated by `rangepath` is a certain left included and right
 * excluded interval,like [startKey_, endKey_), although `ColumnHint` may have many different
 * constraint ranges(e.g., (x, y],(INF,y),(x,INF)). Because the length of index key is fixed, the
 * way to obtain **the smallest key greater than 'x'** is to append several '\xFF' after until the
 * length of 'x' is greater than the length of the indexkey.
 *
 *
 * Member:
 * `QFList_`                : all Qualified strategy need to executed during qualified
 * `nullable_`              : if `index_` contain nullable field, `nullable_[i]` is equal to
 *                            `index_->fields[i].nullable`,else `nullable_` is empty
 * `index_nullable_offset_` : Participate in the index key encode diagram
 * `totalKeyLength_`        : Participate in the index key encode diagram
 * `suffixLength_`          : Participate in the index key encode diagram
 * `serializeString_`       : a string express path
 *
 * Index Key Encode:
 * ┌──────┬─────────────┬────────────────┬──────────┬─────────────────────────────────────────┐
 * │ type | PartitionID | Indexed Values | nullable | suffix({vid} or {srcId,rank,dstId})     |
 * │ 1byte|   3 bytes   |    n bytes     | 0/2 bytes| vid.length or vid.length*2+sizeof(rank) |
 * └──────┴─────────────┴────────────────┴──────────┴─────────────────────────────────────────┘
 *                                       │          └───────────────────┬─────────────────────┘
 *                           index_nullable_offset_                 suffixLength_
 * └──────────────────────────────────┬───────────────────────────────────────────────────────┘
 *                              totalKeyLength_
 *
 * Function:
 * `make`                   : construct `PrefixPath` or `RangePath` according to `hints`
 * `qualified(StringPiece)` : qualified key by bytes
 * `qualified(Map)`         : qualified row by value
 * `resetPart`              : reset current partitionID and reset `iter_`
 * `encodeValue`            : encode a Value to bytes
 *
 *
 * -------------------------------------------------------------
 *
 *
 *
 */

class Path;
class QualifiedStrategySet;
struct RuntimeContext;

class IndexScanNode : public IndexNode {
  FRIEND_TEST(IndexScanTest, Base);
  FRIEND_TEST(IndexScanTest, Vertex);
  FRIEND_TEST(IndexScanTest, Edge);
  // There are too many unittests, so a helper is defined to access private data
  friend class IndexScanTestHelper;

 public:
  IndexScanNode(const IndexScanNode& node);
  IndexScanNode(RuntimeContext* context,
                const std::string& name,
                IndexID indexId,
                const std::vector<cpp2::IndexColumnHint>& columnHints,
                ::nebula::kvstore::KVStore* kvstore)
      : IndexNode(context, name), indexId_(indexId), columnHints_(columnHints), kvstore_(kvstore) {}
  ::nebula::cpp2::ErrorCode init(InitContext& ctx) override;
  std::string identify() override;

 protected:
  nebula::cpp2::ErrorCode doExecute(PartitionID partId) final;
  Result doNext() final;
  void decodePropFromIndex(folly::StringPiece key,
                           const Map<std::string, size_t>& colPosMap,
                           std::vector<Value>& values);
  virtual Row decodeFromIndex(folly::StringPiece key) = 0;
  virtual nebula::cpp2::ErrorCode getBaseData(folly::StringPiece key,
                                              std::pair<std::string, std::string>& kv) = 0;
  virtual Map<std::string, Value> decodeFromBase(const std::string& key,
                                                 const std::string& value) = 0;
  virtual const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& getSchema() = 0;
  bool checkTTL();
  nebula::cpp2::ErrorCode resetIter(PartitionID partId);
  PartitionID partId_;
  const IndexID indexId_;
  std::shared_ptr<nebula::meta::cpp2::IndexItem> index_;
  bool indexNullable_ = false;
  const std::vector<cpp2::IndexColumnHint>& columnHints_;
  std::unique_ptr<Path> path_;
  std::unique_ptr<kvstore::KVIterator> iter_;
  nebula::kvstore::KVStore* kvstore_;
  std::vector<std::string> requiredColumns_;
  Set<std::string> requiredAndHintColumns_;
  std::pair<bool, std::pair<int64_t, std::string>> ttlProps_;
  bool needAccessBase_{false};
  bool fatalOnBaseNotFound_{false};
  Map<std::string, size_t> colPosMap_;
};
class QualifiedStrategy {
 public:
  enum Result { INCOMPATIBLE = 0, UNCERTAIN = 1, COMPATIBLE = 2 };
  /**
   * checkNull<targetIsNull>
   *
   * There are two overload `checkNull` functions:
   * 1. First one which is with template arg `targetIsNull`, checks `columnIndex` at `nullable`
   *    whether equal to `targetIsNull` or not.
   * 2. The other one which is without template, filters key whose `columnIndex` at `nullable` is
   *    true
   *
   * Args:
   * `columnIndex`  : Index of column. **NOTE** , however, that the order in nullable bytes is
   *                  reversed
   * `keyOffset`    : Reference `Index Key Encode` -> `index_nullable_offset_`
   *
   * Return:
   * For convenience, we define a variable x.When the value at `columnIndex` is null, x is true,
   * Otherwise x is false.
   * 1.With template.Return COMPATIBLE if `x`==`targetIsNull`,else INCOMPATIBLE
   * 2.Without template.Return COMPATIBLE if `x`==false, else INCOMPATIBLE
   */
  template <bool targetIsNull>
  static QualifiedStrategy checkNull(size_t columnIndex, size_t keyOffset) {
    QualifiedStrategy q;
    q.func_ = [columnIndex, keyOffset](const folly::StringPiece& key) {
      std::bitset<16> nullableBit;
      auto v = *reinterpret_cast<const u_short*>(key.data() + keyOffset);
      nullableBit = v;
      return nullableBit.test(15 - columnIndex) == targetIsNull ? Result::COMPATIBLE
                                                                : Result::INCOMPATIBLE;
    };
    return q;
  }
  static QualifiedStrategy checkNull(size_t columnIndex, size_t keyOffset) {
    QualifiedStrategy q;
    q.func_ = [columnIndex, keyOffset](const folly::StringPiece& key) {
      std::bitset<16> nullableBit;
      auto v = *reinterpret_cast<const u_short*>(key.data() + keyOffset);
      nullableBit = v;
      return nullableBit.test(15 - columnIndex) ? Result::INCOMPATIBLE : Result::COMPATIBLE;
    };
    return q;
  }
  /**
   * checkNaN
   *
   * Only for double. Check the value at `keyOffset` in indexKey is NaN or not. The logic here needs
   * to be coordinated with the encoding logic of double numbers.
   *
   * Args:
   * `keyOffset`  : value offset at indexKey
   *
   * Return:
   * Return INCOMPATIBLE if v==Nan else COMPATIBLE;
   */
  static QualifiedStrategy checkNaN(size_t keyOffset) {
    const char* chr = "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF";  // '\xFF' * 8
    QualifiedStrategy q;
    q.func_ = [chr, keyOffset](const folly::StringPiece& key) {
      int ret = memcmp(chr, key.data() + keyOffset, 8);
      return ret == 0 ? Result::INCOMPATIBLE : Result::COMPATIBLE;
    };
    return q;
  }
  /**
   * dedupGeoIndex
   *
   * Because a `GEOGRAPHY` type data will generate multiple index keys pointing to the same base
   * data,the base data pointed to by the indexkey should be de duplicated.
   *
   * Args:
   * `dedupSuffixLength`  : If indexed schema is a tag, `dedupSuffixLength` should be vid.len;
   *                        If the indexed schema is an edge, `dedupSuffixLength` should be
   *                        srcId.len+sizeof(rank)+dstId.len
   * Return:
   * When suffix first appears, the function returns `COMPATIBLE`; otherwise, the function returns
   * `INCOMPATIBLE`
   */
  static QualifiedStrategy dedupGeoIndex(size_t dedupSuffixLength) {
    QualifiedStrategy q;
    q.func_ = [suffixSet = Set<std::string>(),
               suffixLength = dedupSuffixLength](const folly::StringPiece& key) mutable -> Result {
      std::string suffix = key.subpiece(key.size() - suffixLength, suffixLength).toString();
      auto result = suffixSet.insert(std::move(suffix)).second;
      return result ? Result::COMPATIBLE : Result::INCOMPATIBLE;
    };
    return q;
  }
  /**
   * constant<result>
   *
   * Always return `result`
   */
  template <Result result>
  static QualifiedStrategy constant() {
    QualifiedStrategy q;
    q.func_ = [](const folly::StringPiece&) { return result; };
    return q;
  }
  /**
   * compareTruncated<LEorGE>
   *
   * For a `String` type index, `val` may be truncated, and it is not enough to determine whether
   * the indexkey complies with the constraint of columnhint only through the interval limit of
   * [start,end) which is generated by `RangeIndex`. Therefore, it is necessary to make additional
   * judgment on the truncated string type index
   * For example:
   * (ab)c means that string is "abc" but index val has been truncated to "ab". (ab)c > ab is
   * `UNCERTAIN`, and (ab)c > aa is COMPATIBLE.
   *
   * Args:
   * `LEorGE`       : It's an assist arg. true means LE and false means GE.
   * `val`          : Truncated `String` index value,whose length has been define in `IndexItem`.
   * `keyStartPos`  : The position in indexKey where start compare with `val`
   *
   * Return:
   * Return `COMPATIBLE` if `val` is `LEorGE` than indexKey.Otherwise, return `UNCERTAIN`.
   */
  template <bool LEorGE>
  static QualifiedStrategy compareTruncated(const std::string& val, size_t keyStartPos) {
    QualifiedStrategy q;
    q.func_ = [val, keyStartPos](const folly::StringPiece& key) {
      int ret = memcmp(val.data(), key.data() + keyStartPos, val.size());
      if constexpr (LEorGE == true) {
        CHECK_LE(ret, 0);
      } else {
        CHECK_GE(ret, 0);
      }
      return ret == 0 ? Result::UNCERTAIN : Result::COMPATIBLE;
    };
    return q;
  }
  // call
  inline Result operator()(const folly::StringPiece& key);

 private:
  std::function<Result(const folly::StringPiece& key)> func_;
};
class QualifiedStrategySet {
 public:
  inline void insert(QualifiedStrategy&& strategy);
  inline QualifiedStrategy::Result operator()(const folly::StringPiece& key);

 private:
  std::vector<QualifiedStrategy> strategyList_;
};

class Path {
 public:
  // enum class Qualified : int16_t { INCOMPATIBLE = 0, UNCERTAIN = 1, COMPATIBLE = 2 };
  // using QualifiedFunction = std::function<Qualified(const std::string&)>;
  using ColumnTypeDef = ::nebula::meta::cpp2::ColumnTypeDef;
  Path(nebula::meta::cpp2::IndexItem* index,
       const meta::SchemaProviderIf* schema,
       const std::vector<cpp2::IndexColumnHint>& hints,
       int64_t vidLen);
  virtual ~Path() = default;

  static std::unique_ptr<Path> make(::nebula::meta::cpp2::IndexItem* index,
                                    const meta::SchemaProviderIf* schema,
                                    const std::vector<cpp2::IndexColumnHint>& hints,
                                    int64_t vidLen);
  QualifiedStrategy::Result qualified(const folly::StringPiece& key);
  virtual bool isRange() {
    return false;
  }

  virtual QualifiedStrategy::Result qualified(const Map<std::string, Value>& rowData) = 0;
  virtual void resetPart(PartitionID partId) = 0;
  const std::string& toString();

 protected:
  std::string encodeValue(const Value& value,
                          const ColumnTypeDef& colDef,
                          size_t index,
                          std::string& key);
  QualifiedStrategySet strategySet_;
  ::nebula::meta::cpp2::IndexItem* index_;
  const meta::SchemaProviderIf* schema_;
  const std::vector<cpp2::IndexColumnHint> hints_;
  std::vector<bool> nullable_;
  int64_t index_nullable_offset_{8};
  int64_t totalKeyLength_{8};
  int64_t suffixLength_;
  std::string serializeString_;
};
class PrefixPath : public Path {
 public:
  PrefixPath(nebula::meta::cpp2::IndexItem* index,
             const meta::SchemaProviderIf* schema,
             const std::vector<cpp2::IndexColumnHint>& hints,
             int64_t vidLen);
  // Override
  QualifiedStrategy::Result qualified(const Map<std::string, Value>& rowData) override;
  void resetPart(PartitionID partId) override;

  const std::string& getPrefixKey() {
    return prefix_;
  }

 private:
  std::string prefix_;
  void buildKey();
};
class RangePath : public Path {
 public:
  RangePath(nebula::meta::cpp2::IndexItem* index,
            const meta::SchemaProviderIf* schema,
            const std::vector<cpp2::IndexColumnHint>& hints,
            int64_t vidLen);
  QualifiedStrategy::Result qualified(const Map<std::string, Value>& rowData) override;
  void resetPart(PartitionID partId) override;

  inline bool includeStart() {
    return includeStart_;
  }
  inline bool includeEnd() {
    return includeEnd_;
  }
  inline const std::string& getStartKey() {
    return startKey_;
  }
  inline const std::string& getEndKey() {
    return endKey_;
  }
  bool isRange() override {
    return true;
  }

 private:
  std::string startKey_, endKey_;
  bool includeStart_ = true;
  bool includeEnd_ = false;

  void buildKey();
  std::tuple<std::string, std::string> encodeRange(
      const cpp2::IndexColumnHint& hint,
      const nebula::meta::cpp2::ColumnTypeDef& colTypeDef,
      size_t colIndex,
      size_t offset);
  inline std::string encodeString(const Value& value, size_t len, bool& truncated);
  inline std::string encodeFloat(const Value& value, bool& isNaN);
  std::string encodeBeginValue(const Value& value,
                               const ColumnTypeDef& colDef,
                               std::string& key,
                               size_t offset);
  std::string encodeEndValue(const Value& value,
                             const ColumnTypeDef& colDef,
                             std::string& key,
                             size_t offset);
};
/* define inline functions */
QualifiedStrategy::Result QualifiedStrategySet::operator()(const folly::StringPiece& key) {
  QualifiedStrategy::Result ret = QualifiedStrategy::COMPATIBLE;
  for (auto& s : strategyList_) {
    ret = std::min(ret, s(key));
  }
  return ret;
}
void QualifiedStrategySet::insert(QualifiedStrategy&& strategy) {
  strategyList_.emplace_back(std::move(strategy));
}
inline QualifiedStrategy::Result QualifiedStrategy::operator()(const folly::StringPiece& key) {
  return func_(key);
}

}  // namespace storage

}  // namespace nebula
#endif
