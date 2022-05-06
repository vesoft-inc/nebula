/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_INDEXSCANNODE_H
#define STORAGE_EXEC_INDEXSCANNODE_H
#include <gtest/gtest_prod.h>

#include <cstring>
#include <functional>

#include "common/base/Base.h"
#include "common/datatypes/DataSet.h"
#include "common/utils/IndexKeyUtils.h"
#include "interface/gen-cpp2/meta_types.h"
#include "interface/gen-cpp2/storage_types.h"
#include "storage/CommonUtils.h"
#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {

class Path;
class QualifiedStrategySet;

/**
 * @brief `IndexScanNode` is the base class of the node which need to access disk. It has two derive
 * class `IndexVertexScanNode` and `IndexEdgeScanNode`.
 *
 * `IndexScanNode` will access index data, and then access base data if necessary.
 *
 * @implements IndexNode
 * @see IndexNode, IndexVertexScanNode, IndexEdgeScanNode
 *
 */
class IndexScanNode : public IndexNode {
  FRIEND_TEST(IndexScanTest, Base);
  FRIEND_TEST(IndexScanTest, Vertex);
  FRIEND_TEST(IndexScanTest, Edge);
  // There are too many unittests, so a helper is defined to access private data
  friend class IndexScanTestHelper;

 public:
  /**
   * @brief shallow copy.
   * @attention This constructor will create a new Path
   * @see IndexNode::IndexNode(const IndexNode& node)
   */
  IndexScanNode(const IndexScanNode& node);

  /**
   * @brief Construct a new Index Scan Node object
   *
   * @param context
   * @param name
   * @param indexId
   * @param columnHints
   * @param kvstore
   */
  IndexScanNode(RuntimeContext* context,
                const std::string& name,
                IndexID indexId,
                const std::vector<cpp2::IndexColumnHint>& columnHints,
                ::nebula::kvstore::KVStore* kvstore,
                bool hasNullableCol)
      : IndexNode(context, name),
        indexId_(indexId),
        columnHints_(columnHints),
        kvstore_(kvstore),
        indexNullable_(hasNullableCol) {}
  ::nebula::cpp2::ErrorCode init(InitContext& ctx) override;
  std::string identify() override;

 protected:
  nebula::cpp2::ErrorCode doExecute(PartitionID partId) final;
  Result doNext() final;

  /**
   * @brief decode values from index key
   *
   * @param key index key
   * @param colPosMap needed columns and these position(order) in values
   * @param values result.Its order must meet the requirements of colPosMap.
   */
  void decodePropFromIndex(folly::StringPiece key,
                           const Map<std::string, size_t>& colPosMap,
                           std::vector<Value>& values);
  /**
   * @brief decode all props from index key
   *
   * decodePropFromIndex() will decode props who are defined in tag/edge properties.And,
   * IndexScanNode sometime needs not only those but alse vid,edge_type,tag_id. decodeFromIndex()
   * should be override by derived class and decode these special prop and then call
   * decodePropFromIndex() to decode general props.
   *
   * @param key index key
   * @return Row
   * @see decodePropFromIndex
   */
  virtual Row decodeFromIndex(folly::StringPiece key) = 0;

  /**
   * @brief get the base data key-value according to index key
   *
   * @param key index key
   * @param kv base data key-value
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode getBaseData(folly::StringPiece key,
                                              std::pair<std::string, std::string>& kv) = 0;

  /**
   * @brief decode all props from base data key-value.
   *
   * @param key base data key
   * @param value base data value
   * @return Map<std::string, Value>
   */
  virtual Map<std::string, Value> decodeFromBase(const std::string& key,
                                                 const std::string& value) = 0;
  virtual const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& getSchema() = 0;

  /**
   * @brief Check whether the indexkey has expired
   *
   * @return true
   * @return false
   */
  bool checkTTL();

  /**
   * @brief start query a new part
   *
   * @param partId
   * @return nebula::cpp2::ErrorCode
   * @see Path
   */
  nebula::cpp2::ErrorCode resetIter(PartitionID partId);
  PartitionID partId_;
  /**
   * @brief index_ in this Node to access
   */
  const IndexID indexId_;
  /**
   * @brief index definition
   */
  std::shared_ptr<nebula::meta::cpp2::IndexItem> index_;
  const std::vector<cpp2::IndexColumnHint>& columnHints_;
  /**
   * @see Path
   */
  std::unique_ptr<Path> path_;
  /**
   * @brief current kvstore iterator.It while be reset `doExecute` and iterated during `doNext`
   */
  std::unique_ptr<kvstore::KVIterator> iter_;
  nebula::kvstore::KVStore* kvstore_;
  /**
   * @brief if index contain nullable field or not
   */
  bool indexNullable_ = false;
  /**
   * @brief row format that `doNext` needs to return
   */
  std::vector<std::string> requiredColumns_;
  /**
   * @brief columns that `decodeFromBase` needs to decode
   */
  Set<std::string> requiredAndHintColumns_;
  /**
   * @brief ttl properties `needAccessBase_
   */
  std::pair<bool, std::pair<int64_t, std::string>> ttlProps_;
  bool needAccessBase_{false};
  bool fatalOnBaseNotFound_{false};
  Map<std::string, size_t> colPosMap_;
};
class QualifiedStrategy {
 public:
  /**
   * @brief qualified result
   * - INCOMPATIBLE: Meets strategy constraints
   * - UNCERTAIN: uncertain
   * - COMPATIBLE: Does not meet the strategy constraints
   */
  enum Result { INCOMPATIBLE = 0, UNCERTAIN = 1, COMPATIBLE = 2 };

  /**
   * @brief check target column is Null or not
   *
   * There are two overload `checkNull` functions:
   * 1. First one which is with template arg `targetIsNull`, checks `columnIndex` at `nullable`
   *    whether equal to `targetIsNull` or not.
   * 2. The other one which is without template, filters key whose `columnIndex` at `nullable` is
   *    true
   *
   * @param columnIndex Index of column. **NOTE** , however, that the order in nullable bytes is
   * reversed
   * @param keyOffset Reference `Index Key Encode` -> `index_nullable_offset_`
   * @return For convenience, we define a variable x.When the value at `columnIndex` is null, x is
   * true, Otherwise x is false.
   * - With template.Return COMPATIBLE if `x`==`targetIsNull`,else INCOMPATIBLE
   * - Without template.Return COMPATIBLE if `x`==false, else INCOMPATIBLE
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
   * @brief check float number is NaN or not
   * Only for double. Check the value at `keyOffset` in indexKey is NaN or not. The logic here needs
   * to be coordinated with the encoding logic of double numbers.
   *
   * @param keyOffset value offset at indexKey
   * @return Return INCOMPATIBLE if v==Nan else COMPATIBLE
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
   * @brief dedup geo index data
   *
   * Because a `GEOGRAPHY` type data will generate multiple index keys pointing to the same base
   * data,the base data pointed to by the indexkey should be de duplicated.
   *
   * @param dedupSuffixLength If indexed schema is a tag, `dedupSuffixLength` should be vid.len; If
   * the indexed schema is an edge, `dedupSuffixLength` should be srcId.len+sizeof(rank)+dstId.len
   *
   * @return When suffix first appears, the function returns `COMPATIBLE`; otherwise, the function
   * returns `INCOMPATIBLE`
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
   * @brief always return a constant
   *
   * @tparam result to return
   * @return return
   */
  template <Result result>
  static QualifiedStrategy constant() {
    QualifiedStrategy q;
    q.func_ = [](const folly::StringPiece&) { return result; };
    return q;
  }
  /**
   * @brief compare truncated string
   *
   * For a `String` type index, `val` may be truncated, and it is not enough to determine whether
   * the indexkey complies with the constraint of columnhint only through the interval limit of
   * [start,end) which is generated by `RangeIndex`. Therefore, it is necessary to make additional
   * judgment on the truncated string type index
   * For example:
   * (ab)c means that string is "abc" but index val has been truncated to "ab". (ab)c > ab is
   * `UNCERTAIN`, and (ab)c > aa is COMPATIBLE.
   *
   * @tparam LEorGE It's an assist arg. true means LE and false means GE.
   * @param val Truncated `String` index value,whose length has been define in `IndexItem`.
   * @param keyStartPos The position in indexKey where start compare with `val`
   * @return Return `COMPATIBLE` if `val` is `LEorGE` than indexKey.Otherwise, return `UNCERTAIN`.
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

/**
 * @brief `Path` is the range and constraints generated according to `ColumnHints`.
 *
 * `Path` is the most important part of `IndexScanNode`. By analyzing `ColumnHint`, it obtains
 * the mode(`Prefix` or `Range`) and range(key of `Prefix` or [start,end) of `Range`) of keys that
 * `IndexScanNode` need to query in kvstore.
 *
 * `Path` not only generate the key to access, but also `qualified` whether the key complies with
 * the columnhint constraint or not.For example, if there is a truncated string index, we cannot
 * simply compare bytes to determine whether the current key complies with the columnhints
 * constraint, the result of `qualified(bytes)` should be `UNCERTAIN` and `IndexScanNode` will
 * access base data then `Path` reconfirm `ColumnHint` constraint by `qualified(RowData)`. In
 * addition to the above examples, there are other cases to deal with.`Path` and it's derive class
 * will dynamic different strategy by `ColumnHint`,`IndexItem`,and `Schema`.All strategy will be
 * added to `strategySet_`(QualifiedStrategySet) during `buildKey`, and executed during `qualified`.
 *
 * `Path` will be reset when `IndexScanNode` execute on a new part.
 *
 * It should be noted that the range generated by `rangepath` is a certain left included and right
 * excluded interval,like [startKey_, endKey_), although `ColumnHint` may have many different
 * constraint ranges(e.g., (x, y],(INF,y),(x,INF)). Because the length of index key is fixed, the
 * way to obtain **the smallest key greater than 'x'** is to append several '\xFF' after until the
 * length of 'x' is greater than the length of the indexkey.
 *
 * @see QualifiedStrategySet
 * @todo
 */
class Path {
 public:
  using ColumnTypeDef = ::nebula::meta::cpp2::ColumnTypeDef;
  Path(nebula::meta::cpp2::IndexItem* index,
       const meta::SchemaProviderIf* schema,
       const std::vector<cpp2::IndexColumnHint>& hints,
       int64_t vidLen);
  virtual ~Path() = default;

  /**
   * @brief analyze hints and build a PrefixPath or RangePath
   *
   * @param index index of path
   * @param schema indexed tag or edge
   * @param hints property constraints.Like a=1, 1<b<2
   * @param vidLen
   * @return std::unique_ptr<Path>
   */
  static std::unique_ptr<Path> make(::nebula::meta::cpp2::IndexItem* index,
                                    const meta::SchemaProviderIf* schema,
                                    const std::vector<cpp2::IndexColumnHint>& hints,
                                    int64_t vidLen);

  /**
   * @brief apply all qulified strategy on indexKey.
   *
   * @param key indexKey
   * @return QualifiedStrategy::Result
   * @see QualifiedStrategy::Result
   */
  QualifiedStrategy::Result qualified(const folly::StringPiece& key);

  virtual bool isRange() {
    return false;
  }

  /**
   * @brief apply all qulified strategy on base data with format map<key,value>.
   *
   * @param rowData map<string,Value> who contain all base data
   * @return QualifiedStrategy::Result
   * @see QualifiedStrategy::Result
   */
  virtual QualifiedStrategy::Result qualified(const Map<std::string, Value>& rowData) = 0;

  /**
   * @brief reset the current part
   *
   * @param partId
   * @see IndexScanNode::execute(PartitionID partId)
   */
  virtual void resetPart(PartitionID partId) = 0;

  /**
   * @brief Seralize Path to string
   *
   * @return const std::string&
   */
  const std::string& toString();

 protected:
  /**
   * @brief encoding value to bytes who make up indexKey
   *
   * If there is truncated string or null type data, additional qualified strategies need to be
   * added.
   *
   * @param value the value to be encoded
   * @param colDef value's column definition
   * @param index position of colDef in IndexItem.Needed by checking nullable.
   * @param key bytes after encode need to be append to key
   * @return std::string
   */
  std::string encodeValue(const Value& value,
                          const ColumnTypeDef& colDef,
                          size_t index,
                          std::string& key);
  /**
   * @brief strategy set of current path
   * @see QualifiedStrategySet
   */
  QualifiedStrategySet strategySet_;
  /**
   * @brief index of current path
   */
  ::nebula::meta::cpp2::IndexItem* index_;
  /**
   * @brief tag/edge schema of current path
   */
  const meta::SchemaProviderIf* schema_;
  /**
   * @brief IndexColumnHints of current path
   */
  const std::vector<cpp2::IndexColumnHint> hints_;
  /**
   * @brief if `index_` contain nullable field, `nullable_[i]` is equal to
   * `index_->fields[i].nullable`,else `nullable_` is empty
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
   */
  std::vector<bool> nullable_;

  /**
   * @brief Participate in the index key encode diagram
   */
  int64_t index_nullable_offset_{8};

  /**
   * @brief Participate in the index key encode diagram
   */
  int64_t totalKeyLength_{8};

  /**
   * @brief Participate in the index key encode diagram
   */
  int64_t suffixLength_;
  std::string serializeString_;
};

/**
 * @brief A derived class of Path who has a prefix query, like a=1 and b=2
 */
class PrefixPath : public Path {
 public:
  /**
   * @brief Construct a new Prefix Path object
   * @see Path
   */
  PrefixPath(nebula::meta::cpp2::IndexItem* index,
             const meta::SchemaProviderIf* schema,
             const std::vector<cpp2::IndexColumnHint>& hints,
             int64_t vidLen);

  /**
   * @brief override
   * @param rowData
   * @return QualifiedStrategy::Result
   */
  QualifiedStrategy::Result qualified(const Map<std::string, Value>& rowData) override;

  void resetPart(PartitionID partId) override;

  /**
   * @brief get prefix key
   *
   * @return const std::string&
   */
  const std::string& getPrefixKey() {
    return prefix_;
  }

 private:
  /**
   * @brief the bytes who is used to query in kvstore
   */
  std::string prefix_;

  /**
   * @brief build prefix_
   */
  void buildKey();
};
/**
 * @brief A derived class of Path who has a range query, like a=1 and b<2
 */
class RangePath : public Path {
 public:
  /**
   * @brief Construct a new Range Path object
   * @see Path
   */
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

  /**
   * @brief build startKey_ and endKey_
   */
  void buildKey();

  /**
   * @brief encode hint into interval [start,end),and start and end are bytes.
   *
   * @param hint
   * @param colTypeDef
   * @param colIndex
   * @param offset
   * @return std::tuple<std::string, std::string>
   */
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
