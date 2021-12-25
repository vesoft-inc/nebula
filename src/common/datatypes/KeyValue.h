/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_DATATYPES_KEYVALUE_H_
#define COMMON_DATATYPES_KEYVALUE_H_

namespace nebula {

struct KeyValue {
  std::string key;
  std::string value;

  KeyValue() {}
  KeyValue(KeyValue&& rhs) noexcept : key(std::move(rhs.key)), value(std::move(rhs.value)) {}
  KeyValue(const KeyValue& rhs) : key(rhs.key), value(rhs.value) {}
  explicit KeyValue(std::pair<std::string, std::string> kv)
      : key(std::move(kv.first)), value(std::move(kv.second)) {}

  void clear() {
    key.clear();
    value.clear();
  }

  void __clear() {
    clear();
  }

  auto& operator=(const KeyValue& rhs) {
    this->key = rhs.key;
    this->value = rhs.value;
    return *this;
  }

  auto& operator=(KeyValue&& rhs) {
    this->key = std::move(rhs.key);
    this->value = std::move(rhs.value);
    return *this;
  }

  bool operator==(const KeyValue& rhs) const {
    if (key != rhs.key) {
      return false;
    }
    return value == rhs.value;
  }

  bool operator<(const KeyValue& rhs) const {
    if (key != rhs.key) {
      return key < rhs.key;
    }
    return value < rhs.value;
  }
};

}  // namespace nebula
#endif  // COMMON_DATATYPES_KEYVALUE_H_
