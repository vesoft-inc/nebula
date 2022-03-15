/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <common/datatypes/Edge.h>
#include <folly/String.h>
#include <folly/hash/Hash.h>

#include <sstream>

namespace nebula {

std::string Edge::toString() const {
  std::stringstream os;
  os << "(" << src << ")"
     << "-"
     << "[" << name << "(" << type << ")]"
     << "->"
     << "(" << dst << ")"
     << "@" << ranking;
  if (!props.empty()) {
    std::vector<std::string> value(props.size());
    std::transform(props.begin(), props.end(), value.begin(), [](const auto& iter) -> std::string {
      std::stringstream out;
      out << iter.first << ":" << iter.second;
      return out.str();
    });
    os << " " << folly::join(",", value);
  }
  return os.str();
}

// format:
// {
//    "prop1": val1,
//    "prop2": val2,
// }
folly::dynamic Edge::toJson() const {
  folly::dynamic propObj = folly::dynamic::object();

  for (const auto& iter : props) {
    propObj.insert(iter.first, iter.second.toJson());
  }

  return propObj;
}

// Used in Json form query result
// format:
// {
//   "id": {
//      "name": _name,
//      "src": srcVID,
//      "dst": dstVID,
//      "type": _type,
//      "ranking": _rankding
//    }
//   "type": "edge"
// }
folly::dynamic Edge::getMetaData() const {
  folly::dynamic edgeMetadataObj = folly::dynamic::object();

  folly::dynamic edgeIdObj = folly::dynamic::object("name", name)("src", src.toJson())(
      "dst", dst.toJson())("type", type)("ranking", ranking);

  edgeMetadataObj.insert("id", edgeIdObj);
  edgeMetadataObj.insert("type", "edge");

  return edgeMetadataObj;
}

bool Edge::contains(const Value& key) const {
  if (!key.isStr()) {
    return false;
  }
  return props.find(key.getStr()) != props.end();
}

const Value& Edge::value(const std::string& key) const {
  auto find = props.find(key);
  if (find != props.end()) {
    return find->second;
  } else {
    return Value::kNullValue;
  }
}

bool Edge::operator<(const Edge& rhs) const {
  if (src != rhs.src) {
    return src < rhs.src;
  }
  if (dst != rhs.dst) {
    return dst < rhs.dst;
  }
  if (type != rhs.type) {
    return type < rhs.type;
  }
  if (ranking != rhs.ranking) {
    return ranking < rhs.ranking;
  }
  if (props.size() != rhs.props.size()) {
    return props.size() < rhs.props.size();
  }
  return false;
}

bool Edge::operator==(const Edge& rhs) const {
  if (type != rhs.type && type != -rhs.type) {
    return false;
  }
  if (type == rhs.type) {
    return src == rhs.src && dst == rhs.dst && ranking == rhs.ranking && props == rhs.props;
  }
  return src == rhs.dst && dst == rhs.src && ranking == rhs.ranking && props == rhs.props;
}

void Edge::reverse() {
  type = -type;
  auto tmp = std::move(src);
  src = std::move(dst);
  dst = std::move(tmp);
}

void Edge::clear() {
  src.clear();
  dst.clear();
  type = 0;
  name.clear();
  ranking = 0;
  props.clear();
}

bool Edge::keyEqual(const Edge& rhs) const {
  if (type != rhs.type && type != -rhs.type) {
    return false;
  }
  if (type == rhs.type) {
    return src == rhs.src && dst == rhs.dst && ranking == rhs.ranking;
  }
  return src == rhs.dst && dst == rhs.src && ranking == rhs.ranking;
}

}  // namespace nebula

namespace std {

// Inject a customized hash function
std::size_t hash<nebula::Edge>::operator()(const nebula::Edge& h) const noexcept {
  const auto& src = h.type > 0 ? h.src.toString() : h.dst.toString();
  const auto& dst = h.type > 0 ? h.dst.toString() : h.src.toString();
  auto type = h.type > 0 ? h.type : -h.type;
  size_t hv = folly::hash::fnv64(src);
  hv = folly::hash::fnv64(dst, hv);
  hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&type), sizeof(type), hv);
  return folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.ranking), sizeof(h.ranking), hv);
}

}  // namespace std
