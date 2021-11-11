/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/datatypes/Vertex.h"

#include <folly/DynamicConverter.h>
#include <folly/String.h>
#include <folly/hash/Hash.h>

namespace nebula {

std::string Tag::toString() const {
  std::vector<std::string> value(props.size());
  std::transform(props.begin(), props.end(), value.begin(), [](const auto& iter) -> std::string {
    std::stringstream out;
    out << iter.first << ":" << iter.second;
    return out.str();
  });

  std::stringstream os;
  os << "Tag: " << name << ", " << folly::join(",", value);
  return os.str();
}

//  {
//    "player.name" : "Tim Duncan",
//    "player.age" : 42,
//  }
folly::dynamic Tag::toJson() const {
  folly::dynamic tagJsonObj = folly::dynamic::object();

  for (const auto& iter : props) {
    tagJsonObj.insert(name + "." + iter.first, iter.second.toJson());
  }

  return tagJsonObj;
}

bool Vertex::contains(const Value& key) const {
  if (!key.isStr()) {
    return false;
  }
  auto& keyStr = key.getStr();
  for (const auto& tag : tags) {
    if (tag.props.find(keyStr) != tag.props.end()) {
      return true;
    }
  }
  return false;
}

const Value& Vertex::value(const std::string& key) const {
  for (const auto& tag : tags) {
    auto find = tag.props.find(key);
    if (find != tag.props.end()) {
      return find->second;
    }
  }
  return Value::kNullValue;
}

std::string Vertex::toString() const {
  std::stringstream os;
  os << "(" << vid << ")";
  if (!tags.empty()) {
    os << " ";
    for (const auto& tag : tags) {
      os << tag.toString();
    }
  }
  return os.str();
}

// {
//   "player.name" : "Tim Duncan",
//   "player.age" : 42,
//   "bachelor.name" : "Tim Duncan",
//   "bachelor.speciality" : "psychology"
// }
folly::dynamic Vertex::toJson() const {
  folly::dynamic propJsonObj = folly::dynamic::object();

  for (const auto& tag : tags) {
    propJsonObj.update(tag.toJson());
  }
  return propJsonObj;
}

// format:
// {
//   "id": _vid
//   "type": "vertex"
// }
folly::dynamic Vertex::getMetaData() const {
  folly::dynamic vertexMetadataObj = folly::dynamic::object("id", vid.toJson())("type", "vertex");
  return vertexMetadataObj;
}

Vertex& Vertex::operator=(Vertex&& rhs) noexcept {
  if (&rhs != this) {
    vid = std::move(rhs.vid);
    tags = std::move(rhs.tags);
  }
  return *this;
}

Vertex& Vertex::operator=(const Vertex& rhs) {
  if (&rhs != this) {
    vid = rhs.vid;
    tags = rhs.tags;
  }
  return *this;
}

bool Vertex::operator<(const Vertex& rhs) const {
  if (vid != rhs.vid) {
    return vid < rhs.vid;
  }
  if (tags.size() != rhs.tags.size()) {
    return tags.size() < rhs.tags.size();
  }
  return false;
}

}  // namespace nebula

namespace std {

// Inject a customized hash function
std::size_t hash<nebula::Tag>::operator()(const nebula::Tag& h) const noexcept {
  return folly::hash::fnv64(h.name);
}

std::size_t hash<nebula::Vertex>::operator()(const nebula::Vertex& h) const noexcept {
  size_t hv = folly::hash::fnv64(h.vid.toString());
  for (auto& t : h.tags) {
    hv += (hv << 1) + (hv << 4) + (hv << 5) + (hv << 7) + (hv << 8) + (hv << 40);
    hv ^= hash<nebula::Tag>()(t);
  }

  return hv;
}

}  // namespace std
