/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/CommonUtils.h"

#include "storage/exec/QueryUtils.h"

DEFINE_bool(ttl_use_ms,
            false,
            "whether the ttl is configured as milliseconds instead of default seconds");

namespace nebula {
namespace storage {

bool CommonUtils::checkDataExpiredForTTL(const meta::NebulaSchemaProvider* schema,
                                         RowReaderWrapper* reader,
                                         const std::string& ttlCol,
                                         int64_t ttlDuration) {
  auto v = QueryUtils::readValue(reader, ttlCol, schema);
  if (!v.ok()) {
    VLOG(3) << "failed to read ttl property";
    return false;
  }
  return checkDataExpiredForTTL(schema, v.value(), ttlCol, ttlDuration);
}

bool CommonUtils::checkDataExpiredForTTL(const meta::NebulaSchemaProvider* schema,
                                         const Value& v,
                                         const std::string& ttlCol,
                                         int64_t ttlDuration) {
  const auto& ftype = schema->getFieldType(ttlCol);
  if (ftype != nebula::cpp2::PropertyType::TIMESTAMP &&
      ftype != nebula::cpp2::PropertyType::INT64) {
    return false;
  }

  int64_t now;
  // The unit of ttl expiration unit is controlled by user, we just use a gflag here.
  if (!FLAGS_ttl_use_ms) {
    now = std::time(nullptr);
  } else {
    auto t = std::chrono::system_clock::now();
    now = std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
  }

  // if the value is not INT type (sush as NULL), it will never expire.
  // TODO (sky) : DateTime
  if (v.isInt() && (now > (v.getInt() + ttlDuration))) {
    VLOG(2) << "ttl expired";
    return true;
  }
  return false;
}

std::pair<bool, std::pair<int64_t, std::string>> CommonUtils::ttlProps(
    const meta::NebulaSchemaProvider* schema) {
  DCHECK(schema != nullptr);
  const auto* ns = dynamic_cast<const meta::NebulaSchemaProvider*>(schema);
  const auto sp = ns->getProp();
  int64_t duration = 0;
  if (sp.get_ttl_duration()) {
    duration = *sp.get_ttl_duration();
  }
  std::string col;
  if (sp.get_ttl_col()) {
    col = *sp.get_ttl_col();
  }
  return std::make_pair(!(duration <= 0 || col.empty()), std::make_pair(duration, col));
}

StatusOr<Value> CommonUtils::ttlValue(const meta::NebulaSchemaProvider* schema,
                                      RowReaderWrapper* reader) {
  DCHECK(schema != nullptr);
  const auto* ns = dynamic_cast<const meta::NebulaSchemaProvider*>(schema);
  auto ttlProp = ttlProps(ns);
  if (!ttlProp.first) {
    return Status::Error();
  }
  return reader->getValueByName(std::move(ttlProp).second.second);
}

}  // namespace storage
}  // namespace nebula
