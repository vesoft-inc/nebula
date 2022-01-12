/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

/*
 * UUID v1: time-based and mac-based
 * UUID v2: DCE security
 * UUID v3: name-based MD5
 * UUID v4: random
 * UUID v5: name-based SHA-1
 * boost implements uuid v4 and v5(is deprecated due to security concerns)
 */
namespace nebula {

// the probability to find a duplicate within version-4 UUIDs is one in a billion.
// the computed is in: https://en.wikipedia.org/wiki/Universally_unique_identifier
class UUIDV4 {
 public:
  static boost::uuids::uuid getId();

  static std::string getIdStr();
};
}  // namespace nebula
