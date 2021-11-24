/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <boost/uuid/uuid.hpp>             // uuid class
#include <boost/uuid/uuid_generators.hpp>  // generators
#include <boost/uuid/uuid_io.hpp>          // streaming operators etc

namespace nebula {
namespace meta {

class UUID {
 public:
  string getId() {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    const string uuid_str = boost::uuids::to_string(uuid);
  }
};

}  // namespace meta
}  // namespace nebula
