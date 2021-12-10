/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <boost/uuid/uuid.hpp>             // uuid class
#include <boost/uuid/uuid_generators.hpp>  // generators
#include <boost/uuid/uuid_io.hpp>          // streaming operators etc

namespace nebula {

class UUID {
 public:
  boost::uuids::uuid getId() {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    return uuid;
  }
};

}  // namespace nebula
