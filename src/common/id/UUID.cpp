/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/id/UUID.h"

namespace nebula {

boost::uuids::uuid UUIDV4::getId() {
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  return uuid;
}

std::string UUIDV4::getIdStr() {
  boost::uuids::uuid uuid = getId();
  return boost::uuids::to_string(uuid);
}

}  // namespace nebula
