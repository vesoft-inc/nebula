/* Copyright (c) 2019 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include <string>
#include "dataman/RowWriter.h"
#include "NebulaCodecImpl.h"

namespace nebula {
namespace dataman {

std::string NebulaCodecImpl::encode(std::vector<Value> values) {
  RowWriter writer(nullptr);
  for (auto&  value : values) {
    if (value.type() == typeid(int)) {
      writer <<  boost::any_cast<int>(value);
    } else if (value.type() == typeid(std::string)) {
      writer <<  boost::any_cast<std::string>(value);
    } else if (value.type() == typeid(double)) {
      writer <<  boost::any_cast<double>(value);
    } else if (value.type() == typeid(float)) {
      writer <<  boost::any_cast<float>(value);
    } else if (value.type() == typeid(bool)) {
      writer <<  boost::any_cast<bool>(value);
    }
  }
  std::string result =  writer.encode();
  return result;
}

}  // namespace dataman
}  // namespace nebula
