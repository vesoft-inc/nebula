/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_DATATYPES_VectorOPS_H_
#define COMMON_DATATYPES_VectorOPS_H_

#include <thrift/lib/cpp/protocol/TType.h>
#include <thrift/lib/cpp2/GeneratedCodeHelper.h>
#include <thrift/lib/cpp2/TypeClass.h>
#include <thrift/lib/cpp2/gen/module_types_tcc.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>

#include "common/base/Base.h"
#include "common/datatypes/CommonCpp2Ops.h"
#include "common/datatypes/Vector.h"

namespace apache {
namespace thrift {

namespace detail {

template <>
struct TccStructTraits<nebula::Vector> {
  static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                 MAYBE_UNUSED int16_t& fid,
                                 MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
    if (_fname == "values") {
      fid = 1;
      _ftype = apache::thrift::protocol::T_LIST;
    }
  }
};

}  // namespace detail

inline constexpr protocol::TType Cpp2Ops<nebula::Vector>::thriftType() {
  return apache::thrift::protocol::T_STRUCT;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Vector>::write(Protocol* proto, nebula::Vector const* obj) {
  uint32_t xfer = 0;
  xfer += proto->writeStructBegin("Vector");

  xfer += proto->writeFieldBegin("values", apache::thrift::protocol::T_LIST, 1);
  // Convert float vector to double vector for serialization
  std::vector<double> doubleValues;
  doubleValues.reserve(obj->values.size());
  for (const auto& val : obj->values) {
    doubleValues.emplace_back(static_cast<double>(val));
  }
  xfer += detail::pm::protocol_methods<type_class::list<type_class::floating_point>,
                                       std::vector<double>>::write(*proto, doubleValues);
  xfer += proto->writeFieldEnd();

  xfer += proto->writeFieldStop();
  xfer += proto->writeStructEnd();
  return xfer;
}

template <class Protocol>
void Cpp2Ops<nebula::Vector>::read(Protocol* proto, nebula::Vector* obj) {
  apache::thrift::detail::ProtocolReaderStructReadState<Protocol> readState;

  readState.readStructBegin(proto);

  using apache::thrift::protocol::TProtocolException;

  if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, protocol::T_I64))) {
    goto _loop;
  }

_readField_values : {
  // Read as double vector and convert to float vector
  std::vector<double> doubleValues;
  detail::pm::protocol_methods<type_class::list<type_class::floating_point>,
                               std::vector<double>>::read(*proto, doubleValues);
  obj->values.clear();
  obj->values.reserve(doubleValues.size());
  for (const auto& val : doubleValues) {
    obj->values.emplace_back(static_cast<float>(val));
  }
}

  if (UNLIKELY(!readState.advanceToNextField(proto, 1, 0, protocol::T_STOP))) {
    goto _loop;
  }

_end:
  readState.readStructEnd(proto);

  return;

_loop:
  if (readState.fieldType == apache::thrift::protocol::T_STOP) {
    goto _end;
  }

  switch (readState.fieldId) {
    case 1: {
      if (LIKELY(readState.fieldType == apache::thrift::protocol::T_LIST)) {
        goto _readField_values;
      } else {
        goto _skip;
      }
    }
    default: {
_skip:
      proto->skip(readState.fieldType);
      readState.readFieldEnd(proto);
      readState.readFieldBeginNoInline(proto);
      goto _loop;
    }
  }
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Vector>::serializedSize(Protocol const* proto, nebula::Vector const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("Vector");

  xfer += proto->serializedFieldSize("values", apache::thrift::protocol::T_LIST, 2);
  // Convert float vector to double vector for size calculation
  std::vector<double> doubleValues;
  doubleValues.reserve(obj->values.size());
  for (const auto& val : obj->values) {
    doubleValues.emplace_back(static_cast<double>(val));
  }
  xfer += detail::pm::protocol_methods<type_class::list<type_class::floating_point>,
                                       std::vector<double>>::serializedSize<false>(*proto,
                                                                                   doubleValues);
  xfer += proto->serializedSizeStop();
  return xfer;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Vector>::serializedSizeZC(Protocol const* proto,
                                                   nebula::Vector const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("Vector");

  xfer += proto->serializedFieldSize("values", apache::thrift::protocol::T_LIST, 2);
  // Convert float vector to double vector for size calculation
  std::vector<double> doubleValues;
  doubleValues.reserve(obj->values.size());
  for (const auto& val : obj->values) {
    doubleValues.emplace_back(static_cast<double>(val));
  }
  xfer += detail::pm::protocol_methods<type_class::list<type_class::floating_point>,
                                       std::vector<double>>::serializedSize<false>(*proto,
                                                                                   doubleValues);
  xfer += proto->serializedSizeStop();
  return xfer;
}

}  // namespace thrift
}  // namespace apache
#endif  // COMMON_DATATYPES_VectorOPS_H_
