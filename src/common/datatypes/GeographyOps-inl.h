/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_GEOGRAPHYOPS_H_
#define COMMON_DATATYPES_GEOGRAPHYOPS_H_

#include <thrift/lib/cpp2/GeneratedCodeHelper.h>
#include <thrift/lib/cpp2/gen/module_types_tcc.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>

#include "common/base/Base.h"
#include "common/datatypes/CommonCpp2Ops.h"
#include "common/datatypes/Geography.h"

namespace apache {
namespace thrift {

/**************************************
 *
 * Ops for class Geography
 *
 *************************************/
namespace detail {

template <>
struct TccStructTraits<nebula::Geography> {
  static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                 MAYBE_UNUSED int16_t& fid,
                                 MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
    if (_fname == "wkb") {
      fid = 1;
      _ftype = apache::thrift::protocol::T_STRING;
    }
  }
};

}  // namespace detail

inline constexpr protocol::TType Cpp2Ops<nebula::Geography>::thriftType() {
  return apache::thrift::protocol::T_STRUCT;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Geography>::write(Protocol* proto, nebula::Geography const* obj) {
  uint32_t xfer = 0;
  xfer += proto->writeStructBegin("Geography");
  xfer += proto->writeFieldBegin("wkb", apache::thrift::protocol::T_STRING, 1);
  xfer += proto->writeString(obj->wkb);
  xfer += proto->writeFieldEnd();
  xfer += proto->writeFieldStop();
  xfer += proto->writeStructEnd();
  return xfer;
}

template <class Protocol>
void Cpp2Ops<nebula::Geography>::read(Protocol* proto, nebula::Geography* obj) {
  apache::thrift::detail::ProtocolReaderStructReadState<Protocol> readState;

  readState.readStructBegin(proto);

  using apache::thrift::TProtocolException;

  if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, apache::thrift::protocol::T_STRING))) {
    goto _loop;
  }
_readField_wkb : { proto->readString(obj->wkb); }

  if (UNLIKELY(!readState.advanceToNextField(proto, 1, 0, apache::thrift::protocol::T_STOP))) {
    goto _loop;
  }

_end:
  readState.readStructEnd(proto);

  return;

_loop:
  if (readState.fieldType == apache::thrift::protocol::T_STOP) {
    goto _end;
  }

  if (proto->kUsesFieldNames()) {
    detail::TccStructTraits<nebula::Geography>::translateFieldName(
        readState.fieldName(), readState.fieldId, readState.fieldType);
  }

  switch (readState.fieldId) {
    case 1: {
      if (LIKELY(readState.fieldType == apache::thrift::protocol::T_STRING)) {
        goto _readField_wkb;
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
uint32_t Cpp2Ops<nebula::Geography>::serializedSize(Protocol const* proto,
                                                    nebula::Geography const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("Geography");
  xfer += proto->serializedFieldSize("wkb", apache::thrift::protocol::T_STRING, 1);
  xfer += proto->serializedSizeString(obj->wkb);
  xfer += proto->serializedSizeStop();
  return xfer;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Geography>::serializedSizeZC(Protocol const* proto,
                                                      nebula::Geography const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("Geography");
  xfer += proto->serializedFieldSize("wkb", apache::thrift::protocol::T_STRING, 1);
  xfer += proto->serializedSizeString(obj->wkb);
  xfer += proto->serializedSizeStop();
  return xfer;
}

}  // namespace thrift
}  // namespace apache
#endif  // COMMON_DATATYPES_GEOGRAPHYOPS_H_
