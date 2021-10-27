/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * obj source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_HOSTANDPATHOPS_H_
#define COMMON_DATATYPES_HOSTANDPATHOPS_H_

#include "common/base/Base.h"
#include "common/datatypes/CommonCpp2Ops.h"
#include "common/datatypes/HostAndPath.h"

namespace apache {
namespace thrift {

namespace detail {

template <>
struct TccStructTraits<nebula::HostAndPath> {
  static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                 MAYBE_UNUSED int16_t& fid,
                                 MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
    if (_fname == "host") {
      fid = 1;
      _ftype = apache::thrift::protocol::T_STRUCT;
    } else if (_fname == "path") {
      fid = 2;
      _ftype = apache::thrift::protocol::T_STRING;
    }
  }
};

}  // namespace detail

inline constexpr protocol::TType Cpp2Ops<nebula::HostAndPath>::thriftType() {
  return apache::thrift::protocol::T_STRUCT;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::HostAndPath>::write(Protocol* proto, nebula::HostAndPath const* obj) {
  uint32_t xfer = 0;
  xfer += proto->writeStructBegin("HostAndPath");

  xfer += proto->writeFieldBegin("host", apache::thrift::protocol::T_STRUCT, 1);
  xfer += Cpp2Ops<nebula::HostAddr>::write(proto, &obj->host);
  xfer += proto->writeFieldEnd();

  xfer += proto->writeFieldBegin("path", apache::thrift::protocol::T_STRING, 2);
  xfer += proto->writeString(obj->path);
  xfer += proto->writeFieldEnd();

  xfer += proto->writeFieldStop();
  xfer += proto->writeStructEnd();
  return xfer;
}

template <class Protocol>
void Cpp2Ops<nebula::HostAndPath>::read(Protocol* proto, nebula::HostAndPath* obj) {
  detail::ProtocolReaderStructReadState<Protocol> readState;
  readState.readStructBegin(proto);

  using apache::thrift::protocol::TProtocolException;

  if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, protocol::T_STRUCT))) {
    goto _loop;
  }

_readField_host : { ::apache::thrift::Cpp2Ops<nebula::HostAddr>::read(proto, &obj->host); }

  if (UNLIKELY(!readState.advanceToNextField(proto, 1, 2, protocol::T_STRING))) {
    goto _loop;
  }

_readField_path : { proto->readBinary(obj->path); }

  if (UNLIKELY(!readState.advanceToNextField(proto, 2, 0, protocol::T_STOP))) {
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
    detail::TccStructTraits<nebula::HostAndPath>::translateFieldName(
        readState.fieldName(), readState.fieldId, readState.fieldType);
  }

  switch (readState.fieldId) {
    case 1: {
      if (LIKELY(readState.fieldType == apache::thrift::protocol::T_STRUCT)) {
        goto _readField_host;
      } else {
        goto _skip;
      }
    }
    case 2: {
      if (LIKELY(readState.fieldType == apache::thrift::protocol::T_STRING)) {
        goto _readField_path;
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
uint32_t Cpp2Ops<nebula::HostAndPath>::serializedSize(Protocol const* proto,
                                                      nebula::HostAndPath const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("HostAndPath");

  xfer += proto->serializedFieldSize("host", apache::thrift::protocol::T_STRUCT, 1);
  xfer += ::apache::thrift::Cpp2Ops<nebula::HostAddr>::serializedSize(proto, &obj->host);

  xfer += proto->serializedFieldSize("path", apache::thrift::protocol::T_STRING, 2);
  xfer += proto->serializedSizeBinary(obj->path);

  xfer += proto->serializedSizeStop();
  return xfer;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::HostAndPath>::serializedSizeZC(Protocol const* proto,
                                                        nebula::HostAndPath const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("HostAndPath");

  xfer += proto->serializedFieldSize("host", apache::thrift::protocol::T_STRUCT, 1);
  xfer += ::apache::thrift::Cpp2Ops<nebula::HostAddr>::serializedSize(proto, &obj->host);

  xfer += proto->serializedFieldSize("path", apache::thrift::protocol::T_STRING, 2);
  xfer += proto->serializedSizeBinary(obj->path);

  xfer += proto->serializedSizeStop();
  return xfer;
}

}  // namespace thrift
}  // namespace apache

#endif  // COMMON_DATATYPES_HOSTANDPATHOPS_H_
