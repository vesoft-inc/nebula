/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_KEYVALUEOPS_H_
#define COMMON_DATATYPES_KEYVALUEOPS_H_

#include "common/base/Base.h"

#include <thrift/lib/cpp2/GeneratedCodeHelper.h>
#include <thrift/lib/cpp2/gen/module_types_tcc.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>

#include "common/datatypes/KeyValue.h"
#include "common/datatypes/CommonCpp2Ops.h"

namespace apache {
namespace thrift {

namespace detail {

template <>
struct TccStructTraits<nebula::KeyValue> {
    static void translateFieldName(
            MAYBE_UNUSED folly::StringPiece _fname,
            MAYBE_UNUSED int16_t& fid,
            MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (_fname == "key") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "value") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_STRING;
        }
    }
};

}  // namespace detail


inline constexpr protocol::TType Cpp2Ops<nebula::KeyValue>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}


template<class Protocol>
uint32_t Cpp2Ops<nebula::KeyValue>::write(Protocol* proto, nebula::KeyValue const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("KeyValue");

    xfer += proto->writeFieldBegin("key", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->writeBinary(obj->key);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldBegin("value", apache::thrift::protocol::T_STRING, 2);
    xfer += proto->writeBinary(obj->value);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}


template<class Protocol>
void Cpp2Ops<nebula::KeyValue>::read(Protocol* proto, nebula::KeyValue* obj) {
    detail::ProtocolReaderStructReadState<Protocol> readState;

    readState.readStructBegin(proto);

    using apache::thrift::protocol::TProtocolException;

    if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, protocol::T_STRING))) {
        goto _loop;
    }

_readField_key:
    {
        proto->readBinary(obj->key);
    }

    if (UNLIKELY(!readState.advanceToNextField(proto, 1, 2, protocol::T_STRING))) {
        goto _loop;
    }

_readField_value:
    {
        proto->readBinary(obj->value);
    }

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
        detail::TccStructTraits<nebula::KeyValue>::translateFieldName(
            readState.fieldName(), readState.fieldId, readState.fieldType);
    }

    switch (readState.fieldId) {
        case 1:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_key;
            } else {
                goto _skip;
            }
        }
        case 2:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_value;
            } else {
                goto _skip;
            }
        }
        default:
        {
_skip:
            proto->skip(readState.fieldType);
            readState.readFieldEnd(proto);
            readState.readFieldBeginNoInline(proto);
            goto _loop;
        }
    }
}


template<class Protocol>
uint32_t Cpp2Ops<nebula::KeyValue>::serializedSize(Protocol const* proto,
                                                   nebula::KeyValue const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("KeyValue");

    xfer += proto->serializedFieldSize("key", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->serializedSizeBinary(obj->key);

    xfer += proto->serializedFieldSize("value", apache::thrift::protocol::T_STRING, 2);
    xfer += proto->serializedSizeBinary(obj->value);

    xfer += proto->serializedSizeStop();
    return xfer;
}


template<class Protocol>
uint32_t Cpp2Ops<nebula::KeyValue>::serializedSizeZC(Protocol const* proto,
                                                     nebula::KeyValue const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("KeyValue");

  xfer += proto->serializedFieldSize("key", apache::thrift::protocol::T_STRING, 1);
  xfer += proto->serializedSizeZCBinary(obj->key);

  xfer += proto->serializedFieldSize("value", apache::thrift::protocol::T_STRING, 2);
  xfer += proto->serializedSizeZCBinary(obj->value);

  xfer += proto->serializedSizeStop();
  return xfer;
}

}  // namespace thrift
}  // namespace apache
#endif  // COMMON_DATATYPES_KEYVALUEOPS_H_
