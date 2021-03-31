/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_GRAPH_PAIROPS_H_
#define COMMON_GRAPH_PAIROPS_H_

#include "common/base/Base.h"

#include <thrift/lib/cpp2/GeneratedCodeHelper.h>
#include <thrift/lib/cpp2/gen/module_types_tcc.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>

#include "common/graph/Response.h"
#include "common/graph/GraphCpp2Ops.h"

namespace apache {
namespace thrift {

/**************************************
 *
 * Ops for class ProfilingStatsOps
 *
 *************************************/
namespace detail {

template <>
struct TccStructTraits<nebula::Pair> {
    static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                   MAYBE_UNUSED int16_t& fid,
                                   MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (false) {
        } else if (_fname == "key") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "value") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_STRING;
        }
    }
};

}   // namespace detail

inline constexpr apache::thrift::protocol::TType Cpp2Ops<::nebula::Pair>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}

template <class Protocol>
uint32_t Cpp2Ops<::nebula::Pair>::write(Protocol* proto, ::nebula::Pair const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("Pair");
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

template <class Protocol>
void Cpp2Ops<::nebula::Pair>::read(Protocol* proto, ::nebula::Pair* obj) {
    apache::thrift::detail::ProtocolReaderStructReadState<Protocol> _readState;

    _readState.readStructBegin(proto);

    using apache::thrift::TProtocolException;

    bool isset_key = false;
    bool isset_value = false;

    if (UNLIKELY(!_readState.advanceToNextField(proto, 0, 1, apache::thrift::protocol::T_STRING))) {
        goto _loop;
    }
_readField_key:
    {
        proto->readBinary(obj->key);
        isset_key = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 1, 2, apache::thrift::protocol::T_STRING))) {
        goto _loop;
    }
_readField_value:
    {
        proto->readBinary(obj->value);
        isset_value = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 2, 0, apache::thrift::protocol::T_STOP))) {
        goto _loop;
    }

_end:
    _readState.readStructEnd(proto);

    if (!isset_key) {
        TProtocolException::throwMissingRequiredField("key", "Pair");
    }
    if (!isset_value) {
        TProtocolException::throwMissingRequiredField("value", "Pair");
    }
    return;

_loop:
    if (_readState.fieldType == apache::thrift::protocol::T_STOP) {
        goto _end;
    }
    if (proto->kUsesFieldNames()) {
        apache::thrift::detail::TccStructTraits<nebula::Pair>::translateFieldName(
            _readState.fieldName(), _readState.fieldId, _readState.fieldType);
    }

    switch (_readState.fieldId) {
        case 1: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_key;
            } else {
                goto _skip;
            }
        }
        case 2: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_value;
            } else {
                goto _skip;
            }
        }
        default: {
_skip:
            proto->skip(_readState.fieldType);
            _readState.readFieldEnd(proto);
            _readState.readFieldBeginNoInline(proto);
            goto _loop;
        }
    }
}

template <class Protocol>
uint32_t Cpp2Ops<::nebula::Pair>::serializedSize(Protocol const* proto, ::nebula::Pair const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("Pair");
    xfer += proto->serializedFieldSize("key", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->serializedSizeBinary(obj->key);
    xfer += proto->serializedFieldSize("value", apache::thrift::protocol::T_STRING, 2);
    xfer += proto->serializedSizeBinary(obj->value);
    xfer += proto->serializedSizeStop();
    return xfer;
}

template <class Protocol>
uint32_t Cpp2Ops<::nebula::Pair>::serializedSizeZC(Protocol const* proto,
                                                   ::nebula::Pair const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("Pair");
    xfer += proto->serializedFieldSize("key", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->serializedSizeZCBinary(obj->key);
    xfer += proto->serializedFieldSize("value", apache::thrift::protocol::T_STRING, 2);
    xfer += proto->serializedSizeZCBinary(obj->value);
    xfer += proto->serializedSizeStop();
    return xfer;
}

}   // namespace thrift
}   // namespace apache
#endif   // COMMON_GRAPH_PAIROPS_H_
