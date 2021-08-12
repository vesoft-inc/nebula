/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * obj source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_HOSTADDROPS_H_
#define COMMON_DATATYPES_HOSTADDROPS_H_

#include "common/base/Base.h"

#include <thrift/lib/cpp2/GeneratedCodeHelper.h>
#include <thrift/lib/cpp2/gen/module_types_tcc.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>

#include "common/datatypes/HostAddr.h"
#include "common/datatypes/CommonCpp2Ops.h"

namespace apache {
namespace thrift {

namespace detail {

template <>
struct TccStructTraits<nebula::HostAddr> {
    static void translateFieldName(
            MAYBE_UNUSED folly::StringPiece _fname,
            MAYBE_UNUSED int16_t& fid,
            MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (_fname == "host") {
          fid = 1;
          _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "port") {
          fid = 2;
          _ftype = apache::thrift::protocol::T_I32;
       }
    }
};

}  // namespace detail


inline constexpr protocol::TType Cpp2Ops<nebula::HostAddr>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}


template<class Protocol>
uint32_t Cpp2Ops<nebula::HostAddr>::write(Protocol* proto, nebula::HostAddr const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("HostAddr");

    xfer += proto->writeFieldBegin("host", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->writeString(obj->host);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldBegin("port", apache::thrift::protocol::T_I32, 2);
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::Port>
        ::write(*proto, obj->port);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}


template<class Protocol>
void Cpp2Ops<nebula::HostAddr>::read(Protocol* proto, nebula::HostAddr* obj) {
    detail::ProtocolReaderStructReadState<Protocol> readState;

    readState.readStructBegin(proto);

    using apache::thrift::protocol::TProtocolException;

    if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, protocol::T_STRING))) {
        goto _loop;
    }

_readField_host:
    {
        proto->readString(obj->host);
    }

    if (UNLIKELY(!readState.advanceToNextField(proto, 1, 2, protocol::T_I32))) {
        goto _loop;
    }

_readField_port:
    {
        detail::pm::protocol_methods<type_class::integral, nebula::Port>
            ::read(*proto, obj->port);
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
        detail::TccStructTraits<nebula::HostAddr>::translateFieldName(
            readState.fieldName(),
            readState.fieldId,
            readState.fieldType);
    }

    switch (readState.fieldId) {
        case 1:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_host;
            } else {
                goto _skip;
            }
        }
        case 2:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_I32)) {
                goto _readField_port;
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
uint32_t Cpp2Ops<nebula::HostAddr>::serializedSize(Protocol const* proto,
                                                   nebula::HostAddr const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("HostAddr");

    xfer += proto->serializedFieldSize("host", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->serializedSizeString(obj->host);

    xfer += proto->serializedFieldSize("port", apache::thrift::protocol::T_I32, 2);
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::Port>
        ::serializedSize<false>(*proto, obj->port);

    xfer += proto->serializedSizeStop();
    return xfer;
}


template<class Protocol>
uint32_t Cpp2Ops<nebula::HostAddr>::serializedSizeZC(Protocol const* proto,
                                                     nebula::HostAddr const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("HostAddr");

    xfer += proto->serializedFieldSize("host", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->serializedSizeString(obj->host);

    xfer += proto->serializedFieldSize("port", apache::thrift::protocol::T_I32, 2);
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::Port>
        ::serializedSize<false>(*proto, obj->port);

    xfer += proto->serializedSizeStop();
    return xfer;
}

}  // namespace thrift
}  // namespace apache
#endif  // COMMON_DATATYPES_HOSTADDROPS_H_
