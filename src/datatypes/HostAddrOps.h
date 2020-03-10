/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATATYPES_HOSTADDROPS_H_
#define DATATYPES_HOSTADDROPS_H_

#include "base/Base.h"

#include <thrift/lib/cpp2/GeneratedSerializationCodeHelper.h>
#include <thrift/lib/cpp2/gen/module_types_tcc.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>

#include "datatypes/HostAddr.h"

namespace apache {
namespace thrift {

namespace detail {

template <>
struct TccStructTraits<nebula::HostAddr> {
    static void translateFieldName(
            FOLLY_MAYBE_UNUSED folly::StringPiece _fname,
            FOLLY_MAYBE_UNUSED int16_t& fid,
            FOLLY_MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (_fname == "ip") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_I32;
        } else if (_fname == "port") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_I32;
        }
    }
};

}  // namespace detail


template <>
inline void Cpp2Ops<nebula::HostAddr>::clear(nebula::HostAddr* obj) {
    return obj->clear();
}


template<>
inline constexpr protocol::TType Cpp2Ops<nebula::HostAddr>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}


template<>
template<class Protocol>
uint32_t Cpp2Ops<nebula::HostAddr>::write(Protocol* proto, nebula::HostAddr const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("HostAddr");

    xfer += proto->writeFieldBegin("ip", apache::thrift::protocol::T_I32, 1);
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::IPv4>
        ::write(*proto, obj->ip);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldBegin("port", apache::thrift::protocol::T_I32, 2);
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::Port>
        ::write(*proto, obj->port);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}


template<>
template<class Protocol>
void Cpp2Ops<nebula::HostAddr>::read(Protocol* proto, nebula::HostAddr* obj) {
    detail::ProtocolReaderStructReadState<Protocol> readState;

    readState.readStructBegin(proto);

    using apache::thrift::TProtocolException;

    if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, protocol::T_I32))) {
        goto _loop;
    }

_readField_ip:
    {
        detail::pm::protocol_methods<type_class::integral, nebula::IPv4>
            ::read(*proto, obj->ip);
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
            readState.fieldName(), readState.fieldId, readState.fieldType);
    }

    switch (readState.fieldId) {
        case 1:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_I32)) {
                goto _readField_ip;
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


template<>
template<class Protocol>
uint32_t Cpp2Ops<nebula::HostAddr>::serializedSize(Protocol const* proto,
                                                   nebula::HostAddr const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("HostAddr");

    xfer += proto->serializedFieldSize("ip", apache::thrift::protocol::T_I32, 1);
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::IPv4>
        ::serializedSize<false>(*proto, obj->ip);

    xfer += proto->serializedFieldSize("port", apache::thrift::protocol::T_I32, 2);
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::Port>
        ::serializedSize<false>(*proto, obj->port);

    xfer += proto->serializedSizeStop();
    return xfer;
}


template<>
template<class Protocol>
uint32_t Cpp2Ops<nebula::HostAddr>::serializedSizeZC(Protocol const* proto,
                                                     nebula::HostAddr const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("HostAddr");

    xfer += proto->serializedFieldSize("ip", apache::thrift::protocol::T_I32, 1);
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::IPv4>
        ::serializedSize<false>(*proto, obj->ip);

    xfer += proto->serializedFieldSize("port", apache::thrift::protocol::T_I32, 2);
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::Port>
        ::serializedSize<false>(*proto, obj->port);

    xfer += proto->serializedSizeStop();
    return xfer;
}

}  // namespace thrift
}  // namespace apache
#endif  // DATATYPES_HOSTADDROPS_H_

