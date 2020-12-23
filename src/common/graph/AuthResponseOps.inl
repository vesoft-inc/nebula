/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_GRAPH_AUTHRESPONSEOPS_H_
#define COMMON_GRAPH_AUTHRESPONSEOPS_H_

#include "common/base/Base.h"

#include <thrift/lib/cpp2/GeneratedSerializationCodeHelper.h>
#include <thrift/lib/cpp2/gen/module_types_tcc.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>
#include <memory>

#include "common/graph/Response.h"

namespace apache {
namespace thrift {

/**************************************
 *
 * Ops for class AuthResponse
 *
 *************************************/
namespace detail {

template <>
struct TccStructTraits<nebula::AuthResponse> {
    static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                   MAYBE_UNUSED int16_t& fid,
                                   MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (false) {
        } else if (_fname == "error_code") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_I32;
        } else if (_fname == "error_msg") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "session_id") {
            fid = 3;
            _ftype = apache::thrift::protocol::T_I64;
        }
    }
};

}   // namespace detail

template <>
inline void Cpp2Ops<::nebula::AuthResponse>::clear(::nebula::AuthResponse* obj) {
    return obj->clear();
}

template <>
inline constexpr apache::thrift::protocol::TType Cpp2Ops<::nebula::AuthResponse>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}

template <>
template <class Protocol>
uint32_t Cpp2Ops<::nebula::AuthResponse>::write(Protocol* proto,
                                                ::nebula::AuthResponse const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("nebula::AuthResponse");
    xfer += proto->writeFieldBegin("error_code", apache::thrift::protocol::T_I32, 1);
    xfer +=
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::enumeration,
                                                       ::nebula::ErrorCode>::write(*proto,
                                                                                   obj->errorCode);
    xfer += proto->writeFieldEnd();
    if (obj->errorMsg != nullptr) {
        xfer += proto->writeFieldBegin("error_msg", apache::thrift::protocol::T_STRING, 2);
        xfer += proto->writeBinary(*obj->errorMsg);
        xfer += proto->writeFieldEnd();
    }
    if (obj->sessionId != nullptr) {
        xfer += proto->writeFieldBegin("session_id", apache::thrift::protocol::T_I64, 3);
        xfer +=
            ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                           int64_t>::write(*proto,
                                                                           *obj->sessionId);
        xfer += proto->writeFieldEnd();
    }
    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}

template <>
template <class Protocol>
void Cpp2Ops<::nebula::AuthResponse>::read(Protocol* proto, ::nebula::AuthResponse* obj) {
    apache::thrift::detail::ProtocolReaderStructReadState<Protocol> _readState;

    _readState.readStructBegin(proto);

    using apache::thrift::TProtocolException;

    bool isset_error_code = false;

    if (UNLIKELY(!_readState.advanceToNextField(proto, 0, 1, apache::thrift::protocol::T_I32))) {
        goto _loop;
    }
_readField_error_code:
    {
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::enumeration,
                                                      ::nebula::ErrorCode>::read(*proto,
                                                                                  obj->errorCode);
        isset_error_code = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 1, 2, apache::thrift::protocol::T_STRING))) {
        goto _loop;
    }
_readField_error_msg:
    {
        obj->errorMsg = std::make_unique<std::string>();
        proto->readBinary(*obj->errorMsg);
        //    this->__isset.error_msg = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 2, 3, apache::thrift::protocol::T_I64))) {
        goto _loop;
    }
_readField_session_id:
    {
        obj->sessionId = std::make_unique<int64_t>(-1);
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                      int64_t>::read(*proto, *obj->sessionId);
        //    this->__isset.session_id = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 3, 0, apache::thrift::protocol::T_STOP))) {
        goto _loop;
    }

_end:
    _readState.readStructEnd(proto);

    if (!isset_error_code) {
        TProtocolException::throwMissingRequiredField("error_code", "nebula::AuthResponse");
    }
    return;

_loop:
    if (_readState.fieldType == apache::thrift::protocol::T_STOP) {
        goto _end;
    }
    if (proto->kUsesFieldNames()) {
        apache::thrift::detail::TccStructTraits<nebula::AuthResponse>::translateFieldName(
            _readState.fieldName(), _readState.fieldId, _readState.fieldType);
    }

    switch (_readState.fieldId) {
        case 1: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_I32)) {
                goto _readField_error_code;
            } else {
                goto _skip;
            }
        }
        case 2: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_error_msg;
            } else {
                goto _skip;
            }
        }
        case 3: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_I64)) {
                goto _readField_session_id;
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

template <>
template <class Protocol>
uint32_t Cpp2Ops<::nebula::AuthResponse>::serializedSize(Protocol const* proto,
                                                         ::nebula::AuthResponse const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("nebula::AuthResponse");
    xfer += proto->serializedFieldSize("error_code", apache::thrift::protocol::T_I32, 1);
    xfer += ::apache::thrift::detail::pm::protocol_methods<
        ::apache::thrift::type_class::enumeration,
        ::nebula::ErrorCode>::serializedSize<false>(*proto, obj->errorCode);
    if (obj->errorMsg != nullptr) {
        xfer += proto->serializedFieldSize("error_msg", apache::thrift::protocol::T_STRING, 2);
        xfer += proto->serializedSizeBinary(*obj->errorMsg);
    }
    if (obj->sessionId != nullptr) {
        xfer += proto->serializedFieldSize("session_id", apache::thrift::protocol::T_I64, 3);
        xfer += ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::integral,
            int64_t>::serializedSize<false>(*proto, *obj->sessionId);
    }
    xfer += proto->serializedSizeStop();
    return xfer;
}

template <>
template <class Protocol>
uint32_t Cpp2Ops<::nebula::AuthResponse>::serializedSizeZC(Protocol const* proto,
                                                           ::nebula::AuthResponse const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("nebula::AuthResponse");
    xfer += proto->serializedFieldSize("error_code", apache::thrift::protocol::T_I32, 1);
    xfer += ::apache::thrift::detail::pm::protocol_methods<
        ::apache::thrift::type_class::enumeration,
        ::nebula::ErrorCode>::serializedSize<false>(*proto, obj->errorCode);
    if (obj->errorMsg != nullptr) {
        xfer += proto->serializedFieldSize("error_msg", apache::thrift::protocol::T_STRING, 2);
        xfer += proto->serializedSizeZCBinary(*obj->errorMsg);
    }
    if (obj->sessionId != nullptr) {
        xfer += proto->serializedFieldSize("session_id", apache::thrift::protocol::T_I64, 3);
        xfer += ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::integral,
            int64_t>::serializedSize<false>(*proto, *obj->sessionId);
    }
    xfer += proto->serializedSizeStop();
    return xfer;
}

}   // namespace thrift
}   // namespace apache
#endif   // COMMON_GRAPH_AUTHRESPONSEOPS_H_
