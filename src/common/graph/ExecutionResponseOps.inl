/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_GRAPH_EXECUTIONRESPONSE_H_
#define COMMON_GRAPH_EXECUTIONRESPONSE_H_

#include <memory>

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
 * Ops for class ExecutionResponse
 *
 *************************************/
namespace detail {

template <>
struct TccStructTraits<::nebula::ExecutionResponse> {
    static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                   MAYBE_UNUSED int16_t& fid,
                                   MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (false) {
        } else if (_fname == "error_code") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_I32;
        } else if (_fname == "latency_in_us") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_I32;
        } else if (_fname == "data") {
            fid = 3;
            _ftype = apache::thrift::protocol::T_STRUCT;
        } else if (_fname == "space_name") {
            fid = 4;
            _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "error_msg") {
            fid = 5;
            _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "plan_desc") {
            fid = 6;
            _ftype = apache::thrift::protocol::T_STRUCT;
        } else if (_fname == "comment") {
            fid = 7;
            _ftype = apache::thrift::protocol::T_STRING;
        }
    }
};

}   // namespace detail

inline constexpr apache::thrift::protocol::TType
Cpp2Ops<::nebula::ExecutionResponse>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}

template <class Protocol>
uint32_t Cpp2Ops<::nebula::ExecutionResponse>::write(Protocol* proto,
                                                     ::nebula::ExecutionResponse const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("ExecutionResponse");
    xfer += proto->writeFieldBegin("error_code", apache::thrift::protocol::T_I32, 1);
    xfer +=
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::enumeration,
                                                       ::nebula::ErrorCode>::write(*proto,
                                                                                   obj->errorCode);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldBegin("latency_in_us", apache::thrift::protocol::T_I32, 2);
    xfer +=
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                       int32_t>::write(*proto, obj->latencyInUs);
    xfer += proto->writeFieldEnd();
    if (obj->data != nullptr) {
        xfer += proto->writeFieldBegin("data", apache::thrift::protocol::T_STRUCT, 3);
        xfer += ::apache::thrift::Cpp2Ops<nebula::DataSet>::write(proto, obj->data.get());
        xfer += proto->writeFieldEnd();
    }
    if (obj->spaceName != nullptr) {
        xfer += proto->writeFieldBegin("space_name", apache::thrift::protocol::T_STRING, 4);
        xfer += proto->writeBinary(*obj->spaceName);
        xfer += proto->writeFieldEnd();
    }
    if (obj->errorMsg != nullptr) {
        xfer += proto->writeFieldBegin("error_msg", apache::thrift::protocol::T_STRING, 5);
        xfer += proto->writeBinary(*obj->errorMsg);
        xfer += proto->writeFieldEnd();
    }
    if (obj->planDesc != nullptr) {
        xfer += proto->writeFieldBegin("plan_desc", apache::thrift::protocol::T_STRUCT, 6);
        xfer += ::apache::thrift::Cpp2Ops<::nebula::PlanDescription>::write(proto,
                                                                            obj->planDesc.get());
        xfer += proto->writeFieldEnd();
    }
    if (obj->comment != nullptr) {
        xfer += proto->writeFieldBegin("comment", apache::thrift::protocol::T_STRING, 7);
        xfer += proto->writeBinary(*obj->comment);
        xfer += proto->writeFieldEnd();
    }
    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}

template <class Protocol>
void Cpp2Ops<::nebula::ExecutionResponse>::read(Protocol* proto, ::nebula::ExecutionResponse* obj) {
    apache::thrift::detail::ProtocolReaderStructReadState<Protocol> _readState;

    _readState.readStructBegin(proto);

    using apache::thrift::TProtocolException;

    bool isset_error_code = false;
    bool isset_latency_in_us = false;

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

    if (UNLIKELY(!_readState.advanceToNextField(proto, 1, 2, apache::thrift::protocol::T_I32))) {
        goto _loop;
    }
_readField_latency_in_us:
    {
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                      int32_t>::read(*proto, obj->latencyInUs);
        isset_latency_in_us = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 2, 3, apache::thrift::protocol::T_STRUCT))) {
        goto _loop;
    }
_readField_data:
    {
        obj->data = std::make_unique<nebula::DataSet>();
        ::apache::thrift::Cpp2Ops<nebula::DataSet>::read(proto, obj->data.get());
        //    this->__isset.data = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 3, 4, apache::thrift::protocol::T_STRING))) {
        goto _loop;
    }
_readField_space_name:
    {
        obj->spaceName = std::make_unique<std::string>();
        proto->readBinary(*obj->spaceName);
        //    this->__isset.space_name = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 4, 5, apache::thrift::protocol::T_STRING))) {
        goto _loop;
    }
_readField_error_msg:
    {
        obj->errorMsg = std::make_unique<std::string>();
        proto->readBinary(*obj->errorMsg);
        //    this->__isset.error_msg = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 5, 6, apache::thrift::protocol::T_STRUCT))) {
        goto _loop;
    }
_readField_plan_desc:
    {
        obj->planDesc = std::make_unique<::nebula::PlanDescription>();
        ::apache::thrift::Cpp2Ops<::nebula::PlanDescription>::read(proto, obj->planDesc.get());
        //    this->__isset.plan_desc = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 6, 7, apache::thrift::protocol::T_STRING))) {
        goto _loop;
    }
_readField_comment:
    {
        obj->comment = std::make_unique<std::string>();
        proto->readBinary(*obj->comment);
        //    this->__isset.comment = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 7, 0, apache::thrift::protocol::T_STOP))) {
        goto _loop;
    }

_end:
    _readState.readStructEnd(proto);

    if (!isset_error_code) {
        TProtocolException::throwMissingRequiredField("error_code", "ExecutionResponse");
    }
    if (!isset_latency_in_us) {
        TProtocolException::throwMissingRequiredField("latency_in_us", "ExecutionResponse");
    }
    return;

_loop:
    if (_readState.fieldType == apache::thrift::protocol::T_STOP) {
        goto _end;
    }
    if (proto->kUsesFieldNames()) {
        apache::thrift::detail::TccStructTraits<nebula::ExecutionResponse>::translateFieldName(
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
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_I32)) {
                goto _readField_latency_in_us;
            } else {
                goto _skip;
            }
        }
        case 3: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_STRUCT)) {
                goto _readField_data;
            } else {
                goto _skip;
            }
        }
        case 4: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_space_name;
            } else {
                goto _skip;
            }
        }
        case 5: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_error_msg;
            } else {
                goto _skip;
            }
        }
        case 6: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_STRUCT)) {
                goto _readField_plan_desc;
            } else {
                goto _skip;
            }
        }
        case 7: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_comment;
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
uint32_t Cpp2Ops<::nebula::ExecutionResponse>::serializedSize(
    Protocol const* proto,
    ::nebula::ExecutionResponse const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("ExecutionResponse");
    xfer += proto->serializedFieldSize("error_code", apache::thrift::protocol::T_I32, 1);
    xfer += ::apache::thrift::detail::pm::protocol_methods<
        ::apache::thrift::type_class::enumeration,
        ::nebula::ErrorCode>::serializedSize<false>(*proto, obj->errorCode);
    xfer += proto->serializedFieldSize("latency_in_us", apache::thrift::protocol::T_I32, 2);
    xfer += ::apache::thrift::detail::pm::
        protocol_methods<::apache::thrift::type_class::integral, int32_t>::serializedSize<false>(
            *proto, obj->latencyInUs);
    if (obj->data != nullptr) {
        xfer += proto->serializedFieldSize("data", apache::thrift::protocol::T_STRUCT, 3);
        xfer += ::apache::thrift::Cpp2Ops<nebula::DataSet>::serializedSize(proto, obj->data.get());
    }
    if (obj->spaceName != nullptr) {
        xfer += proto->serializedFieldSize("space_name", apache::thrift::protocol::T_STRING, 4);
        xfer += proto->serializedSizeBinary(obj->spaceName);
    }
    if (obj->errorMsg != nullptr) {
        xfer += proto->serializedFieldSize("error_msg", apache::thrift::protocol::T_STRING, 5);
        xfer += proto->serializedSizeBinary(obj->errorMsg);
    }
    if (obj->planDesc != nullptr) {
        xfer += proto->serializedFieldSize("plan_desc", apache::thrift::protocol::T_STRUCT, 6);
        xfer += ::apache::thrift::Cpp2Ops<::nebula::PlanDescription>::serializedSize(
            proto, obj->planDesc.get());
    }
    if (obj->comment != nullptr) {
        xfer += proto->serializedFieldSize("comment", apache::thrift::protocol::T_STRING, 7);
        xfer += proto->serializedSizeBinary(obj->comment);
    }
    xfer += proto->serializedSizeStop();
    return xfer;
}

template <class Protocol>
uint32_t Cpp2Ops<::nebula::ExecutionResponse>::serializedSizeZC(
    Protocol const* proto,
    ::nebula::ExecutionResponse const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("ExecutionResponse");
    xfer += proto->serializedFieldSize("error_code", apache::thrift::protocol::T_I32, 1);
    xfer += ::apache::thrift::detail::pm::protocol_methods<
        ::apache::thrift::type_class::enumeration,
        ::nebula::ErrorCode>::serializedSize<false>(*proto, obj->errorCode);
    xfer += proto->serializedFieldSize("latency_in_us", apache::thrift::protocol::T_I32, 2);
    xfer += ::apache::thrift::detail::pm::
        protocol_methods<::apache::thrift::type_class::integral, int32_t>::serializedSize<false>(
            *proto, obj->latencyInUs);
    if (obj->data != nullptr) {
        xfer += proto->serializedFieldSize("data", apache::thrift::protocol::T_STRUCT, 3);
        xfer +=
            ::apache::thrift::Cpp2Ops<nebula::DataSet>::serializedSizeZC(proto, obj->data.get());
    }
    if (obj->spaceName != nullptr) {
        xfer += proto->serializedFieldSize("space_name", apache::thrift::protocol::T_STRING, 4);
        xfer += proto->serializedSizeZCBinary(*obj->spaceName);
    }
    if (obj->errorMsg != nullptr) {
        xfer += proto->serializedFieldSize("error_msg", apache::thrift::protocol::T_STRING, 5);
        xfer += proto->serializedSizeZCBinary(*obj->errorMsg);
    }
    if (obj->planDesc != nullptr) {
        xfer += proto->serializedFieldSize("plan_desc", apache::thrift::protocol::T_STRUCT, 6);
        xfer += ::apache::thrift::Cpp2Ops<::nebula::PlanDescription>::serializedSizeZC(
            proto, obj->planDesc.get());
    }
    if (obj->comment != nullptr) {
        xfer += proto->serializedFieldSize("comment", apache::thrift::protocol::T_STRING, 7);
        xfer += proto->serializedSizeZCBinary(*obj->comment);
    }
    xfer += proto->serializedSizeStop();
    return xfer;
}

}   // namespace thrift
}   // namespace apache
#endif   // COMMON_GRAPH_EXECUTIONRESPONSE_H_
