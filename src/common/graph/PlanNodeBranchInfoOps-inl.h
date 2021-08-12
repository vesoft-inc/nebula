/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_GRAPH_PLANNODEBRANCHINFOOPS_H_
#define COMMON_GRAPH_PLANNODEBRANCHINFOOPS_H_

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
 * Ops for class nebula::PlanNodeBranchInfo
 *
 *************************************/
namespace detail {

template <>
struct TccStructTraits<nebula::PlanNodeBranchInfo> {
    static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                   MAYBE_UNUSED int16_t& fid,
                                   MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (false) {
        } else if (_fname == "is_do_branch") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_BOOL;
        } else if (_fname == "condition_node_id") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_I64;
        }
    }
};

}   // namespace detail

inline constexpr apache::thrift::protocol::TType
Cpp2Ops<::nebula::PlanNodeBranchInfo>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}

template <class Protocol>
uint32_t Cpp2Ops<::nebula::PlanNodeBranchInfo>::write(Protocol* proto,
                                                      ::nebula::PlanNodeBranchInfo const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("nebula::PlanNodeBranchInfo");
    xfer += proto->writeFieldBegin("is_do_branch", apache::thrift::protocol::T_BOOL, 1);
    xfer += proto->writeBool(obj->isDoBranch);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldBegin("condition_node_id", apache::thrift::protocol::T_I64, 2);
    xfer += ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                           int64_t>::write(*proto,
                                                                           obj->conditionNodeId);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}

template <class Protocol>
void Cpp2Ops<::nebula::PlanNodeBranchInfo>::read(Protocol* proto,
                                                 ::nebula::PlanNodeBranchInfo* obj) {
    apache::thrift::detail::ProtocolReaderStructReadState<Protocol> _readState;

    _readState.readStructBegin(proto);

    using apache::thrift::TProtocolException;

    bool isset_is_do_branch = false;
    bool isset_condition_node_id = false;

    if (UNLIKELY(!_readState.advanceToNextField(proto, 0, 1, apache::thrift::protocol::T_BOOL))) {
        goto _loop;
    }
_readField_is_do_branch:
    {
        proto->readBool(obj->isDoBranch);
        isset_is_do_branch = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 1, 2, apache::thrift::protocol::T_I64))) {
        goto _loop;
    }
_readField_condition_node_id:
    {
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                       int64_t>::read(*proto,
                                                                       obj->conditionNodeId);
        isset_condition_node_id = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 2, 0, apache::thrift::protocol::T_STOP))) {
        goto _loop;
    }

_end:
    _readState.readStructEnd(proto);

    if (!isset_is_do_branch) {
        TProtocolException::throwMissingRequiredField("is_do_branch", "nebula::PlanNodeBranchInfo");
    }
    if (!isset_condition_node_id) {
        TProtocolException::throwMissingRequiredField("condition_node_id",
                                                      "nebula::PlanNodeBranchInfo");
    }
    return;

_loop:
    if (_readState.fieldType == apache::thrift::protocol::T_STOP) {
        goto _end;
    }
    if (proto->kUsesFieldNames()) {
        apache::thrift::detail::TccStructTraits<nebula::PlanNodeBranchInfo>::translateFieldName(
            _readState.fieldName(), _readState.fieldId, _readState.fieldType);
    }

    switch (_readState.fieldId) {
        case 1: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_BOOL)) {
                goto _readField_is_do_branch;
            } else {
                goto _skip;
            }
        }
        case 2: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_I64)) {
                goto _readField_condition_node_id;
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
uint32_t Cpp2Ops<::nebula::PlanNodeBranchInfo>::serializedSize(
    Protocol const* proto,
    ::nebula::PlanNodeBranchInfo const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("nebula::PlanNodeBranchInfo");
    xfer += proto->serializedFieldSize("is_do_branch", apache::thrift::protocol::T_BOOL, 1);
    xfer += proto->serializedSizeBool(obj->isDoBranch);
    xfer += proto->serializedFieldSize("condition_node_id", apache::thrift::protocol::T_I64, 2);
    xfer += ::apache::thrift::detail::pm::
        protocol_methods<::apache::thrift::type_class::integral, int64_t>::serializedSize<false>(
            *proto, obj->conditionNodeId);
    xfer += proto->serializedSizeStop();
    return xfer;
}

template <class Protocol>
uint32_t Cpp2Ops<::nebula::PlanNodeBranchInfo>::serializedSizeZC(
    Protocol const* proto,
    ::nebula::PlanNodeBranchInfo const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("nebula::PlanNodeBranchInfo");
    xfer += proto->serializedFieldSize("is_do_branch", apache::thrift::protocol::T_BOOL, 1);
    xfer += proto->serializedSizeBool(obj->isDoBranch);
    xfer += proto->serializedFieldSize("condition_node_id", apache::thrift::protocol::T_I64, 2);
    xfer += ::apache::thrift::detail::pm::
        protocol_methods<::apache::thrift::type_class::integral, int64_t>::serializedSize<false>(
            *proto, obj->conditionNodeId);
    xfer += proto->serializedSizeStop();
    return xfer;
}

}   // namespace thrift
}   // namespace apache
#endif   // COMMON_GRAPH_PLANNODEBRANCHINFOOPS_H_
