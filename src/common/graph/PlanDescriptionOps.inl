/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_GRAPH_PLANDESCRIPTIONOPS_H_
#define COMMON_GRAPH_PLANDESCRIPTIONOPS_H_

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
struct TccStructTraits<::nebula::PlanDescription> {
    static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                   MAYBE_UNUSED int16_t& fid,
                                   MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (false) {
        } else if (_fname == "plan_node_descs") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_LIST;
        } else if (_fname == "node_index_map") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_MAP;
        } else if (_fname == "format") {
            fid = 3;
            _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "optimize_time_in_us") {
            fid = 4;
            _ftype = apache::thrift::protocol::T_I32;
        }
    }
};

}   // namespace detail

inline constexpr apache::thrift::protocol::TType Cpp2Ops<::nebula::PlanDescription>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}

template <class Protocol>
uint32_t Cpp2Ops<::nebula::PlanDescription>::write(Protocol* proto,
                                                   ::nebula::PlanDescription const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("PlanDescription");
    xfer += proto->writeFieldBegin("plan_node_descs", apache::thrift::protocol::T_LIST, 1);
    xfer += ::apache::thrift::detail::pm::protocol_methods<
        ::apache::thrift::type_class::list<::apache::thrift::type_class::structure>,
        std::vector<::nebula::PlanNodeDescription>>::write(*proto, obj->planNodeDescs);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldBegin("node_index_map", apache::thrift::protocol::T_MAP, 2);
    xfer += ::apache::thrift::detail::pm::protocol_methods<
        ::apache::thrift::type_class::map<::apache::thrift::type_class::integral,
                                          ::apache::thrift::type_class::integral>,
        std::unordered_map<int64_t, int64_t>>::write(*proto, obj->nodeIndexMap);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldBegin("format", apache::thrift::protocol::T_STRING, 3);
    xfer += proto->writeBinary(obj->format);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldBegin("optimize_time_in_us", apache::thrift::protocol::T_I32, 4);
    xfer +=
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                       int32_t>::write(*proto,
                                                                       obj->optimize_time_in_us);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}

template <class Protocol>
void Cpp2Ops<::nebula::PlanDescription>::read(Protocol* proto, ::nebula::PlanDescription* obj) {
    apache::thrift::detail::ProtocolReaderStructReadState<Protocol> _readState;

    _readState.readStructBegin(proto);

    using apache::thrift::TProtocolException;

    bool isset_plan_node_descs = false;
    bool isset_node_index_map = false;
    bool isset_format = false;
    bool isset_optimize_time_in_us = false;

    if (UNLIKELY(!_readState.advanceToNextField(proto, 0, 1, apache::thrift::protocol::T_LIST))) {
        goto _loop;
    }
_readField_plan_node_descs:
    {
        obj->planNodeDescs = std::vector<::nebula::PlanNodeDescription>();
        ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::list<::apache::thrift::type_class::structure>,
            std::vector<::nebula::PlanNodeDescription>>::read(*proto, obj->planNodeDescs);
        isset_plan_node_descs = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 1, 2, apache::thrift::protocol::T_MAP))) {
        goto _loop;
    }
_readField_node_index_map:
    {
        obj->nodeIndexMap = std::unordered_map<int64_t, int64_t>();
        ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::map<::apache::thrift::type_class::integral,
                                              ::apache::thrift::type_class::integral>,
            std::unordered_map<int64_t, int64_t>>::read(*proto, obj->nodeIndexMap);
        isset_node_index_map = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 2, 3, apache::thrift::protocol::T_STRING))) {
        goto _loop;
    }
_readField_format:
    {
        proto->readBinary(obj->format);
        isset_format = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 3, 4, apache::thrift::protocol::T_I32))) {
        goto _loop;
    }
_readField_optimize_in_us:
    {
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                       int32_t>::read(*proto,
                                                                      obj->optimize_time_in_us);
        isset_optimize_time_in_us = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 4, 0, apache::thrift::protocol::T_STOP))) {
        goto _loop;
    }

_end:
    _readState.readStructEnd(proto);

    if (!isset_plan_node_descs) {
        TProtocolException::throwMissingRequiredField("plan_node_descs", "PlanDescription");
    }
    if (!isset_node_index_map) {
        TProtocolException::throwMissingRequiredField("node_index_map", "PlanDescription");
    }
    if (!isset_format) {
        TProtocolException::throwMissingRequiredField("format", "PlanDescription");
    }
    if (!isset_optimize_time_in_us) {
        TProtocolException::throwMissingRequiredField("optimize_time_in_us", "PlanDescription");
    }
    return;

_loop:
    if (_readState.fieldType == apache::thrift::protocol::T_STOP) {
        goto _end;
    }
    if (proto->kUsesFieldNames()) {
        apache::thrift::detail::TccStructTraits<nebula::PlanDescription>::translateFieldName(
            _readState.fieldName(), _readState.fieldId, _readState.fieldType);
    }

    switch (_readState.fieldId) {
        case 1: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_LIST)) {
                goto _readField_plan_node_descs;
            } else {
                goto _skip;
            }
        }
        case 2: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_MAP)) {
                goto _readField_node_index_map;
            } else {
                goto _skip;
            }
        }
        case 3: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_format;
            } else {
                goto _skip;
            }
        }
        case 4: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_I32)) {
                goto _readField_optimize_in_us;
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
uint32_t Cpp2Ops<::nebula::PlanDescription>::serializedSize(Protocol const* proto,
                                                            ::nebula::PlanDescription const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("PlanDescription");
    xfer += proto->serializedFieldSize("plan_node_descs", apache::thrift::protocol::T_LIST, 1);
    xfer += ::apache::thrift::detail::pm::protocol_methods<
        ::apache::thrift::type_class::list<::apache::thrift::type_class::structure>,
        std::vector<::nebula::PlanNodeDescription>>::serializedSize<false>(*proto,
                                                                           obj->planNodeDescs);
    xfer += proto->serializedFieldSize("node_index_map", apache::thrift::protocol::T_MAP, 2);
    xfer += ::apache::thrift::detail::pm::protocol_methods<
        ::apache::thrift::type_class::map<::apache::thrift::type_class::integral,
                                          ::apache::thrift::type_class::integral>,
        std::unordered_map<int64_t, int64_t>>::serializedSize<false>(*proto, obj->nodeIndexMap);
    xfer += proto->serializedFieldSize("format", apache::thrift::protocol::T_STRING, 3);
    xfer += proto->serializedSizeBinary(obj->format);
    xfer += proto->serializedFieldSize("optimize_time_in_us", apache::thrift::protocol::T_I32, 4);
    xfer += ::apache::thrift::detail::pm::
        protocol_methods<::apache::thrift::type_class::integral, int32_t>::serializedSize<false>(
            *proto, obj->optimize_time_in_us);
    xfer += proto->serializedSizeStop();
    return xfer;
}

template <class Protocol>
uint32_t Cpp2Ops<::nebula::PlanDescription>::serializedSizeZC(
    Protocol const* proto,
    ::nebula::PlanDescription const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("PlanDescription");
    xfer += proto->serializedFieldSize("plan_node_descs", apache::thrift::protocol::T_LIST, 1);
    xfer += ::apache::thrift::detail::pm::protocol_methods<
        ::apache::thrift::type_class::list<::apache::thrift::type_class::structure>,
        std::vector<::nebula::PlanNodeDescription>>::serializedSize<false>(*proto,
                                                                           obj->planNodeDescs);
    xfer += proto->serializedFieldSize("node_index_map", apache::thrift::protocol::T_MAP, 2);
    xfer += ::apache::thrift::detail::pm::protocol_methods<
        ::apache::thrift::type_class::map<::apache::thrift::type_class::integral,
                                          ::apache::thrift::type_class::integral>,
        std::unordered_map<int64_t, int64_t>>::serializedSize<false>(*proto, obj->nodeIndexMap);
    xfer += proto->serializedFieldSize("format", apache::thrift::protocol::T_STRING, 3);
    xfer += proto->serializedSizeZCBinary(obj->format);
    xfer += proto->serializedFieldSize("optimize_time_in_us", apache::thrift::protocol::T_I32, 4);
    xfer += ::apache::thrift::detail::pm::
        protocol_methods<::apache::thrift::type_class::integral, int32_t>::serializedSize<false>(
            *proto, obj->optimize_time_in_us);
    xfer += proto->serializedSizeStop();
    return xfer;
}

}   // namespace thrift
}   // namespace apache
#endif   // COMMON_GRAPH_PLANDESCRIPTIONOPS_H_
