/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_GRAPH_PROFILINGSTATSOPS_H_
#define COMMON_GRAPH_PROFILINGSTATSOPS_H_

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
 * Ops for class ProfilingStatsOps
 *
 *************************************/
namespace detail {

template <>
struct TccStructTraits<nebula::ProfilingStats> {
    static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                   MAYBE_UNUSED int16_t& fid,
                                   MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (false) {
        } else if (_fname == "rows") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_I64;
        } else if (_fname == "exec_duration_in_us") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_I64;
        } else if (_fname == "total_duration_in_us") {
            fid = 3;
            _ftype = apache::thrift::protocol::T_I64;
        } else if (_fname == "other_stats") {
            fid = 4;
            _ftype = apache::thrift::protocol::T_MAP;
        }
    }
};
}   // namespace detail

inline constexpr apache::thrift::protocol::TType Cpp2Ops<::nebula::ProfilingStats>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}

template <class Protocol>
uint32_t Cpp2Ops<::nebula::ProfilingStats>::write(Protocol* proto,
                                                  ::nebula::ProfilingStats const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("ProfilingStats");
    xfer += proto->writeFieldBegin("rows", apache::thrift::protocol::T_I64, 1);
    xfer += ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                           int64_t>::write(*proto, obj->rows);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldBegin("exec_duration_in_us", apache::thrift::protocol::T_I64, 2);
    xfer +=
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                       int64_t>::write(*proto,
                                                                       obj->execDurationInUs);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldBegin("total_duration_in_us", apache::thrift::protocol::T_I64, 3);
    xfer +=
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                       int64_t>::write(*proto,
                                                                       obj->totalDurationInUs);
    xfer += proto->writeFieldEnd();
    if (obj->otherStats != nullptr) {
        xfer += proto->writeFieldBegin("other_stats", apache::thrift::protocol::T_MAP, 4);
        xfer += ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::map<::apache::thrift::type_class::binary,
                                              ::apache::thrift::type_class::binary>,
            std::unordered_map<std::string, std::string>>::write(*proto, *obj->otherStats);
        xfer += proto->writeFieldEnd();
    }
    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}

template <class Protocol>
void Cpp2Ops<::nebula::ProfilingStats>::read(Protocol* proto, ::nebula::ProfilingStats* obj) {
    apache::thrift::detail::ProtocolReaderStructReadState<Protocol> _readState;

    _readState.readStructBegin(proto);

    using apache::thrift::TProtocolException;

    bool isset_rows = false;
    bool isset_exec_duration_in_us = false;
    bool isset_total_duration_in_us = false;

    if (UNLIKELY(!_readState.advanceToNextField(proto, 0, 1, apache::thrift::protocol::T_I64))) {
        goto _loop;
    }
_readField_rows:
    {
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                      int64_t>::read(*proto, obj->rows);
        isset_rows = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 1, 2, apache::thrift::protocol::T_I64))) {
        goto _loop;
    }
_readField_exec_duration_in_us:
    {
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                       int64_t>::read(*proto,
                                                                       obj->execDurationInUs);
        isset_exec_duration_in_us = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 2, 3, apache::thrift::protocol::T_I64))) {
        goto _loop;
    }
_readField_total_duration_in_us:
    {
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                      int64_t>::read(*proto,
                                                                      obj->totalDurationInUs);
        isset_total_duration_in_us = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 3, 4, apache::thrift::protocol::T_MAP))) {
        goto _loop;
    }
_readField_other_stats:
    {
        obj->otherStats = std::make_unique<std::unordered_map<std::string, std::string>>();
        ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::map<::apache::thrift::type_class::binary,
                                              ::apache::thrift::type_class::binary>,
            std::unordered_map<std::string, std::string>>::read(*proto, *obj->otherStats);
        //    this->__isset.other_stats = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 4, 0, apache::thrift::protocol::T_STOP))) {
        goto _loop;
    }

_end:
    _readState.readStructEnd(proto);

    if (!isset_rows) {
        TProtocolException::throwMissingRequiredField("rows", "ProfilingStats");
    }
    if (!isset_exec_duration_in_us) {
        TProtocolException::throwMissingRequiredField("exec_duration_in_us", "ProfilingStats");
    }
    if (!isset_total_duration_in_us) {
        TProtocolException::throwMissingRequiredField("total_duration_in_us", "ProfilingStats");
    }
    return;

_loop:
    if (_readState.fieldType == apache::thrift::protocol::T_STOP) {
        goto _end;
    }
    if (proto->kUsesFieldNames()) {
        apache::thrift::detail::TccStructTraits<nebula::ProfilingStats>::translateFieldName(
            _readState.fieldName(), _readState.fieldId, _readState.fieldType);
    }

    switch (_readState.fieldId) {
        case 1: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_I64)) {
                goto _readField_rows;
            } else {
                goto _skip;
            }
        }
        case 2: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_I64)) {
                goto _readField_exec_duration_in_us;
            } else {
                goto _skip;
            }
        }
        case 3: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_I64)) {
                goto _readField_total_duration_in_us;
            } else {
                goto _skip;
            }
        }
        case 4: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_MAP)) {
                goto _readField_other_stats;
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
uint32_t Cpp2Ops<::nebula::ProfilingStats>::serializedSize(Protocol const* proto,
                                                           ::nebula::ProfilingStats const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("ProfilingStats");
    xfer += proto->serializedFieldSize("rows", apache::thrift::protocol::T_I64, 1);
    xfer +=
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                       int64_t>::serializedSize<false>(*proto,
                                                                                       obj->rows);
    xfer += proto->serializedFieldSize("exec_duration_in_us", apache::thrift::protocol::T_I64, 2);
    xfer += ::apache::thrift::detail::pm::
        protocol_methods<::apache::thrift::type_class::integral, int64_t>::serializedSize<false>(
            *proto, obj->execDurationInUs);
    xfer += proto->serializedFieldSize("total_duration_in_us", apache::thrift::protocol::T_I64, 3);
    xfer += ::apache::thrift::detail::pm::
        protocol_methods<::apache::thrift::type_class::integral, int64_t>::serializedSize<false>(
            *proto, obj->totalDurationInUs);
    if (obj->otherStats != nullptr) {
        xfer += proto->serializedFieldSize("other_stats", apache::thrift::protocol::T_MAP, 4);
        xfer += ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::map<::apache::thrift::type_class::binary,
                                              ::apache::thrift::type_class::binary>,
            std::unordered_map<std::string, std::string>>::serializedSize<false>(*proto,
                                                                                 *obj->otherStats);
    }
    xfer += proto->serializedSizeStop();
    return xfer;
}

template <class Protocol>
uint32_t Cpp2Ops<::nebula::ProfilingStats>::serializedSizeZC(Protocol const* proto,
                                                             ::nebula::ProfilingStats const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("ProfilingStats");
    xfer += proto->serializedFieldSize("rows", apache::thrift::protocol::T_I64, 1);
    xfer +=
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                       int64_t>::serializedSize<false>(*proto,
                                                                                       obj->rows);
    xfer += proto->serializedFieldSize("exec_duration_in_us", apache::thrift::protocol::T_I64, 2);
    xfer += ::apache::thrift::detail::pm::
        protocol_methods<::apache::thrift::type_class::integral, int64_t>::serializedSize<false>(
            *proto, obj->execDurationInUs);
    xfer += proto->serializedFieldSize("total_duration_in_us", apache::thrift::protocol::T_I64, 3);
    xfer += ::apache::thrift::detail::pm::
        protocol_methods<::apache::thrift::type_class::integral, int64_t>::serializedSize<false>(
            *proto, obj->totalDurationInUs);
    if (obj->otherStats != nullptr) {
        xfer += proto->serializedFieldSize("other_stats", apache::thrift::protocol::T_MAP, 4);
        xfer += ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::map<::apache::thrift::type_class::binary,
                                              ::apache::thrift::type_class::binary>,
            std::unordered_map<std::string, std::string>>::serializedSize<false>(*proto,
                                                                                 *obj->otherStats);
    }
    xfer += proto->serializedSizeStop();
    return xfer;
}

}   // namespace thrift
}   // namespace apache
#endif   // COMMON_GRAPH_PROFILINGSTATS_H_
