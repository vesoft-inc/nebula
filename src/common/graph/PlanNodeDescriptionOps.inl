/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_GRAPH_PLANNODEDESCRIPTIONOPS_H_
#define COMMON_GRAPH_PLANNODEDESCRIPTIONOPS_H_

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
 * Ops for class PlanNodeDescription
 *
 *************************************/
namespace detail {

template <>
struct TccStructTraits<::nebula::PlanNodeDescription> {
    static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                   MAYBE_UNUSED int16_t& fid,
                                   MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (false) {
        } else if (_fname == "name") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "id") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_I64;
        } else if (_fname == "output_var") {
            fid = 3;
            _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "description") {
            fid = 4;
            _ftype = apache::thrift::protocol::T_LIST;
        } else if (_fname == "profiles") {
            fid = 5;
            _ftype = apache::thrift::protocol::T_LIST;
        } else if (_fname == "branch_info") {
            fid = 6;
            _ftype = apache::thrift::protocol::T_STRUCT;
        } else if (_fname == "dependencies") {
            fid = 7;
            _ftype = apache::thrift::protocol::T_LIST;
        }
    }
};

}   // namespace detail

inline constexpr apache::thrift::protocol::TType
Cpp2Ops<::nebula::PlanNodeDescription>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}

template <class Protocol>
uint32_t Cpp2Ops<::nebula::PlanNodeDescription>::write(Protocol* proto,
                                                       ::nebula::PlanNodeDescription const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("PlanNodeDescription");
    xfer += proto->writeFieldBegin("name", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->writeBinary(obj->name);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldBegin("id", apache::thrift::protocol::T_I64, 2);
    xfer += ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                           int64_t>::write(*proto, obj->id);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldBegin("output_var", apache::thrift::protocol::T_STRING, 3);
    xfer += proto->writeBinary(obj->outputVar);
    xfer += proto->writeFieldEnd();
    if (obj->description != nullptr) {
        xfer += proto->writeFieldBegin("description", apache::thrift::protocol::T_LIST, 4);
        xfer += ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::list<::apache::thrift::type_class::structure>,
            std::vector<::nebula::Pair>>::write(*proto, *obj->description);
        xfer += proto->writeFieldEnd();
    }
    if (obj->profiles != nullptr) {
        xfer += proto->writeFieldBegin("profiles", apache::thrift::protocol::T_LIST, 5);
        xfer += ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::list<::apache::thrift::type_class::structure>,
            std::vector<::nebula::ProfilingStats>>::write(*proto, *obj->profiles);
        xfer += proto->writeFieldEnd();
    }
    if (obj->branchInfo != nullptr) {
        xfer += proto->writeFieldBegin("branch_info", apache::thrift::protocol::T_STRUCT, 6);
        xfer += ::apache::thrift::Cpp2Ops<::nebula::PlanNodeBranchInfo>::write(
            proto, obj->branchInfo.get());
        xfer += proto->writeFieldEnd();
    }
    if (obj->dependencies != nullptr) {
        xfer += proto->writeFieldBegin("dependencies", apache::thrift::protocol::T_LIST, 7);
        xfer += ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::list<::apache::thrift::type_class::integral>,
            std::vector<int64_t>>::write(*proto, *obj->dependencies);
        xfer += proto->writeFieldEnd();
    }
    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}

template <class Protocol>
void Cpp2Ops<::nebula::PlanNodeDescription>::read(Protocol* proto,
                                                  ::nebula::PlanNodeDescription* obj) {
    apache::thrift::detail::ProtocolReaderStructReadState<Protocol> _readState;

    _readState.readStructBegin(proto);

    using apache::thrift::TProtocolException;

    bool isset_name = false;
    bool isset_id = false;
    bool isset_output_var = false;

    if (UNLIKELY(!_readState.advanceToNextField(proto, 0, 1, apache::thrift::protocol::T_STRING))) {
        goto _loop;
    }
_readField_name:
    {
        proto->readBinary(obj->name);
        isset_name = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 1, 2, apache::thrift::protocol::T_I64))) {
        goto _loop;
    }
_readField_id:
    {
        ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                      int64_t>::read(*proto, obj->id);
        isset_id = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 2, 3, apache::thrift::protocol::T_STRING))) {
        goto _loop;
    }
_readField_output_var:
    {
        proto->readBinary(obj->outputVar);
        isset_output_var = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 3, 4, apache::thrift::protocol::T_LIST))) {
        goto _loop;
    }
_readField_description:
    {
        obj->description = std::make_unique<std::vector<::nebula::Pair>>();
        ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::list<::apache::thrift::type_class::structure>,
            std::vector<::nebula::Pair>>::read(*proto, *obj->description);
        //    this->__isset.description = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 4, 5, apache::thrift::protocol::T_LIST))) {
        goto _loop;
    }
_readField_profiles:
    {
        obj->profiles = std::make_unique<std::vector<::nebula::ProfilingStats>>();
        ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::list<::apache::thrift::type_class::structure>,
            std::vector<::nebula::ProfilingStats>>::read(*proto, *obj->profiles);
        //    this->__isset.profiles = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 5, 6, apache::thrift::protocol::T_STRUCT))) {
        goto _loop;
    }
_readField_branch_info:
    {
        obj->branchInfo = std::make_unique<nebula::PlanNodeBranchInfo>();
        ::apache::thrift::Cpp2Ops<::nebula::PlanNodeBranchInfo>::read(proto,
                                                                      obj->branchInfo.get());
        //    this->__isset.branch_info = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 6, 7, apache::thrift::protocol::T_LIST))) {
        goto _loop;
    }
_readField_dependencies:
    {
        obj->dependencies = std::make_unique<std::vector<int64_t>>();
        ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::list<::apache::thrift::type_class::integral>,
            std::vector<int64_t>>::read(*proto, *obj->dependencies);
        //    this->__isset.dependencies = true;
    }

    if (UNLIKELY(!_readState.advanceToNextField(proto, 7, 0, apache::thrift::protocol::T_STOP))) {
        goto _loop;
    }

_end:
    _readState.readStructEnd(proto);

    if (!isset_name) {
        TProtocolException::throwMissingRequiredField("name", "PlanNodeDescription");
    }
    if (!isset_id) {
        TProtocolException::throwMissingRequiredField("id", "PlanNodeDescription");
    }
    if (!isset_output_var) {
        TProtocolException::throwMissingRequiredField("output_var", "PlanNodeDescription");
    }
    return;

_loop:
    if (_readState.fieldType == apache::thrift::protocol::T_STOP) {
        goto _end;
    }
    if (proto->kUsesFieldNames()) {
        apache::thrift::detail::TccStructTraits<nebula::PlanNodeDescription>::translateFieldName(
            _readState.fieldName(), _readState.fieldId, _readState.fieldType);
    }

    switch (_readState.fieldId) {
        case 1: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_name;
            } else {
                goto _skip;
            }
        }
        case 2: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_I64)) {
                goto _readField_id;
            } else {
                goto _skip;
            }
        }
        case 3: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_output_var;
            } else {
                goto _skip;
            }
        }
        case 4: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_LIST)) {
                goto _readField_description;
            } else {
                goto _skip;
            }
        }
        case 5: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_LIST)) {
                goto _readField_profiles;
            } else {
                goto _skip;
            }
        }
        case 6: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_STRUCT)) {
                goto _readField_branch_info;
            } else {
                goto _skip;
            }
        }
        case 7: {
            if (LIKELY(_readState.fieldType == apache::thrift::protocol::T_LIST)) {
                goto _readField_dependencies;
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
uint32_t Cpp2Ops<::nebula::PlanNodeDescription>::serializedSize(
    Protocol const* proto,
    ::nebula::PlanNodeDescription const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("PlanNodeDescription");
    xfer += proto->serializedFieldSize("name", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->serializedSizeBinary(obj->name);
    xfer += proto->serializedFieldSize("id", apache::thrift::protocol::T_I64, 2);
    xfer += ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                           int64_t>::serializedSize<false>(*proto,
                                                                                           obj->id);
    xfer += proto->serializedFieldSize("output_var", apache::thrift::protocol::T_STRING, 3);
    xfer += proto->serializedSizeBinary(obj->outputVar);
    if (obj->description != nullptr) {
        xfer += proto->serializedFieldSize("description", apache::thrift::protocol::T_LIST, 4);
        xfer += ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::list<::apache::thrift::type_class::structure>,
            std::vector<::nebula::Pair>>::serializedSize<false>(*proto, *obj->description);
    }
    if (obj->profiles != nullptr) {
        xfer += proto->serializedFieldSize("profiles", apache::thrift::protocol::T_LIST, 5);
        xfer += ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::list<::apache::thrift::type_class::structure>,
            std::vector<::nebula::ProfilingStats>>::serializedSize<false>(*proto, *obj->profiles);
    }
    if (obj->branchInfo != nullptr) {
        xfer += proto->serializedFieldSize("branch_info", apache::thrift::protocol::T_STRUCT, 6);
        xfer += ::apache::thrift::Cpp2Ops<::nebula::PlanNodeBranchInfo>::serializedSize(
            proto, obj->branchInfo.get());
    }
    if (obj->dependencies != nullptr) {
        xfer += proto->serializedFieldSize("dependencies", apache::thrift::protocol::T_LIST, 7);
        xfer += ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::list<::apache::thrift::type_class::integral>,
            std::vector<int64_t>>::serializedSize<false>(*proto, *obj->dependencies);
    }
    xfer += proto->serializedSizeStop();
    return xfer;
}

template <class Protocol>
uint32_t Cpp2Ops<::nebula::PlanNodeDescription>::serializedSizeZC(
    Protocol const* proto,
    ::nebula::PlanNodeDescription const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("PlanNodeDescription");
    xfer += proto->serializedFieldSize("name", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->serializedSizeZCBinary(obj->name);
    xfer += proto->serializedFieldSize("id", apache::thrift::protocol::T_I64, 2);
    xfer += ::apache::thrift::detail::pm::protocol_methods<::apache::thrift::type_class::integral,
                                                           int64_t>::serializedSize<false>(*proto,
                                                                                           obj->id);
    xfer += proto->serializedFieldSize("output_var", apache::thrift::protocol::T_STRING, 3);
    xfer += proto->serializedSizeZCBinary(obj->outputVar);
    if (obj->description != nullptr) {
        xfer += proto->serializedFieldSize("description", apache::thrift::protocol::T_LIST, 4);
        xfer += ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::list<::apache::thrift::type_class::structure>,
            std::vector<::nebula::Pair>>::serializedSize<false>(*proto, *obj->description);
    }
    if (obj->profiles != nullptr) {
        xfer += proto->serializedFieldSize("profiles", apache::thrift::protocol::T_LIST, 5);
        xfer += ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::list<::apache::thrift::type_class::structure>,
            std::vector<::nebula::ProfilingStats>>::serializedSize<false>(*proto, *obj->profiles);
    }
    if (obj->branchInfo != nullptr) {
        xfer += proto->serializedFieldSize("branch_info", apache::thrift::protocol::T_STRUCT, 6);
        xfer += ::apache::thrift::Cpp2Ops<::nebula::PlanNodeBranchInfo>::serializedSizeZC(
            proto, obj->branchInfo.get());
    }
    if (obj->dependencies != nullptr) {
        xfer += proto->serializedFieldSize("dependencies", apache::thrift::protocol::T_LIST, 7);
        xfer += ::apache::thrift::detail::pm::protocol_methods<
            ::apache::thrift::type_class::list<::apache::thrift::type_class::integral>,
            std::vector<int64_t>>::serializedSize<false>(*proto, *obj->dependencies);
    }
    xfer += proto->serializedSizeStop();
    return xfer;
}

}   // namespace thrift
}   // namespace apache
#endif   // COMMON_GRAPH_PLANNODEDESCRIPTIONOPS_H_
