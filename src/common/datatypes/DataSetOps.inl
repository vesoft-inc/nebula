/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_DATASETOPS_H_
#define COMMON_DATATYPES_DATASETOPS_H_

#include "common/base/Base.h"

#include <thrift/lib/cpp2/GeneratedCodeHelper.h>
#include <thrift/lib/cpp2/gen/module_types_tcc.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>

#include "common/datatypes/DataSet.h"
#include "common/datatypes/CommonCpp2Ops.h"

namespace apache {
namespace thrift {

/**************************************
 *
 * Ops for class DataSet
 *
 *************************************/
namespace detail {

template<>
struct TccStructTraits<nebula::DataSet> {
    static void translateFieldName(
            MAYBE_UNUSED folly::StringPiece _fname,
            MAYBE_UNUSED int16_t& fid,
            MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (_fname == "column_names") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_LIST;
        } else if (_fname == "rows") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_LIST;
        }
    }
};

}  // namespace detail


inline constexpr protocol::TType Cpp2Ops<nebula::DataSet>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}


template <class Protocol>
uint32_t Cpp2Ops<nebula::DataSet>::write(Protocol* proto,
                                         nebula::DataSet const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("DataSet");

    xfer += proto->writeFieldBegin("column_names", protocol::T_LIST, 1);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::binary>,
            std::vector<std::string>
        >::write(*proto, obj->colNames);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldBegin("rows", apache::thrift::protocol::T_LIST, 2);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::structure>,
            std::vector<nebula::Row>
        >::write(*proto, obj->rows);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}


template <class Protocol>
void Cpp2Ops<nebula::DataSet>::read(Protocol* proto, nebula::DataSet* obj) {
    apache::thrift::detail::ProtocolReaderStructReadState<Protocol> readState;

    readState.readStructBegin(proto);

    using apache::thrift::protocol::TProtocolException;

    if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, protocol::T_LIST))) {
        goto _loop;
    }

_readField_column_names:
    {
        obj->colNames = std::vector<std::string>();
        detail::pm::protocol_methods<
                type_class::list<type_class::binary>,
                std::vector<std::string>
            >::read(*proto, obj->colNames);
    }

    if (UNLIKELY(!readState.advanceToNextField(proto, 1, 2, protocol::T_LIST))) {
        goto _loop;
    }

_readField_rows:
    {
        obj->rows = std::vector<nebula::Row>();
        detail::pm::protocol_methods<
                type_class::list<type_class::structure>,
                std::vector<nebula::Row>
            >::read(*proto, obj->rows);
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
        detail::TccStructTraits<nebula::DataSet>::translateFieldName(
            readState.fieldName(),
            readState.fieldId,
            readState.fieldType);
    }

    switch (readState.fieldId) {
        case 1:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_LIST)) {
                goto _readField_column_names;
            } else {
                goto _skip;
            }
        }
        case 2:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_LIST)) {
                goto _readField_rows;
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


template <class Protocol>
uint32_t Cpp2Ops<nebula::DataSet>::serializedSize(Protocol const* proto,
                                                  nebula::DataSet const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("DataSet");

    xfer += proto->serializedFieldSize("column_names", protocol::T_LIST, 1);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::binary>,
            std::vector<std::string>
        >::serializedSize<false>(*proto, obj->colNames);

    xfer += proto->serializedFieldSize("rows", apache::thrift::protocol::T_LIST, 2);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::structure>,
            std::vector<nebula::Row>
        >::serializedSize<false>(*proto, obj->rows);

    xfer += proto->serializedSizeStop();
    return xfer;
}


template <class Protocol>
uint32_t Cpp2Ops<nebula::DataSet>::serializedSizeZC(Protocol const* proto,
                                                    nebula::DataSet const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("DataSet");

    xfer += proto->serializedFieldSize("column_names", protocol::T_LIST, 1);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::binary>,
            std::vector<std::string>
        >::serializedSize<false>(*proto, obj->colNames);

    xfer += proto->serializedFieldSize("rows", protocol::T_LIST, 2);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::structure>,
            std::vector<nebula::Row>
        >::serializedSize<false>(*proto, obj->rows);

    xfer += proto->serializedSizeStop();
    return xfer;
}

}  // namespace thrift
}  // namespace apache

#endif  // COMMON_DATATYPES_DATASETOPS_H_
