/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_LISTOPS_H_
#define COMMON_DATATYPES_LISTOPS_H_

#include "common/base/Base.h"

#include <thrift/lib/cpp2/GeneratedCodeHelper.h>
#include <thrift/lib/cpp2/gen/module_types_tcc.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>

#include "common/datatypes/List.h"
#include "common/datatypes/CommonCpp2Ops.h"

namespace apache {
namespace thrift {

namespace detail {

template<>
struct TccStructTraits<nebula::List> {
    static void translateFieldName(
            MAYBE_UNUSED folly::StringPiece _fname,
            MAYBE_UNUSED int16_t& fid,
            MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (_fname == "values") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_LIST;
        }
    }
};

}  // namespace detail


inline constexpr protocol::TType Cpp2Ops<nebula::List>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}


template<class Protocol>
uint32_t Cpp2Ops<nebula::List>::write(Protocol* proto, nebula::List const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("NList");

    xfer += proto->writeFieldBegin("values", apache::thrift::protocol::T_LIST, 1);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::structure>,
            std::vector<nebula::Value>
        >::write(*proto, obj->values);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}


template<class Protocol>
void Cpp2Ops<nebula::List>::read(Protocol* proto, nebula::List* obj) {
    detail::ProtocolReaderStructReadState<Protocol> readState;

    readState.readStructBegin(proto);

    using apache::thrift::protocol::TProtocolException;

    if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, protocol::T_LIST))) {
        goto _loop;
    }

_readField_values:
    {
        obj->values = std::vector<nebula::Value>();
        detail::pm::protocol_methods<
                type_class::list<type_class::structure>,
                std::vector<nebula::Value>
            >::read(*proto, obj->values);
    }

    if (UNLIKELY(!readState.advanceToNextField(proto, 1, 0, protocol::T_STOP))) {
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
        detail::TccStructTraits<nebula::List>::translateFieldName(
            readState.fieldName(), readState.fieldId, readState.fieldType);
    }

    switch (readState.fieldId) {
        case 1:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_LIST)) {
                goto _readField_values;
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
uint32_t Cpp2Ops<nebula::List>::serializedSize(Protocol const* proto,
                                               nebula::List const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("NList");

    xfer += proto->serializedFieldSize("values", apache::thrift::protocol::T_LIST, 1);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::structure>,
            std::vector<nebula::Value>
        >::serializedSize<false>(*proto, obj->values);
    xfer += proto->serializedSizeStop();
    return xfer;
}


template<class Protocol>
uint32_t Cpp2Ops<nebula::List>::serializedSizeZC(Protocol const* proto,
                                                 nebula::List const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("NList");

    xfer += proto->serializedFieldSize("values", apache::thrift::protocol::T_LIST, 1);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::structure>,
            std::vector<nebula::Value>
        >::serializedSize<false>(*proto, obj->values);
    xfer += proto->serializedSizeStop();
    return xfer;
}

}  // namespace thrift
}  // namespace apache
#endif  // COMMON_DATATYPES_LISTOPS_H_
