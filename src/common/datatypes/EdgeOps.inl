/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * obj source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_EDGEOPS_H_
#define COMMON_DATATYPES_EDGEOPS_H_

#include "common/base/Base.h"

#include <thrift/lib/cpp2/GeneratedSerializationCodeHelper.h>
#include <thrift/lib/cpp2/gen/module_types_tcc.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>

#include "common/datatypes/Edge.h"

namespace apache {
namespace thrift {

namespace detail {

template <>
struct TccStructTraits<nebula::Edge> {
    static void translateFieldName(
            MAYBE_UNUSED folly::StringPiece _fname,
            MAYBE_UNUSED int16_t& fid,
            MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (_fname == "src") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "dst") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "type") {
            fid = 3;
            _ftype = apache::thrift::protocol::T_I32;
        } else if (_fname == "name") {
            fid = 4;
            _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "ranking") {
            fid = 5;
            _ftype = apache::thrift::protocol::T_I64;
        } else if (_fname == "props") {
            fid = 6;
            _ftype = apache::thrift::protocol::T_MAP;
        }
    }
};

}  // namespace detail


template<>
inline void Cpp2Ops<nebula::Edge>::clear(nebula::Edge* obj) {
    return obj->clear();
}


template<>
inline constexpr protocol::TType Cpp2Ops<nebula::Edge>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}


template<>
template<class Protocol>
uint32_t Cpp2Ops<nebula::Edge>::write(Protocol* proto, nebula::Edge const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("Edge");

    xfer += proto->writeFieldBegin("src", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->writeBinary(obj->src);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldBegin("dst", apache::thrift::protocol::T_STRING, 2);
    xfer += proto->writeBinary(obj->dst);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldBegin("type", apache::thrift::protocol::T_I32, 3);
    // NOTICE: The original id will be transformed to +1/-1, to indicate the edge direction.
    auto type = obj->type;
    if (type != 0) {
        type > 0 ? type = 1 : type = -1;
    }
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::EdgeType>
        ::write(*proto, type);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldBegin("name", apache::thrift::protocol::T_STRING, 4);
    xfer += proto->writeBinary(obj->name);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldBegin("ranking", apache::thrift::protocol::T_I64, 5);
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::EdgeRanking>
        ::write(*proto, obj->ranking);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldBegin("props", apache::thrift::protocol::T_MAP, 6);
    xfer += detail::pm::protocol_methods<
            type_class::map<type_class::binary, type_class::structure>,
            std::unordered_map<std::string, nebula::Value>
        >::write(*proto, obj->props);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}


template<>
template<class Protocol>
void Cpp2Ops<nebula::Edge>::read(Protocol* proto, nebula::Edge* obj) {
    detail::ProtocolReaderStructReadState<Protocol> readState;

    readState.readStructBegin(proto);

    using apache::thrift::protocol::TProtocolException;

    if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, protocol::T_STRING))) {
        goto _loop;
    }

_readField_src:
    {
        proto->readBinary(obj->src);
    }

    if (UNLIKELY(!readState.advanceToNextField(proto, 1, 2, protocol::T_STRING))) {
        goto _loop;
    }

_readField_dst:
    {
        proto->readBinary(obj->dst);
    }

    if (UNLIKELY(!readState.advanceToNextField(proto, 2, 3, protocol::T_I32))) {
        goto _loop;
    }

_readField_type:
    {
        detail::pm::protocol_methods<type_class::integral, nebula::EdgeType>
            ::read(*proto, obj->type);
    }

    if (UNLIKELY(!readState.advanceToNextField(proto, 3, 4, protocol::T_STRING))) {
        goto _loop;
    }

_readField_name:
    {
        proto->readBinary(obj->name);
    }

    if (UNLIKELY(!readState.advanceToNextField(proto, 4, 5, protocol::T_I64))) {
        goto _loop;
    }

_readField_ranking:
    {
        detail::pm::protocol_methods<type_class::integral, nebula::EdgeRanking>
            ::read(*proto, obj->ranking);
    }

    if (UNLIKELY(!readState.advanceToNextField(proto, 5, 6, protocol::T_MAP))) {
        goto _loop;
    }

_readField_props:
    {
        obj->props = std::unordered_map<std::string, nebula::Value>();
        detail::pm::protocol_methods<
                type_class::map<type_class::binary, type_class::structure>,
                std::unordered_map<std::string, nebula::Value>
            >::read(*proto, obj->props);
    }

    if (UNLIKELY(!readState.advanceToNextField(proto, 6, 0, protocol::T_STOP))) {
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
        detail::TccStructTraits<nebula::Edge>::translateFieldName(readState.fieldName(),
                                                                  readState.fieldId,
                                                                  readState.fieldType);
    }

    switch (readState.fieldId) {
        case 1:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_src;
            } else {
                goto _skip;
            }
        }
        case 2:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_dst;
            } else {
                goto _skip;
            }
        }
        case 3:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_I32)) {
                goto _readField_type;
            } else {
                goto _skip;
            }
        }
        case 4:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_name;
            } else {
                goto _skip;
            }
        }
        case 5:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_I64)) {
                goto _readField_ranking;
            } else {
                goto _skip;
            }
        }
        case 6:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_MAP)) {
                goto _readField_props;
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
uint32_t Cpp2Ops<nebula::Edge>::serializedSize(Protocol const* proto,
                                               nebula::Edge const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("Edge");

    xfer += proto->serializedFieldSize("src", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->serializedSizeBinary(obj->src);

    xfer += proto->serializedFieldSize("dst", apache::thrift::protocol::T_STRING, 2);
    xfer += proto->serializedSizeBinary(obj->dst);

    xfer += proto->serializedFieldSize("type", apache::thrift::protocol::T_I32, 3);
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::EdgeType>
        ::serializedSize<false>(*proto, obj->type);

    xfer += proto->serializedFieldSize("name", apache::thrift::protocol::T_STRING, 4);
    xfer += proto->serializedSizeBinary(obj->name);

    xfer += proto->serializedFieldSize("ranking", apache::thrift::protocol::T_I64, 5);
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::EdgeRanking>
        ::serializedSize<false>(*proto, obj->ranking);

    xfer += proto->serializedFieldSize("props", apache::thrift::protocol::T_MAP, 6);
    xfer += detail::pm::protocol_methods<
            type_class::map<type_class::binary, type_class::structure>,
            std::unordered_map<std::string, nebula::Value>
        >::serializedSize<false>(*proto, obj->props);

    xfer += proto->serializedSizeStop();
    return xfer;
}


template<>
template<class Protocol>
uint32_t Cpp2Ops<nebula::Edge>::serializedSizeZC(Protocol const* proto,
                                                 nebula::Edge const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("Edge");

    xfer += proto->serializedFieldSize("src", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->serializedSizeZCBinary(obj->src);

    xfer += proto->serializedFieldSize("dst", apache::thrift::protocol::T_STRING, 2);
    xfer += proto->serializedSizeZCBinary(obj->dst);

    xfer += proto->serializedFieldSize("type", apache::thrift::protocol::T_I32, 3);
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::EdgeType>
        ::serializedSize<false>(*proto, obj->type);

    xfer += proto->serializedFieldSize("name", apache::thrift::protocol::T_STRING, 4);
    xfer += proto->serializedSizeZCBinary(obj->name);

    xfer += proto->serializedFieldSize("ranking", apache::thrift::protocol::T_I64, 5);
    xfer += detail::pm::protocol_methods<type_class::integral, nebula::EdgeRanking>
        ::serializedSize<false>(*proto, obj->ranking);

    xfer += proto->serializedFieldSize("props", apache::thrift::protocol::T_MAP, 6);
    xfer += detail::pm::protocol_methods<
            type_class::map<type_class::binary, type_class::structure>,
            std::unordered_map<std::string, nebula::Value>
        >::serializedSize<false>(*proto, obj->props);

    xfer += proto->serializedSizeStop();
    return xfer;
}

}  // namespace thrift
}  // namespace apache
#endif  // COMMON_DATATYPES_EDGEOPS_H_
