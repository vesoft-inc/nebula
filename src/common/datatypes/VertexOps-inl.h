/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_VERTEXOPS_H_
#define COMMON_DATATYPES_VERTEXOPS_H_

#include "common/base/Base.h"

#include <thrift/lib/cpp2/GeneratedCodeHelper.h>
#include <thrift/lib/cpp2/gen/module_types_tcc.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>

#include "common/datatypes/Vertex.h"
#include "common/datatypes/CommonCpp2Ops.h"

namespace apache {
namespace thrift {

/**************************************
 *
 * Ops for class Tag
 *
 *************************************/
namespace detail {

template<>
struct TccStructTraits<nebula::Tag> {
    static void translateFieldName(
            MAYBE_UNUSED folly::StringPiece _fname,
            MAYBE_UNUSED int16_t& fid,
            MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (_fname == "name") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "props") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_MAP;
        }
    }
};

}  // namespace detail


inline constexpr apache::thrift::protocol::TType Cpp2Ops<nebula::Tag>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}


template <class Protocol>
uint32_t Cpp2Ops<nebula::Tag>::write(Protocol* proto, nebula::Tag const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("Tag");

    xfer += proto->writeFieldBegin("name", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->writeBinary(obj->name);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldBegin("props", apache::thrift::protocol::T_MAP, 2);
    xfer += detail::pm::protocol_methods<
            type_class::map<type_class::binary, type_class::structure>,
            std::unordered_map<std::string, nebula::Value>
        >::write(*proto, obj->props);
    xfer += proto->writeFieldEnd();

    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}


template <class Protocol>
void Cpp2Ops<nebula::Tag>::read(Protocol* proto, nebula::Tag* obj) {
    apache::thrift::detail::ProtocolReaderStructReadState<Protocol> readState;

    readState.readStructBegin(proto);

    using apache::thrift::protocol::TProtocolException;

    if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, protocol::T_STRING))) {
        goto _loop;
    }

_readField_name:
    {
        proto->readBinary(obj->name);
    }

    if (UNLIKELY(!readState.advanceToNextField(proto, 1, 2, protocol::T_MAP))) {
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
        detail::TccStructTraits<nebula::Tag>::translateFieldName(readState.fieldName(),
                                                                 readState.fieldId,
                                                                 readState.fieldType);
    }

    switch (readState.fieldId) {
        case 1:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_name;
            } else {
                goto _skip;
            }
        }
        case 2:
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


template <class Protocol>
uint32_t Cpp2Ops<nebula::Tag>::serializedSize(Protocol const* proto,
                                              nebula::Tag const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("Tag");

    xfer += proto->serializedFieldSize("name", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->serializedSizeBinary(obj->name);

    xfer += proto->serializedFieldSize("props", apache::thrift::protocol::T_MAP, 2);
    xfer += detail::pm::protocol_methods<
            type_class::map<type_class::binary, type_class::structure>,
            std::unordered_map<std::string, nebula::Value>
        >::serializedSize<false>(*proto, obj->props);

    xfer += proto->serializedSizeStop();
    return xfer;
}


template <class Protocol>
uint32_t Cpp2Ops<nebula::Tag>::serializedSizeZC(Protocol const* proto,
                                                nebula::Tag const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("Tag");

    xfer += proto->serializedFieldSize("name", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->serializedSizeZCBinary(obj->name);

    xfer += proto->serializedFieldSize("props", apache::thrift::protocol::T_MAP, 2);
    xfer += detail::pm::protocol_methods<
            type_class::map<type_class::binary, type_class::structure>,
            std::unordered_map<std::string, nebula::Value>
        >::serializedSize<false>(*proto, obj->props);

    xfer += proto->serializedSizeStop();
    return xfer;
}


/**************************************
 *
 * Ops for class Vertex
 *
 *************************************/
namespace detail {

template <>
struct TccStructTraits<nebula::Vertex> {
    static void translateFieldName(
            MAYBE_UNUSED folly::StringPiece _fname,
            MAYBE_UNUSED int16_t& fid,
            MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (_fname == "vid") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_STRUCT;
        } else if (_fname == "tags") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_LIST;
        }
    }
};

}  // namespace detail


inline constexpr protocol::TType Cpp2Ops<nebula::Vertex>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}


template<class Protocol>
uint32_t Cpp2Ops<nebula::Vertex>::write(Protocol* proto, nebula::Vertex const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("Vertex");
    xfer += proto->writeFieldBegin("vid", apache::thrift::protocol::T_STRUCT, 1);
    xfer += ::apache::thrift::Cpp2Ops<nebula::Value>::write(proto, &obj->vid);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldBegin("tags", apache::thrift::protocol::T_LIST, 2);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::structure>,
            std::vector<nebula::Tag>
        >::write(*proto, obj->tags);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}


template<class Protocol>
void Cpp2Ops<nebula::Vertex>::read(Protocol* proto, nebula::Vertex* obj) {
    detail::ProtocolReaderStructReadState<Protocol> readState;

    readState.readStructBegin(proto);

    using apache::thrift::protocol::TProtocolException;


    if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, protocol::T_STRUCT))) {
        goto _loop;
    }

_readField_vid:
    {
        ::apache::thrift::Cpp2Ops<nebula::Value>::read(proto, &obj->vid);
    }

    if (UNLIKELY(!readState.advanceToNextField(proto, 1, 2, protocol::T_LIST))) {
        goto _loop;
    }

_readField_tags:
    {
        obj->tags = std::vector<nebula::Tag>();
        detail::pm::protocol_methods<
                type_class::list<type_class::structure>,
                std::vector<nebula::Tag>
            >::read(*proto, obj->tags);
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
        detail::TccStructTraits<nebula::Vertex>::translateFieldName(readState.fieldName(),
                                                                    readState.fieldId,
                                                                    readState.fieldType);
    }

    switch (readState.fieldId) {
        case 1:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_STRUCT)) {
                goto _readField_vid;
            } else {
                goto _skip;
            }
        }
        case 2:
        {
          if (LIKELY(readState.fieldType == apache::thrift::protocol::T_LIST)) {
                goto _readField_tags;
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
uint32_t Cpp2Ops<nebula::Vertex>::serializedSize(Protocol const* proto,
                                                 nebula::Vertex const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("Vertex");
    xfer += proto->serializedFieldSize("vid", apache::thrift::protocol::T_STRUCT, 1);
    xfer += ::apache::thrift::Cpp2Ops<nebula::Value>::serializedSize(proto, &obj->vid);
    xfer += proto->serializedFieldSize("tags", apache::thrift::protocol::T_LIST, 2);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::structure>,
            std::vector<nebula::Tag>
        >::serializedSize<false>(*proto, obj->tags);
    xfer += proto->serializedSizeStop();
    return xfer;
}


template<class Protocol>
uint32_t Cpp2Ops<nebula::Vertex>::serializedSizeZC(Protocol const* proto,
                                                   nebula::Vertex const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("Vertex");
    xfer += proto->serializedFieldSize("vid", apache::thrift::protocol::T_STRUCT, 1);
    xfer += ::apache::thrift::Cpp2Ops<nebula::Value>::serializedSizeZC(proto, &obj->vid);
    xfer += proto->serializedFieldSize("tags", apache::thrift::protocol::T_LIST, 2);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::structure>,
            std::vector<nebula::Tag>
        >::serializedSize<false>(*proto, obj->tags);
    xfer += proto->serializedSizeStop();
    return xfer;
}

}  // namespace thrift
}  // namespace apache
#endif  // COMMON_DATATYPES_VERTEXOPS_H_
