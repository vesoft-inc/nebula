/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_DATATYPES_GEOGRAPHYOPS_H_
#define COMMON_DATATYPES_GEOGRAPHYOPS_H_

#include <thrift/lib/cpp2/GeneratedCodeHelper.h>
#include <thrift/lib/cpp2/gen/module_types_tcc.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>

#include "common/base/Base.h"
#include "common/datatypes/CommonCpp2Ops.h"
#include "common/datatypes/Geography.h"

namespace apache {
namespace thrift {

/**************************************
 *
 * Ops for struct Coordinate
 *
 *************************************/
namespace detail {

template <>
struct TccStructTraits<nebula::Coordinate> {
  static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                 MAYBE_UNUSED int16_t& fid,
                                 MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
    if (_fname == "x") {
      fid = 1;
      _ftype = apache::thrift::protocol::T_DOUBLE;
    } else if (_fname == "y") {
      fid = 2;
      _ftype = apache::thrift::protocol::T_DOUBLE;
    }
  }
};

}  // namespace detail

inline constexpr protocol::TType Cpp2Ops<nebula::Coordinate>::thriftType() {
  return apache::thrift::protocol::T_STRUCT;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Coordinate>::write(Protocol* proto, nebula::Coordinate const* obj) {
  uint32_t xfer = 0;
  xfer += proto->writeStructBegin("Coordinate");
  xfer += proto->writeFieldBegin("x", protocol::T_DOUBLE, 1);
  xfer += proto->writeDouble(obj->x);
  xfer += proto->writeFieldEnd();
  xfer += proto->writeFieldBegin("y", protocol::T_DOUBLE, 2);
  xfer += proto->writeDouble(obj->y);
  xfer += proto->writeFieldEnd();
  xfer += proto->writeFieldStop();
  xfer += proto->writeStructEnd();
  return xfer;
}

template <class Protocol>
void Cpp2Ops<nebula::Coordinate>::read(Protocol* proto, nebula::Coordinate* obj) {
  detail::ProtocolReaderStructReadState<Protocol> readState;

  readState.readStructBegin(proto);

  using apache::thrift::TProtocolException;

  if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, protocol::T_DOUBLE))) {
    goto _loop;
  }
_readField_x : { proto->readDouble(obj->x); }

  if (UNLIKELY(!readState.advanceToNextField(proto, 1, 2, protocol::T_DOUBLE))) {
    goto _loop;
  }
_readField_y : { proto->readDouble(obj->y); }

  if (UNLIKELY(!readState.advanceToNextField(proto, 2, 0, apache::thrift::protocol::T_STOP))) {
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
    detail::TccStructTraits<nebula::Coordinate>::translateFieldName(
        readState.fieldName(), readState.fieldId, readState.fieldType);
  }

  switch (readState.fieldId) {
    case 1: {
      if (LIKELY(readState.fieldType == apache::thrift::protocol::T_DOUBLE)) {
        goto _readField_x;
      } else {
        goto _skip;
      }
    }
    case 2: {
      if (LIKELY(readState.fieldType == apache::thrift::protocol::T_DOUBLE)) {
        goto _readField_y;
      } else {
        goto _skip;
      }
    }
    default: {
_skip:
      proto->skip(readState.fieldType);
      readState.readFieldEnd(proto);
      readState.readFieldBeginNoInline(proto);
      goto _loop;
    }
  }
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Coordinate>::serializedSize(Protocol const* proto,
                                                     nebula::Coordinate const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("Coordinate");
  xfer += proto->serializedFieldSize("x", apache::thrift::protocol::T_DOUBLE, 1);
  xfer += proto->serializedSizeDouble(obj->x);
  xfer += proto->serializedFieldSize("y", apache::thrift::protocol::T_DOUBLE, 2);
  xfer += proto->serializedSizeDouble(obj->y);
  xfer += proto->serializedSizeStop();
  return xfer;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Coordinate>::serializedSizeZC(Protocol const* proto,
                                                       nebula::Coordinate const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("Coordinate");
  xfer += proto->serializedFieldSize("x", apache::thrift::protocol::T_DOUBLE, 1);
  xfer += proto->serializedSizeDouble(obj->x);
  xfer += proto->serializedFieldSize("y", apache::thrift::protocol::T_DOUBLE, 2);
  xfer += proto->serializedSizeDouble(obj->y);
  xfer += proto->serializedSizeStop();
  return xfer;
}

/**************************************
 *
 * Ops for struct Point
 *
 *************************************/
namespace detail {

template <>
struct TccStructTraits<nebula::Point> {
  static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                 MAYBE_UNUSED int16_t& fid,
                                 MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
    if (_fname == "coord") {
      fid = 1;
      _ftype = apache::thrift::protocol::T_STRUCT;
    }
  }
};

}  // namespace detail

inline constexpr protocol::TType Cpp2Ops<nebula::Point>::thriftType() {
  return apache::thrift::protocol::T_STRUCT;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Point>::write(Protocol* proto, nebula::Point const* obj) {
  uint32_t xfer = 0;
  xfer += proto->writeStructBegin("Point");
  xfer += proto->writeFieldBegin("coord", apache::thrift::protocol::T_STRUCT, 1);
  xfer += Cpp2Ops<nebula::Coordinate>::write(proto, &obj->coord);
  xfer += proto->writeFieldEnd();
  xfer += proto->writeFieldStop();
  xfer += proto->writeStructEnd();
  return xfer;
}

template <class Protocol>
void Cpp2Ops<nebula::Point>::read(Protocol* proto, nebula::Point* obj) {
  apache::thrift::detail::ProtocolReaderStructReadState<Protocol> readState;

  readState.readStructBegin(proto);

  using apache::thrift::TProtocolException;

  if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, apache::thrift::protocol::T_STRUCT))) {
    goto _loop;
  }
_readField_coord : { Cpp2Ops<nebula::Coordinate>::read(proto, &obj->coord); }

  if (UNLIKELY(!readState.advanceToNextField(proto, 1, 0, apache::thrift::protocol::T_STOP))) {
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
    detail::TccStructTraits<nebula::Point>::translateFieldName(
        readState.fieldName(), readState.fieldId, readState.fieldType);
  }

  switch (readState.fieldId) {
    case 1: {
      if (LIKELY(readState.fieldType == apache::thrift::protocol::T_STRUCT)) {
        goto _readField_coord;
      } else {
        goto _skip;
      }
    }
    default: {
_skip:
      proto->skip(readState.fieldType);
      readState.readFieldEnd(proto);
      readState.readFieldBeginNoInline(proto);
      goto _loop;
    }
  }
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Point>::serializedSize(Protocol const* proto, nebula::Point const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("Point");
  xfer += proto->serializedFieldSize("coord", apache::thrift::protocol::T_STRUCT, 1);
  xfer += Cpp2Ops<nebula::Coordinate>::serializedSize(proto, &obj->coord);
  xfer += proto->serializedSizeStop();
  return xfer;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Point>::serializedSizeZC(Protocol const* proto, nebula::Point const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("Point");
  xfer += proto->serializedFieldSize("coord", apache::thrift::protocol::T_STRUCT, 1);
  xfer += Cpp2Ops<nebula::Coordinate>::serializedSize(proto, &obj->coord);
  xfer += proto->serializedSizeStop();
  return xfer;
}

/**************************************
 *
 * Ops for struct LineString
 *
 *************************************/
namespace detail {

template <>
struct TccStructTraits<nebula::LineString> {
  static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                 MAYBE_UNUSED int16_t& fid,
                                 MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
    if (_fname == "coordList") {
      fid = 1;
      _ftype = apache::thrift::protocol::T_LIST;
    }
  }
};

}  // namespace detail

inline constexpr protocol::TType Cpp2Ops<nebula::LineString>::thriftType() {
  return apache::thrift::protocol::T_LIST;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::LineString>::write(Protocol* proto, nebula::LineString const* obj) {
  uint32_t xfer = 0;
  xfer += proto->writeStructBegin("LineString");
  xfer += proto->writeFieldBegin("coordList", apache::thrift::protocol::T_LIST, 1);
  xfer +=
      detail::pm::protocol_methods<type_class::list<type_class::structure>,
                                   std::vector<nebula::Coordinate>>::write(*proto, obj->coordList);
  xfer += proto->writeFieldEnd();
  xfer += proto->writeFieldStop();
  xfer += proto->writeStructEnd();
  return xfer;
}

template <class Protocol>
void Cpp2Ops<nebula::LineString>::read(Protocol* proto, nebula::LineString* obj) {
  apache::thrift::detail::ProtocolReaderStructReadState<Protocol> readState;

  readState.readStructBegin(proto);

  using apache::thrift::TProtocolException;

  if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, apache::thrift::protocol::T_LIST))) {
    goto _loop;
  }
_readField_coordList : {
  obj->coordList = std::vector<nebula::Coordinate>();
  detail::pm::protocol_methods<type_class::list<type_class::structure>,
                               std::vector<nebula::Coordinate>>::read(*proto, obj->coordList);
}

  if (UNLIKELY(!readState.advanceToNextField(proto, 1, 0, apache::thrift::protocol::T_STOP))) {
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
    detail::TccStructTraits<nebula::LineString>::translateFieldName(
        readState.fieldName(), readState.fieldId, readState.fieldType);
  }

  switch (readState.fieldId) {
    case 1: {
      if (LIKELY(readState.fieldType == apache::thrift::protocol::T_LIST)) {
        goto _readField_coordList;
      } else {
        goto _skip;
      }
    }
    default: {
_skip:
      proto->skip(readState.fieldType);
      readState.readFieldEnd(proto);
      readState.readFieldBeginNoInline(proto);
      goto _loop;
    }
  }
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::LineString>::serializedSize(Protocol const* proto,
                                                     nebula::LineString const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("LineString");
  xfer += proto->serializedFieldSize("coordList", apache::thrift::protocol::T_LIST, 1);
  xfer += detail::pm::protocol_methods<
      type_class::list<type_class::structure>,
      std::vector<nebula::Coordinate>>::serializedSize<false>(*proto, obj->coordList);
  xfer += proto->serializedSizeStop();
  return xfer;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::LineString>::serializedSizeZC(Protocol const* proto,
                                                       nebula::LineString const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("LineString");
  xfer += proto->serializedFieldSize("coordList", apache::thrift::protocol::T_LIST, 1);
  xfer += detail::pm::protocol_methods<
      type_class::list<type_class::structure>,
      std::vector<nebula::Coordinate>>::serializedSize<false>(*proto, obj->coordList);
  xfer += proto->serializedSizeStop();
  return xfer;
}

/**************************************
 *
 * Ops for struct Polygon
 *
 *************************************/
namespace detail {

template <>
struct TccStructTraits<nebula::Polygon> {
  static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                 MAYBE_UNUSED int16_t& fid,
                                 MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
    if (_fname == "coordListList") {
      fid = 1;
      _ftype = apache::thrift::protocol::T_LIST;
    }
  }
};

}  // namespace detail

inline constexpr protocol::TType Cpp2Ops<nebula::Polygon>::thriftType() {
  return apache::thrift::protocol::T_LIST;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Polygon>::write(Protocol* proto, nebula::Polygon const* obj) {
  uint32_t xfer = 0;
  xfer += proto->writeStructBegin("Polygon");
  xfer += proto->writeFieldBegin("coordListList", apache::thrift::protocol::T_LIST, 1);
  xfer += detail::pm::protocol_methods<
      type_class::list<type_class::structure>,
      std::vector<std::vector<nebula::Coordinate>>>::write(*proto, obj->coordListList);
  xfer += proto->writeFieldEnd();
  xfer += proto->writeFieldStop();
  xfer += proto->writeStructEnd();
  return xfer;
}

template <class Protocol>
void Cpp2Ops<nebula::Polygon>::read(Protocol* proto, nebula::Polygon* obj) {
  apache::thrift::detail::ProtocolReaderStructReadState<Protocol> readState;

  readState.readStructBegin(proto);

  using apache::thrift::TProtocolException;

  if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, apache::thrift::protocol::T_LIST))) {
    goto _loop;
  }
_readField_coordListList : {
  obj->coordListList = std::vector<std::vector<nebula::Coordinate>>();
  detail::pm::protocol_methods<
      type_class::list<type_class::structure>,
      std::vector<std::vector<nebula::Coordinate>>>::read(*proto, obj->coordListList);
}

  if (UNLIKELY(!readState.advanceToNextField(proto, 1, 0, apache::thrift::protocol::T_STOP))) {
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
    detail::TccStructTraits<nebula::Polygon>::translateFieldName(
        readState.fieldName(), readState.fieldId, readState.fieldType);
  }

  switch (readState.fieldId) {
    case 1: {
      if (LIKELY(readState.fieldType == apache::thrift::protocol::T_LIST)) {
        goto _readField_coordListList;
      } else {
        goto _skip;
      }
    }
    default: {
_skip:
      proto->skip(readState.fieldType);
      readState.readFieldEnd(proto);
      readState.readFieldBeginNoInline(proto);
      goto _loop;
    }
  }
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Polygon>::serializedSize(Protocol const* proto,
                                                  nebula::Polygon const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("Polygon");
  xfer += proto->serializedFieldSize("coordListList", apache::thrift::protocol::T_LIST, 1);
  xfer += detail::pm::protocol_methods<
      type_class::list<type_class::structure>,
      std::vector<std::vector<nebula::Coordinate>>>::serializedSize<false>(*proto,
                                                                           obj->coordListList);

  xfer += proto->serializedSizeStop();
  return xfer;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Polygon>::serializedSizeZC(Protocol const* proto,
                                                    nebula::Polygon const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("Polygon");
  xfer += proto->serializedFieldSize("coordListList", apache::thrift::protocol::T_LIST, 1);
  xfer += detail::pm::protocol_methods<
      type_class::list<type_class::structure>,
      std::vector<std::vector<nebula::Coordinate>>>::serializedSize<false>(*proto,
                                                                           obj->coordListList);

  xfer += proto->serializedSizeStop();
  return xfer;
}

/**************************************
 *
 * Ops for class Geography
 *
 *************************************/
namespace detail {

template <>
struct TccStructTraits<nebula::Geography> {
  static void translateFieldName(MAYBE_UNUSED folly::StringPiece _fname,
                                 MAYBE_UNUSED int16_t& fid,
                                 MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
    if (_fname == "ptVal") {
      fid = 1;
      _ftype = apache::thrift::protocol::T_STRUCT;
    } else if (_fname == "lsVal") {
      fid = 2;
      _ftype = apache::thrift::protocol::T_STRUCT;
    } else if (_fname == "pgVal") {
      fid = 3;
      _ftype = apache::thrift::protocol::T_STRUCT;
    }
  }
};

}  // namespace detail

inline constexpr protocol::TType Cpp2Ops<nebula::Geography>::thriftType() {
  return apache::thrift::protocol::T_STRUCT;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Geography>::write(Protocol* proto, nebula::Geography const* obj) {
  uint32_t xfer = 0;
  xfer += proto->writeStructBegin("Geography");
  switch (obj->shape()) {
    case nebula::GeoShape::POINT: {
      xfer += proto->writeFieldBegin("ptVal", apache::thrift::protocol::T_STRUCT, 1);
      xfer += Cpp2Ops<nebula::Point>::write(proto, &obj->point());
      xfer += proto->writeFieldEnd();
      break;
    }
    case nebula::GeoShape::LINESTRING: {
      xfer += proto->writeFieldBegin("lsVal", apache::thrift::protocol::T_STRUCT, 2);
      xfer += Cpp2Ops<nebula::LineString>::write(proto, &obj->lineString());
      xfer += proto->writeFieldEnd();
      break;
    }
    case nebula::GeoShape::POLYGON: {
      xfer += proto->writeFieldBegin("ptVal", apache::thrift::protocol::T_STRUCT, 3);
      xfer += Cpp2Ops<nebula::Polygon>::write(proto, &obj->polygon());
      xfer += proto->writeFieldEnd();
      break;
    }
    case nebula::GeoShape::UNKNOWN: {
      break;
    }
  }
  xfer += proto->writeFieldStop();
  xfer += proto->writeStructEnd();
  return xfer;
}

template <class Protocol>
void Cpp2Ops<nebula::Geography>::read(Protocol* proto, nebula::Geography* obj) {
  apache::thrift::detail::ProtocolReaderStructReadState<Protocol> readState;
  readState.fieldId = 0;

  readState.readStructBegin(proto);

  using apache::thrift::protocol::TProtocolException;

  readState.readFieldBegin(proto);
  if (readState.fieldType == apache::thrift::protocol::T_STOP) {
    obj->clear();
  } else {
    if (proto->kUsesFieldNames()) {
      detail::TccStructTraits<nebula::Geography>::translateFieldName(
          readState.fieldName(), readState.fieldId, readState.fieldType);
    }
    switch (readState.fieldId) {
      case 1: {
        if (readState.fieldType == apache::thrift::protocol::T_STRUCT) {
          obj->geo_ = nebula::Point();
          Cpp2Ops<nebula::Point>::read(proto, &obj->mutablePoint());
        } else {
          proto->skip(readState.fieldType);
        }
        break;
      }
      case 2: {
        if (readState.fieldType == apache::thrift::protocol::T_STRUCT) {
          obj->geo_ = nebula::LineString();
          Cpp2Ops<nebula::LineString>::read(proto, &obj->mutableLineString());
        } else {
          proto->skip(readState.fieldType);
        }
        break;
      }
      case 3: {
        if (readState.fieldType == apache::thrift::protocol::T_STRUCT) {
          obj->geo_ = nebula::Polygon();
          Cpp2Ops<nebula::Polygon>::read(proto, &obj->mutablePolygon());
        } else {
          proto->skip(readState.fieldType);
        }
        break;
      }
      default: {
        proto->skip(readState.fieldType);
        break;
      }
    }
    readState.readFieldEnd(proto);
    readState.readFieldBegin(proto);
    if (UNLIKELY(readState.fieldType != apache::thrift::protocol::T_STOP)) {
      using apache::thrift::protocol::TProtocolException;
      TProtocolException::throwUnionMissingStop();
    }
  }
  readState.readStructEnd(proto);
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Geography>::serializedSize(Protocol const* proto,
                                                    nebula::Geography const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("Geography");
  switch (obj->shape()) {
    case nebula::GeoShape::POINT: {
      xfer += proto->serializedFieldSize("ptVal", protocol::T_STRUCT, 1);
      xfer += Cpp2Ops<nebula::Point>::serializedSize(proto, &obj->point());
      break;
    }
    case nebula::GeoShape::LINESTRING: {
      xfer += proto->serializedFieldSize("lsVal", protocol::T_STRUCT, 2);
      xfer += Cpp2Ops<nebula::LineString>::serializedSize(proto, &obj->lineString());
      break;
    }
    case nebula::GeoShape::POLYGON: {
      xfer += proto->serializedFieldSize("pgVal", protocol::T_STRUCT, 3);
      xfer += Cpp2Ops<nebula::Polygon>::serializedSize(proto, &obj->polygon());
      break;
    }
    case nebula::GeoShape::UNKNOWN: {
      break;
    }
  }
  xfer += proto->serializedSizeStop();
  return xfer;
}

template <class Protocol>
uint32_t Cpp2Ops<nebula::Geography>::serializedSizeZC(Protocol const* proto,
                                                      nebula::Geography const* obj) {
  uint32_t xfer = 0;
  xfer += proto->serializedStructSize("Geography");
  switch (obj->shape()) {
    case nebula::GeoShape::POINT: {
      xfer += proto->serializedFieldSize("ptVal", protocol::T_STRUCT, 1);
      xfer += Cpp2Ops<nebula::Point>::serializedSize(proto, &obj->point());
      break;
    }
    case nebula::GeoShape::LINESTRING: {
      xfer += proto->serializedFieldSize("lsVal", protocol::T_STRUCT, 2);
      xfer += Cpp2Ops<nebula::LineString>::serializedSize(proto, &obj->lineString());
      break;
    }
    case nebula::GeoShape::POLYGON: {
      xfer += proto->serializedFieldSize("pgVal", protocol::T_STRUCT, 3);
      xfer += Cpp2Ops<nebula::Polygon>::serializedSize(proto, &obj->polygon());
      break;
    }
    case nebula::GeoShape::UNKNOWN: {
      break;
    }
  }
  xfer += proto->serializedSizeStop();
  return xfer;
}

}  // namespace thrift
}  // namespace apache
#endif  // COMMON_DATATYPES_GEOGRAPHYOPS_H_
