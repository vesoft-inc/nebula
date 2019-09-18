/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

namespace nebula {

/*********************************************
 *
 * class RowReader::Cell
 *
 ********************************************/
template<typename T>
typename std::enable_if<std::is_integral<T>::value, ResultType>::type
RowReader::Cell::getInt(T& v) const noexcept {
    RR_CELL_GET_VALUE(Int);
}


/*********************************************
 *
 * class RowReader
 *
 ********************************************/
template<typename T>
typename std::enable_if<std::is_integral<T>::value, ResultType>::type
RowReader::getInt(int64_t index, int64_t& offset, T& v) const noexcept {
    switch (schema_->getFieldType(index).get_type()) {
        case cpp2::SupportedType::INT:
        case cpp2::SupportedType::TIMESTAMP: {
            int32_t numBytes = readInteger(offset, v);
            if (numBytes < 0) {
                return static_cast<ResultType>(numBytes);
            }
            offset += numBytes;
            break;
        }
        default: {
            return ResultType::E_INCOMPATIBLE_TYPE;
        }
    }

    return ResultType::SUCCEEDED;
}


template<typename T>
typename std::enable_if<std::is_integral<T>::value, ResultType>::type
RR_GET_VALUE_BY_NAME(Int, T)

template<typename T>
typename std::enable_if<std::is_integral<T>::value, ResultType>::type
RowReader::getInt(int64_t index, T& v) const noexcept {
    RR_GET_OFFSET()
    return getInt(index, offset, v);
}


template<typename T>
typename std::enable_if<std::is_integral<T>::value, int32_t>::type
RowReader::readInteger(int64_t offset, T& v) const noexcept {
    const uint8_t* start = reinterpret_cast<const uint8_t*>(&(data_[offset]));
    folly::ByteRange range(start, data_.size() - offset);

    // TODO We might want to catch the exception here
    v = folly::decodeVarint(range);
    return range.begin() - start;
}

}  // namespace nebula

