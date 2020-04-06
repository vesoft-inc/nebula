/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

namespace nebula {

template<typename T>
typename std::enable_if<std::is_integral<T>::value, RowWriterV1&>::type
RowWriterV1::operator<<(T v) noexcept {
    switch (schema_->getFieldType(colNum_)) {
        case meta::cpp2::PropertyType::INT64:
        case meta::cpp2::PropertyType::TIMESTAMP: {
            writeInt(v);
            break;
        }
        case meta::cpp2::PropertyType::VID: {
            cord_ << (uint64_t)v;
            break;
        }
        default: {
            LOG(ERROR) << "Incompatible value type \"int\"";
            writeInt(0);
            break;
        }
    }

    colNum_++;
    if (colNum_ != 0 && (colNum_ >> 4 << 4) == colNum_) {
        /* We need to record offset for every 16 fields */
        blockOffsets_.emplace_back(cord_.size());
    }

    return *this;
}


template<typename T>
typename std::enable_if<std::is_integral<T>::value>::type
RowWriterV1::writeInt(T v) {
    uint8_t buf[10];
    size_t len = folly::encodeVarint(v, buf);
    DCHECK_GT(len, 0UL);
    cord_.write(reinterpret_cast<const char*>(&buf[0]), len);
}

}  // namespace nebula

