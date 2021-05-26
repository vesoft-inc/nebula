/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "FlattenList.h"
#include "Base.h"

namespace nebula {

// FlattenList should start and end with magic.
static constexpr uint64_t FlattenListBeginMagic = 0xFF18D1C0B1A326FF;
static constexpr uint64_t FlattenListEndMagic   = 0xFF26A3B1C0D118FF;

FlattenListWriter& FlattenListWriter::operator<<(int64_t v) {
    if (cord_.empty()) {
        cord_ << FlattenListBeginMagic;
    }
    cord_ << uint8_t(VAR_INT64);
    cord_ << v;
    return *this;
}

FlattenListWriter& FlattenListWriter::operator<<(double v) {
    if (cord_.empty()) {
        cord_ << FlattenListBeginMagic;
    }
    cord_ << uint8_t(VAR_DOUBLE);
    cord_ << v;
    return *this;
}

FlattenListWriter& FlattenListWriter::operator<<(bool v) {
    if (cord_.empty()) {
        cord_ << FlattenListBeginMagic;
    }
    cord_ << uint8_t(VAR_BOOL);
    cord_ << v;
    return *this;
}

FlattenListWriter& FlattenListWriter::operator<<(const char* v) {
    if (cord_.empty()) {
        cord_ << FlattenListBeginMagic;
    }
    cord_ << uint8_t(VAR_STR);
    uint32_t size = strlen(v);
    cord_ << size;
    cord_ << v;
    return *this;
}

FlattenListWriter& FlattenListWriter::operator<<(const std::string &v) {
    if (cord_.empty()) {
        cord_ << FlattenListBeginMagic;
    }
    cord_ << uint8_t(VAR_STR);
    uint32_t size = v.size();
    cord_ << size;
    cord_ << v;
    return *this;
}

FlattenListWriter& FlattenListWriter::operator<<(folly::StringPiece v) {
    if (cord_.empty()) {
        cord_ << FlattenListBeginMagic;
    }
    cord_ << uint8_t(VAR_STR);
    uint32_t size = v.size();
    cord_ << size;
    cord_ << v;
    return *this;
}

FlattenListWriter& FlattenListWriter::operator<<(const VariantType &v) {
    switch (v.which()) {
        case VAR_INT64:
            this->operator<<(boost::get<int64_t>(v));
            break;
        case VAR_BOOL:
            this->operator<<(boost::get<bool>(v));
            break;
        case VAR_STR:
            this->operator<<(boost::get<std::string>(v));
            break;
        case VAR_DOUBLE:
            this->operator<<(boost::get<double>(v));
            break;
        default:
            break;
    }
    return *this;
}

std::string FlattenListWriter::finish() {
    if (cord_.empty()) {
        cord_ << FlattenListBeginMagic;
    }
    cord_ << FlattenListEndMagic;
    return cord_.str();
}

size_t FlattenListReader::Iterator::remain() const {
    return s_.size() - offset_;
}

FlattenListReader::Iterator & FlattenListReader::Iterator::operator++() {
    load();
    offset_ += size_;
    size_ = 0;
    v_ = StatusOr<VariantType>();
    return *this;
}

const StatusOr<VariantType>* FlattenListReader::Iterator::operator->() const {
    load();
    return &v_;
}

const StatusOr<VariantType>& FlattenListReader::Iterator::operator*() const {
    load();
    return v_;
}

StatusOr<folly::StringPiece> FlattenListReader::Iterator::raw() const {
    load();
    if (!v_.ok()) {
        return v_.status();
    }
    switch (v_.value().which()) {
        case VAR_INT64:
            return s_.subpiece(offset_ + sizeof(uint8_t), size_ - sizeof(uint8_t));
        case VAR_DOUBLE:
            return s_.subpiece(offset_ + sizeof(uint8_t), size_ - sizeof(uint8_t));
        case VAR_BOOL:
            return s_.subpiece(offset_ + sizeof(uint8_t), size_ - sizeof(uint8_t));
        case VAR_STR:
            return s_.subpiece(offset_ + sizeof(uint8_t) + sizeof(uint32_t),
                               size_ - sizeof(uint8_t) - sizeof(uint32_t));
        default:
            return Status::Error("unknown type.");
    }
}

FlattenListReader::Iterator::Iterator(const FlattenListReader& r, bool end) {
    auto s = r.s_;
    if (s.size() < sizeof(FlattenListBeginMagic) + sizeof(FlattenListEndMagic)) {
        v_ = Status::Error("not list type.");
        return;
    }

    if (FlattenListBeginMagic != *reinterpret_cast<const uint64_t*>(s.data() + offset_)
               || FlattenListEndMagic !=
                  *reinterpret_cast<const uint64_t*>(s.end() - sizeof(FlattenListEndMagic))) {
        v_ = Status::Error("not list type.");
        return;
    }

    s_ = s.subpiece(sizeof(FlattenListBeginMagic),
                    s.size() - sizeof(FlattenListBeginMagic) - sizeof(FlattenListEndMagic));
    if (end) {
        offset_ = s_.size();
    }
}

void FlattenListReader::Iterator::load() const {
    if (v_.ok()) {
        return;
    }
    uint8_t type = 0;
    if (remain() < sizeof(type)) {
        v_ = Status::Error("data corrupt.");
        return;
    }
    memcpy(&type, s_.data() + offset_ + size_, sizeof(type));
    size_ += sizeof(type);

    switch (type) {
        case VAR_INT64:
        {
            int64_t v = 0;
            if (remain() < sizeof(v)) {
                v_ = Status::Error("data corrupt.");
                return;
            }
            memcpy(&v, s_.data() + offset_ + size_, sizeof(v));
            size_ += sizeof(v);
            v_ = v;
            break;
        }
        case VAR_BOOL:
        {
            bool v = false;
            if (remain() < sizeof(v)) {
                v_ = Status::Error("data corrupt.");
                return;
            }
            memcpy(&v, s_.data() + offset_ + size_, sizeof(v));
            size_ += sizeof(v);
            v_ = v;
            break;
        }
        case VAR_DOUBLE:
        {
            double v = 0.0;
            if (remain() < sizeof(v)) {
                v_ = Status::Error("data corrupt.");
                return;
            }
            memcpy(&v, s_.data() + offset_ + size_, sizeof(v));
            size_ += sizeof(v);
            v_ = v;
            break;
        }
        case VAR_STR:
        {
            uint32_t size = 0;
            if (remain() < sizeof(size)) {
                v_ = Status::Error("data corrupt.");
                return;
            }
            memcpy(&size, s_.data() + offset_ + size_, sizeof(size));
            size_ += sizeof(size);

            if (remain() < size) {
                v_ = Status::Error("data corrupt.");
                return;
            }
            std::string v(s_.data() + offset_ + size_, size);
            size_ += size;
            v_ = std::move(v);
            break;
        }
        default:
            v_ = Status::Error("data corrupt.");
    }
}

}