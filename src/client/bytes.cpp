
#include "client/bytes.h"
#include <cstring> // std::memcpy

bytes::bytes(const bytes& other) : size_(0), data_(nullptr) {
    if (other.size_ > 0) {
        copy(other);
    }
}

bytes& bytes::operator=(const bytes& other) {
    if (this != &other) {
        clear();
        if (other.size_ > 0) {
            copy(other);
        }
    }
    return *this;
}

bytes::bytes(bytes&& moving) noexcept : size_(0), data_(nullptr) {
    moving.swap(*this);
}

bytes& bytes::operator=(bytes&& moving) noexcept {
    moving.swap(*this);
    return *this;
}

bytes::~bytes() {
    clear();
}

void bytes::clear() {
    if (size_ > 0) {
        size_ = 0;
        delete[] data_;
        data_ = nullptr;
    }
}

void bytes::swap(bytes& src) noexcept {
    std::swap(size_, src.size_);
    std::swap(data_, src.data_);
}

void bytes::copy(const bytes& other) {
    size_ = other.size_;
    char* tmp = new char[other.size_];
    std::memcpy(tmp, other.data_, other.size_);
    data_ = tmp;
}

void swap(bytes& lhs, bytes& rhs) {
    lhs.swap(rhs);
}
