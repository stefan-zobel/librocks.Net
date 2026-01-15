#pragma once

#include <cassert>
#include <string>

class KVStore;

class bytes {
public:

    bytes(const bytes& other);

    bytes& operator=(const bytes& other);

    bytes(bytes&& moving) noexcept;

    bytes& operator=(bytes&& moving) noexcept;

    ~bytes();

    inline size_t size() const noexcept {
        return size_;
    }

    inline const char* data() const noexcept {
        return data_;
    }

    inline char operator[](size_t i) const noexcept {
        assert(i < size_);
        return data_[i];
    }

    explicit operator bool() const noexcept {
        return data_;
    }

    inline bool isEmpty() const noexcept {
        return size_ == 0;
    }

    inline std::string toString() const {
        return { data_, size_ };
    }

    void clear();

    void swap(bytes& src) noexcept;

    friend class KVStore;

private:
    explicit bytes(char* bytes, size_t length) : size_(length), data_(bytes) {
    }

    void copy(const bytes& other);

private:
    size_t size_;
    const char* data_;
};

