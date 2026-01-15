#pragma once

#include "api/api.h"
#include <cstddef>

struct LIBROCKS_API RocksType {

    virtual bool equals(const RocksType* other) const noexcept = 0;

    virtual size_t hash() const noexcept = 0;

    virtual bool operator==(const RocksType& other) const noexcept = 0;

    virtual ~RocksType() = default;
};
