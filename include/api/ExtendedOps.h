#pragma once

#include "api/BasicOps.h"

struct LIBROCKS_API ExtendedOps : public BasicOps {

    virtual void singleRemove(int* status, const Kind& kind, const char* key, size_t keyLen) noexcept = 0;

    [[nodiscard("return value must be delete[]d")]]
    virtual char* singleRemoveIfPresent(int* status, const Kind& kind, size_t* resultLen, const char* key,
        size_t keyLen) noexcept = 0;

    [[nodiscard("return value must be delete[]d")]]
    virtual char* removeIfPresent(int* status, const Kind& kind, size_t* resultLen, const char* key,
        size_t keyLen) noexcept = 0;

    virtual void putIfAbsent(int* status, const Kind& kind, const char* key, size_t keyLen, const char* value,
        size_t valLen) noexcept = 0;

    virtual void syncWAL() noexcept = 0;

    virtual void flush() noexcept = 0;

    virtual void flushNoWait() noexcept = 0;

    [[nodiscard("return value must be delete[]d")]]
    virtual char* findMinKey(int* status, const Kind& kind, size_t* resultLen) const noexcept = 0;

    [[nodiscard("return value must be delete[]d")]]
    virtual char* findMaxKey(int* status, const Kind& kind, size_t* resultLen) const noexcept = 0;

    virtual void removeRange(int* status, const Kind& kind, const char* beginKeyInclusive,
        size_t beginKeyLen, const char* endKeyExclusive, size_t endKeyLen) noexcept = 0;

    ~ExtendedOps() override = default;
};
