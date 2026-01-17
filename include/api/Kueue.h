#pragma once

#include <chrono>
#include "api/api.h"
#include "api/Clearable.h"

struct LIBROCKS_API Kueue : public Clearable {

    virtual void put(int* status, const char* value, size_t valLen) noexcept = 0;

    [[nodiscard("return value must be delete[]d")]]
    virtual char* take(int* status, size_t* valLen) noexcept = 0;

    [[nodiscard("return value must be delete[]d")]]
    virtual char* take(int* status, size_t* valLen, std::chrono::milliseconds timeout) noexcept = 0;

    virtual void clear(int* status) noexcept override = 0;

    virtual long long size() const noexcept = 0;

    virtual bool isEmpty() const noexcept = 0;

    virtual unsigned long long totalPuts() const noexcept = 0;

    virtual unsigned long long totalTakes() const noexcept = 0;

    virtual ~Kueue() = default;
};
