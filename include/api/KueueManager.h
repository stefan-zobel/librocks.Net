#pragma once

#include "api/api.h"
#include "api/Kueue.h"

struct LIBROCKS_API KueueManager {

    [[nodiscard("return value must be closed and deleted")]]
    virtual Kueue* get(int* status, const char* id) noexcept = 0;

    virtual bool isOpen() const noexcept = 0;

    virtual void close() noexcept = 0;

    virtual ~KueueManager() = default;
};
