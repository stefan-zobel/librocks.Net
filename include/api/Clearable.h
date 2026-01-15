#pragma once

#include <api/api.h>

struct LIBROCKS_API Clearable {

    virtual void clear(int* status) noexcept = 0;

    virtual ~Clearable() = default;
};
