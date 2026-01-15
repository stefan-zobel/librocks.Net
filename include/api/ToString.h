#pragma once

#include "api/api.h"

struct LIBROCKS_API ToString {

    virtual const char* toString() const noexcept = 0;

    virtual ~ToString() = default;
};

