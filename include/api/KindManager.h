#pragma once

#include "api/api.h"
#include "api/Kind.h"

struct LIBROCKS_API KindManager {

    virtual const Kind& getDefaultKind(int* status) const noexcept = 0;

    virtual const Kind& getOrCreateKind(int* status, const char* kindName) noexcept = 0;

    virtual const Kind** getKinds(int* status, size_t* resultLen) const noexcept = 0;

    virtual ~KindManager() = default;
};
