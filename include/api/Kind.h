#pragma once

#include "api/api.h"
#include "api/RocksType.h"
#include "api/ToString.h"

struct LIBROCKS_API Kind : public RocksType, public ToString {

    virtual const char* name() const noexcept = 0;

    virtual bool isValid() const noexcept = 0;

    virtual bool operator<(const Kind& other) const noexcept = 0;

    ~Kind() override = default;
};
