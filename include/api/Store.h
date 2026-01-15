#pragma once

#include "api/api.h"
#include "api/KindManager.h"
#include "api/ExtendedOps.h"

struct LIBROCKS_API Store : public ExtendedOps {

    virtual void close() = 0;

    virtual bool isOpen() const noexcept = 0;

    virtual KindManager& getKindManager(int* status) const noexcept = 0;

    virtual void compact(int* status, const Kind& kind) noexcept = 0;

    virtual void compactAll(int* status) noexcept = 0;

    ~Store() override = default;
};
