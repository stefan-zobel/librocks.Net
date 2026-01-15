#pragma once

#include "api/StatusCode.h"
#include <string>
#include <stdexcept>

class RocksException : public std::runtime_error {

public:
    RocksException() : std::runtime_error(NAME), code_(Status::Unknown) {}
    virtual ~RocksException() = default;

    explicit RocksException(int code, char* message);
    explicit RocksException(int code, const char* message);
    RocksException(int code, const char* file, long line, const char* message);
    explicit RocksException(int code, const std::string& message);
    explicit RocksException(int code, std::string& message);
    RocksException(int code, const char* file, long line, std::string& message);

    int code() const noexcept { return code_; }

private:
    static std::string l2s(long value);
    static const std::string NAME;
    int code_;
};
