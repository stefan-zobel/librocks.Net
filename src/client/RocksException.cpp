
#include "client/RocksException.h"


const std::string RocksException::NAME = "RocksException - ";

RocksException::RocksException(int code, char* message) 
    : std::runtime_error(std::string(NAME).append(message).append("\n")), code_(code) {
}

RocksException::RocksException(int code, const char* message)
    : std::runtime_error(std::string(NAME).append(message).append("\n")), code_(code) {
}

RocksException::RocksException(int code, const char* file, long line, const char* message)
    : std::runtime_error(std::string(NAME).append(file).append(":").append(l2s(line))
        .append(": ").append(message).append("\n")), code_(code) {
}

RocksException::RocksException(int code, const std::string& message)
    : std::runtime_error(std::string(NAME).append(message).append("\n")), code_(code) {
}

RocksException::RocksException(int code, std::string& message)
    : std::runtime_error(std::string(NAME).append(message).append("\n")), code_(code) {
}

RocksException::RocksException(int code, const char* file, long line, std::string& message)
    : std::runtime_error(std::string(NAME).append(file).append(":").append(l2s(line))
        .append(": ").append(message).append("\n")), code_(code) {
}

std::string RocksException::l2s(long value) {
    char buffer[64] = {};
    sprintf_s(buffer, 64, "%ld", value);
    return { buffer };
}
