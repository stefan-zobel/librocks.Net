#pragma once

#include <functional>
#include <map>
#include <set>
#include <string_view>
#include "bytes.h"
#include "api/Kind.h"
#include "api/Store.h"

using KindSet = std::set<std::reference_wrapper<const Kind>, bool(*)(const std::reference_wrapper<const Kind>&, const std::reference_wrapper<const Kind>&)>;

class KVStore {
public:

    explicit KVStore(std::string_view path);

    explicit KVStore(Store* store);

    ~KVStore();

    void put(const Kind& kind, std::string_view key, std::string_view value);

    void remove(const Kind& kind, std::string_view key);

    bytes get(const Kind& kind, std::string_view key) const;

    bytes updateIfPresent(const Kind& kind, std::string_view key, std::string_view value);

    void singleRemove(const Kind& kind, std::string_view key);

    bytes singleRemoveIfPresent(const Kind& kind, std::string_view key);

    bytes removeIfPresent(const Kind& kind, std::string_view key);

    bool putIfAbsent(const Kind& kind, std::string_view key, std::string_view value);

    bytes findMinKey(const Kind& kind) const;

    bytes findMaxKey(const Kind& kind) const;

    void close();

    bool isOpen() const noexcept;

    const Kind& getDefaultKind() const;

    const Kind& getOrCreateKind(std::string_view kindName);

    const KindSet getKinds() const;

    void compact(const Kind& kind);

    void compactAll();

private:
    Store* store;

private:
    static const std::map<int, std::string> codes;
    static bool throwForStatus(int status);
    KindManager& getKindManager() const;
};
