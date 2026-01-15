
#include "api/librocks.h"
#include "client/KVStore.h"
#include "client/RocksException.h"


KVStore::KVStore(std::string_view path) {
    int status = Status::Ok;
    store = openStore(&status, std::string(path).c_str());
    if (status != Status::Ok) {
        throwForStatus(status);
    }
}

KVStore::KVStore(Store* pStore) : store(pStore) {
}

KVStore::~KVStore() {
    if (store) {
        delete store;
        store = nullptr;
    }
}

void KVStore::close() {
    if (store) {
        store->close();
    }
}

bool KVStore::isOpen() const noexcept {
    return store->isOpen();
}

const Kind& KVStore::getDefaultKind() const {
    int status = Status::Ok;
    const Kind& k = getKindManager().getDefaultKind(&status);
    if (status != Status::Ok) {
        throwForStatus(status);
    }
    return k;
}

const Kind& KVStore::getOrCreateKind(std::string_view kindName) {
    int status = Status::Ok;
    // create a copy of kindName to ensure null-termination
    const Kind& k = getKindManager().getOrCreateKind(&status, std::string(kindName).c_str());
    if (status != Status::Ok) {
        throwForStatus(status);
    }
    return k;
}

static bool comparator(const std::reference_wrapper<const Kind>& a, const std::reference_wrapper<const Kind>& b) noexcept {
    return a.get() < b.get();
}

const KindSet KVStore::getKinds() const {
    int status = Status::Ok;
    size_t resultLen = 0;
    const Kind** ppK = getKindManager().getKinds(&status, &resultLen);
    KindSet kinds(&comparator);
    if (status == Status::Ok) {
        for (size_t i = 0; i < resultLen; ++i) {
            const Kind& kind = dynamic_cast<const Kind&>(*ppK[i]);
            kinds.insert(std::cref(kind));
        }
        delete[] ppK;
    }
    else {
        throwForStatus(status);
    }
    return kinds;
}

KindManager& KVStore::getKindManager() const {
    int status = Status::Ok;
    KindManager& mgr = store->getKindManager(&status);
    if (status != Status::Ok) {
        throwForStatus(status);
    }
    return mgr;
}

void KVStore::put(const Kind& kind, std::string_view key, std::string_view value) {
    int status = Status::Ok;
    store->put(&status, kind, key.data(), key.size(), value.data(), value.size());
    if (status != Status::Ok) {
        throwForStatus(status);
    }
}

void KVStore::remove(const Kind& kind, std::string_view key) {
    int status = Status::Ok;
    store->remove(&status, kind, key.data(), key.size());
    if (status != Status::Ok) {
        throwForStatus(status);
    }
}

bytes KVStore::get(const Kind& kind, std::string_view key) const {
    int status = Status::Ok;
    size_t resultLen = 0;
    char* val = store->get(&status, kind, &resultLen, key.data(), key.size());
    if (!(status == Status::Ok || status == Status::NotFound)) {
        throwForStatus(status);
    }
    return bytes(val, resultLen);
}

bytes KVStore::updateIfPresent(const Kind& kind, std::string_view key, std::string_view value) {
    int status = Status::Ok;
    size_t resultLen = 0;
    char* oldVal = store->updateIfPresent(&status, kind, &resultLen, key.data(), key.size(), value.data(), value.size());
    if (!(status == Status::Ok || status == Status::NotFound)) {
        throwForStatus(status);
    }
    return bytes(oldVal, resultLen);
}

void KVStore::singleRemove(const Kind& kind, std::string_view key) {
    int status = Status::Ok;
    store->singleRemove(&status, kind, key.data(), key.size());
    if (status != Status::Ok) {
        throwForStatus(status);
    }
}

bytes KVStore::singleRemoveIfPresent(const Kind& kind, std::string_view key) {
    int status = Status::Ok;
    size_t resultLen = 0;
    char* removed = store->singleRemoveIfPresent(&status, kind, &resultLen, key.data(), key.size());
    if (!(status == Status::Ok || status == Status::NotFound)) {
        throwForStatus(status);
    }
    return bytes(removed, resultLen);
}

bytes KVStore::removeIfPresent(const Kind& kind, std::string_view key) {
    int status = Status::Ok;
    size_t resultLen = 0;
    char* removed = store->removeIfPresent(&status, kind, &resultLen, key.data(), key.size());
    if (!(status == Status::Ok || status == Status::NotFound)) {
        throwForStatus(status);
    }
    return bytes(removed, resultLen);
}

bool KVStore::putIfAbsent(const Kind& kind, std::string_view key, std::string_view value) {
    int status = Status::Ok;
    store->putIfAbsent(&status, kind, key.data(), key.size(), value.data(), value.size());
    if (status == Status::Ok) {
        return true;
    }
    else if (status == Status::AlreadyExists) {
        return false;
    }
    return throwForStatus(status);
}

bytes KVStore::findMinKey(const Kind& kind) const {
    int status = Status::Ok;
    size_t resultLen = 0;
    char* minKey = store->findMinKey(&status, kind, &resultLen);
    if (status != Status::Ok) {
        throwForStatus(status);
    }
    return bytes(minKey, resultLen);
}

bytes KVStore::findMaxKey(const Kind& kind) const {
    int status = Status::Ok;
    size_t resultLen = 0;
    char* maxKey = store->findMaxKey(&status, kind, &resultLen);
    if (status != Status::Ok) {
        throwForStatus(status);
    }
    return bytes(maxKey, resultLen);
}

void KVStore::compact(const Kind& kind) {
    int status = Status::Ok;
    store->compact(&status, kind);
    if (status != Status::Ok) {
        throwForStatus(status);
    }
}

void KVStore::compactAll() {
    int status = Status::Ok;
    store->compactAll(&status);
    if (status != Status::Ok) {
        throwForStatus(status);
    }
}


bool KVStore::throwForStatus(int status) {
    if (status != Status::Ok) {
        if (KVStore::codes.count(status)) {
            throw RocksException(status, KVStore::codes.at(status));
        }
        else {
            throw RocksException(status, "Unknown");
        }
    }
    return false;
}

static std::map<int, std::string> initCodes() {
    return {
        {Status::Invalid, "Invalid"},
        {Status::NoIterator, "NoIterator"},
        {Status::AlreadyExists, "AlreadyExists"},
        {Status::NoTransaction, "NoTransaction"},
        {Status::Closed, "Closed"},
        {Status::Ok, "Ok"},
        {Status::NotFound, "NotFound"},
        {Status::Corruption, "Corruption"},
        {Status::NotSupported, "NotSupported"},
        {Status::InvalidArgument, "InvalidArgument"},
        {Status::IOError, "IOError"},
        {Status::MergeInProgress, "MergeInProgress"},
        {Status::Incomplete, "Incomplete"},
        {Status::ShutdownInProgress, "ShutdownInProgress"},
        {Status::TimedOut, "TimedOut"},
        {Status::Aborted, "Aborted"},
        {Status::Busy, "Busy"},
        {Status::Expired, "Expired"},
        {Status::TryAgain, "TryAgain"},
        {Status::CompactionTooLarge, "CompactionTooLarge"},
        {Status::ColumnFamilyDropped, "ColumnFamilyDropped"},
        {Status::Unknown, "Unknown"}
    };
}

const std::map<int, std::string> KVStore::codes = initCodes();
