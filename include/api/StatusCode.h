#pragma once

namespace Status {
    constexpr int Invalid = -5;
    constexpr int NoIterator = -4;
    constexpr int AlreadyExists = -3;
    constexpr int NoTransaction = -2;
    constexpr int Closed = -1;
    constexpr int Ok = 0;
    constexpr int NotFound = 1;
    constexpr int Corruption = 2;
    constexpr int NotSupported = 3;
    constexpr int InvalidArgument = 4;
    constexpr int IOError = 5;
    constexpr int MergeInProgress = 6;
    constexpr int Incomplete = 7;
    constexpr int ShutdownInProgress = 8;
    constexpr int TimedOut = 9;
    constexpr int Aborted = 10;
    constexpr int Busy = 11;
    constexpr int Expired = 12;
    constexpr int TryAgain = 13;
    constexpr int CompactionTooLarge = 14;
    constexpr int ColumnFamilyDropped = 15;
    constexpr int Unknown = 16;
}
