#pragma once

#include "api/StatusCode.h"
#include "RocksDbException.h"

using namespace System;
using namespace System::Collections::Generic;

namespace librocks::Net {

    ref class Codes sealed
    {
        internal:
            static void ThrowForStatus(int status) {
                if (status != Status::Ok) {
                    String^ statusName;
                    if (codes->TryGetValue(status, statusName)) {
                        throw gcnew RocksDbException(status, statusName);
                    }
                    else {
                        throw gcnew RocksDbException(status, "Unknown");
                    }
                }
            }

        private:
            static Codes() {
                codes = gcnew Dictionary<int, String^>(23);
                codes->Add(Status::Invalid, "Invalid");
                codes->Add(Status::NoIterator, "NoIterator");
                codes->Add(Status::AlreadyExists, "AlreadyExists");
                codes->Add(Status::NoTransaction, "NoTransaction");
                codes->Add(Status::Closed, "Closed");
                codes->Add(Status::Ok, "Ok");
                codes->Add(Status::NotFound, "NotFound");
                codes->Add(Status::Corruption, "Corruption");
                codes->Add(Status::NotSupported, "NotSupported");
                codes->Add(Status::InvalidArgument, "InvalidArgument");
                codes->Add(Status::IOError, "IOError");
                codes->Add(Status::MergeInProgress, "MergeInProgress");
                codes->Add(Status::Incomplete, "Incomplete");
                codes->Add(Status::ShutdownInProgress, "ShutdownInProgress");
                codes->Add(Status::TimedOut, "TimedOut");
                codes->Add(Status::Aborted, "Aborted");
                codes->Add(Status::Busy, "Busy");
                codes->Add(Status::Expired, "Expired");
                codes->Add(Status::TryAgain, "TryAgain");
                codes->Add(Status::CompactionTooLarge, "CompactionTooLarge");
                codes->Add(Status::ColumnFamilyDropped, "ColumnFamilyDropped");
                codes->Add(Status::Unknown, "Unknown");
            }

        private:
            static Dictionary<int, String^>^ codes;
    };
}


