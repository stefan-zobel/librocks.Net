/*
 * Copyright 2026 Stefan Zobel
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "api/Kueue.h"
#include "client/KVStore.h"
#include "Codes.h"
#include "NativeBytes.h"

using namespace System;
using namespace System::Runtime::InteropServices;

namespace librocks::Net {

    public delegate bool QueueMsgConsumer(NativeBytes^ msg);

    public ref class Queue sealed
    {
        internal:
            Queue(::Kueue* nativeKueue) : _nativePtr(nativeKueue) {}

        public:

            void Clear() {
                int status = Status::Ok;
                _nativePtr->clear(&status);
                if (status != Status::Ok) {
                    Codes::ThrowForStatus(status);
                }
            }

            property unsigned long long TotalPuts {
                unsigned long long get() {
                    return _nativePtr->totalPuts();
                }
            }

            property unsigned long long TotalTakes {
                unsigned long long get() {
                    return _nativePtr->totalTakes();
                }
            }

            property bool IsEmpty {
                bool get() {
                    return _nativePtr->isEmpty();
                }
            }

            property long long Size {
                long long get() {
                    return _nativePtr->size();
                }
            }

            bool TryTake([Out] NativeBytes^ %data, [Optional] Nullable<TimeSpan> timeout) {
                int status = Status::Ok;
                size_t valLen = 0;
                char* nativeBytes;
                if (!timeout.HasValue) {
                    nativeBytes = _nativePtr->take(&status, &valLen);
                    if (status != Status::Ok) {
                        Codes::ThrowForStatus(status);
                    }
                }
                else {
                    std::chrono::milliseconds nativeTimeout = ConvertToChrono(timeout.Value);
                    nativeBytes = _nativePtr->take(&status, &valLen, nativeTimeout);
                    if (!(status == Status::Ok || status == Status::TimedOut)) {
                        Codes::ThrowForStatus(status);
                    }
                }
                if (!nativeBytes) {
                    data = nullptr;
                    return false;
                }
                data = gcnew NativeBytes(std::move(KVStore::constructBytes(nativeBytes, valLen)));
                return true;
            }

            bool TryAccept(QueueMsgConsumer^ consumer, [Optional] Nullable<TimeSpan> timeout) {
                if (!consumer) {
                    throw gcnew ArgumentNullException("consumer");
                }
                std::chrono::milliseconds nativeTimeout = std::chrono::milliseconds(0LL);
                if (timeout.HasValue) {
                    nativeTimeout = ConvertToChrono(timeout.Value);
                }
                int status = Status::Ok;
                size_t valLen = 0;
                unsigned long long key = 0L;
                char* nativeBytes = _nativePtr->readNext(&status, &valLen, &key, nativeTimeout);
                if (!(status == Status::Ok || status == Status::TimedOut)) {
                    Codes::ThrowForStatus(status);
                }
                if (nativeBytes) {
                    bool accepted = consumer->Invoke(gcnew NativeBytes(std::move(KVStore::constructBytes(nativeBytes, valLen))));
                    if (accepted) {
                        bool erased = _nativePtr->erase(key);
                        if (!erased) {
                            throw gcnew Exception("Failed to erase message from queue after acceptance.");
                        }
                    }
                    return true;
                }
                return false;
            }

#pragma warning(push)
#pragma warning(disable:4996)

            void Put(ReadOnlySpan<Byte> value) {
                int status = Status::Ok;
                pin_ptr<const Byte> pValue;

                if (value.Length > 0) {
                    pValue = &MemoryMarshal::GetReference(value);
                    size_t valLen = static_cast<size_t>(value.Length);
                    _nativePtr->put(&status, reinterpret_cast<const char*>(pValue), valLen);
                }
                if (status != Status::Ok) {
                    Codes::ThrowForStatus(status);
                }
            }

#pragma warning(pop)

        private:
            ::Kueue* _nativePtr;

            static std::chrono::milliseconds ConvertToChrono(System::TimeSpan ts) {
                double totalMs = ts.TotalMilliseconds;
                long long millis = static_cast<long long>(totalMs);
                return std::chrono::milliseconds(millis);
            }
    };
}
