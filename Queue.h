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
#include "Codes.h"

using namespace System;

namespace librocks::Net {

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

        private:
            ::Kueue* _nativePtr;

            static std::chrono::milliseconds ConvertToChrono(System::TimeSpan ts) {
                double totalMs = ts.TotalMilliseconds;
                long long millis = static_cast<long long>(totalMs);
                return std::chrono::milliseconds(millis);
            }
    };
}


