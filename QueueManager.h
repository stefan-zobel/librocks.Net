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

#include <msclr/marshal_cppstd.h>
#include "api/librocks.h"
#include "api/KueueManager.h"
#include "Codes.h"
#include "Queue.h"

namespace marshal = msclr::interop;

using namespace System;
using namespace System::Collections::Concurrent;

namespace librocks::Net {

    public ref class QueueManager /* : public IDisposable */
    {
        public:
            QueueManager(String^ path)
            {
                if (path == nullptr) throw gcnew ArgumentNullException("path");
                int status = Status::Ok;
                std::string dbPath{ marshal::marshal_as<std::string>(path) };
                try {
                    _nativePtr = openKueueManager(&status, dbPath.c_str());
                }
                catch (...) {
                    _nativePtr = nullptr;
                    throw gcnew Exception("An unknown error occurred during QueueManager initialization.");
                }
                if (status != Status::Ok) {
                    _nativePtr = nullptr;
                    Codes::ThrowForStatus(status);
                }
            }

            // Inherited via IDisposable
            ~QueueManager() { // Dispose()
                this->!QueueManager();
            }

        protected:
            !QueueManager() {
                if (_nativePtr) {
                    delete _nativePtr;
                    _nativePtr = nullptr;
                }
            }

        public:

            Queue^ Get(String^ id);

            void Close() {
                delete this;
            }

            property bool IsOpen {
                bool get() {
                    return _nativePtr != nullptr && _nativePtr->isOpen();
                }
            }

        private:
            ::KueueManager* _nativePtr;

            void ThrowIfDisposed() {
                if (_nativePtr == nullptr) {
                    throw gcnew ObjectDisposedException("QueueManager");
                }
            }

            Queue^ WrapKueue(::Kueue* nativePtr);
            Queue^ CreateKueueWrapper(IntPtr key);
            ConcurrentDictionary<IntPtr, Queue^>^ _queueCache;
    };
}


