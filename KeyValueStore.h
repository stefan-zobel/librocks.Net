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

#include "client/KVStore.h"
#include "client/RocksException.h"

#include <msclr/marshal_cppstd.h>
#include "RocksDbException.h"
#include "Kind.h"
#include "NativeBytes.h"

namespace marshal = msclr::interop;

using namespace System;
using namespace System::Collections::Concurrent;
using namespace System::Collections::Generic;
using namespace System::Runtime::InteropServices;

namespace librocks::Net {

    public ref class KeyValueStore
    {
        public:
            KeyValueStore(String^ path) {
                if (path == nullptr) throw gcnew ArgumentNullException("path");
                std::string dbPath { marshal::marshal_as<std::string>(path) };
                try {
                    _nativePtr = new KVStore(dbPath);
                }
                catch (const RocksException& e) {
                    _nativePtr = nullptr;
                    throw gcnew RocksDbException(e.code(), gcnew String(e.what()));
                }
                catch (...) {
                    _nativePtr = nullptr;
                    throw gcnew Exception("An unknown error occurred during KeyValueStore initialization.");
                }
            }

            // Inherited via IDisposable
            ~KeyValueStore() { this->!KeyValueStore(); } // Dispose()

        protected:
            // Finalizer
            !KeyValueStore() {
                if (_nativePtr) {
                    if (_kindCache) {
                        _kindCache->Clear();
                        _kindCache = nullptr;
                    }
                    delete _nativePtr;
                    _nativePtr = nullptr;
                }
            }

        public:
            void Close() {
                delete this;
            }

            property bool IsOpen {
                bool get() {
                    return _nativePtr != nullptr && _nativePtr->isOpen();
                }
            }

            // starting here each method needs to call ThrowIfDisposed!

            Kind^ GetDefaultKind();

            Kind^ GetOrCreateKind(String^ kindName);

            IReadOnlyCollection<Kind^>^ GetKinds();

            void Compact(Kind^ kind);

            void CompactAll();

#pragma warning(push)
#pragma warning(disable:4996)

            NativeBytes^ UpdateIfPresent(Kind^ kind, ReadOnlySpan<Byte> key, ReadOnlySpan<Byte> value);

            bool PutIfAbsent(Kind^ kind, ReadOnlySpan<Byte> key, ReadOnlySpan<Byte> value);

            void Put(Kind^ kind, ReadOnlySpan<Byte> key, ReadOnlySpan<Byte> value);

            NativeBytes^ Get(Kind^ kind, ReadOnlySpan<Byte> key);

            NativeBytes^ SingleRemoveIfPresent(Kind^ kind, ReadOnlySpan<Byte> key);

            NativeBytes^ RemoveIfPresent(Kind^ kind, ReadOnlySpan<Byte> key);

            void SingleRemove(Kind^ kind, ReadOnlySpan<Byte> key);

            void Remove(Kind^ kind, ReadOnlySpan<Byte> key);

            NativeBytes^ FindMinKey(Kind^ kind);

            NativeBytes^ FindMaxKey(Kind^ kind);

            bool TryUpdateIfPresent(Kind^ kind, ReadOnlySpan<Byte> key, ReadOnlySpan<Byte> value, Span<Byte> dest, [Out] int% bytesWritten);

            bool TryGet(Kind^ kind, ReadOnlySpan<Byte> key, Span<Byte> dest, [Out] int% bytesWritten);

            bool TrySingleRemoveIfPresent(Kind^ kind, ReadOnlySpan<Byte> key, Span<Byte> dest, [Out] int% bytesWritten);

            bool TryRemoveIfPresent(Kind^ kind, ReadOnlySpan<Byte> key, Span<Byte> dest, [Out] int% bytesWritten);

            bool TryFindMinKey(Kind^ kind, Span<Byte> dest, [Out] int% bytesWritten);

            bool TryFindMaxKey(Kind^ kind, Span<Byte> dest, [Out] int% bytesWritten);


#pragma warning(pop)
        private:
            KVStore* _nativePtr;

            void ThrowIfDisposed() {
                if (_nativePtr == nullptr) {
                    throw gcnew ObjectDisposedException("KeyValueStore");
                }
            }

            Kind^ WrapKind(const ::Kind* nativePtr);
            Kind^ CreateKindWrapper(IntPtr key);
            ConcurrentDictionary<IntPtr, Kind^>^ _kindCache;
    };
}
