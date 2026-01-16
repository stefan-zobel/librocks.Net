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
#include "pch.h"
#include "KeyValueStore.h"

using namespace System::Runtime::InteropServices;

namespace librocks::Net {
    // private
    Kind^ KeyValueStore::WrapKind(const ::Kind* nativePtr)
    {
        if (nativePtr == nullptr) return nullptr;
        if (!_kindCache) {
            _kindCache = gcnew ConcurrentDictionary<IntPtr, Kind^>(Environment::ProcessorCount, 31);
        }
        IntPtr key = (IntPtr)(void*)nativePtr;
        return _kindCache->GetOrAdd(key, gcnew System::Func<IntPtr, Kind^>(this, &KeyValueStore::CreateKindWrapper));
    }

    // private
    // Helper method for the factory (gets called from GetOrAdd())
    Kind^ KeyValueStore::CreateKindWrapper(IntPtr key)
    {
        const ::Kind* nativePtr = (const ::Kind*)key.ToPointer();
        return gcnew Kind(nativePtr);
    }

    Kind^ KeyValueStore::GetDefaultKind()
    {
        ThrowIfDisposed();
		const ::Kind& nativeKind = _nativePtr->getDefaultKind();
        return WrapKind(&nativeKind);
    }

    Kind^ KeyValueStore::GetOrCreateKind(String^ kindName)
    {
        ThrowIfDisposed();
        if (kindName == nullptr) throw gcnew ArgumentNullException("kindName");
        std::string colFamily{ marshal::marshal_as<std::string>(kindName) };
        const ::Kind& nativeKind = _nativePtr->getOrCreateKind(colFamily);
        return WrapKind(&nativeKind);
    }

    IReadOnlyCollection<Kind^>^ KeyValueStore::GetKinds()
    {
        ThrowIfDisposed();
        const KindSet nativeSet = _nativePtr->getKinds();
        List<Kind^>^ managedList = gcnew List<Kind^>((int)nativeSet.size());
        for (const auto& kindRef : nativeSet) {
            // kindRef is a std::reference_wrapper<const Kind>,
            // .get() returns const Kind&, with & we get the address.
            const ::Kind* pNativeKind = &kindRef.get();
            // WrapKind uses the _kindCache Dictionary internally, that guarantees
            // we are re-using the already existing .NET Kind^ instances.
            managedList->Add(WrapKind(pNativeKind));
        }
        return managedList->AsReadOnly();
    }

    void KeyValueStore::Compact(Kind^ kind)
    {
        ThrowIfDisposed();
        if (kind == nullptr) throw gcnew ArgumentNullException("kind");
        const ::Kind* pKind = kind->_nativePtr;
        if (pKind && pKind->isValid()) {
            try {
                _nativePtr->compact(*pKind);
            }
            catch (const RocksException& ex) {
				throw gcnew RocksDbException(ex.code(), gcnew String(ex.what()));
			}
            catch (...)
            {
                throw gcnew Exception("An unexpected error occurred during compaction of : "
                    + kind->Name);
            }
		}
    }

    void KeyValueStore::CompactAll()
    {
        ThrowIfDisposed();
        try {
            _nativePtr->compactAll();
        }
        catch (const RocksException& ex) {
            throw gcnew RocksDbException(ex.code(), gcnew String(ex.what()));
        }
        catch (...)
        {
            throw gcnew Exception("An unexpected error occurred during CompactAll() compaction.");
        }
    }

    NativeBytes^ KeyValueStore::UpdateIfPresent(Kind^ kind, ReadOnlySpan<Byte> key, ReadOnlySpan<Byte> value)
    {
        ThrowIfDisposed();
        if (kind == nullptr) throw gcnew ArgumentNullException("kind");

		// Create initially empty string_views
        std::string_view nativeKeyView;
        std::string_view nativeValueView;

        // The pin_ptr's must be declared here at method scope level!
		// (Otherwise the .NET GC could move the underlying memory
        // while we are still using the pointers!)
        pin_ptr<const Byte> pKey;
        pin_ptr<const Byte> pValue;

		// Pin the managed spans to get a stable pointer for the
        // duration of the use of the string_views.
        if (key.Length > 0) {
            pKey = &MemoryMarshal::GetReference(key);
            nativeKeyView = std::string_view(reinterpret_cast<const char*>(pKey), key.Length);
        }

        if (value.Length > 0) {
            pValue = &MemoryMarshal::GetReference(value);
            nativeValueView = std::string_view(reinterpret_cast<const char*>(pValue), value.Length);
        }

        try {
			// (If Length was 0 the empty string_view from above gets passed)
            bytes result = _nativePtr->updateIfPresent(*(kind->_nativePtr), nativeKeyView, nativeValueView);

            if (!result) return nullptr;
            // std::move casts the l-value 'result' into a r-value so that the
            // cheap move constructors can be used (only a pointer and length swap).
            return gcnew NativeBytes(std::move(result));
        }
        catch (const RocksException& e) {
            throw gcnew RocksDbException(e.code(), gcnew String(e.what()));
        }
        catch (...) {
            throw gcnew Exception("An unexpected error occurred during UpdateIfPresent() operation.");
        }
    }

    bool KeyValueStore::PutIfAbsent(Kind^ kind, ReadOnlySpan<Byte> key, ReadOnlySpan<Byte> value)
    {
        ThrowIfDisposed();
        if (kind == nullptr) throw gcnew ArgumentNullException("kind");

        std::string_view nativeKeyView;
        std::string_view nativeValueView;

        pin_ptr<const Byte> pKey;
        pin_ptr<const Byte> pValue;

        if (key.Length > 0) {
            pKey = &MemoryMarshal::GetReference(key);
            nativeKeyView = std::string_view(reinterpret_cast<const char*>(pKey), key.Length);
        }

        if (value.Length > 0) {
            pValue = &MemoryMarshal::GetReference(value);
            nativeValueView = std::string_view(reinterpret_cast<const char*>(pValue), value.Length);
        }

        try {
            return _nativePtr->putIfAbsent(*(kind->_nativePtr), nativeKeyView, nativeValueView);
        }
        catch (const RocksException& e) {
            throw gcnew RocksDbException(e.code(), gcnew String(e.what()));
        }
        catch (...) {
            throw gcnew Exception("An unexpected error occurred during PutIfAbsent() operation.");
        }
    }

    void KeyValueStore::Put(Kind^ kind, ReadOnlySpan<Byte> key, ReadOnlySpan<Byte> value)
    {
        ThrowIfDisposed();
        if (kind == nullptr) throw gcnew ArgumentNullException("kind");

        std::string_view nativeKeyView;
        std::string_view nativeValueView;

        pin_ptr<const Byte> pKey;
        pin_ptr<const Byte> pValue;

        if (key.Length > 0) {
            pKey = &MemoryMarshal::GetReference(key);
            nativeKeyView = std::string_view(reinterpret_cast<const char*>(pKey), key.Length);
        }

        if (value.Length > 0) {
            pValue = &MemoryMarshal::GetReference(value);
            nativeValueView = std::string_view(reinterpret_cast<const char*>(pValue), value.Length);
        }

        try {
            _nativePtr->put(*(kind->_nativePtr), nativeKeyView, nativeValueView);
        }
        catch (const RocksException& e) {
            throw gcnew RocksDbException(e.code(), gcnew String(e.what()));
        }
        catch (...) {
            throw gcnew Exception("An unexpected error occurred during Put() operation.");
        }
    }

    NativeBytes^ KeyValueStore::Get(Kind^ kind, ReadOnlySpan<Byte> key)
    {
        ThrowIfDisposed();
        if (kind == nullptr) throw gcnew ArgumentNullException("kind");

        std::string_view nativeKeyView;
        pin_ptr<const Byte> pKey;

        if (key.Length > 0) {
            pKey = &MemoryMarshal::GetReference(key);
            nativeKeyView = std::string_view(reinterpret_cast<const char*>(pKey), key.Length);
        }

        try {
            bytes result = _nativePtr->get(*(kind->_nativePtr), nativeKeyView);
            if (!result) return nullptr;
            return gcnew NativeBytes(std::move(result));
        }
        catch (const RocksException& e) {
            throw gcnew RocksDbException(e.code(), gcnew String(e.what()));
        }
        catch (...) {
            throw gcnew Exception("An unexpected error occurred during Get() operation.");
        }
    }

    NativeBytes^ KeyValueStore::SingleRemoveIfPresent(Kind^ kind, ReadOnlySpan<Byte> key)
    {
        ThrowIfDisposed();
        if (kind == nullptr) throw gcnew ArgumentNullException("kind");

        std::string_view nativeKeyView;
        pin_ptr<const Byte> pKey;

        if (key.Length > 0) {
            pKey = &MemoryMarshal::GetReference(key);
            nativeKeyView = std::string_view(reinterpret_cast<const char*>(pKey), key.Length);
        }

        try {
            bytes result = _nativePtr->singleRemoveIfPresent(*(kind->_nativePtr), nativeKeyView);
            if (!result) return nullptr;
            return gcnew NativeBytes(std::move(result));
        }
        catch (const RocksException& e) {
            throw gcnew RocksDbException(e.code(), gcnew String(e.what()));
        }
        catch (...) {
            throw gcnew Exception("An unexpected error occurred during SingleRemoveIfPresent() operation.");
        }
    }

    NativeBytes^ KeyValueStore::RemoveIfPresent(Kind^ kind, ReadOnlySpan<Byte> key)
    {
        ThrowIfDisposed();
        if (kind == nullptr) throw gcnew ArgumentNullException("kind");

        std::string_view nativeKeyView;
        pin_ptr<const Byte> pKey;

        if (key.Length > 0) {
            pKey = &MemoryMarshal::GetReference(key);
            nativeKeyView = std::string_view(reinterpret_cast<const char*>(pKey), key.Length);
        }

        try {
            bytes result = _nativePtr->removeIfPresent(*(kind->_nativePtr), nativeKeyView);
            if (!result) return nullptr;
            return gcnew NativeBytes(std::move(result));
        }
        catch (const RocksException& e) {
            throw gcnew RocksDbException(e.code(), gcnew String(e.what()));
        }
        catch (...) {
            throw gcnew Exception("An unexpected error occurred during RemoveIfPresent() operation.");
        }
    }

    void KeyValueStore::SingleRemove(Kind^ kind, ReadOnlySpan<Byte> key)
    {
        ThrowIfDisposed();
        if (kind == nullptr) throw gcnew ArgumentNullException("kind");

        std::string_view nativeKeyView;
        pin_ptr<const Byte> pKey;

        if (key.Length > 0) {
            pKey = &MemoryMarshal::GetReference(key);
            nativeKeyView = std::string_view(reinterpret_cast<const char*>(pKey), key.Length);
        }

        try {
            _nativePtr->singleRemove(*(kind->_nativePtr), nativeKeyView);
        }
        catch (const RocksException& e) {
            throw gcnew RocksDbException(e.code(), gcnew String(e.what()));
        }
        catch (...) {
            throw gcnew Exception("An unexpected error occurred during SingleRemove() operation.");
        }
    }

    void KeyValueStore::Remove(Kind^ kind, ReadOnlySpan<Byte> key)
    {
        ThrowIfDisposed();
        if (kind == nullptr) throw gcnew ArgumentNullException("kind");

        std::string_view nativeKeyView;
        pin_ptr<const Byte> pKey;

        if (key.Length > 0) {
            pKey = &MemoryMarshal::GetReference(key);
            nativeKeyView = std::string_view(reinterpret_cast<const char*>(pKey), key.Length);
        }

        try {
            _nativePtr->remove(*(kind->_nativePtr), nativeKeyView);
        }
        catch (const RocksException& e) {
            throw gcnew RocksDbException(e.code(), gcnew String(e.what()));
        }
        catch (...) {
            throw gcnew Exception("An unexpected error occurred during Remove() operation.");
        }
    }

    NativeBytes^ KeyValueStore::FindMinKey(Kind^ kind)
    {
        ThrowIfDisposed();
        if (kind == nullptr) throw gcnew ArgumentNullException("kind");

        try {
            bytes result = _nativePtr->findMinKey(*(kind->_nativePtr));
            if (!result) return nullptr;
            return gcnew NativeBytes(std::move(result));
        }
        catch (const RocksException& e) {
            throw gcnew RocksDbException(e.code(), gcnew String(e.what()));
        }
        catch (...) {
            throw gcnew Exception("An unexpected error occurred during FindMinKey() operation.");
        }
    }

    NativeBytes^ KeyValueStore::FindMaxKey(Kind^ kind)
    {
        ThrowIfDisposed();
        if (kind == nullptr) throw gcnew ArgumentNullException("kind");

        try {
            bytes result = _nativePtr->findMaxKey(*(kind->_nativePtr));
            if (!result) return nullptr;
            return gcnew NativeBytes(std::move(result));
        }
        catch (const RocksException& e) {
            throw gcnew RocksDbException(e.code(), gcnew String(e.what()));
        }
        catch (...) {
            throw gcnew Exception("An unexpected error occurred during FindMaxKey() operation.");
        }
    }
}
