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
#include "QueueManager.h"

namespace librocks::Net {

    // private
    Queue^ QueueManager::WrapKueue(::Kueue* nativePtr)
    {
        if (nativePtr == nullptr) return nullptr;
        if (!_queueCache) {
            _queueCache = gcnew ConcurrentDictionary<IntPtr, Queue^>(Environment::ProcessorCount, 31);
        }
        IntPtr key = (IntPtr)(void*)nativePtr;
        return _queueCache->GetOrAdd(key, gcnew System::Func<IntPtr, Queue^>(this, &QueueManager::CreateKueueWrapper));
    }

    // private
    Queue^ QueueManager::CreateKueueWrapper(IntPtr key)
    {
        ::Kueue* nativePtr = (::Kueue*)key.ToPointer();
        return gcnew Queue(nativePtr);
    }

    Queue^ QueueManager::Get(String^ queueId)
    {
        ThrowIfDisposed();
        if (queueId == nullptr) throw gcnew ArgumentNullException("queueId");
        try {
            int status = Status::Ok;
            std::string nativeQueueId{ marshal::marshal_as<std::string>(queueId) };
            ::Kueue* nativeKueue = _nativePtr->get(&status, nativeQueueId.c_str());
            if (status != Status::Ok) {
                Codes::ThrowForStatus(status);
            }
            return WrapKueue(nativeKueue);
        }
        catch (RocksDbException^) {
            throw;
        }
        catch (...) {
            throw gcnew Exception("An unexpected error occurred while retrieving the Queue: " + queueId);
        }
    }
}
