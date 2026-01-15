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

#include "client/bytes.h"

using namespace System;

namespace librocks::Net {

	public ref class NativeBytes sealed : public IDisposable
	{
	internal:
		NativeBytes(bytes&& native) {
			_nativePtr = new bytes(std::move(native));
		}

	public:
		// Inherited via IDisposable
		~NativeBytes() { this->!NativeBytes(); } // Dispose()

	protected:
		// Finalizer
		!NativeBytes() {
			if (_nativePtr) {
				delete _nativePtr;
				_nativePtr = nullptr;
			}
		}

	public:
#pragma warning(push)
#pragma warning(disable:4996)
		property ReadOnlySpan<Byte> Span {
			ReadOnlySpan<Byte> get() {
				if (!_nativePtr) return ReadOnlySpan<Byte>();
				return ReadOnlySpan<Byte>((void*)_nativePtr->data(),
					(int)_nativePtr->size());
			}
		}
#pragma warning(pop)

		virtual String^ ToString() override {
			if (!_nativePtr || _nativePtr->data() == nullptr)
			{
				return String::Empty;
			}
			return gcnew String(_nativePtr->data(), 0, (int)_nativePtr->size(),
				System::Text::Encoding::UTF8);
		}

	private:
		bytes* _nativePtr;
	};
}
