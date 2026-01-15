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

#include "api/Kind.h"

using namespace System;

namespace librocks::Net {

	public ref class Kind sealed : IComparable<Kind^>
	{
		internal:
			Kind(const ::Kind* nativeKind) : _nativePtr(nativeKind) {}

		public:
			property String^ Name {
				String^ get() {
					if (!_nativePtr) return String::Empty;
					return gcnew String(_nativePtr->name());
				}
			}

			property bool IsValid {
				bool get() {
					if (!_nativePtr) return false;
					return _nativePtr->isValid();
				}
			}

			virtual bool Equals(Object^ obj) override {
				if (ReferenceEquals(obj, nullptr)) return false;
				if (ReferenceEquals(this, obj)) return true;
				Kind^ other = dynamic_cast<Kind^>(obj);
				if (other == nullptr) return false;
				if (!_nativePtr) return false;
				return _nativePtr->equals(other->_nativePtr);
			}

			virtual int GetHashCode() override {
				if (!_nativePtr) return 0;
				return static_cast<int>(_nativePtr->hash());
			}

			virtual String^ ToString() override {
				if (!_nativePtr) return "Disposed Kind";
				return gcnew String(_nativePtr->toString(), 0,
					(int)strlen(_nativePtr->toString()), System::Text::Encoding::UTF8);
			}

			virtual int CompareTo(Kind^ other) {
				if (other == nullptr) return 1;
				if (_nativePtr == other->_nativePtr) return 0;
				if (this->_nativePtr == nullptr) return -1;
				if (other->_nativePtr == nullptr) return 1;
				if (*_nativePtr < *(other->_nativePtr)) return -1;
				if (*(other->_nativePtr) < *_nativePtr) return 1;
				return 0;
			}

			static bool operator < (Kind^ a, Kind^ b) {
				if (ReferenceEquals(a, b)) return false;
				if (ReferenceEquals(a, nullptr)) return true;
				if (ReferenceEquals(b, nullptr)) return false;
				return a->CompareTo(b) < 0;
			}

			static bool operator > (Kind^ a, Kind^ b) {
				return b < a;
			}

			static bool operator == (Kind^ a, Kind^ b) {
				if (ReferenceEquals(a, b)) return true;
				if (ReferenceEquals(a, nullptr) || ReferenceEquals(b, nullptr)) return false;
				return a->Equals(b);
			}

			static bool operator != (Kind^ a, Kind^ b) {
				return !(a == b);
			}

		internal:
			const ::Kind* _nativePtr;
	};
}
