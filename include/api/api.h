
#ifdef LIBROCKS_EXPORTS
#define LIBROCKS_API __declspec(dllexport)
#else
#define LIBROCKS_API __declspec(dllimport)
#endif

#ifdef _MSC_VER
#define API_METHOD
#else
#ifdef LIBROCKS_EXPORTS
#define API_METHOD __declspec(dllexport)
#else
#define API_METHOD __declspec(dllimport)
#endif
#endif


#include "api/StatusCode.h"

#define WIN32_LEAN_AND_MEAN // Exclude rarely-used stuff from Windows headers
#include <windows.h>
