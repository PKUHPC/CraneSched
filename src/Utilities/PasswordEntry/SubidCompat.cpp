/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// Compatibility shim for older libsubid (shadow-utils < 4.11).
//
// These versions of libsubid.so export 'Prog' and 'shadow_logfd' as
// undefined (U) symbols and expect the calling program to provide them.
// Newer libsubid (SUBID_ABI_MAJOR >= 4) removed this requirement and
// uses libsubid_init() instead.
//
// We provide strong definitions here so the linker can satisfy the
// shared library's references.

extern "C" {
#include <shadow/subid.h>
}

#include <cstdio>

#if !defined(SUBID_ABI_MAJOR) || SUBID_ABI_MAJOR < 4

extern "C" {

// Program name used in libsubid log/error messages.
const char* Prog = "crane";

// File pointer for libsubid diagnostic output.
// Set to nullptr; libsubid will fall back to /dev/null internally.
FILE* shadow_logfd = nullptr;

}  // extern "C"

#endif
