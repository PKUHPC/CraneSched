#
# Copyright (c) 2024 Peking University and Peking University
# Changsha Institute for Computing and Digital Economy
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

# Native product entry points. Configure the CMake tree before using cpp targets.
CMAKE_BUILD_DIR ?= build
BUILD_JOBS ?= 4

.PHONY: all cpp go test test-cpp test-go install install-cpp install-go package package-cpp package-go

all: cpp go

cpp:
	cmake --build "$(CMAKE_BUILD_DIR)" --parallel $(BUILD_JOBS)

go:
	$(MAKE) -C go all tool

test: test-cpp test-go

test-cpp:
	ctest --test-dir "$(CMAKE_BUILD_DIR)" --output-on-failure

test-go:
	$(MAKE) -C go test

install: install-cpp install-go

install-cpp:
	DESTDIR="$(DESTDIR)" cmake --install "$(CMAKE_BUILD_DIR)"

install-go:
	$(MAKE) -C go install

package: package-cpp package-go

package-cpp:
	cd "$(CMAKE_BUILD_DIR)" && cpack

package-go:
	$(MAKE) -C go package
