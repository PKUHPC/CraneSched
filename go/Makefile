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

# Makefile for Golang components of CraneSched.
# This file will generate protobuf files, build executables and plugins. 

# Notes for developer: 
# - Please use proper indentation for neater output.

# Variables

GIT_COMMIT_HASH := $(shell git rev-parse --short HEAD 2>/dev/null)
LAST_COMMIT_EPOCH := $(shell git log -1 --format=%ct 2>/dev/null)

VERSION_FILE := VERSION
VERSION := $(shell if [ -f "$(VERSION_FILE)" ]; then cat "$(VERSION_FILE)"; else echo "unknown"; fi)
VERSION_DISPLAY := $(VERSION)
ifneq ($(strip $(GIT_COMMIT_HASH)),)
	VERSION_DISPLAY := $(VERSION) (git: $(GIT_COMMIT_HASH))
endif

# Package version (replace + with - for packaging); allow override
PKG_VERSION ?= $(shell if [ -f "$(VERSION_FILE)" ]; then tr '+' '-' < "$(VERSION_FILE)"; else echo "unknown"; fi)

# Build timestamp determination (inject Unix epoch for reproducible builds)
ifeq ($(strip $(SOURCE_DATE_EPOCH)),)
	ifneq ($(strip $(LAST_COMMIT_EPOCH)),)
		SOURCE_DATE_EPOCH := $(strip $(LAST_COMMIT_EPOCH))
	else
		SOURCE_DATE_EPOCH := $(shell date +%s)
		SOURCE_DATE_EPOCH_FALLBACK := true
	endif
endif

SOURCE_DATE_EPOCH := $(strip $(SOURCE_DATE_EPOCH))
ifneq ($(strip $(SOURCE_DATE_EPOCH_FALLBACK)),)
  $(warning SOURCE_DATE_EPOCH not set; falling back to current time $(SOURCE_DATE_EPOCH))
endif

# Debug info control
STRIP ?= false

# LDFLAGS adjustments based on STRIP
ifeq ($(STRIP), true)
	LDFLAGS := -ldflags \
				"-s -w -X 'CraneFrontEnd/internal/util.VERSION=$(VERSION_DISPLAY)' \
				-X 'CraneFrontEnd/internal/util.SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH)'"
else
	LDFLAGS := -ldflags \
				"-X 'CraneFrontEnd/internal/util.VERSION=$(VERSION_DISPLAY)' \
				-X 'CraneFrontEnd/internal/util.SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH)'"
endif

# Installation paths
PREFIX ?= /usr/local
DESTDIR ?=

BIN_DIR := build/bin
PLUGIN_DIR := build/plugin
TOOL_DIR := build/tool

SERVICE_TEMPLATES := $(wildcard etc/*.service.in)
SERVICE_NAMES := $(patsubst etc/%.service.in,%,$(SERVICE_TEMPLATES))
SERVICE_OUTPUT_DIR := build/etc

GO ?= go
GO_VERSION := $(shell $(GO) version 2>/dev/null || echo "go (unknown)")
BUILD_FLAGS := -trimpath

NVIDIA_LIB_PATH := /usr/lib/x86_64-linux-gnu
CUDA_INCLUDE_PATH := /usr/include
PLUGIN_CGO_CFLAGS := -I$(CUDA_INCLUDE_PATH)
PLUGIN_CGO_LDFLAGS := -L$(NVIDIA_LIB_PATH) -lnvidia-ml -Wl,-rpath,$(NVIDIA_LIB_PATH)
# CHECK_GPU: auto (default), set to 1/true to force NVML, 0/false to disable.
CHECK_GPU ?= auto
CHECK_GPU_DETECTED := $(shell command -v nvidia-smi 2> /dev/null)
ifeq ($(CHECK_GPU),auto)
	CHECK_GPU_ENABLED := $(CHECK_GPU_DETECTED)
else ifneq (,$(filter 0 false no off,$(CHECK_GPU)))
	CHECK_GPU_ENABLED :=
else
	CHECK_GPU_ENABLED := $(CHECK_GPU)
endif

# Targets
.PHONY: all build test protos clean install plugin plugin-monitor plugin-trace plugin-other tool service format package check-goreleaser

all: build plugin service

# FIXME: This is a workaround for sub-packages in proto files
# We have to refactor the proto layout and CMakeLists in future
protos:
	@echo "- Generating Protobuf files..."
	@mkdir -p ./generated/protos
	@protoc --go_out=paths=source_relative:generated/protos --go-grpc_out=paths=source_relative:generated/protos --proto_path=protos protos/*.proto
	@mkdir -p ./generated/protos/supervisor && mv generated/protos/Supervisor*.go ./generated/protos/supervisor
	@echo "  - Summary:"
	@echo "    - Protobuf files generated in ./generated/protos/"

test: protos
	@echo "- Running Go tests..."
	@$(GO) test ./...

build: protos
	@echo "- Building executables with $(GO_VERSION)..."
	@mkdir -p $(BIN_DIR)
	@for dir in cmd/*/ ; do \
		echo "  - Building $$dir"; \
		(cd $$dir && $(GO) build $(BUILD_FLAGS) $(LDFLAGS) \
			-o ../../$(BIN_DIR)/$$(basename $$dir)) || exit 1; \
	done
	@echo "  - Summary:"
	@echo "    - Version: $(VERSION)"
	@echo "    - Source date epoch: $(SOURCE_DATE_EPOCH)"
	@echo "    - Commit hash: $(GIT_COMMIT_HASH)"
	@echo "    - Binaries are in ./$(BIN_DIR)/"

plugin: plugin-monitor plugin-trace plugin-other

plugin-monitor: protos
	@echo "- Building monitor plugin with $(GO_VERSION)..."
	@mkdir -p $(PLUGIN_DIR)
	@cd plugin/monitor && \
	if [ -n "$(CHECK_GPU_ENABLED)" ]; then \
		echo "  - NVML enabled, to disable, set CHECK_GPU=0"; \
		CGO_ENABLED=1 \
		CGO_CFLAGS="$(PLUGIN_CGO_CFLAGS)" \
		CGO_LDFLAGS="$(PLUGIN_CGO_LDFLAGS)" \
		$(GO) build $(BUILD_FLAGS) $(LDFLAGS) -buildmode=plugin -tags with_nvml -o ../../$(PLUGIN_DIR)/monitor.so .; \
	else \
		CGO_ENABLED=1 \
		$(GO) build $(BUILD_FLAGS) $(LDFLAGS) -buildmode=plugin -o ../../$(PLUGIN_DIR)/monitor.so .; \
	fi

plugin-trace: protos
	@echo "- Building trace plugin with $(GO_VERSION)..."
	@mkdir -p $(PLUGIN_DIR)
	@cd plugin/trace && \
		CGO_ENABLED=1 \
		$(GO) build $(BUILD_FLAGS) $(LDFLAGS) -buildmode=plugin -o ../../$(PLUGIN_DIR)/trace.so .

plugin-other: protos
	@echo "- Building other plugins..."
	@mkdir -p $(PLUGIN_DIR)
	@for dir in plugin/*/ ; do \
		if [ "$$(basename $$dir)" != "monitor" ] && [ "$$(basename $$dir)" != "trace" ]; then \
			echo "  - Building: $$(basename $$dir).so"; \
			(cd $$dir && $(GO) build $(BUILD_FLAGS) $(LDFLAGS) \
				-buildmode=plugin -o ../../$(PLUGIN_DIR)/$$(basename $$dir).so) || exit 1; \
		fi \
	done
	@echo "  - Summary:"
	@echo "    - Plugins are in ./$(PLUGIN_DIR)/"

tool: protos
	@echo "- Building tools with $(GO_VERSION)..."
	@mkdir -p $(TOOL_DIR)
	@for dir in tool/*/ ; do \
		echo "  - Building $$dir"; \
		(cd $$dir && $(GO) build $(BUILD_FLAGS) $(LDFLAGS) \
			-o ../../$(TOOL_DIR)/$$(basename $$dir)) || exit 1; \
	done
	@echo "  - Summary:"
	@echo "    - Tools are in ./$(TOOL_DIR)/"

service:
	@echo "- Rendering systemd service files with PREFIX=$(PREFIX)..."
	@mkdir -p $(SERVICE_OUTPUT_DIR)
	@if [ -z "$(SERVICE_NAMES)" ]; then \
		echo "  - No templates found under etc/*.service.in"; \
	else \
		for svc in $(SERVICE_NAMES); do \
			echo "  - $$svc.service"; \
			sed "s|@PREFIX@|$(PREFIX)|g" etc/$$svc.service.in > $(SERVICE_OUTPUT_DIR)/$$svc.service; \
		done; \
	fi

clean:
	@echo "Cleaning up..."
	@rm -rf build generated

install: build plugin service
	@echo "- Installing executables, plugins and auxiliary files..."
	@if [ ! -d "$(BIN_DIR)" ]; then \
		echo "Error: $(BIN_DIR) does not exist. Please build first."; \
		exit 1; \
	fi
	@if [ ! -d "$(PLUGIN_DIR)" ]; then \
		echo "Error: $(PLUGIN_DIR) does not exist. Please build plugins first."; \
		exit 1; \
	fi
	@echo "  - Installing binaries to $(DESTDIR)$(PREFIX)/bin/"
	@mkdir -p $(DESTDIR)$(PREFIX)/bin
	@cp $(BIN_DIR)/* $(DESTDIR)$(PREFIX)/bin/
	@echo "  - Installing plugins to $(DESTDIR)$(PREFIX)/lib/crane/plugin/"
	@mkdir -p $(DESTDIR)$(PREFIX)/lib/crane/plugin
	@cp $(PLUGIN_DIR)/*.so $(DESTDIR)$(PREFIX)/lib/crane/plugin/
	@echo "  - Installing systemd service files to $(DESTDIR)/usr/lib/systemd/system/"
	@mkdir -p $(DESTDIR)/usr/lib/systemd/system
	@cp $(SERVICE_OUTPUT_DIR)/*.service $(DESTDIR)/usr/lib/systemd/system/
	@if [ -z "$(DESTDIR)" ]; then \
		echo "  - You may need to reload systemd daemons: 'systemctl daemon-reload'"; \
	fi
	@echo "  - Done."

check-goreleaser:
	@echo "- Checking packaging dependencies..."
	@command -v goreleaser >/dev/null 2>&1 || { \
		echo "  - ERROR: goreleaser not found. Install it with:"; \
		echo "     go install github.com/goreleaser/goreleaser/v2@latest"; \
		exit 1; \
	}
	@echo "  - All packaging dependencies satisfied"

package: check-goreleaser
ifneq ($(SKIP_REBUILD),true)
	@echo "- Rebuilding binaries and plugins for packaging..."
	@$(MAKE) STRIP=true protos build plugin tool
else
	@echo "- Skipping rebuild (SKIP_REBUILD=true). Using existing build artifacts."
endif
	@if [ ! -d "$(BIN_DIR)" ] || [ -z "$$(ls -A $(BIN_DIR) 2>/dev/null)" ]; then \
		echo "Error: $(BIN_DIR) is empty. Run 'make build' before packaging or remove SKIP_REBUILD."; \
		exit 1; \
	fi
	@if [ ! -d "$(PLUGIN_DIR)" ] || [ -z "$$(ls -A $(PLUGIN_DIR) 2>/dev/null)" ]; then \
		echo "Error: $(PLUGIN_DIR) is empty. Run 'make plugin' before packaging or remove SKIP_REBUILD."; \
		exit 1; \
	fi
	@$(MAKE) PREFIX=/usr service
	@echo "- Building packages with GoReleaser..."
	@export GORELEASER_CURRENT_TAG=$(PKG_VERSION) && \
	goreleaser release --snapshot --clean
	@echo "  - Summary:"
	@echo "    - Packages are in ./build/dist/"
	@echo "    - Version: $(PKG_VERSION)"

format:
	@echo "- Running go mod tidy, go vet and gofmt..."
	@$(GO) mod tidy
	@$(GO) vet ./...
	@gofmt -w .
	@echo "  - Done."
