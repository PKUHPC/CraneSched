/**
 * Copyright (c) 2025 Peking University and Peking University
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

package ccon

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	config *util.Config
	stub   protos.CraneCtldClient
)

type CconJsonSchema struct {
	Action  string `json:"action"`
	Message string `json:"message,omitempty"`
	Flags   any    `json:"flags,omitempty"`
	Data    any    `json:"data,omitempty"`
}

func outputJson(action string, message string, flags any, data any) {
	// Workaround: convert protobuf message to map[string]any
	if msg, ok := data.(protoreflect.ProtoMessage); ok {
		json.Unmarshal([]byte(util.FmtJson.FormatReply(msg)), &data)
	}
	output := CconJsonSchema{
		Action:  action,
		Message: message,
		Flags:   flags,
		Data:    data,
	}
	jsonData, _ := json.Marshal(output)
	fmt.Println(string(jsonData))
}
