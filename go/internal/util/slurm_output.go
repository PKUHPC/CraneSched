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

package util

import (
	"CraneFrontEnd/generated/protos"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func FormatSlurmJobsJSON(reply *protos.QueryJobsInfoReply) (string, error) {
	data, err := MarshalProtoJSON(reply)
	if err != nil {
		return "", fmt.Errorf("failed to marshal job output: %w", err)
	}

	var output map[string]json.RawMessage
	if err := json.Unmarshal(data, &output); err != nil {
		return "", fmt.Errorf("failed to decode job output: %w", err)
	}

	var jobs []map[string]json.RawMessage
	if err := json.Unmarshal(output["job_info_list"], &jobs); err != nil {
		return "", fmt.Errorf("failed to decode job_info_list: %w", err)
	}
	for index, job := range jobs {
		renameJSONField(job, "craned_list", "node_list")

		var steps []map[string]json.RawMessage
		if err := json.Unmarshal(job["step_info_list"], &steps); err != nil {
			return "", fmt.Errorf("failed to decode job_info_list[%d].step_info_list: %w", index, err)
		}
		for _, step := range steps {
			renameJSONField(step, "craned_list", "node_list")
		}
		job["step_info_list"], err = json.Marshal(steps)
		if err != nil {
			return "", fmt.Errorf("failed to marshal job_info_list[%d].step_info_list: %w", index, err)
		}
	}

	output["job_info_list"], err = json.Marshal(jobs)
	if err != nil {
		return "", fmt.Errorf("failed to marshal job_info_list: %w", err)
	}
	data, err = json.Marshal(output)
	if err != nil {
		return "", fmt.Errorf("failed to marshal Slurm job output: %w", err)
	}
	return string(data), nil
}

func renameJSONField(fields map[string]json.RawMessage, oldName, newName string) {
	value, ok := fields[oldName]
	if !ok {
		return
	}
	fields[newName] = value
	delete(fields, oldName)
}

func MarshalSlurmProtoField(name string, msg protoreflect.ProtoMessage) (json.RawMessage, error) {
	if msg == nil || !msg.ProtoReflect().IsValid() {
		return nil, nil
	}
	data, err := MarshalProtoJSON(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal %s: %w", name, err)
	}
	return data, nil
}
