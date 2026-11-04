/**
 * Copyright (c) 2024 Peking University and Peking University
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
	"fmt"
	"strconv"
	"strings"
	"time"
)

const unknown = "Unknown"

var VERSION = unknown
var SOURCE_DATE_EPOCH = unknown

func VersionTemplate() string {
	return `{{.Version}}` + "\n"
}

func Version() string {
	displayTime := unknown

	// Try to parse SOURCE_DATE_EPOCH as Unix epoch
	epoch, err := strconv.ParseInt(strings.TrimSpace(SOURCE_DATE_EPOCH), 10, 64)
	if err == nil {
		displayTime = time.Unix(epoch, 0).
			In(time.FixedZone("UTC", 0)).
			Format(time.RFC1123Z)
	}

	return fmt.Sprintf("CraneSched %s\nSource Time: %s", VERSION, displayTime)
}
