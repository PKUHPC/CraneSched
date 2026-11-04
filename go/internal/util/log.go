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
	"path"
	"runtime"
	"strings"

	nested "github.com/antonfisher/nested-logrus-formatter"
	log "github.com/sirupsen/logrus"
)

type CraneFormatter struct {
	log.TextFormatter
}

func (f *CraneFormatter) Format(entry *log.Entry) ([]byte, error) {
	msg := entry.Message
	if strings.HasSuffix(msg, "\n") {
		return []byte(msg), nil
	}
	return append([]byte(msg), '\n'), nil
}

// A simple formatter that only outputs the message,
// without any timestamp, level or caller info.
// It's used for normal output of simple commands (excluding cfored/cplugind and ccon/crun...)
func InitCraneLogger() {
	log.SetFormatter(&CraneFormatter{})
}

func InitDiagLogger(level string) {
	lv, err := log.ParseLevel(level)
	if err != nil {
		log.Warnf("Invalid log level %s, using info level", level)
		lv = log.InfoLevel
	}
	if lv > log.InfoLevel {
		// DEBUG/TRACE
		log.SetReportCaller(true)
	} else {
		// INFO/WARN/ERROR/FATAL/PANIC
		log.SetReportCaller(false)
	}

	log.SetLevel(lv)
	log.SetFormatter(&nested.Formatter{
		HideKeys:    false,
		CallerFirst: true,
		CustomCallerFormatter: func(frame *runtime.Frame) string {
			// File name + line number only.
			return fmt.Sprintf(" %s:%d", path.Base(frame.File), frame.Line)
		},
	})
}
