// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Strftime implements the C-locale time vocabulary used by the command,
// including GNU padding/case modifiers. Slurm sinfo does not call setlocale.
func Strftime(pattern string, t time.Time) string {
	isoYear, isoWeek := t.ISOWeek()
	yearDay, weekday := t.YearDay()-1, int(t.Weekday())
	isoDay := (weekday+6)%7 + 1
	number := func(n, width int) string { return fmt.Sprintf("%0*d", width, n) }
	var out strings.Builder
	for i := 0; i < len(pattern); i++ {
		if pattern[i] != '%' {
			out.WriteByte(pattern[i])
			continue
		}
		start := i
		i++
		flags := ""
		for i < len(pattern) && strings.ContainsRune("_-0^#", rune(pattern[i])) {
			flags += pattern[i : i+1]
			i++
		}
		widthStart := i
		for i < len(pattern) && pattern[i] >= '0' && pattern[i] <= '9' {
			i++
		}
		width, _ := strconv.Atoi(pattern[widthStart:i])
		if width > 256 {
			// Callers use a 256-byte output buffer; avoid allocating to an
			// unbounded user width only to replace it with the overflow marker.
			return strings.Repeat("#", 256)
		}
		modifier := byte(0)
		if i < len(pattern) && (pattern[i] == 'E' || pattern[i] == 'O') {
			modifier = pattern[i]
			i++
		}
		if i == len(pattern) {
			out.WriteString(pattern[start:])
			break
		}
		code := pattern[i]
		directive := code
		if modifier == 'E' && !strings.ContainsRune("cCnPprRstTuxXyYzZ%", rune(code)) ||
			modifier == 'O' && !strings.ContainsRune("bBCdegGhHIjklmMnPprRsStTuUVwWyzZ%", rune(code)) {
			code = 0 // Invalid modifier pairs remain literal directives.
		}
		value := ""
		switch code {
		case '%':
			value = "%"
		case 'a':
			value = t.Format("Mon")
		case 'A':
			value = t.Format("Monday")
		case 'b', 'h':
			value = t.Format("Jan")
		case 'B':
			value = t.Format("January")
		case 'c':
			value = t.Format("Mon Jan _2 15:04:05 2006")
		case 'C':
			value = number(t.Year()/100, 2)
		case 'd':
			value = t.Format("02")
		case 'D', 'x':
			value = t.Format("01/02/06")
		case 'e':
			value = t.Format("_2")
		case 'F':
			value = t.Format("2006-01-02")
		case 'g':
			value = number(isoYear%100, 2)
		case 'G':
			value = number(isoYear, 4)
		case 'H':
			value = t.Format("15")
		case 'I':
			value = t.Format("03")
		case 'j':
			value = number(t.YearDay(), 3)
		case 'k':
			value = fmt.Sprintf("%2d", t.Hour())
		case 'l':
			value = fmt.Sprintf("%2d", (t.Hour()+11)%12+1)
		case 'm':
			value = t.Format("01")
		case 'M':
			value = t.Format("04")
		case 'n':
			value = "\n"
		case 'p':
			value = t.Format("PM")
		case 'P':
			value = t.Format("pm")
		case 'r':
			value = t.Format("03:04:05 PM")
		case 'R':
			value = t.Format("15:04")
		case 's':
			value = strconv.FormatInt(t.Unix(), 10)
		case 'S':
			value = t.Format("05")
		case 't':
			value = "\t"
		case 'T', 'X':
			value = t.Format("15:04:05")
		case 'u':
			value = strconv.Itoa(isoDay)
		case 'U':
			value = number((yearDay+7-weekday)/7, 2)
		case 'V':
			value = number(isoWeek, 2)
		case 'w':
			value = strconv.Itoa(weekday)
		case 'W':
			value = number((yearDay+7-(isoDay-1))/7, 2)
		case 'y':
			value = t.Format("06")
		case 'Y':
			value = t.Format("2006")
		case 'z':
			_, offset := t.Zone()
			sign := "+"
			if offset < 0 {
				sign, offset = "-", -offset
			}
			digits := strconv.Itoa(offset/3600*100 + offset%3600/60)
			minimum, pad, signPad := 4, "0", " "
			for _, flag := range flags {
				switch flag {
				case '-':
					minimum, pad, signPad = 1, " ", " "
				case '_':
					minimum, pad, signPad = 4, " ", " "
				case '0':
					minimum, pad, signPad = 4, "0", "0"
				}
			}
			value = strings.Repeat(signPad, max(0, width-1)) + sign + strings.Repeat(pad, max(0, max(width, minimum)-len(digits))) + digits
		case 'Z':
			value = t.Format("MST")
		default:
			value = pattern[start : i+1]
		}
		if strings.ContainsRune("CdeHgGHIjklmMSsuUVwWyY", rune(code)) {
			padWidth := max(width, len(value))
			pad := byte('0')
			if strings.ContainsRune("ekl", rune(code)) {
				pad = ' '
			}
			for _, flag := range flags {
				switch flag {
				case '-':
					pad = 0
				case '_':
					pad = ' '
				case '0':
					pad = '0'
				}
			}
			if width > 0 || strings.ContainsAny(flags, "_-0") {
				value = strings.TrimLeft(value, " 0")
				if value == "" {
					value = "0"
				}
				if pad != 0 && len(value) < padWidth {
					value = strings.Repeat(string(pad), padWidth-len(value)) + value
				}
			}
		}
		if directive == 'P' || strings.Contains(flags, "#") && strings.ContainsRune("pZ", rune(directive)) {
			value = strings.ToLower(value)
		} else if strings.Contains(flags, "^") || strings.Contains(flags, "#") && (strings.ContainsRune("bBh", rune(directive)) || code != 0 && strings.ContainsRune("aA", rune(code))) {
			value = strings.ToUpper(value)
		}
		if len(value) < width {
			pad := " "
			for _, flag := range flags {
				switch flag {
				case '0':
					pad = "0"
				case '-', '_':
					pad = " "
				}
			}
			value = strings.Repeat(pad, width-len(value)) + value
		}
		out.WriteString(value)
	}
	return out.String()
}

// CommandTimezone supports both zoneinfo names/files and POSIX TZ expressions.
// Go's time.Local only supports the former. Supplying the expression as a TZif
// v2 footer lets the standard library evaluate offsets and DST transitions.
func CommandTimezone(env map[string]string) *time.Location {
	name, set := env["TZ"]
	if !set {
		return time.Local
	}
	name = strings.TrimPrefix(name, ":")
	if name == "" {
		return time.UTC
	}
	path := name
	if !filepath.IsAbs(path) {
		dir, ok := env["TZDIR"]
		if !ok {
			dir = "/usr/share/zoneinfo"
		}
		path = filepath.Join(dir, name)
	}
	if data, err := os.ReadFile(path); err == nil {
		if location, err := time.LoadLocationFromTZData(name, data); err == nil {
			return location
		}
	}
	// Each TZif section contains one UTC type and no transitions; the footer
	// supplies the rule for all instants. The UTC type is also the fallback for
	// malformed expressions. The two sections have the same size without times.
	section := make([]byte, 54)
	copy(section, "TZif2")
	section[39] = 1 // typecnt
	section[43] = 4 // charcnt
	copy(section[50:], "UTC\x00")
	data := append(append([]byte{}, section...), section...)
	data = append(data, ('\n'))
	data = append(data, name...)
	data = append(data, '\n')
	location, err := time.LoadLocationFromTZData(name, data)
	if err != nil {
		return time.UTC
	}
	return location
}
