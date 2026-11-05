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

package cbatch

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

type LineProcessor interface {
	Process(line string, sh *[]string, args *[]CbatchArg) error
}

// For Crane args
type cLineProcessor struct {
}

func (c *cLineProcessor) Process(line string, sh *[]string, args *[]CbatchArg) error {
	split := strings.Fields(line)
	if len(split) == 3 {
		*args = append(*args, CbatchArg{name: split[1], val: split[2]})
	} else if len(split) == 2 {
		parts := strings.SplitN(split[1], "=", 2)
		if len(parts) > 1 {
			*args = append(*args, CbatchArg{name: parts[0], val: parts[1]})
		} else {
			*args = append(*args, CbatchArg{name: parts[0]})
		}
	} else {
		return errors.New("fields out of bound")
	}
	return nil
}

// For Slurm args
type sLineProcessor struct {
	supported map[string]bool
}

func (s *sLineProcessor) init() {
	s.supported = map[string]bool{
		"-c": true, "--cpus-per-task": true, "-J": true, "--job-name": true, "-N": true, "-q": true, "--qos": true,
		"--nodes": true, "-A": true, "--account": true, "-e": true, "--error": true, "-x": true, "--exclude": true, "-D": true, "--chdir": true,
		"--export": true, "--mem": true, "-p": true, "--partition": true, "-i": true, "--input": true, "-o": true, "--output": true,
		"--nodelist": true, "-w": true, "--get-user-env": true, "--time": true, "-t": true, "--ntasks-per-node": true,
		"--ntasks": true, "-n": true, "--mail-type": true, "--mail-user": true, "--comment": true, "--open-mode": true,
		"-H": true, "--hold": true, "--requeue": true, "--no-requeue": true,
		"--reservation": true, "-r": true, "--wrap": true, "--gres": true, "--gpus-per-node": true, "--mem-per-cpu": true,
		"--exclusive": true, "--wckey": true, "--licenses": true, "-L": true, "--dependency": true, "-d": true,
		"--begin": true, "-b": true, "--deadline": true, "--array": true, "-a": true, "-s": true, "--signal": true,
	}
}

func (s *sLineProcessor) Process(line string, sh *[]string, args *[]CbatchArg) error {
	if s.supported == nil {
		s.init()
	}
	split := strings.Fields(line)
	if len(split) == 3 {
		name := split[1]
		if s.supported[name] {
			val := split[2]
			if name == "-t" || name == "--time" {
				val = ConvertSlurmTimeFormat(val)
			}
			*args = append(*args, CbatchArg{name: name, val: val})
		} else if _, found := unsupportedFlagMessage(name); found {
			log.Warnf("Slurm option %v is not supported", name)
		} else {
			return fmt.Errorf("line `%v` is not supported by cwrapper", line)
		}
	} else if len(split) == 2 {
		parts := strings.SplitN(split[1], "=", 2)
		name := parts[0]
		if s.supported[name] {
			if len(parts) > 1 {
				val := parts[1]
				if name == "-t" || name == "--time" {
					val = ConvertSlurmTimeFormat(val)
				}
				*args = append(*args, CbatchArg{name: name, val: val})
			} else {
				*args = append(*args, CbatchArg{name: name})
			}
		} else if _, found := unsupportedFlagMessage(name); found {
			log.Warnf("Slurm option %v is not supported", name)
		} else {
			return fmt.Errorf("line `%v` is not supported by cwrapper", line)
		}
	} else {
		return errors.New("fields out of bound")
	}
	return nil
}

func ConvertSlurmTimeFormat(t string) string {
	// minutes | minutes:seconds | days-hours | days-hours:minutes
	re := regexp.MustCompile(`^(?:(\d+)-(\d+)(?::(\d+))?|(\d+)(?::(\d+))?)$`)
	x := re.FindStringSubmatch(t)
	if x == nil {
		return t
	}
	if x[1] != "" {
		M := x[3]
		if M == "" {
			M = "0"
		}
		return fmt.Sprintf("%s-%s:%s:0", x[1], x[2], M)
	}
	S := x[5]
	if S == "" {
		S = "0"
	}
	return fmt.Sprintf("0:%s:%s", x[4], S)
}

// for LSF args
type lLineProcessor struct {
	mapping map[string]string
}

func (l *lLineProcessor) init() {
	l.mapping = map[string]string{
		"-J": "-J", "-o": "-o", "-e": "-e", "-nnode": "--nodes",
		"-n": "--ntasks-per-node", "-W": "--time", "-M": "--mem", "-cwd": "--chdir",
		"-q": "--partition", "-env": "--export",
	}
}

func (l *lLineProcessor) Process(line string, sh *[]string, args *[]CbatchArg) error {
	if l.mapping == nil {
		l.init()
	}
	split := strings.Fields(line)
	if len(split) == 3 {
		if name, ok := l.mapping[split[1]]; ok {
			val := split[2]
			if name == "--time" {
				val = ConvertLSFRuntimeLimit(val)
			}
			*args = append(*args, CbatchArg{name: name, val: val})
		} else {
			log.Warnf("LSF option %v is not supported", split[1])
		}
	} else {
		return fmt.Errorf("line `%v` is not supported by cwrapper", line)
	}
	return nil
}

func ConvertLSFRuntimeLimit(t string) string {
	if t == "" {
		return t
	}
	// [hour:]minute
	re := regexp.MustCompile(`(?:(\d+):)?(\d+)`)
	x := re.FindStringSubmatch(t)
	if x[0] != t {
		log.Fatalf("Failed to parse LSF time format: %s\n", t)
	}
	H, M := x[1], x[2]
	if H == "" {
		H = "0"
	}
	return fmt.Sprintf("%s:%s:0", H, M)
}

// for sh commands
type defaultProcessor struct {
}

func (d *defaultProcessor) Process(line string, sh *[]string, args *[]CbatchArg) error {
	*sh = append(*sh, line)
	return nil
}
