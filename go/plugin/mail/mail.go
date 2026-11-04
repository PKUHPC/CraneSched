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

package main

import (
	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"
	"os"

	"bytes"
	"fmt"
	"os/exec"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"gopkg.in/yaml.v3"
)

var _ api.Plugin = &MailPlugin{}
var _ api.JobLifecycleHooks = &MailPlugin{}

var PluginInstance = MailPlugin{}

// Plugin internal config
type config struct {
	SenderAddr  string `yaml:"SenderAddr"`
	SubjectOnly bool   `yaml:"SubjectOnly"`
}

type MailPlugin struct {
	config
}

func (p *MailPlugin) parseExtraAttrInJob(t *protos.JobInfo) (mailtype string, mailuser string, err error) {
	// We treat "" as a valid JSON string
	if t.ExtraAttr != "" && !gjson.Valid(t.ExtraAttr) {
		return "", "", fmt.Errorf("invalid JSON string")
	}

	mailtype = gjson.Get(t.ExtraAttr, "mail.type").String()
	mailuser = gjson.Get(t.ExtraAttr, "mail.user").String()

	return mailtype, mailuser, nil
}

func (p *MailPlugin) subject(t *protos.JobInfo) string {
	mailtype := gjson.Get(t.ExtraAttr, "mail.type").String()

	subject := fmt.Sprintf("[CraneSched] JobID=%v, Name=%v, MailType=%v, Status=%v",
		t.JobId, t.Name, mailtype, t.Status.String())

	if t.Status != protos.JobStatus_Running {
		subject += fmt.Sprintf(", ElapsedTime=%v, ExitCode=%v", t.ElapsedTime.AsDuration(), t.ExitCode)
	}
	return subject
}

func (p *MailPlugin) body(t *protos.JobInfo) string {
	body := fmt.Sprintf(
		"Job ID: %v\nJob Name: %v\nState: %v\nWorking Dir: %v\nStart Time: %v\n",
		t.JobId, t.Name, t.Status.String(), t.Cwd, t.StartTime.AsTime().Local())

	if t.Status != protos.JobStatus_Running {
		body += fmt.Sprintf("End Time: %v\nElapsed Time: %v\nExit Code: %v\n",
			t.EndTime.AsTime().Local(), t.ElapsedTime.AsDuration(), t.ExitCode)
	}

	body += fmt.Sprintf("Node Number: %d\nNodes List: %s\n",
		t.NodeNum, t.GetCranedList())

	body += "\nThis mail is automatically sent by CraneSched. Please do not reply.\n"

	return body
}

func (p *MailPlugin) send(subject, body, to, cc, bcc string) error {
	// Construct the mail command
	command := fmt.Sprintf("mail -r %s -s \"%s\"", p.SenderAddr, subject)
	if cc != "" {
		command += " -c " + cc
	}
	if bcc != "" {
		command += " -b " + bcc
	}
	command += " " + to

	log.Tracef("Generated mail command: `%v`.", command)

	// Execute the mail command
	cmd := exec.Command("sh", "-c", command)
	cmd.Stdin = bytes.NewBufferString(body)

	return cmd.Run()
}

func (p *MailPlugin) Name() string {
	return "Mail"
}

func (p *MailPlugin) Version() string {
	return "v0.0.1"
}

func (p *MailPlugin) Load(meta api.PluginMeta) error {
	if meta.Config == "" {
		return fmt.Errorf("no config file specified")
	}

	content, err := os.ReadFile(meta.Config)
	if err != nil {
		return err
	}

	if err := yaml.Unmarshal(content, &p.config); err != nil {
		return err
	}

	log.Infoln("Mail plugin is initialized.")
	log.Tracef("Mail plugin config: %v", p.config)

	return nil
}

func (p *MailPlugin) Unload(meta api.PluginMeta) error {
	log.Infoln("Mail plugin is unloaded.")
	return nil
}

func (p *MailPlugin) StartHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.StartHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected StartHookRequest.")
		return
	}

	subject := ""
	body := ""
	for _, job := range req.GetJobInfoList() {
		mailtype, mailuser, err := p.parseExtraAttrInJob(job)
		if err != nil {
			log.Tracef("Failed to parse extra attributes: %v", err)
			continue
		}

		if mailtype == "" || mailuser == "" {
			log.Tracef("Mail type or mail user not specified in job %v", job.JobId)
			continue
		}

		if mailtype == "ALL" || mailtype == "BEGIN" {
			subject = p.subject(job)
			if !p.SubjectOnly {
				body = p.body(job)
			}

			if err := p.send(subject, body, mailuser, "", ""); err != nil {
				log.Warnf("Failed to send mail: %v", err)
			}
		}
	}
}

func (p *MailPlugin) EndHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.EndHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected EndHookRequest.")
		return
	}

	subject := ""
	body := ""
	for _, job := range req.GetJobInfoList() {
		mailtype, mailuser, err := p.parseExtraAttrInJob(job)
		if err != nil {
			log.Tracef("Failed to parse extra attributes: %v", err)
			continue
		}

		if mailtype == "" || mailuser == "" {
			log.Tracef("Mail type or mail user not specified in job %v", job.JobId)
			continue
		}

		if mailtype == "ALL" || mailtype == "END" ||
			(mailtype == "FAIL" && job.Status == protos.JobStatus_Failed) ||
			(mailtype == "TIMELIMIT" && job.Status == protos.JobStatus_ExceedTimeLimit) ||
			(mailtype == "OOM" && job.Status == protos.JobStatus_OutOfMemory) {
			subject = p.subject(job)
			if !p.SubjectOnly {
				body = p.body(job)
			}

			if err := p.send(subject, body, mailuser, "", ""); err != nil {
				log.Warnf("Failed to send mail: %v", err)
			}
		}
	}
}
