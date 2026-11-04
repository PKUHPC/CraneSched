package main

import (
	"fmt"
	"strings"

	"CraneFrontEnd/tool/meta-cni/pkg/engine"
	metatypes "CraneFrontEnd/tool/meta-cni/pkg/types"
	"CraneFrontEnd/tool/meta-cni/pkg/utils"

	"github.com/containernetworking/cni/pkg/skel"
	cnitypes "github.com/containernetworking/cni/pkg/types"
	"github.com/containernetworking/cni/pkg/types/create"
	"github.com/containernetworking/cni/pkg/version"

	log "github.com/sirupsen/logrus"
)

const (
	aboutPlugin = `Meta CNI Plugin for CraneSched

This plugin organizes CNI plugins into pipelines, where each pipeline manages
a single network interface. Static pipelines always execute; template pipelines
are driven by GRES annotations to support dynamic multi-NIC scenarios.
`
)

func main() {
	skel.PluginMainFuncsWithError(
		skel.CNIFuncs{
			Add:    cmdAdd,
			Check:  cmdCheck,
			Del:    cmdDel,
			Status: cmdStatus,
		},
		version.All,
		aboutPlugin,
	)
}

func cmdAdd(args *skel.CmdArgs) error {
	conf, err := metatypes.LoadMetaPluginConf(args.StdinData)
	if err != nil {
		return err
	}

	utils.InitLogger(conf.LogLevel)

	log.Debugf("Loaded config: %+v", conf)
	if err := conf.Validate(); err != nil {
		return err
	}

	log.Tracef("CNI args from runtime: %q", args.Args)
	log.Infof("Pipelines: [%s]", getPipelineSummary(conf))

	result, err := engine.Execute(conf, engine.ActionAdd, args)
	if err != nil {
		return err
	}

	if result == nil {
		result, err = emptyResult(conf.CNIVersion)
		if err != nil {
			return err
		}
	}

	return cnitypes.PrintResult(result, conf.CNIVersion)
}

func cmdCheck(args *skel.CmdArgs) error {
	conf, err := metatypes.LoadMetaPluginConf(args.StdinData)
	if err != nil {
		return err
	}

	utils.InitLogger(conf.LogLevel)

	log.Debugf("Loaded config: %+v", conf)
	if err := conf.Validate(); err != nil {
		return err
	}

	log.Tracef("CNI args from runtime: %q", args.Args)
	log.Infof("Pipelines: [%s]", getPipelineSummary(conf))

	_, err = engine.Execute(conf, engine.ActionCheck, args)
	return err
}

func cmdDel(args *skel.CmdArgs) error {
	conf, err := metatypes.LoadMetaPluginConf(args.StdinData)
	if err != nil {
		return err
	}

	utils.InitLogger(conf.LogLevel)

	log.Debugf("Loaded config: %+v", conf)
	if err := conf.Validate(); err != nil {
		return err
	}

	log.Tracef("CNI args from runtime: %q", args.Args)
	log.Infof("Pipelines: [%s]", getPipelineSummary(conf))

	_, err = engine.Execute(conf, engine.ActionDel, args)
	return err
}

func cmdStatus(args *skel.CmdArgs) error {
	conf, err := metatypes.LoadMetaPluginConf(args.StdinData)
	if err != nil {
		return err
	}

	utils.InitLogger(conf.LogLevel)

	log.Debugf("Loaded config: %+v", conf)
	if err := conf.Validate(); err != nil {
		return err
	}

	log.Infof("Pipelines: [%s]", getPipelineSummary(conf))

	_, err = engine.Execute(conf, engine.ActionStatus, args)
	return err
}

func getPipelineSummary(conf *metatypes.MetaPluginConf) string {
	parts := make([]string, 0, len(conf.Pipelines))
	for i := range conf.Pipelines {
		p := &conf.Pipelines[i]

		delegates := make([]string, len(p.Delegates))
		for j := range p.Delegates {
			delegates[j] = p.Delegates[j].Identifier()
		}

		var mode string
		if p.IsTemplate() {
			mode = fmt.Sprintf("template:%s*", p.IfNamePrefix)
		} else {
			mode = p.IfName
		}

		parts = append(parts, fmt.Sprintf("%s(%s)[%s]",
			p.Name, mode, strings.Join(delegates, "→")))
	}
	return strings.Join(parts, ", ")
}

func emptyResult(cniVersion string) (cnitypes.Result, error) {
	if cniVersion == "" {
		cniVersion = version.Current()
	}
	payload := fmt.Appendf(nil, `{"cniVersion":%q}`, cniVersion)
	return create.Create(cniVersion, payload)
}
