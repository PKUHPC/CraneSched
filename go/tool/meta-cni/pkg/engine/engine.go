package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"CraneFrontEnd/tool/meta-cni/pkg/result"
	metatemplate "CraneFrontEnd/tool/meta-cni/pkg/template"
	metatypes "CraneFrontEnd/tool/meta-cni/pkg/types"
	"CraneFrontEnd/tool/meta-cni/pkg/utils"

	"github.com/containernetworking/cni/pkg/invoke"
	"github.com/containernetworking/cni/pkg/skel"
	cnitypes "github.com/containernetworking/cni/pkg/types"
	log "github.com/sirupsen/logrus"
)

// Action enumerates supported CNI verbs.
type Action string

const (
	ActionAdd    Action = "ADD"
	ActionCheck  Action = "CHECK"
	ActionDel    Action = "DEL"
	ActionStatus Action = "STATUS"

	gresAnnotationPrefix = "cranesched.internal/meta-cni/gres/"
)

// ResolvedPipeline is an expanded pipeline ready for execution.
type ResolvedPipeline struct {
	metatypes.Pipeline
	InstanceIndex int    // Template instance index, -1 for static pipelines
	GRESDevice    string // GRES annotation value for this instance
}

// Execute runs pipelines for a single CNI action using the provided args.
func Execute(conf *metatypes.MetaPluginConf, action Action, args *skel.CmdArgs) (cnitypes.Result, error) {
	if conf == nil {
		return nil, errors.New("meta-cni: config is nil")
	}
	if action == ActionCheck && conf.PrevResult == nil {
		return nil, fmt.Errorf("meta-cni: prevResult is required for %v", action)
	}

	ctx, cancel := contextWithTimeout(conf.TimeoutSeconds)
	defer cancel()

	switch action {
	case ActionAdd:
		return executeAdd(ctx, conf, args)
	case ActionDel:
		return executeDel(ctx, conf, args)
	case ActionCheck:
		return executeCheck(ctx, conf, args)
	case ActionStatus:
		return executeStatus(ctx, conf)
	default:
		return nil, fmt.Errorf("meta-cni: unsupported action %s", action)
	}
}

// --- Pipeline resolution ---

func resolvePipelines(conf *metatypes.MetaPluginConf, args *skel.CmdArgs) ([]ResolvedPipeline, error) {
	annotations := conf.Annotations()
	cniArgs := utils.ParseArgs(args.Args)
	var resolved []ResolvedPipeline

	for i := range conf.Pipelines {
		p := &conf.Pipelines[i]
		if !p.IsTemplate() {
			resolved = append(resolved, ResolvedPipeline{
				Pipeline:      *p,
				InstanceIndex: -1,
			})
			continue
		}

		instances := findGRESAnnotations(annotations, p.Name)
		if len(instances) == 0 {
			// No GRES allocated for this container: skip template pipeline on ADD.
			continue
		}

		for _, inst := range instances {
			rp, err := expandTemplate(p, inst.Index, inst.Device, cniArgs)
			if err != nil {
				return nil, fmt.Errorf("meta-cni: pipeline %q expand: %w", p.Name, err)
			}
			resolved = append(resolved, rp)
		}
	}

	if err := validateUniqueIfNames(resolved); err != nil {
		return nil, err
	}
	return resolved, nil
}

func resolvePipelinesForDel(conf *metatypes.MetaPluginConf, args *skel.CmdArgs) ([]ResolvedPipeline, error) {
	annotations := conf.Annotations()
	cniArgs := utils.ParseArgs(args.Args)
	var resolved []ResolvedPipeline

	for i := range conf.Pipelines {
		p := &conf.Pipelines[i]
		if !p.IsTemplate() {
			resolved = append(resolved, ResolvedPipeline{
				Pipeline:      *p,
				InstanceIndex: -1,
			})
			continue
		}

		instances := findGRESAnnotations(annotations, p.Name)
		if len(instances) == 0 {
			// No GRES allocated for this container: skip template pipeline on DEL/CHECK.
			continue
		}

		for _, inst := range instances {
			rp, err := expandTemplate(p, inst.Index, inst.Device, cniArgs)
			if err != nil {
				return nil, fmt.Errorf("meta-cni: pipeline %q expand for DEL: %w", p.Name, err)
			}
			resolved = append(resolved, rp)
		}
	}

	return resolved, nil
}

type gresInstance struct {
	Index  int
	Device string
}

func findGRESAnnotations(annotations map[string]string, pipelineName string) []gresInstance {
	prefix := gresAnnotationPrefix + pipelineName + "/"
	var instances []gresInstance

	for key, value := range annotations {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		suffix := key[len(prefix):]
		idx, err := strconv.Atoi(suffix)
		if err != nil || idx < 0 {
			log.Warnf("meta-cni: ignoring annotation %q: invalid index %q", key, suffix)
			continue
		}
		instances = append(instances, gresInstance{Index: idx, Device: value})
	}

	sort.Slice(instances, func(i, j int) bool {
		return instances[i].Index < instances[j].Index
	})
	return instances
}

func expandTemplate(p *metatypes.Pipeline, index int, device string, cniArgs map[string]string) (ResolvedPipeline, error) {
	idxStr := strconv.Itoa(index)
	vars := metatemplate.BuildVars(device, idxStr, cniArgs)

	delegates := make([]metatypes.DelegateEntry, len(p.Delegates))
	for i, d := range p.Delegates {
		delegates[i] = metatypes.DelegateEntry{
			Name:            d.Name,
			Type:            d.Type,
			RuntimeOverride: d.RuntimeOverride,
		}
		if len(d.Conf) > 0 {
			rendered, err := metatemplate.Render(d.Conf, vars)
			if err != nil {
				return ResolvedPipeline{}, fmt.Errorf("pipeline %q delegate %s: render conf: %w",
					p.Name, d.Identifier(), err)
			}
			delegates[i].Conf = rendered
		}
	}

	expanded := metatypes.Pipeline{
		Name:            p.Name + "-" + idxStr,
		IfName:          p.IfNamePrefix + idxStr,
		RuntimeOverride: p.RuntimeOverride,
		Delegates:       delegates,
	}

	return ResolvedPipeline{
		Pipeline:      expanded,
		InstanceIndex: index,
		GRESDevice:    device,
	}, nil
}

func validateUniqueIfNames(pipelines []ResolvedPipeline) error {
	seen := make(map[string]string, len(pipelines)) // ifName → pipeline name
	for _, rp := range pipelines {
		if prev, exists := seen[rp.IfName]; exists {
			return fmt.Errorf("meta-cni: duplicate ifName %q between pipelines %q and %q",
				rp.IfName, prev, rp.Name)
		}
		seen[rp.IfName] = rp.Name
	}
	return nil
}

// --- ADD ---

func executeAdd(ctx context.Context, conf *metatypes.MetaPluginConf, args *skel.CmdArgs) (cnitypes.Result, error) {
	resolved, err := resolvePipelines(conf, args)
	if err != nil {
		return nil, err
	}

	var mergedResult cnitypes.Result
	var successfulPipelines []ResolvedPipeline

	for _, rp := range resolved {
		logger := log.WithField("pipeline", rp.Name)
		logger.Infof("ADD pipeline (ifName=%s)", rp.IfName)

		pipelineResult, pErr := executePipelineAdd(ctx, &rp, conf, args)
		if pErr != nil {
			logger.Errorf("ADD pipeline failed: %v", pErr)
			if rollbackErr := rollbackPipelines(ctx, conf, args, successfulPipelines); rollbackErr != nil {
				return nil, errors.Join(
					fmt.Errorf("meta-cni: pipeline %q ADD failed: %w", rp.Name, pErr),
					fmt.Errorf("meta-cni: rollback failed: %w", rollbackErr),
				)
			}
			return nil, fmt.Errorf("meta-cni: pipeline %q ADD failed: %w", rp.Name, pErr)
		}

		successfulPipelines = append(successfulPipelines, rp)
		mergedResult, err = result.Merge(mergedResult, pipelineResult)
		if err != nil {
			if rollbackErr := rollbackPipelines(ctx, conf, args, successfulPipelines); rollbackErr != nil {
				return nil, errors.Join(
					fmt.Errorf("meta-cni: merge result from pipeline %q: %w", rp.Name, err),
					fmt.Errorf("meta-cni: rollback failed: %w", rollbackErr),
				)
			}
			return nil, fmt.Errorf("meta-cni: merge result from pipeline %q: %w", rp.Name, err)
		}
	}

	return mergedResult, nil
}

func rollbackPipelines(ctx context.Context, conf *metatypes.MetaPluginConf, args *skel.CmdArgs,
	successfulPipelines []ResolvedPipeline) error {

	var firstErr error
	for i := len(successfulPipelines) - 1; i >= 0; i-- {
		rp := &successfulPipelines[i]
		rollbackLogger := log.WithField("pipeline", rp.Name)
		rollbackLogger.Info("rolling back pipeline")
		if err := executePipelineDel(ctx, rp, conf, args); err != nil {
			rollbackLogger.Errorf("rollback pipeline failed: %v", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

type delegateSnapshot struct {
	delegate *metatypes.DelegateEntry
	env      map[string]string
}

func executePipelineAdd(ctx context.Context, rp *ResolvedPipeline, conf *metatypes.MetaPluginConf, args *skel.CmdArgs) (cnitypes.Result, error) {
	var chainResult cnitypes.Result
	var successful []delegateSnapshot

	for i := range rp.Delegates {
		delegate := &rp.Delegates[i]
		logger := log.WithFields(log.Fields{
			"pipeline": rp.Name,
			"delegate": delegate.Identifier(),
		})

		env, err := buildRuntimeEnv(args, conf.RuntimeOverride, &rp.Pipeline, delegate)
		if err != nil {
			return nil, fmt.Errorf("delegate %s env: %w", delegate.Identifier(), err)
		}

		restore, err := utils.ApplyEnv(env)
		if err != nil {
			return nil, fmt.Errorf("delegate %s apply env: %w", delegate.Identifier(), err)
		}

		logger.Debug("invoking delegate ADD")
		latestResult, callErr := callDelegate(ctx, delegate, ActionAdd, conf.CNIVersion, chainResult, conf.RuntimeConfig)
		restore()

		if callErr != nil {
			logger.Errorf("delegate ADD failed: %v", callErr)
			// Rollback successful delegates in this pipeline
			for j := len(successful) - 1; j >= 0; j-- {
				snap := successful[j]
				rollRestore, rollEnvErr := utils.ApplyEnv(snap.env)
				if rollEnvErr != nil {
					logger.Errorf("rollback: failed to apply env for delegate %s, attempting DEL with current env: %v", snap.delegate.Identifier(), rollEnvErr)
				}
				_ = callDelegateDel(ctx, snap.delegate, conf.CNIVersion, chainResult, conf.RuntimeConfig)
				if rollRestore != nil {
					rollRestore()
				}
			}
			return nil, fmt.Errorf("delegate %s: %w", delegate.Identifier(), callErr)
		}

		successful = append(successful, delegateSnapshot{delegate: delegate, env: env})
		if latestResult != nil {
			chainResult = latestResult
		}
	}

	return chainResult, nil
}

// --- DEL ---

func executeDel(ctx context.Context, conf *metatypes.MetaPluginConf, args *skel.CmdArgs) (cnitypes.Result, error) {
	resolved, err := resolvePipelinesForDel(conf, args)
	if err != nil {
		return nil, err
	}

	var firstErr error
	for i := len(resolved) - 1; i >= 0; i-- {
		rp := &resolved[i]
		logger := log.WithField("pipeline", rp.Name)
		logger.Infof("DEL pipeline (ifName=%s)", rp.IfName)

		if pErr := executePipelineDel(ctx, rp, conf, args); pErr != nil {
			logger.Errorf("DEL pipeline failed: %v", pErr)
			if firstErr == nil {
				firstErr = pErr
			}
		}
	}

	return nil, firstErr
}

func executePipelineDel(ctx context.Context, rp *ResolvedPipeline, conf *metatypes.MetaPluginConf, args *skel.CmdArgs) error {
	var firstErr error
	for i := len(rp.Delegates) - 1; i >= 0; i-- {
		delegate := &rp.Delegates[i]
		logger := log.WithFields(log.Fields{
			"pipeline": rp.Name,
			"delegate": delegate.Identifier(),
		})

		env, err := buildRuntimeEnv(args, conf.RuntimeOverride, &rp.Pipeline, delegate)
		if err != nil {
			logger.Errorf("DEL env build failed: %v", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		restore, err := utils.ApplyEnv(env)
		if err != nil {
			logger.Errorf("DEL apply env failed: %v", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		logger.Debug("invoking delegate DEL")
		callErr := callDelegateDel(ctx, delegate, conf.CNIVersion, conf.PrevResult, conf.RuntimeConfig)
		restore()

		if callErr != nil {
			logger.Errorf("delegate DEL failed: %v", callErr)
			if firstErr == nil {
				firstErr = callErr
			}
		}
	}
	return firstErr
}

// --- CHECK ---

func executeCheck(ctx context.Context, conf *metatypes.MetaPluginConf, args *skel.CmdArgs) (cnitypes.Result, error) {
	resolved, err := resolvePipelinesForDel(conf, args)
	if err != nil {
		return nil, err
	}

	for _, rp := range resolved {
		for i := range rp.Delegates {
			delegate := &rp.Delegates[i]
			logger := log.WithFields(log.Fields{
				"pipeline": rp.Name,
				"delegate": delegate.Identifier(),
			})

			env, err := buildRuntimeEnv(args, conf.RuntimeOverride, &rp.Pipeline, delegate)
			if err != nil {
				return nil, fmt.Errorf("delegate %s env: %w", delegate.Identifier(), err)
			}

			restore, err := utils.ApplyEnv(env)
			if err != nil {
				return nil, fmt.Errorf("delegate %s apply env: %w", delegate.Identifier(), err)
			}

			logger.Debug("invoking delegate CHECK")
			_, callErr := callDelegate(ctx, delegate, ActionCheck, conf.CNIVersion, conf.PrevResult, conf.RuntimeConfig)
			restore()

			if callErr != nil {
				return nil, fmt.Errorf("meta-cni: pipeline %q delegate %s CHECK failed: %w",
					rp.Name, delegate.Identifier(), callErr)
			}
		}
	}

	return nil, nil
}

// --- Common helpers ---

func executeStatus(ctx context.Context, conf *metatypes.MetaPluginConf) (cnitypes.Result, error) {
	seen := make(map[string]struct{})

	for i := range conf.Pipelines {
		p := &conf.Pipelines[i]
		for j := range p.Delegates {
			delegate := &p.Delegates[j]

			confBytes, pluginType, err := effectiveConf(delegate, conf.CNIVersion, nil, conf.RuntimeConfig)
			if err != nil {
				return nil, fmt.Errorf("meta-cni: pipeline %q delegate %s STATUS config: %w",
					p.Name, delegate.Identifier(), err)
			}
			if _, ok := seen[pluginType]; ok {
				continue
			}
			seen[pluginType] = struct{}{}

			if err := invoke.DelegateStatus(ctx, pluginType, confBytes, nil); err != nil {
				return nil, fmt.Errorf("meta-cni: delegate type %q STATUS failed: %w", pluginType, err)
			}
		}
	}

	return nil, nil
}

func contextWithTimeout(timeoutSeconds int) (context.Context, context.CancelFunc) {
	if timeoutSeconds <= 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
}

func buildRuntimeEnv(args *skel.CmdArgs, globalOverride *metatypes.RuntimeOverride,
	pipeline *metatypes.Pipeline, delegate *metatypes.DelegateEntry) (map[string]string, error) {

	env := map[string]string{
		"CNI_CONTAINERID": args.ContainerID,
		"CNI_NETNS":       args.Netns,
		"CNI_IFNAME":      args.IfName,
		"CNI_ARGS":        args.Args,
		"CNI_PATH":        args.Path,
	}

	apply := func(override *metatypes.RuntimeOverride) error {
		if override == nil {
			return nil
		}
		if override.ContainerID != "" {
			env["CNI_CONTAINERID"] = override.ContainerID
		}
		if override.NetNS != "" {
			env["CNI_NETNS"] = override.NetNS
		}
		if override.IfName != "" {
			env["CNI_IFNAME"] = override.IfName
		}
		if override.CNIPath != "" {
			env["CNI_PATH"] = override.CNIPath
		}
		if len(override.Args) > 0 {
			merged, err := utils.MergeArgs(env["CNI_ARGS"], override.Args)
			if err != nil {
				return fmt.Errorf("meta-cni: invalid args override: %w", err)
			}
			env["CNI_ARGS"] = merged
		}
		if len(override.Envs) > 0 {
			if err := utils.MergeEnvs(env, override.Envs); err != nil {
				return fmt.Errorf("meta-cni: invalid env override: %w", err)
			}
		}
		return nil
	}

	// Layer 1: global override
	if err := apply(globalOverride); err != nil {
		return nil, err
	}

	// Layer 2: pipeline ifName
	if pipeline.IfName != "" {
		env["CNI_IFNAME"] = pipeline.IfName
	}

	// Layer 3: pipeline override
	if err := apply(pipeline.RuntimeOverride); err != nil {
		return nil, err
	}

	// Layer 4: delegate override
	if err := apply(delegate.RuntimeOverride); err != nil {
		return nil, err
	}

	return env, nil
}

func callDelegate(ctx context.Context, delegate *metatypes.DelegateEntry, action Action,
	cniVersion string, prevResult cnitypes.Result, runtimeConfig map[string]any) (cnitypes.Result, error) {

	confBytes, pluginType, err := effectiveConf(delegate, cniVersion, prevResult, runtimeConfig)
	if err != nil {
		return nil, err
	}

	log.Tracef("Delegate %s STDIN: %s", delegate.Identifier(), string(confBytes))

	switch action {
	case ActionAdd:
		return invoke.DelegateAdd(ctx, pluginType, confBytes, nil)
	case ActionCheck:
		return nil, invoke.DelegateCheck(ctx, pluginType, confBytes, nil)
	case ActionDel:
		return nil, invoke.DelegateDel(ctx, pluginType, confBytes, nil)
	default:
		return nil, fmt.Errorf("unsupported action %s", action)
	}
}

func callDelegateDel(ctx context.Context, delegate *metatypes.DelegateEntry,
	cniVersion string, prevResult cnitypes.Result, runtimeConfig map[string]any) error {

	confBytes, pluginType, err := effectiveConf(delegate, cniVersion, prevResult, runtimeConfig)
	if err != nil {
		return err
	}
	return invoke.DelegateDel(ctx, pluginType, confBytes, nil)
}

func effectiveConf(delegate *metatypes.DelegateEntry, parentVersion string,
	prevResult cnitypes.Result, runtimeConfig map[string]any) ([]byte, string, error) {

	if delegate == nil {
		return nil, "", errors.New("delegate entry is nil")
	}

	var (
		payload       map[string]any
		err           error
		effectiveType = delegate.Type
	)

	if len(delegate.Conf) == 0 {
		if effectiveType == "" {
			return nil, "", errors.New("delegate type is required")
		}

		payload = map[string]any{
			"cniVersion": parentVersion,
			"type":       effectiveType,
		}
		if delegate.Name != "" {
			payload["name"] = delegate.Name
		}
	} else {
		if err = json.Unmarshal(delegate.Conf, &payload); err != nil {
			return nil, "", fmt.Errorf("delegate %s config decode: %w", delegate.Identifier(), err)
		}
		if payloadType, ok := payload["type"].(string); ok && payloadType != "" {
			effectiveType = payloadType
		}
		if effectiveType == "" {
			return nil, "", fmt.Errorf("delegate %s missing type", delegate.Identifier())
		}
		if _, ok := payload["type"]; !ok {
			payload["type"] = effectiveType
		}
		if _, ok := payload["cniVersion"]; !ok && parentVersion != "" {
			payload["cniVersion"] = parentVersion
		}
		if delegate.Name != "" {
			if _, ok := payload["name"]; !ok {
				payload["name"] = delegate.Name
			}
		}
	}

	if len(runtimeConfig) > 0 {
		if _, ok := payload["runtimeConfig"]; !ok {
			payload["runtimeConfig"] = runtimeConfig
		}
	}

	delete(payload, "prevResult")

	configVersion := ""
	if rawVersion, ok := payload["cniVersion"].(string); ok {
		configVersion = rawVersion
	}

	if prevResult != nil {
		if configVersion == "" {
			return nil, "", fmt.Errorf("delegate %s missing cniVersion for prevResult", delegate.Identifier())
		}
		converted, err := prevResult.GetAsVersion(configVersion)
		if err != nil {
			return nil, "", fmt.Errorf("delegate %s prevResult convert: %w", delegate.Identifier(), err)
		}
		payload["prevResult"] = converted
	}

	confBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, "", fmt.Errorf("delegate %s marshal: %w", delegate.Identifier(), err)
	}

	return confBytes, effectiveType, nil
}
