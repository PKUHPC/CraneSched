package rapl

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	logrus "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/monitor/pkg/types"
)

var log = logrus.WithField("component", "RAPL")

const (
	RAPLBasePath = "/sys/class/powercap"
)

type RAPLDomain struct {
	Name           string
	Path           string
	Enabled        bool
	MaxEnergyRange float64
	LastEnergy     float64
	LastReadTime   time.Time
}

type RAPLReader struct {
	domains map[string]*RAPLDomain
}

func NewRAPLReader() *RAPLReader {
	reader := &RAPLReader{
		domains: make(map[string]*RAPLDomain),
	}

	if err := reader.discoverDomains(); err != nil {
		log.Warnf("failed to discover RAPL domains: %v", err)
	}

	return reader
}

func (r *RAPLReader) discoverDomains() error {
	entries, err := os.ReadDir(RAPLBasePath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "intel-rapl:") {
			domain := &RAPLDomain{
				Name: entry.Name(),
				Path: filepath.Join(RAPLBasePath, entry.Name()),
			}

			// read the maximum energy range, for handling counter overflow
			maxEnergyRange, err := r.readMaxEnergyRange(domain.Path)
			if err == nil {
				domain.MaxEnergyRange = maxEnergyRange
			}

			domain.Enabled = true
			r.domains[domain.Name] = domain
		}
	}

	log.Info("Discovered RAPL domains:")
	for name, domain := range r.domains {
		log.Infof("  %s: %s", name, domain.Path)
	}

	return nil
}

func (r *RAPLReader) GetMetrics() *types.RAPLMetrics {
	metrics := &types.RAPLMetrics{}

	for name, domain := range r.domains {
		if !domain.Enabled {
			continue
		}

		energy, err := r.readDomainEnergy(domain)
		if err != nil {
			log.Warnf("Error reading energy for domain %s: %v", name, err)
			continue
		}

		parts := strings.Split(name, ":")
		if len(parts) < 2 {
			log.Warnf("Invalid domain name format: %s", name)
			continue
		}

		if len(parts) == 2 {
			metrics.Package += energy
		} else if len(parts) == 3 {
			switch parts[2] {
			case "0":
				metrics.Core += energy
			case "1":
				metrics.Uncore += energy
			case "2":
				metrics.DRAM += energy
			case "3":
				metrics.GT += energy
			default:
				log.Infof("Unknown subdomain: %s", parts[2])
			}
		}
	}

	return metrics
}

func (r *RAPLReader) readDomainEnergy(domain *RAPLDomain) (float64, error) {
	currentTime := time.Now()

	// read the current cumulative value
	data, err := os.ReadFile(filepath.Join(domain.Path, "energy_uj"))
	if err != nil {
		return 0, err
	}
	currentEnergy, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
	if err != nil {
		return 0, err
	}
	currentEnergy = currentEnergy / 1e6 // convert to joule

	if !domain.LastReadTime.IsZero() {
		energyDiff := currentEnergy - domain.LastEnergy
		// handle counter overflow
		if energyDiff < 0 {
			energyDiff += domain.MaxEnergyRange
		}
		domain.LastEnergy = currentEnergy
		domain.LastReadTime = currentTime
		return energyDiff, nil
	}

	domain.LastEnergy = currentEnergy
	domain.LastReadTime = currentTime
	return 0, nil
}

func (r *RAPLReader) readMaxEnergyRange(path string) (float64, error) {
	data, err := os.ReadFile(filepath.Join(path, "max_energy_range_uj"))
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
	return value / 1e6, err // convert to joule
}

func (r *RAPLReader) LogMetrics(metrics *types.RAPLMetrics) {
	log.Debugf("RAPL Metrics:")
	log.Debugf("Package: %.2f J, Core: %.2f J, Uncore: %.2f J, DRAM: %.2f J, GT: %.2f J",
		metrics.Package, metrics.Core, metrics.Uncore, metrics.DRAM, metrics.GT)
}

func (r *RAPLReader) Close() error {
	return nil
}
