package utils

import (
	"sync"

	nested "github.com/antonfisher/nested-logrus-formatter"
	log "github.com/sirupsen/logrus"
)

var loggerSetupOnce sync.Once

// ensureLoggerDefaults configures logrus with sane defaults once.
func ensureLoggerDefaults() {
	loggerSetupOnce.Do(func() {
		log.SetFormatter(&nested.Formatter{})
		log.SetLevel(log.InfoLevel)
		log.SetReportCaller(false)
	})
}

// InitLogger sets the desired log level while keeping other defaults intact.
func InitLogger(level string) {
	ensureLoggerDefaults()

	switch level {
	case "trace":
		log.SetLevel(log.TraceLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	default:
		log.Warnf("Invalid log level %s, using info level", level)
		log.SetLevel(log.InfoLevel)
	}
}
