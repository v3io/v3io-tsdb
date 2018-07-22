package performance

import (
	"github.com/rcrowley/go-metrics"
	"io"
	"os"
	"os/signal"
	"syscall"
	"fmt"
	"sync"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"time"
	"log"
)

type MetricReporter struct {
	lock                  sync.Mutex
	running               bool
	registry              metrics.Registry
	logWriter             io.Writer
	reportPeriodically    bool
	reportIntervalSeconds int
	reportOnShutdown      bool
}

func (mr *MetricReporter) Start() error {
	mr.lock.Lock()
	defer mr.lock.Unlock()

	if !mr.running {
		mr.running = true
	} else {
		return errors.Errorf("metric reporter is already running.")
	}

	return nil
}

func (mr *MetricReporter) Stop() error {
	mr.lock.Lock()
	defer mr.lock.Unlock()

	if mr.running {
		mr.running = false
		if mr.reportOnShutdown {
			metrics.WriteOnce(mr.registry, mr.logWriter)
		}
	} else {
		return errors.Errorf("can't stop metric reporter since it's not running.")
	}

	return nil
}

func (mr *MetricReporter) NewTimer(name string) (metrics.Timer, error) {
	var timer metrics.Timer
	if mr.running {
		timer = metrics.GetOrRegisterTimer(name, mr.registry)
	} else {
		return nil, errors.Errorf("metric reporter in not running")
	}
	return timer, nil
}

func NewMetricReporterFromConfiguration(writer io.Writer, config *config.V3ioConfig) *MetricReporter {
	return NewMetricReporter(
		writer,
		config.MetricsReporter.ReportPeriodically,
		config.MetricsReporter.RepotInterval,
		config.MetricsReporter.ReportOnShutdown)
}

// Listen to the SIGINT and SIGTERM
// SIGINT will listen to CTRL-C.
// SIGTERM will be caught if kill command executed.
func (mr *MetricReporter) registerShutdownHook() {
	var gracefulStop = make(chan os.Signal)
	// Register for specific signals
	signal.Notify(gracefulStop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-gracefulStop
		mr.logWriter.Write([]byte(fmt.Sprintf("caught sig: %+v", sig)))
		metrics.WriteOnce(mr.registry, mr.logWriter)
	}()
}

func NewMetricReporter(outputWriter io.Writer, reportPeriodically bool, reportIntervalSeconds int, reportOnShutdown bool) *MetricReporter {
	var writer io.Writer

	if outputWriter != nil {
		writer = outputWriter
	} else {
		writer = os.Stderr
	}

	reporter := MetricReporter{
		registry:              metrics.NewPrefixedRegistry("v3io-tsdb -> "),
		logWriter:             writer,
		running:               true,
		reportPeriodically:    reportPeriodically,
		reportIntervalSeconds: reportIntervalSeconds,
		reportOnShutdown:      reportOnShutdown,
	}

	if reportPeriodically && reportIntervalSeconds > 0 {
		// Log periodically
		go metrics.Log(reporter.registry,
			time.Duration(reportIntervalSeconds)*time.Second,
			log.New(reporter.logWriter, "metrics: ", log.Lmicroseconds))
	}

	if reportOnShutdown {
		reporter.registerShutdownHook()
	}

	return &reporter
}
