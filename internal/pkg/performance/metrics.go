package performance

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var instance *MetricReporter
var once sync.Once

type MetricReporter struct {
	lock                  sync.Mutex
	running               bool
	registry              metrics.Registry
	logWriter             io.Writer
	reportPeriodically    bool
	reportIntervalSeconds int
	reportOnShutdown      bool
}

func ReporterInstance(writer io.Writer, reportPeriodically bool, reportIntervalSeconds int, reportOnShutdown bool) *MetricReporter {
	once.Do(func() {
		instance = newMetricReporter(writer, reportPeriodically, reportIntervalSeconds, reportOnShutdown)
	})
	return instance
}

func ReporterInstanceFromConfig(writer io.Writer, config *config.V3ioConfig) *MetricReporter {
	return ReporterInstance(writer,
		config.MetricsReporter.ReportPeriodically,
		config.MetricsReporter.RepotInterval,
		config.MetricsReporter.ReportOnShutdown)
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
		return nil, errors.Errorf("failed to create timer '%s'. Reason: metric reporter in not running", name)
	}
	return timer, nil
}

func (mr *MetricReporter) NewCounter(name string) (metrics.Counter, error) {
	var counter metrics.Counter
	if mr.running {
		counter = metrics.GetOrRegisterCounter(name, mr.registry)
	} else {
		return nil, errors.Errorf("failed to create counter '%s'. Reason: metric reporter in not running", name)
	}
	return counter, nil
}

func (mr *MetricReporter) NewMeter(name string) (metrics.Meter, error) {
	var meter metrics.Meter
	if mr.running {
		meter = metrics.GetOrRegisterMeter(name, mr.registry)
	} else {
		return nil, errors.Errorf("failed to create meter '%s'. Reason: metric reporter in not running", name)
	}
	return meter, nil
}

func (mr *MetricReporter) NewHistogram(name string, reservoirSize int) (metrics.Histogram, error) {
	var histogram metrics.Histogram
	if mr.running {
		sample := metrics.NewUniformSample(reservoirSize)
		histogram = metrics.GetOrRegisterHistogram(name, mr.registry, sample)
	} else {
		return nil, errors.Errorf("failed to create histogram '%s'. Reason: metric reporter in not running", name)
	}
	return histogram, nil
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
		mr.logWriter.Write([]byte(fmt.Sprintf("\n**************************\ncaught sig: %+v\n**************************\n", sig)))
		metrics.WriteOnce(mr.registry, mr.logWriter)
	}()
}

func newMetricReporter(outputWriter io.Writer, reportPeriodically bool, reportIntervalSeconds int, reportOnShutdown bool) *MetricReporter {
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
