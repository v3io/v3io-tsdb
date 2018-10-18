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

const (
	reservoirSize = 100
)

var instance *MetricReporter
var once sync.Once

const (
	STDOUT = "stdout"
	STDERR = "stderr"
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

func DefaultReporterInstance() (reporter *MetricReporter, err error) {
	cfg, err := config.GetOrDefaultConfig()

	if err != nil {
		// DO NOT return the error to prevent failures of unit tests
		fmt.Fprintf(os.Stderr, "unable to load configuration. Reason: %v\n"+
			"Will use default reporter configuration instead.", err)
		reporter = ReporterInstance(STDOUT, true, 60, true)
	} else {
		reporter = ReporterInstanceFromConfig(cfg)
	}

	return reporter, nil
}

func ReporterInstance(writeTo string, reportPeriodically bool, reportIntervalSeconds int, reportOnShutdown bool) *MetricReporter {
	once.Do(func() {
		var writer io.Writer
		switch writeTo {
		case STDOUT:
			writer = os.Stdout
		case STDERR:
			writer = os.Stderr
		default:
			writer = os.Stdout
		}

		instance = newMetricReporter(writer, reportPeriodically, reportIntervalSeconds, reportOnShutdown)
	})
	return instance
}

func ReporterInstanceFromConfig(config *config.V3ioConfig) *MetricReporter {
	return ReporterInstance(
		config.MetricsReporter.Output,
		config.MetricsReporter.ReportPeriodically,
		config.MetricsReporter.RepotInterval,
		config.MetricsReporter.ReportOnShutdown)
}

func (mr *MetricReporter) Start() error {
	mr.lock.Lock()
	defer mr.lock.Unlock()

	if mr.isEnabled() && !mr.running {
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
			time.Sleep(300 * time.Millisecond) // postpone performance report on shutdown to avoid mixing with other log messages
			metrics.WriteOnce(mr.registry, mr.logWriter)
		}
		mr.registry.UnregisterAll()
	} else {
		return errors.Errorf("can't stop metric reporter since it's not running.")
	}

	return nil
}

func (mr *MetricReporter) WithTimer(name string, body func()) {
	if mr.isRunning() {
		timer := metrics.GetOrRegisterTimer(name, mr.registry)
		timer.Time(body)
	} else {
		body()
	}
}

func (mr *MetricReporter) IncrementCounter(name string, count int64) {
	if mr.isRunning() {
		counter := metrics.GetOrRegisterCounter(name, mr.registry)
		counter.Inc(count)
	}
}

func (mr *MetricReporter) UpdateMeter(name string, count int64) {
	if mr.isRunning() {
		meter := metrics.GetOrRegisterMeter(name, mr.registry)
		meter.Mark(count)
	}
}

func (mr *MetricReporter) UpdateHistogram(name string, value int64) {
	if mr.isRunning() {
		histogram := metrics.GetOrRegisterHistogram(name, mr.registry, metrics.NewUniformSample(reservoirSize))
		histogram.Update(value)
	}
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

func (mr *MetricReporter) isEnabled() bool {
	return mr.reportOnShutdown || mr.reportPeriodically
}

func (mr *MetricReporter) isRunning() bool {
	mr.lock.Lock()
	defer mr.lock.Unlock()

	return mr.running
}
