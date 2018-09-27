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
	"sync/atomic"
	"syscall"
	"time"
)

var instance *MetricReporter
var once sync.Once

const (
	STDOUT = "stdout"
	STDERR = "stderr"

	notRuningState = 0
	runningState   = 1
)

type MetricReporter struct {
	state                 int64
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

	if !atomic.CompareAndSwapInt64(&mr.state, notRuningState, runningState) {
		return errors.Errorf("metric reporter is already running.")
	}

	return nil
}

func (mr *MetricReporter) Stop() error {
	if atomic.CompareAndSwapInt64(&mr.state, runningState, notRuningState) {
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

func (mr *MetricReporter) GetTimer(name string) (metrics.Timer, error) {
	if mr.isRunning() {
		return metrics.GetOrRegisterTimer(name, mr.registry), nil
	} else {
		return nil, errors.Errorf("failed to create timer '%s'. Reason: metric reporter in not running", name)
	}
}

func (mr *MetricReporter) GetCounter(name string) (metrics.Counter, error) {
	if mr.isRunning() {
		return metrics.GetOrRegisterCounter(name, mr.registry), nil
	} else {
		return nil, errors.Errorf("failed to create counter '%s'. Reason: metric reporter in not running", name)
	}
}

func (mr *MetricReporter) GetMeter(name string) (metrics.Meter, error) {
	if mr.isRunning() {
		return metrics.GetOrRegisterMeter(name, mr.registry), nil
	} else {
		return nil, errors.Errorf("failed to create meter '%s'. Reason: metric reporter in not running", name)
	}
}

func (mr *MetricReporter) GetHistogram(name string, reservoirSize int) (metrics.Histogram, error) {
	if mr.isRunning() {
		sample := metrics.NewUniformSample(reservoirSize)
		return metrics.GetOrRegisterHistogram(name, mr.registry, sample), nil
	} else {
		return nil, errors.Errorf("failed to create histogram '%s'. Reason: metric reporter in not running", name)
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
		state:                 runningState,
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

func (mr *MetricReporter) isRunning() bool {
	return atomic.LoadInt64(&mr.state) == runningState
}
