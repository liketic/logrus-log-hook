package loghook

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Sirupsen/logrus"
	sls "github.com/aliyun/aliyun-log-go-sdk"
	"os"
	"time"
)

const (
	defaultBatchInterval = 5 * time.Second
	defaultBatchSize     = 1000
	defaultMaxAttempts   = 10
	defaultBufferSize    = 32768
)

var defaultLevels = []logrus.Level{
	logrus.PanicLevel,
	logrus.FatalLevel,
	logrus.ErrorLevel,
	logrus.WarnLevel,
	logrus.InfoLevel,
}

var logger = logrus.New()

func init() {
	logger.Formatter = &logrus.TextFormatter{
		DisableTimestamp: true,
		DisableSorting:   true,
		QuoteEmptyFields: true,
	}
	logger.SetLevel(logrus.ErrorLevel)
	logger.Out = os.Stderr
}

// Config represents the configuration for creating Log Service client.
type Config struct {
	AccessKeyId     string
	AccessKeySecret string
	Endpoint        string
	Project         string
	Logstore        string
	UserAgent       string
	Source          string
	Tags            map[string]string
	Levels          []logrus.Level
	BatchSize       uint32
	BatchInterval   time.Duration
	BufferSize      int
	MaxAttempts     int
}

func (config *Config) setDefaults() {
	if config.BatchSize <= 0 {
		config.BatchSize = defaultBatchSize
	}
	if config.BatchInterval <= 0 {
		config.BatchInterval = defaultBatchInterval
	}
	if config.BufferSize <= 0 {
		config.BufferSize = defaultBufferSize
	}
	if len(config.Levels) == 0 {
		config.Levels = defaultLevels
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = defaultMaxAttempts
	}
}

// Hook represents the asynchronous Logrus hook to Log Service.
type Hook struct {
	client  *sls.Client
	config  *Config
	entryCh chan *logrus.Entry
	flushCh chan chan bool
	sendCh  chan *sls.LogGroup
	ticker  *time.Ticker
}

// NewHook create a new async Log Service hook.
func NewHook(config *Config) (*Hook, error) {
	if config == nil {
		return nil, errors.New("config is nil")
	}
	if config.Endpoint == "" {
		return nil, errors.New("endpoint must be specified")
	}
	if config.AccessKeyId == "" {
		return nil, errors.New("AccessKeyId must be specified")
	}
	if config.AccessKeySecret == "" {
		return nil, errors.New("AccessKeySecret must be specified")
	}
	if config.Project == "" {
		return nil, errors.New("project must be specified")
	}
	if config.Logstore == "" {
		return nil, errors.New("logstore must be specified")
	}
	config.setDefaults()
	client := &sls.Client{
		Endpoint:        config.Endpoint,
		AccessKeyID:     config.AccessKeyId,
		AccessKeySecret: config.AccessKeySecret,
		UserAgent:       config.UserAgent,
	}
	hook := &Hook{
		client:  client,
		config:  config,
		entryCh: make(chan *logrus.Entry, config.BufferSize),
		flushCh: make(chan chan bool),
		sendCh:  make(chan *sls.LogGroup),
		ticker:  time.NewTicker(config.BatchInterval),
	}
	go hook.collectEntries()
	go hook.emitEntries()
	return hook, nil
}

func (hook *Hook) collectEntries() {
	entries := make([]*logrus.Entry, 0)
	config := hook.config
	batchSize := int(config.BatchSize)

	for {
		select {
		case entry := <-hook.entryCh:
			entries = append(entries, entry)
			if len(entries) >= batchSize {
				hook.send(entries)
				entries = make([]*logrus.Entry, 0)
			}
		case <-hook.ticker.C:
			if len(entries) > 0 {
				hook.send(entries)
				entries = make([]*logrus.Entry, 0)
			}
		case done := <-hook.flushCh:
			if len(entries) > 0 {
				hook.send(entries)
				entries = make([]*logrus.Entry, 0)
			}
			done <- true
		}
	}
}

func (hook *Hook) send(entries []*logrus.Entry) {
	config := hook.config
	logGroup := &sls.LogGroup{
		Source: &config.Source,
	}
	for k, v := range config.Tags {
		logGroup.LogTags = append(logGroup.LogTags, &sls.LogTag{
			Key:   &k,
			Value: &v,
		})
	}
	for _, entry := range entries {
		logGroup.Logs = append(logGroup.Logs, convertEntryToLog(entry))
	}
	hook.sendCh <- logGroup
}

func convertEntryToLog(entry *logrus.Entry) *sls.Log {
	ts := uint32(entry.Time.Unix())
	log := &sls.Log{
		Time: &ts,
	}
	for k, v := range entry.Data {
		v = formatData(v) // use default formatter
		vStr := fmt.Sprintf("%v", v)
		log.Contents = append(log.Contents, &sls.LogContent{
			Key:   &k,
			Value: &vStr,
		})
	}
	return log
}

// formatData returns value as a suitable format.
func formatData(value interface{}) (formatted interface{}) {
	switch value := value.(type) {
	case json.Marshaler:
		return value
	case error:
		return value.Error()
	case fmt.Stringer:
		return value.String()
	default:
		return value
	}
}

func (hook *Hook) emitEntries() {
	config := hook.config
	for {
		select {
		case logGroup := <-hook.sendCh:
			var err error
			for i := 0; i < config.MaxAttempts; i++ {
				err = hook.client.PutLogs(config.Project, config.Logstore, logGroup)
				if err == nil {
					break
				}
			}
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error": err.Error(),
				}).Warn("Error while sending logs to Log service")
			}
		}
	}
}

// Levels returns logging level to fire this hook.
func (hook *Hook) Levels() []logrus.Level {
	return hook.config.Levels
}

func (hook *Hook) Fire(entry *logrus.Entry) error {
	hook.entryCh <- entry
	return nil
}

func (hook *Hook) Flush() {
	done := make(chan bool)
	hook.flushCh <- done
	<-done
}
