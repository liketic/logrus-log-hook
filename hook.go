package loghook

import (
	"encoding/json"
	"errors"
	"fmt"
	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/sirupsen/logrus"
	"os"
	"time"
)

const defaultBufferTimeout = 10 * time.Second
const defaultBatchSize = 1000
const defaultMaxAttempts = 10

var bufferSize = 32768
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
}

// Hook represents the asynchronous Logrus hook to Log Service.
type Hook struct {
	client       *sls.Client
	config       *Config
	bufferTimout time.Duration
	batchSize    uint32
	maxAttempts  int
	levels       []logrus.Level
	entryCh      chan *logrus.Entry
	flushCh      chan chan bool
	sendCh       chan *sls.LogGroup
}

// NewHook create a new async Log Service hook.
func NewHook(config *Config) (*Hook, error) {
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
	client := &sls.Client{
		Endpoint:        config.Endpoint,
		AccessKeyID:     config.AccessKeyId,
		AccessKeySecret: config.AccessKeySecret,
		UserAgent:       config.UserAgent,
	}
	hook := &Hook{
		client:       client,
		levels:       defaultLevels,
		config:       config,
		bufferTimout: defaultBufferTimeout,
		batchSize:    defaultBatchSize,
		maxAttempts:  defaultMaxAttempts,
		entryCh:      make(chan *logrus.Entry, bufferSize),
		flushCh:      make(chan chan bool),
		sendCh:       make(chan *sls.LogGroup),
	}
	go hook.collectEntries()
	go hook.emitEntries()
	return hook, nil
}

func (hook *Hook) collectEntries() {
	entries := make([]*logrus.Entry, 0)
	batchSize := int(hook.batchSize)
	bufferTimout := hook.bufferTimout
	timer := time.NewTimer(bufferTimout)
	var timerCh <-chan time.Time

	for {
		select {
		case entry := <-hook.entryCh:
			entries = append(entries, entry)
			if len(entries) >= batchSize {
				hook.send(entries)
				entries = make([]*logrus.Entry, 0)
				timerCh = nil
			} else if timerCh == nil && bufferTimout > 0 {
				timer.Reset(bufferTimout)
				timerCh = timer.C
			}

		case <-timerCh:
			if len(entries) > 0 {
				hook.send(entries)
				entries = make([]*logrus.Entry, 0)
			}
			timerCh = nil

		case done := <-hook.flushCh:
			if len(entries) > 0 {
				hook.send(entries)
				entries = make([]*logrus.Entry, 0)
			}
			timerCh = nil
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
	ts := uint32(entry.Time.Second())
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
			for i := 0; i < hook.maxAttempts; i++ {
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
	return hook.levels
}

// SetLevels sets logging level to fire this hook.
func (hook *Hook) SetLevels(levels []logrus.Level) {
	hook.levels = levels
}

// SetBufferTimeout sets max waiting time before flushing to Log Service.
func (hook *Hook) SetBufferTimeout(bufferTimout time.Duration) {
	hook.bufferTimout = bufferTimout
}

// SetBatchSize sets batch size.
func (hook *Hook) SetBatchSize(batchSize uint32) {
	hook.batchSize = batchSize
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
