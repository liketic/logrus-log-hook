package main

import (
	loghook "github.com/liketic/logrus-log-hook"
	"time"

	"github.com/Sirupsen/logrus"
)

func init() {
	// Create and add the hook.
	hook, err := loghook.NewHook(
		&loghook.Config{
			AccessKeyId:     "",
			AccessKeySecret: "",
			Endpoint:        "",
			Project:         "",
			Logstore:        "",
			//BatchSize:       1, // default is 1000
			//BatchInterval:   5, // default is 5 seconds
		})
	if err != nil {
		logrus.Fatalf("Error while creating the hook: %v", err)
	}
	// Add the hook to the standard logger.
	logrus.StandardLogger().Hooks.Add(hook)
}

func main() {
	logrus.WithFields(logrus.Fields{
		"foo":      "bar",
		"greeting": "Hello world!",
	}).Info("This is a log message")
	time.Sleep(10 * time.Second)
}
