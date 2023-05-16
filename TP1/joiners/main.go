package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	factory "tp1/joiners/factory"
)

const (
	logLevelEnv   = "LOG_LEVEL"
	joinerTypeEnv = "JOINER_TYPE"
)

// InitLogger Receives the log level to be set in logrus as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		return err
	}

	customFormatter := &log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   false,
	}
	log.SetFormatter(customFormatter)
	log.SetLevel(level)
	return nil
}

func main() {
	logLevel := os.Getenv(logLevelEnv)
	if logLevel == "" {
		logLevel = "DEBUG"
	}
	if err := InitLogger(logLevel); err != nil {
		log.Fatalf("%s", err)
		return
	}

	joinerType := os.Getenv(joinerTypeEnv)

	joiner, err := factory.NewJoiner(joinerType)
	if err != nil {
		log.Debugf("Error creating joiner: %s", err.Error())
		return
	}

	defer func(joiner factory.Joiner) {
		err := joiner.Kill()
		if err != nil {
			log.Error(getLogMessage(joiner, "error killing joiner", err))
		}
	}(joiner)

	err = joiner.DeclareQueues()
	if err != nil {
		log.Debug(getLogMessage(joiner, "error declaring queues", err))
		return
	}

	err = joiner.DeclareExchanges()
	if err != nil {
		log.Debug(getLogMessage(joiner, "error declaring exchanges", err))
		return
	}

	/* Next steps:
	1. Join Data
	2. Send Result to next stage
	3. Send EOF to EOF Manager
	*/

	err = joiner.JoinData()
	if err != nil {
		log.Debug(getLogMessage(joiner, "error joining data", err))
		return
	}
	log.Info(getLogMessage(joiner, "data joined successfully", nil))

	err = joiner.SendResult()
	if err != nil {
		log.Debug(getLogMessage(joiner, "error sending result", err))
		return
	}
	log.Info(getLogMessage(joiner, "data sent successfully", nil))

	err = joiner.SendEOF()
	if err != nil {
		log.Debug(getLogMessage(joiner, "error sending EOF", err))
		return
	}

	log.Debug(getLogMessage(joiner, "Finish main.go", nil))
}

func getLogMessage(joiner factory.Joiner, message string, err error) string {
	if err != nil {
		return fmt.Sprintf("[caller: main][joiner: %s][joinerID: %s][status: ERROR] %s: %s", joiner.GetType(), joiner.GetID(), message, err.Error())
	}
	return fmt.Sprintf("[caller: main][joiner: %s][joinerID: %s][status: OK] %s", joiner.GetType(), joiner.GetID(), message)
}
