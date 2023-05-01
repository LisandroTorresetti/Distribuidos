package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"tp1/utils"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

func LoadServerConfig() (ServerConfig, error) {
	configFile, err := utils.GetConfigFile("./config/config.yaml")
	if err != nil {
		return ServerConfig{}, err
	}

	var serverConfig ServerConfig
	err = yaml.Unmarshal(configFile, &serverConfig)
	if err != nil {
		return ServerConfig{}, fmt.Errorf("error parsing server config file: %s", err)
	}

	return serverConfig, nil
}

// InitLogger Receives the log level to be set in logrus as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return err
	}

	customFormatter := &logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   false,
	}
	logrus.SetFormatter(customFormatter)
	logrus.SetLevel(level)
	return nil
}

func main() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "DEBUG"
	}
	if err := InitLogger(logLevel); err != nil {
		log.Fatalf("%s", err)
		return
	}

	serverConfig, err := LoadServerConfig()
	if err != nil {
		log.Errorf("Error loading server data: %s", err.Error())
		return
	}
	server := NewServer(serverConfig)
	err = server.DeclareQueues()
	if err != nil {
		log.Errorf("Error declaring RabbitMQ queues: %s", err.Error())
		return
	}

	err = server.Run()
	if err != nil {
		log.Error("Error running server: %s", err.Error())
		return
	}

	log.Debug("[server] Finish main.go")
}
