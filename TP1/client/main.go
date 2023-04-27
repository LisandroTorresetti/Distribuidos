package client

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"tp1/utils"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

func LoadClientConfig() (ClientConfig, error) {
	configFile, err := utils.GetConfigFile("client/config/config.yaml")
	if err != nil {
		return ClientConfig{}, err
	}

	var clientConfig ClientConfig
	err = yaml.Unmarshal(configFile, &clientConfig)
	if err != nil {
		return ClientConfig{}, fmt.Errorf("error parsing client config file: %s", err)
	}

	return clientConfig, nil
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
	if err := InitLogger("info"); err != nil {
		log.Fatalf("%s", err)
	}

	clientConfig, err := LoadClientConfig()
	client := NewClient(clientConfig)

	err = client.OpenConnection()
	if err != nil {
		logrus.Errorf(err.Error())
	}

	err = client.SendWeatherData()

	if err != nil {
		logrus.Errorf(err.Error())
	}

	err = client.CloseConnection()
	if err != nil {
		logrus.Errorf(err.Error())
	}
	log.Debug("Finish main.go")
}
