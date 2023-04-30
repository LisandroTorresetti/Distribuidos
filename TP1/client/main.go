package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"sync"
	"tp1/utils"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

var dataTypes = []string{"weather", "stations", "trips"}

//[]string{"weather", "stations", "trips"}

func LoadClientConfig() (ClientConfig, error) {
	configFile, err := utils.GetConfigFile("./config/config.yaml")
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
	if err != nil {
		logrus.Errorf(err.Error())
		return
	}

	var wg sync.WaitGroup
	for _, data := range dataTypes {
		wg.Add(1)
		client := NewClient(clientConfig)

		go func(data string) {
			defer wg.Done()
			err := sendData(client, data)
			if err != nil {
				fmt.Printf("error sendind %s data from cliente: %s", data, err.Error())
			}
		}(data)
	}

	log.Debug("[client] Waiting for threads")
	wg.Wait()
	log.Debug("[client] Finish main.go")
}

func sendData(client *Client, data string) error {
	err := client.OpenConnection()
	if err != nil {
		logrus.Errorf(err.Error())
		return err
	}

	if data == "weather" {
		err = client.SendWeatherData()
	}

	if data == "trips" {
		err = client.SendTripsData()
	}

	if data == "stations" {
		err = client.SendStationsData()
	}

	if err != nil {
		logrus.Errorf(err.Error())
		return err
	}

	err = client.CloseConnection()
	if err != nil {
		logrus.Errorf(err.Error())
		return err
	}

	return nil
}
