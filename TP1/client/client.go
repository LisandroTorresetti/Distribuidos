package client

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
)

const (
	fileFormat   = "csv"
	tripsFile    = "trips"
	weatherFile  = "weather"
	stationsFile = "stations"
)

var (
	cities       = []string{"montreal", "toronto", "weather"}
	errorMessage = "error sending %s data from %s: %s"
)

type ClientConfig struct {
	BatchSize     int    `yaml:"batch_size"`
	FinMessage    string `yaml:"fin_message"`
	ServerACK     string `yaml:"server_ack"`
	ServerAddress string `yaml:"server_address"`
	PacketLimit   int    `yaml:"packet_limit"`
	CSVDelimiter  string `yaml:"csv_delimiter"`
	DataDelimiter string `yaml:"data_delimiter"`
}

type Client struct {
	config ClientConfig
	socket *Socket
}

func NewClient(clientConfig ClientConfig) *Client {
	return &Client{
		config: clientConfig,
	}
}

// OpenConnection creates a TCP connection with the server
func (c *Client) OpenConnection() error {
	socket := NewSocket(
		SocketConfig{
			ServerAddress: c.config.ServerAddress,
			ServerACK:     c.config.ServerACK,
			PacketLimit:   c.config.PacketLimit,
		},
	)

	err := socket.OpenConnection()
	if err != nil {
		return err
	}
	c.socket = socket
	return nil
}

// CloseConnection close the TCP connection with the server
func (c *Client) CloseConnection() error {
	err := c.socket.CloseConnection()
	if err != nil {
		log.Errorf("error closing connection: %s", err.Error())
		return err
	}
	return nil
}

func (c *Client) SendWeatherData() error {
	for _, city := range cities {
		weatherFilepath := getFilePath(city, weatherFile)
		err := c.sendDataFromFile(weatherFilepath, city, weatherFile)
		if err != nil {
			log.Error(fmt.Sprintf(errorMessage, weatherFile, city, err.Error()))
			return err
		}
	}

	err := c.sendFinMessage(weatherFile)
	if err != nil {
		log.Errorf("[method:SendWeatherData]error sending FIN message: %s", err.Error())
		return err
	}

	return nil
}

func (c *Client) SendStationsData() error {
	for _, city := range cities {
		stationsFilepath := getFilePath(city, stationsFile)
		err := c.sendDataFromFile(stationsFilepath, city, stationsFilepath)
		if err != nil {
			log.Error(fmt.Sprintf(errorMessage, stationsFile, city, err.Error()))
			return err
		}
	}

	err := c.sendFinMessage(stationsFile)
	if err != nil {
		log.Errorf("[method:SendStationsData]error sending FIN message: %s", err.Error())
		return err
	}

	return nil
}

func (c *Client) SendTripsData() error {
	for _, city := range cities {
		tripsFilepath := getFilePath(city, tripsFile)
		err := c.sendDataFromFile(tripsFilepath, city, tripsFilepath)
		if err != nil {
			log.Error(fmt.Sprintf(errorMessage, tripsFile, city, err.Error()))
			return err
		}
	}

	err := c.sendFinMessage(tripsFile)
	if err != nil {
		log.Errorf("[method:SendTripsData]error sending FIN message: %s", err.Error())
		return err
	}

	return nil
}

func (c *Client) sendDataFromFile(filepath string, city string, data string) error {
	dataFile, err := os.Open(filepath)
	if err != nil {
		log.Debugf("[city: %s][data: %s] error opening %s: %s", city, data, filepath, err.Error())
	}

	defer func(dataFile *os.File) {
		err := dataFile.Close()
		if err != nil {
			log.Errorf("error closing %s: %s", filepath, err.Error())
		}
	}(dataFile)

	fileScanner := bufio.NewScanner(dataFile)
	fileScanner.Split(bufio.ScanLines)
	_ = fileScanner.Scan() // Dismiss first line of the csv

	dataCounter := 0
	batchesSent := 0
	var dataToSend []string
	for fileScanner.Scan() {
		if dataCounter == c.config.BatchSize {
			batchesSent += 1
			log.Debugf("[city: %s][data: %s] Sending batch number %v", city, data, batchesSent)
			err = c.socket.SendBatch(dataToSend, c.config.DataDelimiter)
			if err != nil {
				log.Errorf("[city: %s][data: %s] error sending batch number %v: %s", city, data, batchesSent, err.Error())
				return err
			}
			dataCounter = 0
			dataToSend = []string{}
		}
		line := fileScanner.Text()
		if len(line) < 2 {
			// sanity check
			break
		}
		line = city + c.config.CSVDelimiter + line //prepend city to data
		dataToSend = append(dataToSend, line)
		dataCounter += 1
	}

	if len(dataToSend) != 0 {
		log.Debugf("[city: %s][data: %s] Sending batch number %v", city, data, batchesSent+1)
		err = c.socket.SendBatch(dataToSend, c.config.DataDelimiter)
		if err != nil {
			log.Errorf("[city: %s][data: %s] error sending batch number %v: %s", city, data, batchesSent, err.Error())
			return err
		}
	}

	log.Debugf("[city: %s][data: %s]All data was sent!", city, data)
	return nil
}

// sendFinMessage sends a message to the server indicating that all the data from 'dataType' file was sent.
// + dataType possible values: weather, stations, trips
func (c *Client) sendFinMessage(dataType string) error {
	finMessage := dataType + "-" + c.config.FinMessage
	log.Debugf("[data sent: %s] sending FIN MESSAGE %s", dataType, finMessage)
	err := c.socket.SendBatch([]string{finMessage}, "")
	if err != nil {
		log.Errorf("[data sent: %s] error sending fin message: %w", err)
		return err
	}

	log.Debugf("[data sent: %s] waiting for server response to FIN MESSAGE %s", dataType, finMessage)
	err = c.socket.ListenResponse(finMessage)
	if err != nil {
		log.Errorf("[data sent: %s] error waiting for server response to fin message: %w", err)
		return err
	}

	return nil
}

// getFilePath returns the path to the .csv file.
// + City possible values: montreal, toronto or washington
// + Filename possible values: weather, stations, trips
func getFilePath(city string, filename string) string {
	return fmt.Sprintf("/datasets/%s/%s.%s", city, filename, fileFormat)
}
