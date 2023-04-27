package client

import (
	log "github.com/sirupsen/logrus"
	"net"
	"strings"
)

type SocketConfig struct {
	ServerAddress string
	ServerACK     string
	PacketLimit   int
}

type Socket struct {
	config     SocketConfig
	connection net.Conn
}

// NewSocket returns a socket with the corresponding configuration set.
// OBS: none connection opens here.
func NewSocket(socketConfig SocketConfig) *Socket {
	return &Socket{
		config: socketConfig,
	}
}

func (s *Socket) OpenConnection() error {
	connection, err := net.Dial("tcp", s.config.ServerAddress)
	if err != nil {
		log.Fatalf(
			"action: connect | result: fail | error: %v",
			err,
		)
	}
	s.connection = connection
	return nil
}

func (s *Socket) CloseConnection() error {
	return s.connection.Close()
}

func (s *Socket) SendBatch(batch []string, delimiter string) error {
	// Join data with |, e.g data1|data2|data3|...
	dataJoined := strings.Join(batch, delimiter)
	dataAsBytes := []byte(dataJoined)
	messageLength := len(dataAsBytes)

	shortWriteAvoidance := 0
	amountOfBytesSent := 0

	for amountOfBytesSent != messageLength {
		lowerLimit := amountOfBytesSent - shortWriteAvoidance
		upperLimit := lowerLimit + s.config.PacketLimit

		if upperLimit > messageLength {
			upperLimit = messageLength
		}

		bytesToSend := dataAsBytes[lowerLimit:upperLimit]
		bytesSent, err := s.connection.Write(bytesToSend)
		if err != nil {
			return err
		}
		amountOfBytesSent += bytesSent
		shortWriteAvoidance = len(bytesToSend) - bytesSent
	}

	debugCity := strings.SplitN(batch[0], ",", 2)[0]
	log.Debugf("[city: %s] data sent, waiting for server ACK", debugCity)

	err := s.ListenResponse(s.config.ServerACK)
	if err != nil {
		log.Debugf("[city: %s] error while wainting for server ACK: %s", debugCity, err.Error())
	}

	return nil
}

func (s *Socket) ListenResponse(expectedServerACK string) error {
	response := make([]byte, 0) // Will contain the response from the server

	for {
		buffer := make([]byte, s.config.PacketLimit)
		bytesRead, err := s.connection.Read(buffer)
		if err != nil {
			log.Errorf("unexpected error while trying to get server response: %s", err.Error())
			return err
		}

		response = append(response, buffer[:bytesRead]...)
		size := len(response)

		if size >= 4 && string(response[size-4:size]) == expectedServerACK {
			log.Debugf("Got server ACK!")
			break
		}
	}

	return nil
}
