package socket

import (
	log "github.com/sirupsen/logrus"
	"net"
	"tp1/utils"
)

type SocketConfig struct {
	Protocol      string
	ServerAddress string
	ServerACK     string
	PacketLimit   int
}

type Socket struct {
	config     SocketConfig
	connection net.Conn
	listener   net.Listener
}

// NewSocket returns a socket with the corresponding configuration set.
// OBS: none connection opens here.
func NewSocket(socketConfig SocketConfig) *Socket {
	return &Socket{
		config: socketConfig,
	}
}

func (s *Socket) OpenConnection() error {
	connection, err := net.Dial(s.config.Protocol, s.config.ServerAddress)
	if err != nil {
		log.Fatalf(
			"action: connect | result: fail | error: %v",
			err,
		)
	}
	s.connection = connection
	return nil
}

func (s *Socket) SetConnection(connection net.Conn) {
	s.connection = connection
}

func (s *Socket) CloseConnection() error {
	if s.connection != nil {
		return s.connection.Close()
	}
	log.Debug("There is none connection to close")
	return nil
}

func (s *Socket) StartListener() error {
	listener, err := net.Listen(s.config.Protocol, s.config.ServerAddress)
	if err != nil {
		log.Fatalf(
			"action: get listener | result: fail | error: %v",
			err,
		)
	}
	s.listener = listener
	return nil
}

func (s *Socket) AcceptNewConnections() (net.Conn, error) {
	connection, err := s.listener.Accept()
	if err != nil {
		log.Fatalf(
			"action: accept connection| result: fail | error: %v",
			err,
		)
		return nil, err
	}
	return connection, nil
}

func (s *Socket) CloseListener() error {
	return s.listener.Close()
}

func (s *Socket) Send(data string) error {
	dataAsBytes := []byte(data)
	messageLength := len(dataAsBytes)

	shortWriteAvoidance := 0
	amountOfBytesSent := 0

	for amountOfBytesSent < messageLength {
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

	return nil
}

func (s *Socket) Listen(targetEndMessage string, finMessages []string) ([]byte, error) {
	message := make([]byte, 0) // Will contain the message

	for {
		buffer := make([]byte, s.config.PacketLimit)
		bytesRead, err := s.connection.Read(buffer)
		if err != nil {
			log.Errorf("unexpected error while trying to get message: %s", err.Error())
			return nil, err
		}

		message = append(message, buffer[:bytesRead]...)
		size := len(message)

		if size >= 4 && string(message[size-4:size]) == targetEndMessage {
			log.Debugf("Got message correctly!")
			break
		}

		if size >= 4 && utils.ContainsString(string(message), finMessages) {
			log.Debugf("Got FIN message correctly!")
			break
		}
	}

	return message, nil
}
