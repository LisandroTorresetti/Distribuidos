package main

import (
	log "github.com/sirupsen/logrus"
	"tp1/socket"
	"tp1/utils"
)

type ServerConfig struct {
	Port                   string   `yaml:"port"`
	IP                     string   `yaml:"ip"`
	AckMessage             string   `yaml:"ack_message"`
	EndBatchMarker         string   `yaml:"end_batch_marker"`
	FinMessages            []string `yaml:"fin_messages"`
	DataDelimiter          string   `yaml:"data_delimiter"`
	MaxAmountOfConnections int      `yaml:"max_amount_of_connections"`
	Protocol               string   `yaml:"protocol"`
	PacketLimit            int      `yaml:"packet_limit"`
}

type Server struct {
	config       ServerConfig
	serverSocket *socket.Socket
	//messageHandler *MessageHandler
}

func NewServer(config ServerConfig) *Server {
	serverSocket := socket.NewSocket(
		socket.SocketConfig{
			ServerAddress: config.IP + ":" + config.Port,
			ServerACK:     config.AckMessage,
			Protocol:      config.Protocol,
			PacketLimit:   config.PacketLimit,
		},
	)

	return &Server{
		serverSocket: serverSocket,
		config:       config,
	}
}

func (s *Server) Run() error {
	err := s.serverSocket.StartListener()
	if err != nil {
		return err
	}

	defer func(serverSocket *socket.Socket) {
		err := serverSocket.CloseListener()
		if err != nil {
			log.Errorf("[server]error closing listener: %s", err.Error())
		}

		err = serverSocket.CloseConnection()
		if err != nil {
			log.Errorf("[server]error closing connection: %s", err.Error())
		}
	}(s.serverSocket)

	for {
		log.Debug("[server] waiting for new connections")
		// Accept new connection
		conn, err := s.serverSocket.AcceptNewConnections()
		if err != nil {
			log.Errorf("[server] error accepting a new connection: %s", err.Error())
			return err
		}
		log.Debug("[server] connection accepted!")

		newSocket := *s.serverSocket
		newSocket.SetConnection(conn)

		go func(s *Server, serverSocket *socket.Socket) {
			err := processData(s, serverSocket)
			if err != nil {
				log.Errorf("error processing data: %s", err.Error())
			}
		}(s, &newSocket)
	}
}

func processData(s *Server, socket *socket.Socket) error {
	for {
		// Wait till receive the entire message data1|data2|...|dataN|PING or x-PONG
		messageBytes, err := socket.Listen(s.config.EndBatchMarker, s.config.FinMessages)

		if err != nil {
			log.Errorf("[server] error receiving message from a client: %s", err.Error())
			return err
		}
		message := string(messageBytes)
		log.Debug("received message: %s", message)

		// If we received a FIN message, the response is the same FIN message
		if utils.ContainsString(message, s.config.FinMessages) {
			err = socket.Send(message)
			if err != nil {
				log.Errorf("error sending ACK to FIN message %s: %s", message, err.Error())
				return err
			}
			log.Debug("Fin message ACK sended correctly")
			break
		}

		err = socket.Send(s.config.AckMessage)
		if err != nil {
			log.Errorf("[server] error sending ACK to client: %s", err.Error())
			return err
		}
	}
	return nil
}