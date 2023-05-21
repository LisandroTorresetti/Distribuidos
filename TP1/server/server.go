package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"tp1/communication"
	"tp1/server/handler"
	"tp1/socket"
)

type ServerConfig struct {
	Port                   string                                  `yaml:"port"`
	IP                     string                                  `yaml:"ip"`
	AckMessage             string                                  `yaml:"ack_message"`
	EndBatchMarker         string                                  `yaml:"end_batch_marker"`
	FinMessages            []string                                `yaml:"fin_messages"`
	DataDelimiter          string                                  `yaml:"data_delimiter"`
	MaxAmountOfConnections int                                     `yaml:"max_amount_of_connections"`
	Protocol               string                                  `yaml:"protocol"`
	PacketLimit            int                                     `yaml:"packet_limit"`
	RabbitMQConfig         map[string]communication.RabbitMQConfig `yaml:"rabbit_mq"`
}

type Server struct {
	config       ServerConfig
	serverSocket *socket.Socket
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

func (s *Server) DeclareExchanges() error {
	rabbitMQ, err := communication.NewRabbitMQ()
	if err != nil {
		log.Errorf("[server] rabbitMQ initialization error: %s", err.Error())
		return err
	}
	var exchangesDeclarations []communication.ExchangeDeclarationConfig
	for _, value := range s.config.RabbitMQConfig {
		exchangesDeclarations = append(exchangesDeclarations, value.ExchangeDeclarationConfig)
	}

	err = rabbitMQ.DeclareExchanges(exchangesDeclarations)
	if err != nil {
		return fmt.Errorf("[server] error declaring exchanges: %w", err)
	}

	log.Debug("[server] All exchange were declared correctly")

	return nil

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
		log.Info("[server] waiting for new connections")
		// Accept new connection
		conn, err := s.serverSocket.AcceptNewConnections()
		if err != nil {
			log.Errorf("[server] error accepting a new connection: %s", err.Error())
			return err
		}
		log.Info("[server] connection accepted!")

		go func(conn net.Conn) {
			newSocket := socket.NewSocket(s.getSocketConfig())
			newSocket.SetConnection(conn)

			rabbitMQ, err := communication.NewRabbitMQ()
			if err != nil {
				log.Errorf("[server] error with rabbit: %s", err.Error())
				return
			}

			messageHandler := handler.NewMessageHandler(
				handler.MessageHandlerConfig{
					EndBatchMarker: s.config.EndBatchMarker,
					FinMessages:    s.config.FinMessages,
					AckMessage:     s.config.AckMessage,
				},
				newSocket,
				rabbitMQ,
			)

			err = messageHandler.ProcessData()
			if err != nil {
				log.Errorf("[server] error processing data: %s", err.Error())
				return
			}

			log.Info("[server] Data was processed correctly!")
		}(conn)
	}
}

func (s *Server) getSocketConfig() socket.SocketConfig {
	return socket.SocketConfig{
		ServerAddress: s.config.IP + ":" + s.config.Port,
		ServerACK:     s.config.AckMessage,
		Protocol:      s.config.Protocol,
		PacketLimit:   s.config.PacketLimit,
	}
}
