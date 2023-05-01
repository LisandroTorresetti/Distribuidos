package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
	"tp1/domain/communication"
	"tp1/server/handler"
	"tp1/socket"
)

type ServerConfig struct {
	Port                   string                            `yaml:"port"`
	IP                     string                            `yaml:"ip"`
	AckMessage             string                            `yaml:"ack_message"`
	EndBatchMarker         string                            `yaml:"end_batch_marker"`
	FinMessages            []string                          `yaml:"fin_messages"`
	DataDelimiter          string                            `yaml:"data_delimiter"`
	MaxAmountOfConnections int                               `yaml:"max_amount_of_connections"`
	Protocol               string                            `yaml:"protocol"`
	PacketLimit            int                               `yaml:"packet_limit"`
	QueuesConfigs          map[string]communication.RabbitMQ `yaml:"queues"`
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

func (s *Server) DeclareQueues() error {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %s", err.Error())
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %s", err.Error())
	}
	defer ch.Close()

	for _, queueConfig := range s.config.QueuesConfigs {
		declareConfig := queueConfig.DeclarationConfig
		_, err = ch.QueueDeclare(
			queueConfig.Name,
			declareConfig.Durable,
			declareConfig.DeleteWhenUnused,
			declareConfig.Exclusive,
			declareConfig.NoWait,
			nil,
		)
		if err != nil {
			return fmt.Errorf("error declaring queue %s", queueConfig.Name)
		}
		log.Debugf("queue %s declared correctly", queueConfig.Name)
	}
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

		messageHandler := handler.NewMessageHandler(
			handler.MessageHandlerConfig{
				EndBatchMarker: s.config.EndBatchMarker,
				FinMessages:    s.config.FinMessages,
				AckMessage:     s.config.AckMessage,
			},
			&newSocket,
			s.config.QueuesConfigs,
		)

		go func() {
			err := messageHandler.ProcessData()
			if err != nil {
				log.Errorf("error processing data: %s", err.Error())
			}
		}()
	}
}