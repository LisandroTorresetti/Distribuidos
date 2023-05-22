package main

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"strings"
	"tp1/communication"
	"tp1/domain/business/queryresponse"
	"tp1/server/handler"
	"tp1/socket"
)

type ServerConfig struct {
	Port                   string                                  `yaml:"port"`
	ResponsePort           string                                  `yaml:"response_port"`
	IP                     string                                  `yaml:"ip"`
	AckMessage             string                                  `yaml:"ack_message"`
	EndBatchMarker         string                                  `yaml:"end_batch_marker"`
	FinMessages            []string                                `yaml:"fin_messages"`
	DataDelimiter          string                                  `yaml:"data_delimiter"`
	MaxAmountOfConnections int                                     `yaml:"max_amount_of_connections"`
	Protocol               string                                  `yaml:"protocol"`
	PacketLimit            int                                     `yaml:"packet_limit"`
	ResponseQueueConfig    communication.QueueDeclarationConfig    `yaml:"response_queue_config"`
	RabbitMQConfig         map[string]communication.RabbitMQConfig `yaml:"rabbit_mq"`
	EOFType                string                                  `yaml:"eof_type"`
}

type Server struct {
	config                ServerConfig
	serverInputDataSocket *socket.Socket
	serverResponseSocket  *socket.Socket
	responseCache         map[string][]string
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

	serverResponseSocket := socket.NewSocket(
		socket.SocketConfig{
			ServerAddress: config.IP + ":" + config.ResponsePort,
			ServerACK:     config.AckMessage,
			Protocol:      config.Protocol,
			PacketLimit:   config.PacketLimit,
		},
	)

	responseCache := make(map[string][]string)

	return &Server{
		serverInputDataSocket: serverSocket,
		serverResponseSocket:  serverResponseSocket,
		config:                config,
		responseCache:         responseCache,
	}
}

func (s *Server) DeclareQueues() error {
	rabbitMQ, err := communication.NewRabbitMQ()
	if err != nil {
		log.Errorf("[server] rabbitMQ initialization error: %s", err.Error())
		return err
	}

	err = rabbitMQ.DeclareNonAnonymousQueues([]communication.QueueDeclarationConfig{s.config.ResponseQueueConfig})
	if err != nil {
		return fmt.Errorf("[server] error declaring Response Queue: %w", err)
	}

	log.Debug("[server] All queues were declared correctly")
	err = rabbitMQ.KillBadBunny()
	if err != nil {
		log.Error("[server] error killing rabbitMQ in DeclareQueues")
		return err
	}
	return nil
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

	log.Debug("[server] All exchanges were declared correctly")
	err = rabbitMQ.KillBadBunny()
	if err != nil {
		log.Error("[server] error killing rabbitMQ in DeclareExchanges")
		return err
	}
	return nil
}

func (s *Server) Run() error {
	forever := make(chan struct{}, 0)
	go func() {
		_ = s.handleQueryResponses()
	}()

	go func() {
		_ = s.handleInputData()
	}()

	<-forever
	return nil
}

func (s *Server) handleQueryResponses() error {
	foreverYoung := make(chan struct{}, 0)
	rabbitMQ, err := communication.NewRabbitMQ()
	if err != nil {
		return err
	}

	responsesDoneChannel := make(chan bool, 1)

	go func(responsesDoneChannel chan bool) {
		err := s.serverResponseSocket.StartListener()
		if err != nil {
			log.Errorf("error opening conection for responses: %s", err.Error())
		}

		log.Debug("Waiting for new connections in handleQueryResponses")
		conn, err := s.serverResponseSocket.AcceptNewConnections()

		newSocket := socket.NewSocket(s.getResponseSocketConfig())
		newSocket.SetConnection(conn)

	outerLoop:
		for {

			response, err := newSocket.Listen(s.config.EndBatchMarker, s.config.FinMessages)
			responseStr := string(response)
			if err != nil {
				panic(fmt.Sprintf("[server] error receiving message from a client: %s", err.Error()))
			}

			select {
			case <-responsesDoneChannel:
				log.Info("All responses received")
				break outerLoop

			default:
				log.Info("TODAVIA NO ESTAN LAS RESPUESTAS")
				err = newSocket.Send(responseStr)
				if err != nil {
					panic(fmt.Sprintf("%s", err.Error()))
				}
			}
		}

		resultQuery1 := s.responseCache["1"]
		resultQuery2 := s.responseCache["2"]
		resultQuery3 := s.responseCache["3"]

		responseMessage := fmt.Sprintf("%s \n %s \n %s|PONG", strings.Join(resultQuery1, "\n"), strings.Join(resultQuery2, "\n"), strings.Join(resultQuery3, "\n"))
		log.Infof("Mensaje a mandar: %s", responseMessage)
		err = newSocket.Send(responseMessage)
		if err != nil {
			panic(fmt.Sprintf("%s", err.Error()))
		}

		err = newSocket.CloseConnection()
		if err != nil {
			panic(fmt.Sprintf("%s", err.Error()))
		}

	}(responsesDoneChannel)

	go func(rabbitMQ *communication.RabbitMQ, doneChannel chan bool) {
		err := s.processQueryResponses(doneChannel, rabbitMQ)
		if err != nil {
			log.Errorf("ERROR: %s", err.Error())
			return
		}
	}(rabbitMQ, responsesDoneChannel)

	<-foreverYoung
	return nil
}

func (s *Server) handleInputData() error {
	err := s.serverInputDataSocket.StartListener()
	if err != nil {
		return err
	}

	for {
		log.Info("[server] waiting for new connections")
		// Accept new connection
		conn, err := s.serverInputDataSocket.AcceptNewConnections()
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

func (s *Server) processQueryResponses(doneChannel chan bool, rabbitMQ *communication.RabbitMQ) error {
	consumer, err := rabbitMQ.GetQueueConsumer(s.config.ResponseQueueConfig.Name)
	if err != nil {
		return err
	}

queryResponsesLoop:
	for message := range consumer {
		var queryResponses []*queryresponse.QueryResponse
		err = json.Unmarshal(message.Body, &queryResponses)
		if err != nil {
			log.Errorf(fmt.Sprintf("[server][method: GenerateResponse] error unmarshalling data: %s", err.Error()))
			return err
		}

		for idx := range queryResponses {
			queryResponse := queryResponses[idx]
			metadata := queryResponse.GetMetadata()

			if metadata.GetType() == s.config.EOFType {
				// sanity check
				if metadata.GetMessage() != s.config.AckMessage {
					panic(fmt.Sprintf("received an EOF message with an invalid format: Expected: %s - Got: %s", "PONG", metadata.GetMessage()))
				}

				log.Info("[server][method: GenerateResponse] EOF received: PONG")
				break queryResponsesLoop
			}

			log.Debug("[server][method: GenerateResponse] received QueryResponse")
			queryID := queryResponse.GetQueryID()
			queryIDResponses, ok := s.responseCache[queryID]
			if !ok {
				// we don't have data of this query
				s.responseCache[queryID] = []string{queryResponse.GetMetadata().GetMessage()}
				continue
			}
			s.responseCache[queryID] = append(queryIDResponses, queryResponse.GetMetadata().GetMessage())
		}

	}

	doneChannel <- true
	log.Info("[server] Got responses of all queries!")
	return nil
}

func (s *Server) getSocketConfig() socket.SocketConfig {
	return socket.SocketConfig{
		ServerAddress: s.config.IP + ":" + s.config.Port,
		ServerACK:     s.config.AckMessage,
		Protocol:      s.config.Protocol,
		PacketLimit:   s.config.PacketLimit,
	}
}

func (s *Server) getResponseSocketConfig() socket.SocketConfig {
	return socket.SocketConfig{
		ServerAddress: s.config.IP + ":" + s.config.ResponsePort,
		ServerACK:     s.config.AckMessage,
		Protocol:      s.config.Protocol,
		PacketLimit:   s.config.PacketLimit,
	}
}

func (s *Server) Kill() error {
	err := s.serverInputDataSocket.CloseListener()
	if err != nil {
		log.Errorf("[server] error closing listener: %s", err.Error())
		return err
	}

	err = s.serverInputDataSocket.CloseConnection()
	if err != nil {
		log.Errorf("[server] error closing connection: %s", err.Error())
		return err
	}
	return nil
}
