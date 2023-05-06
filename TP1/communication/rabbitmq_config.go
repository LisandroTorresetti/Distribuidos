package communication

type RabbitMQConfig struct {
	Name                      string                    `yaml:"name"`
	QueueDeclarationConfig    QueueDeclarationConfig    `yaml:"queue_declaration_config"`
	ExchangeDeclarationConfig ExchangeDeclarationConfig `yaml:"exchange_declaration_config"`
	PublishingConfig          PublishingConfig          `yaml:"publishing_config"`
	ConsumptionConfig         ConsumptionConfig         `yaml:"consumption_config"`
}

// QueueDeclarationConfig contains the parameters to declare a RabbitMQ queue
type QueueDeclarationConfig struct {
	Name             string `yaml:"name"`
	Durable          bool   `yaml:"durable"`
	DeleteWhenUnused bool   `yaml:"delete_when_unused"`
	Exclusive        bool   `yaml:"exclusive"`
	NoWait           bool   `yaml:"no_wait"`
}

// ExchangeDeclarationConfig contains the parameters to declare a RabbitMQ exchange
type ExchangeDeclarationConfig struct {
	Name        string `yaml:"name"`
	Type        string `yaml:"type"`
	Durable     bool   `yaml:"durable"`
	AutoDeleted bool   `yaml:"auto_deleted"`
	Internal    bool   `yaml:"internal"`
	NoWait      bool   `yaml:"no_wait"`
}

// PublishingConfig config use it for publishing messages in a RabbitMQ queue
type PublishingConfig struct {
	Exchange    string `yaml:"exchange"`
	RoutingKey  string `yaml:"routing_key"`
	Mandatory   bool   `yaml:"mandatory"`
	Immediate   bool   `yaml:"immediate"`
	ContentType string `yaml:"content_type"`
}

// ConsumptionConfig config use it for consuming a RabbitMQ queue
type ConsumptionConfig struct {
	Consumer  string `yaml:"consumer"`
	AutoACK   bool   `yaml:"auto_ack"`
	Exclusive bool   `yaml:"exclusive"`
	NoLocal   bool   `yaml:"no_local"`
	NoWait    bool   `yaml:"no_wait"`
}
