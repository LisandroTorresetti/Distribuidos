package communication

type RabbitMQ struct {
	Name              string            `yaml:"name"`
	DeclarationConfig DeclarationConfig `yaml:"declaration_config"`
	PublishingConfig  PublishingConfig  `yaml:"publishing_config"`
	ConsumptionConfig ConsumptionConfig `yaml:"consumption_config"`
}

// DeclarationConfig contains the parameters to declare a RabbitMQ queue
type DeclarationConfig struct {
	Durable          bool `yaml:"durable"`
	DeleteWhenUnused bool `yaml:"delete_when_unused"`
	Exclusive        bool `yaml:"exclusive"`
	NoWait           bool `yaml:"no_wait"`
}

// PublishingConfig config use it for publishing messages in a RabbitMQ queue
type PublishingConfig struct {
	Exchange    string `yaml:"exchange"`
	Mandatory   bool   `yaml:"mandatory"`
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
