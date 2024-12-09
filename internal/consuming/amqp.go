package consuming

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/centrifugal/centrifuge"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/valyala/fasttemplate"
)

type RabbitMQConfig struct {

	// Address is the RabbitMQ URI used to connect to rabbitMQ.
	// Username and Password are passed in via this uri like the following
	// amqp://username:password@rabbitmq.hostname:5672
	Address string `mapstructure:"address" json:"address" envconfig:"address" yaml:"address" toml:"address"`
	// Vhost is the rabbitmq vhost that will be connected to
	Vhost string `mapstructure:"vhost" json:"vhost" envconfig:"vhost" yaml:"vhost" toml:"vhost"`
	// Queue is the queue that the client will being consuming messages from.
	// The consumer will not create the queue. It must be created beforehand with the desired settings.
	Queue string `mapstructure:"queue" json:"queue" envconfig:"queue" yaml:"queue" toml:"queue"`

	// MaxRetries is the number of attempts to send the message to the centrifugo dispatch.
	// Retries will follow an exponential backoff.
	MaxRetries int `mapstructure:"max_retries" json:"max_retries" envconfig:"max_retries" yaml:"max_retries" toml:"max_retries"`

	// PublicationDataMode is a configuration for the mode where message payload already
	// contains data ready to publish into channels, instead of API command.
	PublicationDataMode RabbitMQPublicationDataModeConfig `mapstructure:"publication_data_mode" json:"publication_data_mode" envconfig:"publication_data_mode" yaml:"publication_data_mode" toml:"publication_data_mode"`
}

type RabbitMQPublicationDataModeConfig struct {
	// Enabled enables publication data mode for the rabbitmq consumer.
	Enabled bool `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled"`

	// TODO determine if any method other than publish can even be reasonably supported. Broadcast MIGHT be able to be supported, but channel template would have to be array aware.
	// Method is the method that will be used to send data to. If unset, it will default to "publish"
	// currently supported methods are "publish" and "broadcast"
	// Method string `mapstructure:"method" json:"method" envconfig:"method" yaml:"method" toml:"method"`

	// ChannelTemplate is the source template parsed and executed by fasttemplate to determine what channel to
	// send any recieved messages from rabbitMQ to.
	ChannelTemplate string `mapstructure:"channel_template" json:"channel_template" envconfig:"channel_template" yaml:"channel_template" toml:"channel_template"`
}

func NewRabbitMQConsumer(name string, logger Logger, dispatcher Dispatcher, config RabbitMQConfig) (*RabbitMQConsumer, error) {
	if config.Address == "" {
		return nil, errors.New("address is required")
	}
	if config.Vhost == "" {
		return nil, errors.New("vhost is required")
	}
	if config.Queue == "" {
		return nil, errors.New("queue is required")
	}

	if config.PublicationDataMode.Enabled {
		if config.PublicationDataMode.ChannelTemplate == "" {
			return nil, errors.New("no channel template provided in publication data mode")
		}

		// if config.PublicationDataMode.Method == "" {
		// 	//TODO determine if I should default to publish or error?
		// 	//Publish is a reasonable default...
		// 	config.PublicationDataMode.Method = "publish"
		// 	// return nil, errors.New("no channel method provided in publication data mode")
		// }
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 10
	}

	var template *fasttemplate.Template
	var err error
	if config.PublicationDataMode.Enabled {
		//TODO determine best template tags to use here. Using {{ }} for similarity re: go templates.
		template, err = fasttemplate.NewTemplate(config.PublicationDataMode.ChannelTemplate, "{{", "}}")
		if err != nil {
			return nil, errors.New("failed to parse channel template")
		}
	}

	client, err := amqp.DialConfig(config.Address, amqp.Config{
		Heartbeat: time.Second * 10,
		Locale:    "en_US",
		Vhost:     config.Vhost,
	})
	if err != nil {
		return nil, err
	}

	return &RabbitMQConsumer{
		name:       name,
		client:     client,
		logger:     logger,
		dispatcher: dispatcher,
		config:     config,
		template:   template,
	}, nil
}

type RabbitMQConsumer struct {
	name          string
	logger        Logger
	config        RabbitMQConfig
	dispatcher    Dispatcher
	client        *amqp.Connection
	template      *fasttemplate.Template
	closeChannel  chan *amqp.Error
	cancelChannel chan string
}

func (c *RabbitMQConsumer) Run(ctx context.Context) error {

	//connect to rabbitMQ and the queue.
	channel, err := c.client.Channel()
	if err != nil {
		return fmt.Errorf("error creating channel: %w", err)
	}
	c.closeChannel = channel.NotifyClose(make(chan *amqp.Error))
	c.cancelChannel = channel.NotifyCancel(make(chan string))

	defer channel.Close()

	//connect to the provided queue
	deliveryChannel, err := channel.ConsumeWithContext(ctx, c.config.Queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error consuming from queue %s: %w", c.config.Queue, err)
	}

	c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelInfo, "connecting to RabbitMQ queue", map[string]any{"queue": c.config.Queue}))

	//start listening for data from the channel.
	for {
		select {
		case <-c.cancelChannel:
			//A cancel event was received. We should log and return an error to attempt to re-connect to the server
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "unexpected RabbitMQ channel cancel", map[string]any{}))
			return errors.New("unexpected RabbitMQ channel cancel")
		case <-c.closeChannel:
			//A close event was received. We should log and return an error to attempt to re-connect to the server
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "unexpected RabbitMQ channel close", map[string]any{}))
			return errors.New("unexpected RabbitMQ channel close")
		case <-ctx.Done():
			//The provided context has completed. Return the error from the context.
			return ctx.Err()

		case delivery := <-deliveryChannel:
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelTrace, "event from RabbitMQ", map[string]any{"queue": c.config.Queue}))
			var method string
			var payload []byte

			switch {
			case c.config.PublicationDataMode.Enabled:
				// method = c.config.PublicationDataMode.Method
				method = "publish"

				//When publication mode is enabled, use the
				payload, err = c.constructPayload(delivery)
				if err != nil {
					c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error constructing publicationModePayload", map[string]any{"error": err.Error(), "queue": c.config.Queue}))
					delivery.Ack(false)
					continue
				}
			default:

				//if channel is unset, use full message as payload.
				var e JSONEvent
				err := json.Unmarshal(delivery.Body, &e)
				if err != nil {
					c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error unmarshaling event from RabbitMQ", map[string]any{"error": err.Error(), "queue": c.config.Queue}))
					delivery.Ack(false)
					continue
				}
				method = e.Method
				payload = e.Payload
			}

			//do a send with retry once the method and payload have been processed.
			fatal, err := c.SendWithRetry(ctx, method, payload)
			if err != nil {
				delivery.Nack(false, true)
				if fatal {
					return err
				}
			} else {
				delivery.Ack(false)
			}

		}

	}
}

// construct the payload from the
func (r *RabbitMQConsumer) constructPayload(delivery amqp.Delivery) (payload []byte, err error) {

	//calculate the header from the delivery data
	s, err := r.template.ExecuteFuncStringWithErr(func(w io.Writer, tag string) (int, error) {
		//remove any .prefix if used
		tag = strings.TrimPrefix(tag, ".")

		//try to get the
		if value, ok := delivery.Headers[tag]; ok {
			//value can be one of many different types (https://pkg.go.dev/github.com/rabbitmq/amqp091-go#Table)

			return w.Write([]byte(fmt.Sprintf("%v", value)))
		}
		return 0, fmt.Errorf("tag not found in headers")
	})

	if err != nil {
		return nil, fmt.Errorf("failed to calculate channel: %w", err)
	}

	//TODO: retest if this will encode properly
	outputBody := map[string]any{
		"channel": s,
		"data":    string(delivery.Body),
		//TODO: consider other variables that need to be set...

		//I may be able to parse the config for what headers to look for?
		//that or parse for raw values from the config. Not really sure.. Not sure.
	}

	body, err := json.Marshal(outputBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal json body: %w", err)
	}

	return body, nil
}

func (r *RabbitMQConsumer) SendWithRetry(ctx context.Context, method string, payload []byte) (fatal bool, err error) {

	//attempt dispatch
	var backoffDuration time.Duration = 0
	retries := 0
	for {
		err := r.dispatcher.Dispatch(ctx, method, payload)
		if err == nil {
			if retries > 0 {
				r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelInfo, "OK processing events after errors", map[string]any{}))
			} else {
				r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelTrace, "event from RabbitMQ delivered", map[string]any{"queue": r.config.Queue}))
			}
			return false, nil
		}

		//allow escaping to prevent locking.
		if retries > r.config.MaxRetries && r.config.MaxRetries > 0 {
			r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "reached max retries processing event", map[string]any{}))
			return false, errors.New("reached max retries processing event")
		}

		retries++
		backoffDuration = getNextBackoffDuration(backoffDuration, retries)
		r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error processing consumed event", map[string]any{"error": err.Error(), "method": method, "nextAttemptIn": backoffDuration.String()}))
		select {
		case <-time.After(backoffDuration):
			continue
		case <-r.cancelChannel:
			r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "unexpected RabbitMQ channel cancel", map[string]any{}))
			return true, errors.New("unexpected RabbitMQ channel cancel")
		case <-r.closeChannel:
			r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "unexpected RabbitMQ channel close", map[string]any{}))
			return true, errors.New("unexpected RabbitMQ channel close")
		case <-ctx.Done():
			return true, ctx.Err()
		}
	}
}

//TODOs:
//re-write to remove simple case and assume everything is templated using the template example discussed previously.
//Implement unit tests
//Implement integration tests with rabbitMQ (see docker compose files.)
