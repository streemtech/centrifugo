package consuming

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/centrifugal/centrifuge"
	amqp "github.com/rabbitmq/amqp091-go"
)

type AMQPConfig struct {
	Address    string `mapstructure:"address" json:"address"`
	Vhost      string `mapstructure:"vhost" json:"vhost"`
	Queue      string `mapstructure:"queue" json:"queue"`
	MaxRetries int    `mapstructure:"max_retries" json:"max_retries"`
}

func NewAMQPConsumer(name string, logger Logger, dispatcher Dispatcher, config AMQPConfig) (*AMQPConsumer, error) {
	if config.Address == "" {
		return nil, errors.New("address is required")
	}
	if config.Vhost == "" {
		return nil, errors.New("vhost is required")
	}
	if config.Queue == "" {
		return nil, errors.New("queue is required")
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 10
	}

	client, err := amqp.DialConfig(config.Address, amqp.Config{
		Heartbeat: time.Second * 10,
		Locale:    "en_US",
		Vhost:     config.Vhost,
	})
	if err != nil {
		return nil, err
	}

	return &AMQPConsumer{
		name:       name,
		client:     client,
		logger:     logger,
		dispatcher: dispatcher,
		config:     config,
		maxRetries: config.MaxRetries,
	}, nil
}

type AMQPConsumer struct {
	name       string
	logger     Logger
	config     AMQPConfig
	dispatcher Dispatcher
	client     *amqp.Connection
	maxRetries int
}

func (r *AMQPConsumer) Run(ctx context.Context) error {

	//connect to rabbitMQ and the queue.
	channel, err := r.client.Channel()
	if err != nil {
		return fmt.Errorf("error creating channel: %w", err)
	}
	closeChannel := channel.NotifyClose(make(chan *amqp.Error))
	defer channel.Close()

	deliveryChannel, err := channel.Consume(r.config.Queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error consuming from queue %s: %w", r.config.Queue, err)
	}

	//start listening for data from the channel.
	for delivery := range deliveryChannel {
		r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelTrace, "event from AMQP", map[string]any{"queue": r.config.Queue}))

		//parse event
		var e JSONEvent
		err := json.Unmarshal(delivery.Body, &e)
		if err != nil {
			r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error unmarshalling event from AMQP", map[string]any{"error": err.Error(), "queue": r.config.Queue}))
			delivery.Ack(false)
			continue
		}

		//attempt dispatch
		var backoffDuration time.Duration = 0
		retries := 0
		for {
			err := r.dispatcher.Dispatch(ctx, e.Method, e.Payload)
			if err == nil {
				if retries > 0 {
					r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelInfo, "OK processing events after errors", map[string]any{}))
				} else {
					r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelTrace, "event from AMQP delivered", map[string]any{"queue": r.config.Queue}))
				}
				delivery.Ack(false)
				break
			}

			//allow escaping to prevent locking.
			if retries > r.maxRetries && r.maxRetries > 0 {
				r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "reached max retries processing event", map[string]any{}))
				delivery.Nack(false, true)
				return errors.New("reached max retries processing event")
			}

			retries++
			backoffDuration = getNextBackoffDuration(backoffDuration, retries)
			r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error processing consumed event", map[string]any{"error": err.Error(), "method": e.Method, "nextAttemptIn": backoffDuration.String()}))
			select {
			case <-time.After(backoffDuration):
				continue
			case <-closeChannel:
				r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "unexpected amqp channel close", map[string]any{}))
				return errors.New("unexpected amqp channel close")
			case <-ctx.Done():
				return nil
			}
		}

	}

	return nil
}
