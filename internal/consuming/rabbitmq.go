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
	"github.com/centrifugal/centrifugo/v5/internal/configtypes"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/valyala/fasttemplate"
)

type RabbitMQConsumer struct {
	name          string
	logger        Logger
	config        configtypes.RabbitMQConsumerConfig
	dispatcher    Dispatcher
	client        *amqp.Connection
	template      *fasttemplate.Template
	closeChannel  chan *amqp.Error
	cancelChannel chan string
	metrics       *commonMetrics
}

func NewRabbitMQConsumer(name string, logger Logger, dispatcher Dispatcher, config configtypes.RabbitMQConsumerConfig, metrics *commonMetrics) (*RabbitMQConsumer, error) {
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
		metrics:    metrics,
	}, nil
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
				var e KafkaJSONEvent
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

	//TODO determine what the channels are from the delivery settings.

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

		//TODO pull associated headers from delivery if not empty.
	}

	if r.config.PublicationDataMode.DeltaHeader != "" {
		deltaKey, ok := delivery.Headers[r.config.PublicationDataMode.IdempotencyKeyHeader]
		if ok {
			outputBody["delta_key_todo"] = fmt.Sprintf("%v", deltaKey)
		}
	}
	if r.config.PublicationDataMode.IdempotencyKeyHeader != "" {
		idpKey, ok := delivery.Headers[r.config.PublicationDataMode.IdempotencyKeyHeader]
		if ok {
			outputBody["idempotency_key_todo"] = fmt.Sprintf("%v", idpKey)
		}
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
			r.metrics.processedTotal.WithLabelValues(r.name).Inc()
			if retries > 0 {
				r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelInfo, "OK processing events after errors", map[string]any{}))
			}
			return false, nil
		}

		r.metrics.errorsTotal.WithLabelValues(r.name).Inc()
		r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error processing consumed event", map[string]any{"error": err.Error(), "method": method, "nextAttemptIn": backoffDuration.String()}))

		//allow escaping to prevent locking.
		if retries > r.config.MaxRetries && r.config.MaxRetries > 0 {
			r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "reached max retries processing event", map[string]any{}))
			return false, errors.New("reached max retries processing event")
		}

		retries++
		backoffDuration = getNextBackoffDuration(backoffDuration, retries)
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
