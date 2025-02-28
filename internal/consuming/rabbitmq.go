package consuming

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"github.com/valyala/fasttemplate"
)

type RabbitMQConsumer struct {
	name          string
	config        configtypes.RabbitMQConsumerConfig
	dispatcher    Dispatcher
	client        *amqp.Connection
	template      *fasttemplate.Template
	closeChannel  chan *amqp.Error
	cancelChannel chan string
	metrics       *commonMetrics
}

func NewRabbitMQConsumer(name string, dispatcher Dispatcher, config configtypes.RabbitMQConsumerConfig, metrics *commonMetrics) (*RabbitMQConsumer, error) {
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
			return nil, errors.New("no channel_template provided for publication data mode")
		}
	}

	var template *fasttemplate.Template
	var err error
	if config.PublicationDataMode.Enabled {
		template, err = fasttemplate.NewTemplate(config.PublicationDataMode.ChannelTemplate, "{{", "}}")
		if err != nil {
			return nil, errors.New("failed to parse channel_template")
		}
	}

	client, err := amqp.DialConfig(config.Address, amqp.Config{
		Vhost: config.Vhost,
	})
	if err != nil {
		return nil, err
	}

	return &RabbitMQConsumer{
		name:       name,
		client:     client,
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
		return fmt.Errorf("error creating RabbitMQ channel: %w", err)
	}

	c.closeChannel = channel.NotifyClose(make(chan *amqp.Error))
	c.cancelChannel = channel.NotifyCancel(make(chan string))

	defer func() {
		go channel.Close()
	}()

	log.Info().Str("consumer_name", c.name).Str("queue", c.config.Queue).Msg("connecting to RabbitMQ queue")
	deliveryChannel, err := channel.ConsumeWithContext(ctx, c.config.Queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error consuming from queue %s: %w", c.config.Queue, err)
	}

	//start listening for data from the channel.
	for {
		select {
		case <-c.cancelChannel:
			if contextDone(ctx) {
				return ctx.Err()
			}

			//A cancel event was received. We should log and return an error to attempt to re-connect to the server
			log.Warn().Str("consumer_name", c.name).Msg("unexpected RabbitMQ channel cancel")
			return errors.New("unexpected RabbitMQ channel cancel")
		case <-c.closeChannel:
			if contextDone(ctx) {
				return ctx.Err()
			}
			//A close event was received. We should log and return an error to attempt to re-connect to the server
			log.Warn().Str("consumer_name", c.name).Msg("unexpected RabbitMQ channel close")
			return errors.New("unexpected RabbitMQ channel close")
		case <-ctx.Done():
			//The provided context has completed. Return the error from the context.
			return ctx.Err()

		case delivery := <-deliveryChannel:
			log.Debug().Str("consumer_name", c.name).Str("queue", c.config.Queue).Msg("event from RabbitMQ")

			var method string
			var payload []byte

			if c.config.PublicationDataMode.Enabled {
				method = "publish"

				//When publication mode is enabled, calculate the payload from the delivery
				payload, err = c.constructPayload(delivery)
				if err != nil {
					log.Err(err).Str("consumer_name", c.name).Str("queue", c.config.Queue).Msg("error constructing publicationDataMode Payload")

					//Drop the message to prevent it from being retried
					delivery.Ack(false)
					continue
				}

			} else {

				//if not publication data mode, parse method and payload from body.
				var e KafkaJSONEvent
				err := json.Unmarshal(delivery.Body, &e)
				if err != nil {
					log.Err(err).Str("consumer_name", c.name).Str("queue", c.config.Queue).Msg("error unmarshaling event from RabbitMQ")

					//drop the message to prevent it from being retried.
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

func contextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return false
}

// construct the payload from the
func (r *RabbitMQConsumer) constructPayload(delivery amqp.Delivery) (payload []byte, err error) {

	//calculate the header from the delivery data
	s, err := r.template.ExecuteFuncStringWithErr(func(w io.Writer, tag string) (int, error) {
		//remove any .prefix if used
		tag = strings.TrimPrefix(tag, ".")
		if value, ok := delivery.Headers[tag]; ok {
			//value can be many different types (https://pkg.go.dev/github.com/rabbitmq/amqp091-go#Table)
			//Use %v to automatically format.

			return w.Write([]byte(fmt.Sprintf("%v", value)))
		}
		return 0, fmt.Errorf("tag %s not found in headers", tag)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to calculate channel: %w", err)
	}

	outputBody := map[string]any{
		"channel": s,
		"data":    string(delivery.Body),
	}

	body, err := json.Marshal(outputBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal json body: %w", err)
	}

	return body, nil
}

func (c *RabbitMQConsumer) SendWithRetry(ctx context.Context, method string, payload []byte) (fatal bool, err error) {

	//attempt dispatch
	var backoffDuration time.Duration = 0
	retries := 0
	for {
		err := c.dispatcher.Dispatch(ctx, method, payload)
		if err == nil {
			c.metrics.processedTotal.WithLabelValues(c.name).Inc()
			if retries > 0 {
				log.Info().Str("consumer_name", c.name).Msg("OK processing events after errors")
			}
			log.Debug().Str("consumer_name", c.name).Msg("processed event from RabbitMQ")
			return false, nil
		}

		retries++
		backoffDuration = getNextBackoffDuration(backoffDuration, retries)
		c.metrics.errorsTotal.WithLabelValues(c.name).Inc()
		log.Error().Err(err).Str("consumer_name", c.name).Str("method", method).Str("next_attempt_in", backoffDuration.String()).Msg("error processing consumed event")

		select {
		case <-time.After(backoffDuration):
			continue
		case <-c.cancelChannel:
			log.Warn().Str("consumer_name", c.name).Str("method", method).Msg("unexpected RabbitMQ channel cancel")
			return true, errors.New("unexpected RabbitMQ channel cancel")
		case <-c.closeChannel:
			log.Warn().Str("consumer_name", c.name).Str("method", method).Msg("unexpected RabbitMQ channel close")
			return true, errors.New("unexpected RabbitMQ channel close")
		case <-ctx.Done():
			return true, ctx.Err()
		}
	}
}
