package consuming

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"text/template"
	"time"

	"github.com/centrifugal/centrifuge"
	amqp "github.com/rabbitmq/amqp091-go"
)

type AMQPConfig struct {
	Address    string            `mapstructure:"address" json:"address"`
	Vhost      string            `mapstructure:"vhost" json:"vhost"`
	Queue      string            `mapstructure:"queue" json:"queue"`
	MaxRetries int               `mapstructure:"max_retries" json:"max_retries"`
	Method     string            `mapstructure:"method" json:"method"`
	Templates  map[string]string `mapstructure:"templates" json:"templates"`
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
	simple := false
	templates := map[string]*template.Template{}
	if config.Method != "" || len(config.Templates) > 0 {
		for key, temp := range config.Templates {
			t := template.New(key)
			t, err := t.Parse(temp)
			if err != nil {
				return nil, fmt.Errorf("invalid template: %s", key)
			}
			templates[key] = t
		}

		method := config.Method
		if method == "" {
			method = "publish"
		}
		t, err := template.New("method").Parse(config.Method)
		if err != nil {
			return nil, fmt.Errorf("invalid template for method")
		}
		templates["method"] = t
		simple = true
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
		templates:  templates,
		simple:     simple,
	}, nil
}

type AMQPConsumer struct {
	name          string
	logger        Logger
	config        AMQPConfig
	dispatcher    Dispatcher
	client        *amqp.Connection
	maxRetries    int
	simple        bool
	templates     map[string]*template.Template
	closeChannel  chan *amqp.Error
	cancelChannel chan string
}

func (c *AMQPConsumer) Run(ctx context.Context) error {

	//connect to rabbitMQ and the queue.
	channel, err := c.client.Channel()
	if err != nil {
		return fmt.Errorf("error creating channel: %w", err)
	}
	c.closeChannel = channel.NotifyClose(make(chan *amqp.Error))
	c.cancelChannel = channel.NotifyCancel(make(chan string))

	defer channel.Close()

	deliveryChannel, err := channel.ConsumeWithContext(ctx, c.config.Queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error consuming from queue %s: %w", c.config.Queue, err)
	}

	c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelInfo, "connecting to amqp queue", map[string]any{"queue": c.config.Queue}))

	//start listening for data from the channel.
	for {
		select {
		case <-c.cancelChannel:
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "unexpected amqp channel cancel", map[string]any{}))
			return errors.New("unexpected amqp channel cancel")
		case <-c.closeChannel:
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "unexpected amqp channel close", map[string]any{}))
			return errors.New("unexpected amqp channel close")
		case <-ctx.Done():
			return ctx.Err()

		case delivery := <-deliveryChannel:
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelTrace, "event from AMQP", map[string]any{"queue": c.config.Queue}))
			var method string
			var payload []byte

			if c.simple {
				//if channel is unset, use full message as payload.
				var e JSONEvent
				err := json.Unmarshal(delivery.Body, &e)
				if err != nil {
					c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error unmarshaling event from AMQP", map[string]any{"error": err.Error(), "queue": c.config.Queue}))
					delivery.Ack(false)
					continue
				}
				method = e.Method
				payload = e.Payload
			} else {

				//if channel is set, use headers and body to set content
				method, payload, err = c.calculateMethodAndPayload(delivery)
				if err != nil {
					c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error custom-unmarshaling event from AMQP", map[string]any{"error": err.Error(), "queue": c.config.Queue}))
					delivery.Ack(false)
					continue
				}
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

func (r *AMQPConsumer) calculateMethodAndPayload(delivery amqp.Delivery) (string, []byte, error) {

	data := map[string]any{
		"Headers":   delivery.Headers,
		"MessageId": delivery.MessageId,
		"Body":      string(delivery.Body),
		"UserId":    delivery.UserId,
	}

	methodObj := r.applyTemplate("method", data)
	method, ok := methodObj.(string)
	if !ok {
		return "", nil, fmt.Errorf("method template must result in string")
	}

	outputBody := map[string]any{}
	for k := range r.templates {
		if k == "method" {
			continue
		}
		data := r.applyTemplate(k, data)
		if data != nil {
			outputBody[k] = data
		}
	}

	body, err := json.Marshal(outputBody)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal json body: %w", err)
	}
	return method, body, nil
}

func (r *AMQPConsumer) applyTemplate(key string, data map[string]any) any {

	w := &bytes.Buffer{}
	t := r.templates[key]
	if t == nil {
		return nil
	}
	err := t.ExecuteTemplate(w, key, data)
	if err != nil {
		r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "template key failed to execute", map[string]any{"error": err.Error(), "queue": r.config.Queue, "templateKey": key}))
		return nil
	}

	//attempt to parse result to json. If it can json, return its object format
	var out any = map[string]any{}
	err = json.Unmarshal(w.Bytes(), &out)
	if err != nil {
		return w.String()
	}

	return out
}

func (r *AMQPConsumer) SendWithRetry(ctx context.Context, method string, payload []byte) (fatal bool, err error) {

	//attempt dispatch
	var backoffDuration time.Duration = 0
	retries := 0
	for {
		err := r.dispatcher.Dispatch(ctx, method, payload)
		if err == nil {
			if retries > 0 {
				r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelInfo, "OK processing events after errors", map[string]any{}))
			} else {
				r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelTrace, "event from AMQP delivered", map[string]any{"queue": r.config.Queue}))
			}
			return false, nil
		}

		//allow escaping to prevent locking.
		if retries > r.maxRetries && r.maxRetries > 0 {
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
			r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "unexpected amqp channel cancel", map[string]any{}))
			return true, errors.New("unexpected amqp channel cancel")
		case <-r.closeChannel:
			r.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "unexpected amqp channel close", map[string]any{}))
			return true, errors.New("unexpected amqp channel close")
		case <-ctx.Done():
			return true, ctx.Err()
		}
	}
}
