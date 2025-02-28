//go:build integration

package consuming

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

const (
	testRabbitMQAddress = "amqp://user:pass@localhost:5672"
	testRabbitMQVhost   = "test_vhost"
)

func setUpTestQueue(ctx context.Context, queueName string, queueArgs amqp.Table) (err error) {
	client, err := amqp.DialConfig(testRabbitMQAddress, amqp.Config{
		Vhost: testRabbitMQVhost,
	})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	channel, err := client.Channel()
	if err != nil {
		return fmt.Errorf("failed to create client channel: %w", err)
	}

	err = channel.ExchangeDeclare(queueName, "fanout", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to create exchange: %w", err)
	}

	queue, err := channel.QueueDeclare(queueName, false, false, false, false, queueArgs)
	if err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}
	if queue.Name != queueName {
		return fmt.Errorf("queue created did not match expected name")
	}

	err = channel.QueueBind(queueName, "", queueName, false, nil)
	if err != nil {
		return fmt.Errorf("failed to bind queue to exchange: %w", err)
	}

	return nil
}

func sendRabbitMQEvent(ctx context.Context, queueName string, data []byte, headers amqp.Table) (err error) {

	client, err := amqp.DialConfig(testRabbitMQAddress, amqp.Config{
		Vhost: testRabbitMQVhost,
	})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	channel, err := client.Channel()
	if err != nil {
		return fmt.Errorf("failed to create client channel: %w", err)
	}

	err = channel.PublishWithContext(ctx, queueName, "", false, false, amqp.Publishing{
		Headers: headers,
		Body:    data,
	})

	if err != nil {
		return fmt.Errorf("failed to publish data to channel: %w", err)
	}
	return nil
}

func TestRabbitMQConsumer_GreenScenario(t *testing.T) {
	t.Parallel()
	testQueueName := "centrifugo-consumer-test-" + uuid.New().String()

	testMethod := "method"
	inputPayload := []byte(`{"method":"method","payload":{"paylod_internal":"payload_internal"}}`)
	resultPayload := []byte(`{"paylod_internal":"payload_internal"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := setUpTestQueue(ctx, testQueueName, nil)
	require.NoError(t, err)

	eventReceived := make(chan struct{})
	consumerClosed := make(chan struct{})

	config := configtypes.RabbitMQConsumerConfig{
		Vhost:   testRabbitMQVhost,
		Address: testRabbitMQAddress,
		Queue:   testQueueName,
	}

	consumer, err := NewRabbitMQConsumer("green_consumer", &MockDispatcher{
		onDispatch: func(ctx context.Context, method string, data []byte) error {
			require.Equal(t, testMethod, method)
			require.Equal(t, resultPayload, data)
			close(eventReceived)
			return nil
		},
	}, config, newCommonMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)

	//start consumer
	go func() {
		err := consumer.Run(ctx)
		require.ErrorIs(t, err, context.Canceled)
		close(consumerClosed)
	}()

	err = sendRabbitMQEvent(ctx, testQueueName, inputPayload, nil)
	require.NoError(t, err)

	waitCh(t, eventReceived, 30*time.Second, "timeout waiting for event")
	cancel()
	waitCh(t, consumerClosed, 30*time.Second, "timeout waiting for consumer closed")

}

func TestRabbitMQConsumer_PublicationDataModeGreenScenario(t *testing.T) {
	t.Parallel()
	testQueueName := "centrifugo-consumer-test-" + uuid.New().String()
	namespaceUUID := uuid.New()
	testNotificationChannel := "centrifugo_test_channel:" + namespaceUUID.String()

	testMethod := "publish"
	inputPayload := []byte(`{"paylod_internal":"payload_internal"}`)
	resultPayload := []byte(`{"channel":"` + testNotificationChannel + `","data":"{\"paylod_internal\":\"payload_internal\"}"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := setUpTestQueue(ctx, testQueueName, nil)
	require.NoError(t, err)

	eventReceived := make(chan struct{})
	consumerClosed := make(chan struct{})

	config := configtypes.RabbitMQConsumerConfig{
		Vhost:   testRabbitMQVhost,
		Address: testRabbitMQAddress,
		Queue:   testQueueName,
		PublicationDataMode: configtypes.RabbitMQPublicationDataModeConfig{
			Enabled:         true,
			ChannelTemplate: "centrifugo_test_channel:{{.user_id}}",
		},
	}

	consumer, err := NewRabbitMQConsumer("publication_data_mode_green_consumer", &MockDispatcher{
		onDispatch: func(ctx context.Context, method string, data []byte) error {
			require.Equal(t, testMethod, method)
			require.Equal(t, resultPayload, data)
			close(eventReceived)
			return nil
		},
	}, config, newCommonMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)

	//start consumer
	go func() {
		err := consumer.Run(ctx)
		require.ErrorIs(t, err, context.Canceled)
		close(consumerClosed)
	}()

	err = sendRabbitMQEvent(ctx, testQueueName, inputPayload, amqp.Table{
		"user_id": namespaceUUID.String(),
	})
	require.NoError(t, err)

	waitCh(t, eventReceived, 30*time.Second, "timeout waiting for event")
	cancel()
	waitCh(t, consumerClosed, 30*time.Second, "timeout waiting for consumer closed")

}
