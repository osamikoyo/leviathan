package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bytedance/sonic"
	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/models"
	"github.com/osamikoyo/leviathan/nodecore"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

var (
	ErrFailedUnmarshal    = errors.New("failed to unmarshal message")
	ErrWriteRequestFailed = errors.New("write request failed")
)

type Consumer struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	logger   *logger.Logger
	qname    string
	cfg      *config.NodeConfig
	nodecore *nodecore.NodeCore
}

func NewConsumer(conn *amqp.Connection, nodecore *nodecore.NodeCore, logger *logger.Logger, cfg *config.NodeConfig) (*Consumer, error) {
	if conn == nil {
		return nil, errors.New("connection cannot be nil")
	}
	if nodecore == nil {
		return nil, errors.New("nodecore cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}

	channel, err := conn.Channel()
	if err != nil {
		logger.Error("failed to get channel", zap.Error(err))
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	// Declare fanout exchange for broadcasting
	err = channel.ExchangeDeclare(
		cfg.ExchangeName,
		"fanout", // type: fanout broadcasts to all bound queues
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		logger.Error("failed to declare exchange",
			zap.String("exchange_name", cfg.ExchangeName),
			zap.Error(err))
		channel.Close()
		return nil, fmt.Errorf("failed to declare exchange %s: %w", cfg.ExchangeName, err)
	}

	// Declare queue with unique name for this consumer
	queueName := fmt.Sprintf("%s_node_%d", cfg.QueueName, cfg.NodeID)
	q, err := channel.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		logger.Error("failed to declare queue",
			zap.String("queue_name", queueName),
			zap.Error(err))
		channel.Close()
		return nil, fmt.Errorf("failed to declare queue %s: %w", queueName, err)
	}

	// Bind queue to exchange so it receives all messages
	err = channel.QueueBind(
		q.Name,           // queue name
		"",               // routing key (ignored for fanout)
		cfg.ExchangeName, // exchange
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		logger.Error("failed to bind queue to exchange",
			zap.String("queue_name", q.Name),
			zap.String("exchange_name", cfg.ExchangeName),
			zap.Error(err))
		channel.Close()
		return nil, fmt.Errorf("failed to bind queue %s to exchange %s: %w", q.Name, cfg.ExchangeName, err)
	}

	// Set QoS to process one message at a time
	err = channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		logger.Error("failed to set QoS", zap.Error(err))
		channel.Close()
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	logger.Info("consumer initialized successfully",
		zap.String("queue_name", q.Name),
		zap.Uint32("node_id", cfg.NodeID))

	return &Consumer{
		qname:    q.Name,
		cfg:      cfg,
		logger:   logger,
		conn:     conn,
		channel:  channel,
		nodecore: nodecore,
	}, nil
}

func (c *Consumer) Close(_ context.Context) error {
	c.logger.Info("closing consumer...")

	if err := c.channel.Close(); err != nil {
		return err
	}

	return c.conn.Close()
}

func (c *Consumer) Run(ctx context.Context) {
	c.logger.Info("starting consumer",
		zap.String("queue_name", c.qname),
		zap.Uint32("node_id", c.cfg.NodeID))

	msgs, err := c.channel.Consume(
		c.qname,
		"",    // consumer tag
		false, // auto-ack (we want manual ack)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		c.logger.Error("failed to start consuming",
			zap.String("queue_name", c.qname),
			zap.Error(err))
		return
	}

	// Process messages in a separate goroutine
	go func() {
		defer c.logger.Info("consumer message processing stopped")

		for {
			select {
			case d, ok := <-msgs:
				if !ok {
					c.logger.Warn("message channel closed")
					return
				}
				c.processMessage(d)

			case <-ctx.Done():
				c.logger.Info("consumer context cancelled")
				return
			}
		}
	}()

	<-ctx.Done()
	c.logger.Info("consumer stopped")
}

// processMessage handles individual message processing with proper error handling and ack/nack
func (c *Consumer) processMessage(delivery amqp.Delivery) {
	startTime := time.Now()
	c.logger.Debug("processing message",
		zap.String("delivery_tag", fmt.Sprintf("%d", delivery.DeliveryTag)),
		zap.Int("body_size", len(delivery.Body)))

	// Parse message into Request struct
	var req models.Request
	if err := sonic.Unmarshal(delivery.Body, &req); err != nil {
		c.logger.Error("failed to unmarshal message body",
			zap.String("delivery_tag", fmt.Sprintf("%d", delivery.DeliveryTag)),
			zap.ByteString("body", delivery.Body),
			zap.Error(err))

		// Reject message and don't requeue (likely malformed)
		if err := delivery.Nack(false, false); err != nil {
			c.logger.Error("failed to nack malformed message", zap.Error(err))
		}
		return
	}

	// Validate request
	if len(req.SQL) == 0 {
		c.logger.Error("received empty SQL in message",
			zap.String("delivery_tag", fmt.Sprintf("%d", delivery.DeliveryTag)))

		// Reject invalid message
		if err := delivery.Nack(false, false); err != nil {
			c.logger.Error("failed to nack invalid message", zap.Error(err))
		}
		return
	}

	c.logger.Debug("executing write request",
		zap.String("sql", req.SQL),
		zap.Int("args_count", len(req.Args)),
		zap.Bool("is_write_request", req.WriteRequest))

	// Execute write request through nodecore
	if err := c.nodecore.RouteWriteRequest(req.SQL, req.Args...); err != nil {
		c.logger.Error("write request execution failed",
			zap.String("delivery_tag", fmt.Sprintf("%d", delivery.DeliveryTag)),
			zap.String("sql", req.SQL),
			zap.Any("args", req.Args),
			zap.Error(err))

		// Reject and requeue for retry (might be temporary DB issue)
		if err := delivery.Nack(false, true); err != nil {
			c.logger.Error("failed to nack failed message", zap.Error(err))
		}
		return
	}

	// Successfully processed - acknowledge message
	if err := delivery.Ack(false); err != nil {
		c.logger.Error("failed to ack processed message",
			zap.String("delivery_tag", fmt.Sprintf("%d", delivery.DeliveryTag)),
			zap.Error(err))
		return
	}

	c.logger.Info("message processed successfully",
		zap.String("delivery_tag", fmt.Sprintf("%d", delivery.DeliveryTag)),
		zap.String("sql", req.SQL),
		zap.Duration("processing_time", time.Since(startTime)))
}
