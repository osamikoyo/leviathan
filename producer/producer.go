package producer

import (
	"context"
	"errors"
	"fmt"

	"github.com/bytedance/sonic"
	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/models"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

var (
	ErrNilRequest    = errors.New("request cannot be nil")
	ErrMarshalFailed = errors.New("failed to marshal request")
	ErrPublishFailed = errors.New("failed to publish message")
)

type Producer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	logger  *logger.Logger
	qname   string
	cfg     *config.HeartConfig
}

func NewProducer(conn *amqp.Connection, cfg *config.HeartConfig, logger *logger.Logger) (*Producer, error) {
	if conn == nil {
		return nil, errors.New("connection cannot be nil")
	}
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	channel, err := conn.Channel()
	if err != nil {
		logger.Error("failed to create channel", zap.Error(err))
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	// Declare fanout exchange for broadcasting to all consumers
	exchangeName := cfg.ExchangeName
	if exchangeName == "" {
		exchangeName = "write_requests_fanout" // default exchange name
	}

	err = channel.ExchangeDeclare(
		exchangeName,
		"fanout", // type: fanout broadcasts to all bound queues
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		logger.Error("failed to declare exchange",
			zap.String("exchange_name", exchangeName),
			zap.Error(err))
		channel.Close()
		return nil, fmt.Errorf("failed to declare exchange %s: %w", exchangeName, err)
	}

	logger.Info("producer initialized successfully",
		zap.String("exchange_name", exchangeName))

	return &Producer{
		channel: channel,
		conn:    conn,
		cfg:     cfg,
		qname:   exchangeName, // store exchange name instead of queue name
		logger:  logger,
	}, nil
}

func (p *Producer) Close(ctx context.Context) error {
	p.logger.Info("stopping producer...")

	if err := p.channel.Close(); err != nil {
		return err
	}

	return p.conn.Close()
}

func (p *Producer) Publish(ctx context.Context, req *models.Request) error {
	if req == nil {
		return ErrNilRequest
	}

	// Validate request
	if len(req.SQL) == 0 {
		p.logger.Error("received request with empty SQL")
		return errors.New("request SQL cannot be empty")
	}

	p.logger.Debug("publishing request",
		zap.String("sql", req.SQL),
		zap.Int("args_count", len(req.Args)),
		zap.Bool("is_write_request", req.WriteRequest))

	// Marshal request to JSON
	body, err := sonic.Marshal(req)
	if err != nil {
		p.logger.Error("failed to marshal request",
			zap.String("sql", req.SQL),
			zap.Any("args", req.Args),
			zap.Error(err))
		return fmt.Errorf("%w: %v", ErrMarshalFailed, err)
	}

	// Publish message to fanout exchange (broadcasts to all bound queues)
	err = p.channel.PublishWithContext(ctx,
		p.qname, // exchange name (stored in qname field)
		"",      // routing key (empty for fanout)
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Timestamp:    req.Timestamp,
			Body:         body,
		},
	)

	if err != nil {
		p.logger.Error("failed to publish message",
			zap.String("exchange_name", p.qname),
			zap.String("sql", req.SQL),
			zap.Int("body_size", len(body)),
			zap.Error(err))
		return fmt.Errorf("%w to exchange %s: %v", ErrPublishFailed, p.qname, err)
	}

	p.logger.Info("message published successfully",
		zap.String("sql", req.SQL),
		zap.Int("body_size", len(body)))

	return nil
}
