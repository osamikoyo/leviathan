package producer

import (
	"context"
	"errors"

	"github.com/bytedance/sonic"
	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/models"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type Producer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	logger  *logger.Logger
	qname   string
	cfg     *config.HeartConfig
}

func NewProducer(conn *amqp.Connection, cfg *config.HeartConfig, logger *logger.Logger) (*Producer, error) {
	channel, err := conn.Channel()
	if err != nil {
		logger.Fatal("failed get channel", zap.Error(err))

		return nil, err
	}

	q, err := channel.QueueDeclare(
		cfg.QueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Fatal("failed declare queue",
			zap.String("name", cfg.QueueName),
			zap.Error(err))

		return nil, err
	}

	return &Producer{
		channel: channel,
		conn:    conn,
		cfg:     cfg,
		qname:   q.Name,
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
		return errors.New("nil input")
	}

	body, err := sonic.Marshal(req)
	if err != nil {
		p.logger.Error("failed marshal req",
			zap.Any("req", req),
			zap.Error(err))

		return err
	}

	err = p.channel.PublishWithContext(ctx,
		"",
		p.qname,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		},
	)

	if err != nil {
		p.logger.Error("failed publish", zap.Error(err))

		return err
	}

	return nil
}
