package consumer

import (
	"context"

	"github.com/bytedance/sonic"
	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/nodecore"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
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
	}

	err = channel.Qos(
		1,
		0,
		false,
	)
	if err != nil {
		logger.Fatal("failed declare qos", zap.Error(err))
	}

	return &Consumer{
		qname:    q.Name,
		cfg:      cfg,
		logger:   logger,
		conn:     conn,
		channel:  channel,
		nodecore: nodecore,
	}, nil
}

func (c *Consumer) Close(ctx context.Context) error {
	c.logger.Info("closing consumer...")

	if err := c.channel.Close(); err != nil {
		return err
	}

	return c.conn.Close()
}

func (c *Consumer) Run(ctx context.Context) {
	c.logger.Info("starting consumer")

	msgs, err := c.channel.Consume(
		c.qname,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		c.logger.Error("failed consume", zap.Error(err))

		return
	}

	go func() {
		for d := range msgs {
			c.logger.Info("received message", zap.ByteString("body", d.Body))

			var message struct {
				Sql  string `json:"sql"`
				Args []any  `json:"args"`
			}

			if err = sonic.Unmarshal(d.Body, &message); err != nil {
				c.logger.Error("failed unmarshal body", zap.Error(err))

				continue
			}

			if err = c.nodecore.RouteWriteRequest(message.Sql, message.Args); err != nil {
				c.logger.Error("failed route write request",
					zap.Any("req", message),
					zap.Error(err))

				continue
			}

			c.logger.Info("successfully wrote request", zap.Any("request", message))
		}
	}()

	<-ctx.Done()
}
