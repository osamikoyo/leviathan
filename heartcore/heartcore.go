package heartcore

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/bytedance/sonic"
	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/models"
	"github.com/osamikoyo/leviathan/nodepb"
	"github.com/osamikoyo/leviathan/producer"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type CNode struct {
	node           nodepb.NodeClient
	requestInRoute int
}

func newCNode(node nodepb.NodeClient) *CNode {
	return &CNode{
		node: node,
	}
}

type HeartCore struct {
	mux      *sync.RWMutex
	nodes    []*CNode
	producer *producer.Producer
	logger   *logger.Logger
	cfg      *config.HeartConfig
}

func NewHeartCore(
	nodes []nodepb.NodeClient,
	logger logger.Logger,
	cfg *config.HeartConfig,
	producer *producer.Producer,
) *HeartCore {
	cnodes := make([]*CNode, len(nodes))

	for i, node := range nodes {
		cnodes[i] = newCNode(node)
	}

	return &HeartCore{
		nodes:    cnodes,
		logger:   &logger,
		cfg:      cfg,
		producer: producer,
	}
}

func (h *HeartCore) getGoodNode() int {
	min := 0

	minindex := 0

	h.mux.RLock()
	defer h.mux.RUnlock()

	min = h.nodes[0].requestInRoute

	for i, node := range h.nodes {
		if node.requestInRoute < min {
			min = node.requestInRoute
			minindex = i
		}
	}

	return minindex
}

func packAny(value any) (*anypb.Any, error) {
	if value == nil {
		return &anypb.Any{}, errors.New("nil input")
	}

	switch v := value.(type) {
	case string:
		return anypb.New(wrapperspb.String(v))
	case bool:
		return anypb.New(wrapperspb.Bool(v))
	case int32:
		return anypb.New(wrapperspb.Int32(v))
	case int64:
		return anypb.New(wrapperspb.Int64(v))
	case uint32:
		return anypb.New(wrapperspb.UInt32(v))
	case uint64:
		return anypb.New(wrapperspb.UInt64(v))
	case float32:
		return anypb.New(wrapperspb.Float(v))
	case float64:
		return anypb.New(wrapperspb.Double(v))
	case []byte:
		return anypb.New(wrapperspb.Bytes(v))
	default:
		return nil, fmt.Errorf("unsupported type: %v", reflect.TypeOf(value))
	}
}

func (h *HeartCore) RouteReadRequest(ctx context.Context, sql string, args ...any) ([]map[string]interface{}, error) {
	if len(sql) == 0 {
		return nil, errors.New("nil input")
	}

	argsPb := make([]*anypb.Any, len(args))

	for i, arg := range args {
		argPb, err := packAny(arg)
		if err != nil {
			h.logger.Error("Ошибка упаковки аргумента", zap.Int("index", i), zap.Error(err))
			return nil, err
		}
		argsPb[i] = argPb
	}

	index := h.getGoodNode()

	h.mux.Lock()
	h.nodes[index].requestInRoute++
	h.mux.Unlock()

	defer func() {
		h.mux.Lock()
		h.nodes[index].requestInRoute--
		h.mux.Unlock()
	}()

	resp, err := h.nodes[index].node.Route(ctx, &nodepb.Request{
		Sql:  sql,
		Args: argsPb,
	})

	if err != nil {
		h.logger.Error("error from node",
			zap.Uint32("node_id", resp.NodeId),
			zap.String("message", resp.Message),
			zap.Error(err))

		return nil, err
	}

	var result []map[string]interface{}

	if err = sonic.Unmarshal([]byte(resp.Payload), &result); err != nil {
		h.logger.Error("failed unmarshal payload",
			zap.String("payload", resp.Payload),
			zap.Error(err))

		return nil, err
	}

	h.logger.Info("successfully send write request")

	return result, nil
}

func (h *HeartCore) RouteWriteRequest(ctx context.Context, sql string, args ...any) error {
	if len(sql) == 0 {
		return errors.New("nil input")
	}

	index := h.getGoodNode()

	h.mux.Lock()
	h.nodes[index].requestInRoute++
	h.mux.Unlock()

	defer func() {
		h.mux.Lock()
		h.nodes[index].requestInRoute--
		h.mux.Unlock()
	}()

	argsPb := make([]*anypb.Any, len(args))

	for i, arg := range args {
		argPb, err := packAny(arg)
		if err != nil {
			h.logger.Error("error get arg", zap.Int("index", i), zap.Error(err))
			return err
		}
		argsPb[i] = argPb
	}

	err := h.producer.Publish(ctx, models.NewRequest(sql, args, true))
	if err != nil {
		h.logger.Error("failed produce request",
			zap.String("sql", sql),
			zap.Error(err))

		return err
	}

	h.logger.Info("success write request")

	return nil
}
