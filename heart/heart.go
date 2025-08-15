package heart

import (
	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/logger"
)

type Heart struct {
	cfg    *config.HeartConfig
	logger *logger.Logger
}
