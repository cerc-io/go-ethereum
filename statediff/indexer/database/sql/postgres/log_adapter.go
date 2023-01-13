package postgres

import (
	"context"

	"github.com/ethereum/go-ethereum/log"
	"github.com/jackc/pgx/v4"
)

type LogAdapter struct {
	l log.Logger
}

func NewLogAdapter(l log.Logger) *LogAdapter {
	return &LogAdapter{l: l}
}

func (l *LogAdapter) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	var logger log.Logger
	if data != nil {
		var args = make([]interface{}, 0)
		for key, value := range data {
			if value != nil {
				args = append(args, key, value)
			}
		}
		logger = l.l.New(args...)
	} else {
		logger = l.l
	}

	switch level {
	case pgx.LogLevelTrace:
		logger.Trace(msg)
	case pgx.LogLevelDebug:
		logger.Debug(msg)
	case pgx.LogLevelInfo:
		logger.Info(msg)
	case pgx.LogLevelWarn:
		logger.Warn(msg)
	case pgx.LogLevelError:
		logger.Error(msg)
	default:
		logger.New("INVALID_PGX_LOG_LEVEL", level).Error(msg)
	}
}
