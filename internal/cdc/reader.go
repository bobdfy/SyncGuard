package cdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Reader 是 CDC 读取器的核心结构体
type Reader struct {
	conn        *pgconn.PgConn
	relations   map[uint32]*pglogrepl.RelationMessage
	confirmed   pglogrepl.LSN //当前已经确认消费到的LSN位置
	slotName    string        // replication slot名字
	publication string        // publication决定哪些表需要进入逻辑复制
	handler     Handler       // 处理变更事件的回调
}

// NewReader 建立复制连接、确保复制槽存在、开始复制。
func NewReader(ctx context.Context, dsn, slotName, publication string, handler Handler) (*Reader, error) {
	if handler == nil {
		handler = LogHandler{}
	}
	// 将连接解析为可配置
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	// 配置 replication 为 datebase
	cfg.RuntimeParams["replication"] = "database"

	// 建立连接
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	// IdentifySystem，拿到 sys.XLogPos
	sys, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		conn.Close(ctx)
		return nil, err
	}

	startLSN := sys.XLogPos

	// 创建复制槽：already exists 时复用，其它错误直接返回
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})

	switch {
	case err == nil:
		slog.Info("复制槽已创建", "slot", slotName)
	case strings.Contains(err.Error(), "already exists"):
		lsn, ok, qerr := querySlotLSN(ctx, dsn, slotName)
		if qerr != nil {
			conn.Close(ctx)
			return nil, fmt.Errorf("查询复制槽位点失败: %w", qerr)
		}
		if ok {
			startLSN = lsn
		}
		slog.Info("复制槽已存在，从历史位点恢复", "slot", slotName, "start_lsn", startLSN.String())
	default:
		conn.Close(ctx)
		return nil, err
	}

	err = pglogrepl.StartReplication(ctx, conn, slotName, startLSN, pglogrepl.StartReplicationOptions{
		Timeline: 0,
		Mode:     pglogrepl.LogicalReplication,
		PluginArgs: []string{
			"proto_version '1'",
			"publication_names '" + publication + "'",
		},
	})

	if err != nil {
		conn.Close(ctx)
		return nil, err
	}

	return &Reader{
		conn:        conn,
		relations:   map[uint32]*pglogrepl.RelationMessage{},
		confirmed:   startLSN,
		slotName:    slotName,
		publication: publication,
		handler:     handler,
	}, nil
}

// Run 消息循环：收 → 解码 → 打印。ctx 取消时干净返回。
func (r *Reader) Run(ctx context.Context) error {
	for {
		msg, err := r.conn.ReceiveMessage(ctx)

		if err != nil {
			if errors.Is(err, context.Canceled) {
				// 主动取消 = 正常退出，不算错误
				return nil
			}
			return err
		}

		// CopyData 信封解析 + switch 消息类型（原样搬）
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			switch m.Data[0] {
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(m.Data[1:])
				if err != nil {
					return err
				}

				// ← 推进已确认位点，保活时回给服务端（原来永远回启动位点，WAL 只增不减）
				if xld.WALStart > r.confirmed {
					r.confirmed = xld.WALStart
				}

				lm, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					slog.Warn("解码失败", "err", err)
					continue
				}

				switch v := lm.(type) {
				case *pglogrepl.RelationMessage:
					r.relations[v.RelationID] = v
					slog.Info("收到表结构", "table", v.Namespace+"."+v.RelationName)
				case *pglogrepl.InsertMessage:
					if err := r.emit(ctx, "INSERT", xld.WALStart, v.RelationID, v.Tuple); err != nil {
						return err
					}
				case *pglogrepl.UpdateMessage:
					if err := r.emit(ctx, "UPDATE", xld.WALStart, v.RelationID, v.NewTuple); err != nil {
						return err
					}
				case *pglogrepl.DeleteMessage:
					if err := r.emit(ctx, "DELETE", xld.WALStart, v.RelationID, v.OldTuple); err != nil {
						return err
					}
				}

				// 每条数据都 ack 一次，让槽的 confirmed_flush_lsn 紧跟进度
				if err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: r.confirmed}); err != nil {
					return err
				}
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if err != nil {
					slog.Warn("解析失败", "err", err)
					continue
				}
				if pkm.ReplyRequested {
					if err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: r.confirmed}); err != nil {
						return err
					}
				}
			}
		}
	}
}

// Close 关闭复制连接（带超时，防止干净关闭卡住）
func (r *Reader) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = r.conn.Close(ctx)
}

// emit 把一条变更组装成 Change，交给 handler 处理。
func (r *Reader) emit(ctx context.Context, op string, lsn pglogrepl.LSN, relID uint32, tuple *pglogrepl.TupleData) error {
	rel := r.relations[relID]
	if rel == nil {
		slog.Warn("未知表结构，跳过", "rel_id", relID)
		return nil
	}
	ch := Event{
		LSN:       lsn.String(),
		Operation: op,
		Schema:    rel.Namespace,
		Table:     rel.RelationName,
		Columns:   tupleToColumns(rel, tuple),
	}
	return r.handler.Handle(ctx, ch)
}

func querySlotLSN(ctx context.Context, dsn, slotName string) (pglogrepl.LSN, bool, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, false, err
	}
	defer conn.Close(ctx)

	var lsnStr *string
	err = conn.QueryRow(ctx,
		"SELECT COALESCE(confirmed_flush_lsn, restart_lsn)::text FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&lsnStr)
	if err != nil {
		return 0, false, err
	}

	if lsnStr == nil || *lsnStr == "" {
		return 0, false, nil
	}

	lsn, err := pglogrepl.ParseLSN(*lsnStr)
	if err != nil {
		return 0, false, err
	}
	return lsn, true, nil
}
