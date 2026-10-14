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

// Reader 是 CDC 读取器的核心结构体。
type Reader struct {
	conn *pgconn.PgConn

	relations map[uint32]*pglogrepl.RelationMessage

	// 已落库的提交位点
	confirmed pglogrepl.LSN

	slotName string

	publication string

	// 事件落库 + 断点推进
	store *EventStore
}

// ensureSlot 建立复制连接并确保复制槽存在。
// 返回：复制连接、当前 WAL 位点（槽新建时即切点 C）、是否本次新建了槽。
func ensureSlot(ctx context.Context, dsn, slotName string) (*pgconn.PgConn, pglogrepl.LSN, bool, error) {
	// 将连接解析为可配置
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, 0, false, err
	}

	// 配置 replication 为 datebase
	cfg.RuntimeParams["replication"] = "database"

	// 建立连接
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, 0, false, err
	}

	// IdentifySystem，拿到 sys.XLogPos（当前 WAL 位点）
	sys, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		conn.Close(ctx)
		return nil, 0, false, err
	}

	// 创建复制槽：already exists 时复用，其它错误直接返回
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})

	switch {
	case err == nil:
		slog.Info("复制槽已创建", "slot", slotName)
		return conn, sys.XLogPos, true, nil
	case strings.Contains(err.Error(), "already exists"):
		return conn, sys.XLogPos, false, nil
	default:
		conn.Close(ctx)
		return nil, 0, false, err
	}
}

// NewReader 建立复制连接、确保复制槽存在、开始复制。
// 事件通过 store 写 cdc_events 表，断点推进与事件写入同事务
func NewReader(ctx context.Context, dsn, slotName, publication string, store *EventStore) (*Reader, error) {
	if store == nil {
		return nil, errors.New("EventStore 不能为空")
	}

	conn, startLSN, created, err := ensureSlot(ctx, dsn, slotName)
	if err != nil {
		return nil, err
	}

	// 槽已存在：恢复顺序 = 优先 cdc_checkpoints 事务性断点，退回 slot 的 confirmed_flush_lsn
	if !created {
		if lsnStr, ok, lerr := store.LoadCheckpoint(ctx); lerr != nil {
			conn.Close(ctx)
			return nil, fmt.Errorf("读取断点失败: %w", lerr)
		} else if ok {
			parsed, perr := pglogrepl.ParseLSN(lsnStr)
			if perr != nil {
				conn.Close(ctx)
				return nil, fmt.Errorf("解析断点 LSN 失败: %w", perr)
			}
			startLSN = parsed
			slog.Info("复制槽已存在，从断点恢复", "slot", slotName, "start_lsn", startLSN.String())
		} else {
			lsn, ok, qerr := querySlotLSN(ctx, dsn, slotName)
			if qerr != nil {
				conn.Close(ctx)
				return nil, fmt.Errorf("查询复制槽位点失败: %w", qerr)
			}
			if ok {
				startLSN = lsn
			}
			slog.Info("复制槽已存在，无断点，从槽位点恢复", "slot", slotName, "start_lsn", startLSN.String())
		}
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
		store:       store,
	}, nil
}

// Run 消息循环：解码 → 按事务边界累积 → 提交时落库 + 推进位点。ctx 取消时干净返回。
func (r *Reader) Run(ctx context.Context) error {
	// 累积当前事务里关心的变更，commit 时整体落库
	var pending []Event

	for {
		msg, err := r.conn.ReceiveMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// 主动取消 = 正常退出，不算错误
				return nil
			}
			return err
		}

		// CopyData 信封解析 + switch 消息类型
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			switch m.Data[0] {
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(m.Data[1:])
				if err != nil {
					return err
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

				case *pglogrepl.BeginMessage:
					pending = pending[:0]

				case *pglogrepl.InsertMessage:
					if ev, ok := r.buildEvent("INSERT", xld.WALStart, v.RelationID, v.Tuple); ok {
						pending = append(pending, ev)
					}
				case *pglogrepl.UpdateMessage:
					if ev, ok := r.buildEvent("UPDATE", xld.WALStart, v.RelationID, v.NewTuple); ok {
						pending = append(pending, ev)
					}
				case *pglogrepl.DeleteMessage:
					if ev, ok := r.buildEvent("DELETE", xld.WALStart, v.RelationID, v.OldTuple); ok {
						pending = append(pending, ev)
					}

				case *pglogrepl.CommitMessage:
					commitLSN := v.CommitLSN
					// 事件 + 断点同一个事务落库；失败直接退出，靠重启从断点重放
					if err := r.store.Append(ctx, pending, commitLSN.String()); err != nil {
						return err
					}
					r.confirmed = commitLSN
					// 推进 slot，让 PG 可以回收这段 WAL
					if err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{
						WALWritePosition: commitLSN,
						WALFlushPosition: commitLSN,
						WALApplyPosition: commitLSN,
					}); err != nil {
						return err
					}
					pending = pending[:0]
				}

			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if err != nil {
					slog.Warn("解析失败", "err", err)
					continue
				}
				if pkm.ReplyRequested {
					if err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{
						WALWritePosition: r.confirmed,
						WALFlushPosition: r.confirmed,
						WALApplyPosition: r.confirmed,
					}); err != nil {
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

// buildEvent 把一条行变更组装成 Event。表结构未知时返回 ok=false。
func (r *Reader) buildEvent(op string, lsn pglogrepl.LSN, relID uint32, tuple *pglogrepl.TupleData) (Event, bool) {
	rel := r.relations[relID]
	if rel == nil {
		slog.Warn("未知表结构，跳过", "rel_id", relID)
		return Event{}, false
	}
	return Event{
		LSN:       lsn.String(),
		Operation: op,
		Schema:    rel.Namespace,
		Table:     rel.RelationName,
		Columns:   tupleToColumns(rel, tuple),
	}, true
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
