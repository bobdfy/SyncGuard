package repository

import (
	"context"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/model"
)

// 编译期检查：internalSyncDest 实现了 engine.Destination 接口
var _ engine.Destination = (*internalSyncDest)(nil)

// internalSyncDest 内部存储适配器：把 SyncedStore 包装成完整的 engine.Destination。
//
// 为什么需要它：SyncedStore 在清理死代码后只剩「写数据」能力，不再是完整接口；
// 但 JobDestination.store 字段现在是 engine.Destination 类型，需要内部存储
// 也以接口身份出现。这个适配器只真实现 Save（委托 SyncedStore.SaveWithUser，
// 带上用户/连接归属），其余 4 个方法空实现（断点/进度由 JobDestination 管）。
type internalSyncDest struct {
	store        *SyncedStore
	userID       int
	connectionID int
}

// Save 委托 SyncedStore.SaveWithUser 写内部库，带用户/连接归属（供隔离与对账）。
func (d *internalSyncDest) Save(ctx context.Context, records []model.Record) error {
	return d.store.SaveWithUser(ctx, d.userID, d.connectionID, records)
}

// NewInternalDest 创建「写内部存储」的 Destination（供 worker 装配内部目标用）。
func NewInternalDest(store *SyncedStore, userID, connectionID int) engine.Destination {
	return &internalSyncDest{store: store, userID: userID, connectionID: connectionID}
}

// 以下 4 个方法空实现：内部路径的断点/进度由 JobDestination 管理。

func (d *internalSyncDest) GetCheckpoint(ctx context.Context, taskName string) (string, error) {
	return "", nil
}

func (d *internalSyncDest) UpdateCheckpoint(ctx context.Context, taskName string, cursor string) error {
	return nil
}

func (d *internalSyncDest) CreateBatch(ctx context.Context) (int64, error) {
	return 0, nil
}

func (d *internalSyncDest) CompleteBatch(ctx context.Context, batchID int64, status string, totalCount int) error {
	return nil
}

// Close 无资源可释放。
func (d *internalSyncDest) Close() error { return nil }
