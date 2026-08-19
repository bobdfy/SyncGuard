package mock_test

import (
	"context"
	"testing"

	"github.com/bobdfy/syncguard/internal/source/mock"
)

func TestFetch(t *testing.T) {
	g := mock.NewGenerator(10)

	records, nextCursor, hasMore, err := g.Fetch(context.Background(), "", 4)
	if err != nil {
		t.Fatalf("Fetch 出错：%v", err)
	}

	if len(records) != 4 {
		t.Errorf("期待每页数据为4, 实际每页%d", len(records))
	}

	if nextCursor != "record_0004" {
		t.Errorf("期望下一页断点游标为record_0004, 实际为%s", nextCursor)
	}

	if !hasMore {
		t.Errorf("还存在更多数据, 期望hasmore为True, 实际为 %v", hasMore)
	}

	records2, _, hasMore2, err1 := g.Fetch(context.Background(), nextCursor, 4)
	if err1 != nil {
		t.Fatalf("第2页Fetch 出错：%v", err1)
	}

	if records2[0].ID != "record_0005" {
		t.Errorf("期望第二页起始ID为record_0005, 实际为%s", records2[0].ID)
	}

	if !hasMore2 {
		t.Errorf("还存在更多数据, 期望hasmore为True, 实际为 %v", hasMore2)
	}

	_, _, _, err2 := g.Fetch(context.Background(), "records_0010", 4)
	if err2 == nil {
		t.Errorf("期望不存在游标返回错误, 实际没有")
	}

}
