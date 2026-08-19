package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bobdfy/syncguard/internal/engine"
	"github.com/bobdfy/syncguard/internal/model"
)

/*
Source 实现 engine.Source 接口，把 GitHub Issues 当成数据源。

GitHub Issues API 是 页码-based（?page=1&per_page=100），但 engine.Source
接口是 cursor-based（cursor → nextCursor）。V3 的适配方案：

	cursor     = 页码字符串，空字符串 = 第 1 页，"1" = 第 2 页，"2" = 第 3 页
	nextCursor = 页码+1 转字符串
	hasMore    = 本页结果数 == limit（GitHub 不返回总数，只能猜）

*/

type Source struct {
	owner      string       // GitHub 仓库所有者，如 "torvalds"
	repo       string       // GitHub 仓库名，如 "linux"
	httpClient *http.Client // 发 HTTP 请求的客户端，设了 30s 超时
	token      string       // GitHub Personal Access Token，从环境变量 GITHUB_TOKEN 读取，可选
}

/*
NewSource 创建 GitHub Source。

owner/repo 从 connections 表的 source_url 字段解析得到。
source_url 的格式是 "owner/repo"，如 "torvalds/linux"、
"golang/go"。

例子：

	src := NewSource("golang", "go")
	// src 会去请求 https://api.github.com/repos/golang/go/issues
*/
func NewSource(owner, repo string) *Source {
	return &Source{
		owner:      owner,
		repo:       repo,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		token:      os.Getenv("GITHUB_TOKEN"), // 没设环境变量就是空字符串，走未认证请求（60 req/h）
	}
}

/*
SourceError 包装 GitHub API 返回的 HTTP 错误状态码。

它实现了 error 接口，Worker 那边用 errors.As 判断具体是哪种错误：

	var srcErr *SourceError
	if errors.As(err, &srcErr) {
	    switch srcErr.StatusCode {
	    case 403, 429: // 限流 → 等一会重试
	    case 404:       // 仓库不存在 → 别重试了
	    }
	}
*/
type SourceError struct {
	StatusCode int // GitHub 返回的 HTTP 状态码（403/404/429 等）
}

// Error 实现 error 接口，让 SourceError 可以像普通 error 一样被返回。
func (e *SourceError) Error() string {
	return fmt.Sprintf("GitHub API 返回状态码: %d", e.StatusCode)
}

/*
Fetch 从 GitHub 拉一页 Issue，转成 []model.Record 返回。

参数：
  - ctx:   上下文（超时控制、取消信号）
  - cursor: 页码字符串，空 = 第 1 页
  - limit:  每页多少条（同时传给 GitHub API 的 per_page）

返回：
  - records:    本页 Record 列表
  - nextCursor: 下一页页码字符串，没有下一页时为空
  - hasMore:    是否还有更多数据
  - err:        错误（网络问题 / API 报错）
*/
func (s *Source) Fetch(ctx context.Context, cursor string, limit int) ([]model.Record, string, bool, error) {
	//
	// 第 1 步：cursor（页码字符串）→ page（整数）
	page := 1
	if cursor != "" {
		page, _ = strconv.Atoi(cursor)
	}

	// 第 2 步：拼接 GitHub Issues API 的 URL
	url := fmt.Sprintf(
		"https://api.github.com/repos/%s/%s/issues?state=all&sort=updated&direction=asc&per_page=%d&page=%d",
		s.owner, s.repo, limit, page,
	)

	// 第 3 步：创建 HTTP GET 请求
	// 用 NewRequestWithContext(ctx, ...) 这样 ctx 超时或取消时，HTTP 请求也会被中断。
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, "", false, fmt.Errorf("创建请求失败: %w", err)
	}

	// Accept 头告诉 GitHub 我们要 v3 版本的 API 格式
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	// 如果配了 GITHUB_TOKEN 环境变量，加上 Authorization 头
	// 未认证：60 req/h，认证后：5000 req/h
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}

	// 第 4 步：发送请求
	resp, err := s.httpClient.Do(req)
	if err != nil {
		// 网络超时、DNS 解析失败、连接被拒等情况
		// Worker 收到这个错误后会 NACK → 进 DLQ，之后可以重试
		return nil, "", false, fmt.Errorf("请求 GitHub API 失败: %w", err)
	}
	defer resp.Body.Close() // 函数返回时自动关闭响应体，防止连接泄漏

	//
	// 第 5 步：检查 HTTP 状态码
	// GitHub API 的典型错误码：
	//   200      — 正常
	//   403/429  — 限流（Rate Limit），等一会能恢复，可重试
	//   404      — 仓库不存在或改名，不可重试
	//   301      — 仓库被重定向
	// 非 200 统一包装成 SourceError，Worker 那边根据 StatusCode 决定
	// 是重试还是放弃。
	if resp.StatusCode != 200 {
		srcErr := &SourceError{StatusCode: resp.StatusCode}
		// 404：仓库不存在或改名，重试无意义，包装成永久错误让 Worker 直接进 DLQ。
		// 403/429：限流，等一会能恢复，保持普通错误走退避重投。
		if resp.StatusCode == 404 {
			return nil, "", false, fmt.Errorf("%w: %w", engine.ErrNonRetryable, srcErr)
		}
		return nil, "", false, srcErr
	}

	// 第 6 步：把 HTTP 响应体解析成 Go 结构体切片

	// GitHub 返回的是一个 JSON 数组：
	//   [ {issue1}, {issue2}, {issue3}, ... ]
	// json.NewDecoder(resp.Body).Decode(&issues)
	//   - 流式解析：边读边解，不把整个 Body 一次性加载到内存
	//   - 自动匹配 JSON 字段名 → 结构体字段的 json tag
	var issues []gitHubIssue
	if err := json.NewDecoder(resp.Body).Decode(&issues); err != nil {
		return nil, "", false, fmt.Errorf("解析响应失败: %w", err)
	}

	// 第 7 步：Issue → Record 转换
	//
	records := make([]model.Record, 0, len(issues))
	for _, issue := range issues {

		//
		// 7a. 过滤 Pull Request
		//
		// GitHub 的 Issue 和 PR 共享同一个编号。
		// 比如 Issue #42 和 PR #42 不可能同时存在，它们共用编号空间。
		// /issues 接口把 Issue 和 PR 一起返回。
		//
		// 区分方法：
		//   - Issue: "pull_request" 字段是 null → Go 里解析为 nil
		//   - PR:    "pull_request" 字段是 {}   → Go 里解析为非 nil 的指针
		//
		// 所以 issue.PullRequest != nil → 这是 PR，跳过。
		//
		if issue.PullRequest != nil {
			continue
		}

		// 7b. 组装 Data（Issue 的业务信息，存到 JSONB）

		// gitHubIssue 是全量的（字段和 API 返回值一一对应），
		// gitHubIssueData 是精简的（只保留对账需要的字段）。
		//
		// 转换过程中：
		//   - labels 从 []gitHubLabel 拍平成 []string（只要 name）
		//   - body 截断到 500 字（避免 JSONB 撑爆）
		issueData := gitHubIssueData{
			Title:    issue.Title,
			State:    issue.State,
			User:     issue.User.Login,
			Labels:   makeLabels(issue.Labels),
			Comments: issue.Comments,
			Body:     truncate(issue.Body, 500),
		}
		data, err := json.Marshal(issueData)
		if err != nil {
			continue // 极少发生（结构体全是简单类型），跳过不影响其他
		}

		//
		// 7c. 构造 Record

		// Record 是 engine.Source 接口要求返回的标准格式。
		//
		// 字段映射：
		//   ID      = "issue_{number}"  如 issue_42
		//   Version = updated_at.Unix() 秒级 Unix 时间戳
		//   Data    = 上面组装的 issueData JSON
		//
		// Version 用秒级时间戳的原因：
		//   GitHub 的 updated_at 精度是秒级，同一秒内两次更新的概率极低。
		//   对账时如果版本一样内容不同 → CONTENT_MISMATCH（Hash 比较兜底）。
		//
		records = append(records, model.Record{
			ID:        fmt.Sprintf("issue_%d", issue.Number),
			Version:   int(issue.UpdatedAt.Unix()),
			UpdatedAt: issue.UpdatedAt,
			Data:      data,
		})
	}

	//
	// 第 8 步：判断是否还有下一页 + 生成 nextCursor
	//
	// GitHub Issues API 不返回总数，只能靠"本页结果数 == limit"推测：
	//
	//   结果数 == limit → 可能还有下一页（最后一页恰好整除时会多一次空请求，无害）
	//   结果数 <  limit → 绝对是最后一页
	hasMore := len(issues) == limit
	nextCursor := ""
	if hasMore {
		nextCursor = strconv.Itoa(page + 1)
	}

	return records, nextCursor, hasMore, nil
}

/*
gitHubIssue 对应 GitHub Issues API 返回的单条 Issue JSON。

示例 JSON：

	{
	  "number": 42,
	  "title": "Kernel panic on boot",
	  "state": "open",
	  "user": { "login": "torvalds" },
	  "labels": [{ "name": "bug" }, { "name": "high-priority" }],
	  "comments": 3,
	  "body": "When I boot the kernel with CONFIG_X...",
	  "updated_at": "2026-08-10T12:00:00Z",
	  "pull_request": null
	}

json tag 的作用：告诉 json.Decode "JSON 里的字段名 'number' 对应 Go 里的 Number"。
Go 的字段名必须是首字母大写（导出），json tag 用小写蛇形匹配 JSON 字段。
*/
type gitHubIssue struct {
	Number    int           `json:"number"`
	Title     string        `json:"title"`
	State     string        `json:"state"`
	User      gitHubUser    `json:"user"`
	Labels    []gitHubLabel `json:"labels"`
	Comments  int           `json:"comments"`
	Body      string        `json:"body"`
	UpdatedAt time.Time     `json:"updated_at"`

	// PullRequest — 整个文件最关键的一个字段。
	//
	// JSON 里 Issue 时是 null，PR 时是 {"url": "..."}。
	// 用 *struct{} 类型表示："我完全不关心里面是什么，只管是不是 nil"。
	// nil = Issue，非 nil = PR。
	//
	// 为什么不用 *PullRequestDetail？
	// 因为我们只做过滤不做解析，*struct{} 和 *PullRequestDetail 在 nil 判断上
	// 完全一样，但 *struct{} 更省内存（空结构体 0 字节）。
	PullRequest *struct{} `json:"pull_request"`
}

// gitHubUser 对应 JSON 里的 "user": {"login": "xxx"}
type gitHubUser struct {
	Login string `json:"login"`
}

// gitHubLabel 对应 JSON 里的 "labels": [{"name": "bug"}, ...]
type gitHubLabel struct {
	Name string `json:"name"`
}

/*
gitHubIssueData 是精簡後的 Issue 資料結構，存進 Record.Data（JSONB）。

和 gitHubIssue 的區別：
  - gitHubIssue：原樣接收 GitHub API 的完整 JSON，字段一一對應
  - gitHubIssueData：只保留對賬需要的欄位，去掉網址/ID 等噪音，body 截斷

存進 synced_records.data 的 JSON 大概長這樣：

	{
	  "title": "Kernel panic on boot",
	  "state": "open",
	  "user": "torvalds",
	  "labels": ["bug", "high-priority"],
	  "comments": 3,
	  "body": "When I boot the kernel with CONFIG_X..."
	}
*/
type gitHubIssueData struct {
	Title    string   `json:"title"`
	State    string   `json:"state"`
	User     string   `json:"user"`
	Labels   []string `json:"labels"`
	Comments int      `json:"comments"`
	Body     string   `json:"body"`
}

// makeLabels 把 []gitHubLabel 拍平成 []string，只保留 Name。
// 输入：[{Name:"bug"}, {Name:"high-priority"}]
// 输出：["bug", "high-priority"]
func makeLabels(labels []gitHubLabel) []string {
	result := make([]string, len(labels))
	for i, l := range labels {
		result[i] = l.Name
	}
	return result
}

// truncate 截斷字串，超過 maxLen 的部分用 "..." 替換。
// 防止超長 Issue body（如 Linux 內核討論帖可能有幾十 KB）把 JSONB 欄位撐爆。
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
