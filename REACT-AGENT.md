# ReAct Agent 完整实现分析

> 基于 Nightingale aiagent 模块的 Go 实现，从 HTTP 请求到 LLM 调用的全链路拆解。

---

## 目录

1. [整体架构](#1-整体架构)
2. [核心概念](#2-核心概念)
3. [入口：Run()](#3-入口run)
4. [流式启动器：runWithStream()](#4-流式启动器runwithstream)
5. [准备阶段：executeReAct()](#5-准备阶段executereact)
6. [ReAct 循环：runReActLoop()](#6-react-循环runreactloop)
7. [LLM 调用层：callLLMAuto](#7-llm-调用层callllmauto)
8. [工具执行：executeTool()](#8-工具执行executetool)
9. [收尾：executeReActWithDone()](#9-收尾executereactwithdone)
10. [数据流全景](#10-数据流全景)
11. [一次完整的对话示例](#11-一次完整的对话示例)
12. [核心设计亮点](#12-核心设计亮点)
13. [关键代码索引](#13-关键代码索引)

---

## 1. 整体架构

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           Router 层 (HTTP API)                           │
│                center/router/router_ai_assistant.go                      │
│                                                                          │
│  构建 AgentConfig → 创建 Agent → 创建 streamChan → 调用 Run()            │
│  for range streamChan → 消费 chunk → streamBus.Append(Redis) → SSE → 前端│
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                           Agent 层 (aiagent 包)                          │
│                                                                          │
│  Run() → runWithStream() → executeReActWithDone()                        │
│                              ↓                                           │
│                         executeReAct() → 准备消息                          │
│                              ↓                                           │
│                         runReActLoop() → 核心循环                         │
│                              ↓                                           │
│                         callLLMAuto() → LLM 调用                          │
│                              ↓                                           │
│                         executeTool() → 工具执行                          │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                           LLM Provider 层 (llm 包)                       │
│                                                                          │
│  GenerateStream() → doHTTPStreamWithRetry() → HTTP SSE 流                 │
│                  ↓                                                       │
│             streamResponse() → 逐行读 SSE → 解析 JSON → ch <- chunk       │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 核心概念

### 2.1 两个 StreamChunk 类型

| 类型 | 所属包 | 定义位置 | 作用 |
|------|--------|----------|------|
| `llm.StreamChunk` | `aiagent/llm/` | `llm.go:80-86` | LLM provider 返回的原始 token，含 `Content`, `ToolCalls`, `Done`, `Error` |
| `*aiagent.StreamChunk` | `aiagent/` | `types.go:149-159` | Agent 层包装后的消息，含 `Type`, `Delta`, `Content`, `Metadata`, `Timestamp` |

### 2.2 两层 Channel 的关系

```
LLM provider 层 (llm 包)         Agent 层 (aiagent 包)              Router 层
                                                                    
claude.streamResponse()                                               
  │                                                                  
  │ 读 SSE → 解析 → 发 token                                         
  │                                                                  
  ├── ch <- llm.StreamChunk{Content:"你"}  ← stream                 
  │    │                                        (chan llm.StreamChunk)
  │    ▼                                                             
  │  callLLMWithStreamOutput()                                       
  │    │                                                             
  │    ├── fullContent.WriteString("你")                             
  │    │                                                             
  │    └── streamChan <- &aiagent.StreamChunk{                      
  │              Type: "text", Delta: "你", Content: "你"}          
  │           ↑                                                       
  │           streamChan                                              
  │           (chan *aiagent.StreamChunk)                              
  │             │                                                     
  │             ▼                                                    
  │           router_ai_assistant.go                                  
  │             for chunk := range streamChan                         
  │               → streamBus.Append(Redis)                          
  │               → SSE → 浏览器                                      
```

**关键区别**：

| | `stream` | `streamChan` |
|---|---|---|
| **所属层** | LLM provider 层 | Agent 层 |
| **类型** | `<-chan llm.StreamChunk` | `chan *aiagent.StreamChunk` |
| **谁创建** | `GenerateStream` 内部 `make(chan, 100)` | router 层 `make(chan *StreamChunk, 100)` |
| **谁写** | `streamResponse` goroutine | `callLLMWithStreamOutput` + `runReActLoop` |
| **谁读** | `callLLMWithStreamOutput` | router 层的 `for range` |
| **语义** | LLM 原始 token | 包装后发给前端的消息 |

### 2.3 streamChan 里的数据类型

| Type 常量 | 值 | 谁往 streamChan 里写 | 含义 |
|-----------|-----|---------------------|------|
| `StreamTypeText` | `"text"` | `callLLMWithStreamOutput` | LLM 输出的 token 增量 |
| `StreamTypeToolCall` | `"tool_call"` | `runReActLoop` 第 75 行 | LLM 决定调用某工具 |
| `StreamTypeToolResult` | `"tool_result"` | `runReActLoop` 第 91 行 | 工具执行的结果 |
| `StreamTypeDone` | `"done"` | `executeReActWithDone` 第 197 行 | 整个执行完成 |
| `StreamTypeError` | `"error"` | `executeReActWithDone` 第 187 行 | 执行出错 |

---

## 3. 入口：Run()

**位置**: `agent.go:110-177`

```go
func (a *Agent) Run(ctx context.Context, req *AgentRequest) (*AgentResponse, error) {
    // 1. 创建带超时的 context
    timeoutCtx, cancel := context.WithTimeout(parentCtx, time.Duration(a.cfg.Timeout)*time.Millisecond)

    // 2. 加载技能（Skills）
    activeSkills := a.selectAndLoadSkills(timeoutCtx, req)

    // 3. 装配工具列表：cfg.Tools → skill 工具 → MCP 工具
    tools := append([]AgentTool(nil), a.cfg.Tools...)
    tools = a.appendSkillTools(tools, activeSkills)
    tools = a.appendMCPTools(mcpCtx, tools)

    // 4. 构造 runCtx（每轮的运行期状态）
    rc := &runCtx{skills: activeSkills, tools: tools}

    // 5. 分流：流式 / 非流式
    if req.StreamChan != nil {           // 调用方传了 channel
        return a.runWithStream(timeoutCtx, cancel, req, rc)
    }
    if a.cfg.Stream {                    // 配置默认流式，自动创建 channel
        req.StreamChan = make(chan *StreamChunk, 100)
        return a.runWithStream(timeoutCtx, cancel, req, rc)
    }

    // 非流式：同步调用
    defer cancel()
    switch a.cfg.AgentMode {
    case AgentModePlanReAct: return a.executePlanReAct(...)
    case AgentModeDirect:    return a.executeDirect(...)
    default:                 return a.executeReAct(...)
    }
}
```

### 流程：

```
Run()
  │
  ├── selectAndLoadSkills()     → 选择激活哪些技能
  ├── appendSkillTools()        → 技能声明的工具
  ├── appendMCPTools()          → MCP 服务器发现的工具
  ├── rc = &runCtx{skills, tools}
  │
  ├── StreamChan != nil? ────YES────→ runWithStream() → goroutine
  │                                          │
  │                                    return (nil, nil) 立即返回
  │
  └── StreamChan == nil? ────NO────→ executeReAct() → 同步阻塞
                                           │
                                     return AgentResponse
```

---

## 4. 流式启动器：runWithStream()

**位置**: `agent.go:181-203`

```go
func (a *Agent) runWithStream(ctx context.Context, cancel context.CancelFunc,
    req *AgentRequest, rc *runCtx) (*AgentResponse, error) {

    streamChan := req.StreamChan

    go func() {
        defer close(streamChan)     // goroutine 退出时关闭 channel → 调用方 range 退出
        if cancel != nil {
            defer cancel()          // goroutine 退出时释放超时 context
        }

        switch a.cfg.AgentMode {
        case AgentModePlanReAct:
            a.executePlanReActWithDone(ctx, req, rc)
        case AgentModeDirect:
            a.executeDirectWithDone(ctx, req, rc)
        default: // AgentModeReAct
            a.executeReActWithDone(ctx, req, rc)
        }
    }()

    return nil, nil  // 立即返回！不阻塞
}
```

### 设计要点：

1. **`defer close(streamChan)`** — 不管正常结束还是 panic，channel 一定被关闭，调用方 `for range` 一定退出
2. **`defer cancel()`** — goroutine 退出时释放超时 context，不泄漏
3. **返回 `(nil, nil)`** — 真正的执行结果通过 channel 传给调用方
4. **`*WithDone` 模式** — 三种执行模式（ReAct / Direct / PlanReAct）分别有自己的 `WithDone` 包装器，统一在 goroutine 里调度

### 此时有三个 goroutine 在并发：

```
主 goroutine (router 层):   for chunk := range streamChan   ← 等数据
后台 goroutine 1:           executeReActWithDone()           ← 干活的
LLM goroutine:              streamResponse()                 ← 读 SSE
```

---

## 5. 准备阶段：executeReAct()

**位置**: `react.go:118-171`

```go
func (a *Agent) executeReAct(ctx context.Context, req *AgentRequest, rc *runCtx) *AgentResponse {
    // 1. 构建用户消息
    userMessage, err := a.buildUserMessage(req)
    // 输出类似:
    // "Please complete the task based on your instructions.
    //  **User Request**: 当前 CPU 使用率多少？
    //  **Context**: - alert_id: 12345
    //  Use the available tools to gather information and complete the task."

    // 2. 构建系统提示词（自动包含 Skills + 工具列表 + 环境信息）
    systemPrompt := a.buildReActSystemPrompt(rc)
    // 输出类似:
    // "你是一个 AI 运维助手...
    //  可用工具:
    //  - query_prometheus: 查询 Prometheus 指标...
    //  环境信息: 当前时间 2026-06-01..."

    // 3. 组装消息列表
    messages := []ChatMessage{
        {Role: "system", Content: systemPrompt},
    }
    if len(req.History) > 0 {
        messages = append(messages, req.History...)
    }
    messages = append(messages, ChatMessage{Role: "user", Content: userMessage})

    // 4. 计算最大迭代次数（取 skill 声明的最高值）
    maxIter := a.cfg.MaxIterations  // 默认 25
    for _, sk := range rc.skills {
        if sk.Metadata.MaxIterations > maxIter {
            maxIter = sk.Metadata.MaxIterations  // skill 可以声明更高
        }
    }

    // 5. 进入 ReAct 循环
    return a.runReActLoop(ctx, req, messages, &ReActLoopConfig{
        MaxIterations:        maxIter,
        TimeoutMessage:       "agent execution timeout",
        LogPrefix:            "AI Agent",
        Tools:                rc.tools,
        StreamChan:           req.StreamChan,
        RequestID:            requestID,
        IsComplete:           func(action string) bool { return action == ActionFinalAnswer },
        ExtractPartialResult: true,
    })
}
```

### 消息列表结构：

```
system: <系统提示词 + Skills 知识 + 工具列表 + 环境信息>
   (可选的) user/assistant 历史对话...
user:  <用户当前输入 + 上下文参数>
```

---

## 6. ReAct 循环：runReActLoop()

**位置**: `react.go:12-115`

这是整个 Agent 的核心。每次迭代就是标准的三步：**Thought → Action → Observation**。

```go
func (a *Agent) runReActLoop(ctx context.Context, req *AgentRequest,
    messages []ChatMessage, config *ReActLoopConfig) *AgentResponse {

    resp := &AgentResponse{Steps: []ReActStep{}}
    streaming := config.StreamChan != nil

    for iteration := 0; iteration < config.MaxIterations; iteration++ {
```

### 6.1 检查超时

```go
        select {
        case <-ctx.Done():
            resp.Error = config.TimeoutMessage
            return resp
        default:  // 没信号就继续
        }
```

### 6.2 调用 LLM

```go
        response, err := a.callLLMAuto(ctx, messages, config.StreamChan, config.RequestID,
            []string{"Observation:"})  // ← stop 序列
```

传给 LLM 的消息包含：
- system 提示词（不可变）
- 所有历史对话（不可变）
- 上一轮的 Observation（本轮新加）

**`stop: ["Observation:"]`** 的作用：让 LLM 写完 `Action Input` 就停，不要自己脑补工具结果。

LLM 返回格式：

```
Thought: 用户想知道 CPU 使用率，我查一下 Prometheus
Action: query_prometheus
Action Input: {"promql": "avg(rate(cpu_seconds_total[5m]))"}
```

如果是最后一步：

```
Thought: CPU 使用率 45.2%，正常
Action: Final Answer
Action Input: 当前 CPU 使用率为 45.2%
```

### 6.3 解析 ReAct 响应

```go
        step := a.parseReActResponse(response)
        // step = ReActStep{Thought, Action, ActionInput, Observation}
```

`parseReActResponse` 解析的字段：

| 格式 | 对应字段 |
|------|---------|
| `Thought: 我想...` | `step.Thought` |
| `Action: query_prometheus` | `step.Action` |
| `Action Input: {"promql":"..."}` | `step.ActionInput` |
| `Final Answer: 结果是...` | 简写，归一化为 `Action="Final Answer"` |

### 6.4 检查是否完成

```go
        if config.IsComplete(step.Action) {  // action == "Final Answer"?
            resp.Content = step.ActionInput
            resp.Success = true
            return resp
        }

        if step.Action == "" {  // 没有 Action，把原始响应当作结果
            resp.Content = response
            resp.Success = true
            return resp
        }
```

### 6.5 发送 ToolCall 事件（流式模式）

```go
        if streaming {
            config.StreamChan <- &StreamChunk{
                Type:      StreamTypeToolCall,
                Content:   step.Action,
                Metadata:  map[string]interface{}{"input": step.ActionInput},
                RequestID: config.RequestID,
                Timestamp: time.Now().UnixMilli(),
            }
        }
        // 前端收到 → 显示 "🔧 正在调用 query_prometheus..."
```

### 6.6 执行工具

```go
        observation := a.executeTool(ctx, step.Action, step.ActionInput, req, config.Tools)
```

查找工具 → 调用 handler → 返回结果字符串。

```
Action: query_prometheus
Action Input: {"promql": "avg(rate(cpu_seconds_total[5m]))"}
     ↓
executeTool → 查找 "query_prometheus" 注册的 handler
     → handler(ctx, deps, args, params)
     → 返回 "45.2%"
     ↓
observation = "45.2%"
```

### 6.7 发送 ToolResult 事件（流式模式）

```go
        if streaming {
            config.StreamChan <- &StreamChunk{
                Type:      StreamTypeToolResult,
                Content:   observation,
                Metadata:  map[string]interface{}{"tool": step.Action},
            }
        }
        // 前端收到 → 显示 "📊 查询结果: 45.2%"
```

### 6.8 将本轮结果追加到消息列表

```go
        observationMsg := fmt.Sprintf("Observation: %s", observation)

        messages = append(messages, ChatMessage{Role: "assistant", Content: response})
        //  ↑ 保存 LLM 本次的 Thought/Action 原文

        messages = append(messages, ChatMessage{Role: "user", Content: observationMsg})
        //  ↑ 把工具结果包装成 Observation 喂给 LLM
```

**工具结果不是通过 channel 回到 LLM 的，是通过 messages！**

```
工具结果 "45.2%"
  │
  ├──→ streamChan (Type: tool_result) ──→ 前端展示
  │
  └──→ messages = append(..., "Observation: 45.2%")
         │
         ▼ 下一轮迭代
         callLLMAuto(ctx, messages, ...)
           ↓
         LLM 看到 Observation: 45.2%
            → 判断：正常 → Final Answer
            → 或：偏高 → 再查历史趋势
```

### 6.9 回到 6.1，继续循环

当 `iteration >= MaxIterations` 还没有 Final Answer：

```go
    // 达到最大迭代次数
    resp.Error = fmt.Sprintf("reached maximum iterations (%d)", config.MaxIterations)
    if config.ExtractPartialResult && len(resp.Steps) > 0 {
        lastStep := resp.Steps[len(resp.Steps)-1]
        resp.Content = fmt.Sprintf("Analysis incomplete. Last thought: %s", lastStep.Thought)
    }
    return resp
}
```

---

## 7. LLM 调用层：callLLMAuto

### 7.1 callLLMAuto — 自动分发

**位置**: `llm_caller.go:93-98`

```go
func (a *Agent) callLLMAuto(ctx context.Context, messages []ChatMessage,
    streamChan chan *StreamChunk, requestID string, stop []string) (string, error) {
    if streamChan != nil {
        return a.callLLMWithStreamOutput(ctx, messages, streamChan, requestID, stop)
    }
    return a.callLLM(ctx, messages, stop)
}
```

- `streamChan != nil` → 流式：`callLLMWithStreamOutput`
- `streamChan == nil` → 非流式：`callLLM`

### 7.2 callLLMWithStreamOutput — 流式 LLM 调用

**位置**: `llm_caller.go:43-80`

```go
func (a *Agent) callLLMWithStreamOutput(ctx context.Context, messages []ChatMessage,
    streamChan chan *StreamChunk, requestID string, stop []string) (string, error) {

    // 调 LLM provider 的流式 API
    stream, err := a.llmClient.GenerateStream(ctx, buildLLMRequest(messages, stop))
    //    ↑
    //    └── <-chan llm.StreamChunk（LLM 层的 channel）

    var fullContent strings.Builder
    for chunk := range stream {                     // ← 从 LLM channel 取数据
        if chunk.Error != nil {
            go drainStream(stream)                  // 排干 provider channel 防泄漏
            return fullContent.String(), err
        }
        if chunk.Content != "" {
            fullContent.WriteString(chunk.Content)  // 累积完整文本

            streamChan <- &StreamChunk{             // ← 转发给 Agent channel
                Type:      StreamTypeText,          //     标记为文本 token
                Delta:     chunk.Content,           //     本次增量
                Content:   fullContent.String(),     //     累积内容
                RequestID: requestID,
                Timestamp: time.Now().UnixMilli(),
            }
        }
        if chunk.Done {
            break
        }
    }
    return fullContent.String(), nil  // 返回完整文本给 ReAct 循环解析
}
```

### 7.3 callLLM — 非流式 LLM 调用

```go
func (a *Agent) callLLM(ctx context.Context, messages []ChatMessage, stop []string) (string, error) {
    resp, err := a.llmClient.Generate(ctx, buildLLMRequest(messages, stop))
    return resp.Content, nil
}
```

### 7.4 drainStream — 防 goroutine 泄漏

**位置**: `llm_caller.go:85-88`

```go
func drainStream(stream <-chan llm.StreamChunk) {
    for range stream {
    }  // 消费完所有剩下的 chunk 直到 channel 被 close
}
```

当流式调用途中发生错误时，provider 侧 `streamResponse` 的 goroutine 还在往 channel 里 blocking send，如果不排干它，那个 goroutine 就永久卡死。`drainStream` 在另一个 goroutine 里消费完剩下的 chunk 让 provider 的 goroutine 正常退出。

---

## 8. 工具执行：executeTool()

**位置**: `tool_executor.go`

```go
func (a *Agent) executeTool(ctx context.Context, action, actionInput string,
    req *AgentRequest, tools []AgentTool) string {

    // 根据 Action 名称查找工具
    tool := findToolByName(action, tools)

    switch tool.Type {
    case ToolTypeBuiltin:
        return executeBuiltinTool(ctx, tool, args, toolDeps, params)
    case ToolTypeHTTP:
        return executeHTTPTool(tool, args)
    case ToolTypeMCP:
        return executeMCPTool(ctx, tool, args)
    case ToolTypeProcessor:
        return externalToolHandler(ctx, tool, args, req)
    case ToolTypeSkill:
        return externalToolHandler(ctx, tool, args, req)
    }
}
```

工具的注册方式：各 tool 包在 `init()` 中注册

```go
// tools/alert.go
func init() {
    register(defs.SearchActiveAlerts, searchActiveAlerts)
    // defs 里定义元数据（名称、描述、参数），handler 里写实际逻辑
}
```

---

## 9. 收尾：executeReActWithDone()

**位置**: `react.go:170-204`

```go
func (a *Agent) executeReActWithDone(ctx context.Context, req *AgentRequest, rc *runCtx) {
    streamChan := req.StreamChan
    requestID := ...
    resp := a.executeReAct(ctx, req, rc)     // ← 同步调用 ReAct 循环

    if resp.Error != "" && !resp.Success {
        streamChan <- &StreamChunk{
            Type: StreamTypeError, Error: resp.Error, Done: true,
        }
        return
    }

    streamChan <- &StreamChunk{
        Type:    StreamTypeDone,
        Content: resp.Content,
        Done:    true,
    }
    // goroutine 自然结束 → defer close(streamChan)
    //                      → defer cancel()
}
```

---

## 10. 数据流全景

### 10.1 完整的 goroutine 拓扑

```
┌─ 主 goroutine（router 层）────────────────────────────┐
│ router_ai_assistant.go                                 │
│                                                        │
│ for chunk := range streamChan {                         │
│   switch chunk.Type {                                   │
│   case StreamTypeText:      → Redis SSE → 前端显示 token │
│   case StreamTypeToolCall:  → 记录工具调用               │
│   case StreamTypeToolResult:→ 记录工具结果               │
│   case StreamTypeDone:      → 保存历史，返回             │
│   }                                                     │
│ }                                                       │
└──────────────────┬─────────────────────────────────────┘
                   │ streamChan (chan *aiagent.StreamChunk)
                   ▼
┌─ goroutine 1（Agent 层）─────────────────────────────┐
│ runWithStream → executeReActWithDone()                │
│                                                        │
│  runReActLoop()                                        │
│    for iteration < maxIterations {                     │
│      callLLMAuto()  →  LLM token 写 streamChan         │
│      parseReActResponse()                              │
│      streamChan <- ToolCall                            │
│      executeTool()                                     │
│      streamChan <- ToolResult                          │
│      append messages                                   │
│    }                                                   │
│                                                        │
│  streamChan <- Done                                    │
│  close(streamChan)                                     │
└──────────────────┬─────────────────────────────────────┘
                   │ stream (<-chan llm.StreamChunk)
                   ▼
┌─ goroutine 2（LLM Provider 层）──────────────────────┐
│ claude.streamResponse()                                │
│  reader := bufio.NewReader(resp.Body)                  │
│  for {                                                 │
│    line, _ := reader.ReadString('\n')                  │
│    if strings.HasPrefix(line, "data: ") {              │
│      parse SSE JSON                                    │
│      ch <- StreamChunk{Content: text}                  │
│    }                                                   │
│  }                                                     │
└────────────────────────────────────────────────────────┘
```

### 10.2 Channel 数据流

```
LLM 输出的 SSE 流:
  data: {"type":"content_block_delta","delta":{"text":"我"}}
  data: {"type":"content_block_delta","delta":{"text":"来"}}
  data: {"type":"content_block_delta","delta":{"text":"查"}}
  data: {"type":"message_stop"}
     │
     ▼
stream (chan llm.StreamChunk)
     │  Content: "我"
     │  Content: "来"
     │  Content: "查"
     │  Done: true
     ▼
callLLMWithStreamOutput:
  for chunk := range stream {
    fullContent.WriteString(chunk.Content)
    streamChan <- &StreamChunk{Type: "text", Delta: chunk.Content, ...}
  }
  return fullContent  →  runReActLoop 解析 Thought/Action
     │
     ▼
streamChan (chan *aiagent.StreamChunk)
     │  Type: "text",       Delta: "我",  Content: "我"
     │  Type: "text",       Delta: "来",  Content: "我来"
     │  Type: "text",       Delta: "查",  Content: "我来查"
     │  ↓ runReActLoop 接管
     │  Type: "tool_call",  Content: "query_prometheus"
     │  Type: "tool_result",Content: "45.2%"
     │  ↓ 下一轮 LLM 调用
     │  Type: "text",       Delta: "CPU",  Content: "CPU 使用率..."
     │  Type: "text",       Delta: "45.2%"
     │  Type: "done",       Content: "当前 CPU 使用率为 45.2%"
     ▼
router_ai_assistant.go:
  for chunk := range streamChan {
    → streamBus.Append(Redis) → SSE → 浏览器
  }
```

---

## 11. 一次完整的对话示例

用户问：**"当前 CPU 使用率多少？如果有问题就告警"**

### 第 1 轮迭代

```
────────────────────────────────────────────────────────────
Step 1.1：callLLMAuto → LLM 返回
────────────────────────────────────────────────────────────
LLM 收到的 messages：
  system: 你是 AI 运维助手，可以通过工具查询指标...
  user:   当前 CPU 使用率多少？如果有问题就告警

LLM 返回：
  Thought: 用户想知道 CPU 使用率，我需要先查询 Prometheus。
  Action: query_prometheus
  Action Input: {"promql": "avg(rate(cpu_seconds_total[5m]))"}

streamChan 输出：
  [text]       "Thought: 用户想知道 CPU 使用率，我需要先查询 Prometheus。"
  [text]       "Action: query_prometheus"
  [text]       "Action Input: {"promql": "avg(rate(cpu_seconds_total[5m]))"}"

────────────────────────────────────────────────────────────
Step 1.2：executeTool
────────────────────────────────────────────────────────────
Action: query_prometheus
Input:  {"promql": "avg(rate(cpu_seconds_total[5m]))"}

→ Prometheus HTTP API
→ 返回 "45.2%"

streamChan 输出：
  [tool_call]  "query_prometheus"  (with input)
  [tool_result]"45.2%"             (with tool name)

────────────────────────────────────────────────────────────
Step 1.3：追加到消息列表
────────────────────────────────────────────────────────────
现在的 messages：
  system:    ...
  user:      当前 CPU 使用率多少？如果有问题就告警
  assistant: Thought: 用户想知道... Action: query_prometheus...
  user:      Observation: 45.2%
```

### 第 2 轮迭代

```
────────────────────────────────────────────────────────────
Step 2.1：callLLMAuto → LLM 返回（带着 Observation）
────────────────────────────────────────────────────────────
LLM 收到的 messages：
  system:    你是 AI 运维助手...
  user:      当前 CPU 使用率多少？如果有问题就告警
  assistant: Thought: 用户想知道... Action: query_prometheus...
  user:      Observation: 45.2%

LLM 返回：
  Thought: CPU 使用率 45.2%，在正常范围内，无需告警。
  Action: Final Answer
  Action Input: 当前 CPU 使用率为 45.2%，处于正常范围。

streamChan 输出：
  [text]   "Thought: CPU 使用率 45.2%，在正常范围内，无需告警。"
  [text]   "Action: Final Answer"

────────────────────────────────────────────────────────────
Step 2.2：检查完成 → IsComplete("Final Answer") == true
────────────────────────────────────────────────────────────
resp.Content = "当前 CPU 使用率为 45.2%，处于正常范围。"
resp.Success = true
return resp
```

### 收尾

```
executeReActWithDone 发送：
  streamChan <- [done]  "当前 CPU 使用率为 45.2%，处于正常范围。"

goroutine 退出：
  deffer close(streamChan)
  defer cancel()

router 层 for range 退出，返回 HTTP 响应给前端。
```

---

## 12. 核心设计亮点

### 12.1 两层 Channel 解耦

LLM provider 的 channel（`stream`）只负责传原始 token，Agent 层的 channel（`streamChan`）负责传封装后的结构化事件。中间通过 `callLLMWithStreamOutput` 桥接。这样任何一个 provider 出错或替换都不会影响上层逻辑。

### 12.2 `drainStream` 防 Goroutine 泄漏

当流式调用中途出错时，provider 的 goroutine 还在往 channel 里 blocking send。`drainStream` 在另一个 goroutine 里消费完剩余数据，让 provider 的 goroutine 正常退出。

### 12.3 ReAct 配置参数化

```go
type ReActLoopConfig struct {
    MaxIterations        int
    TimeoutMessage       string
    LogPrefix            string
    Tools                []AgentTool
    StreamChan           chan *StreamChunk
    RequestID            string
    IsComplete           func(action string) bool  // 可定制的完成判断
    ExtractPartialResult bool
}
```

`IsComplete` 是一个函数字段，ReAct 模式检查 `"Final Answer"`，Plan+ReAct 的模式步骤检查 `"Step Complete"`。同一个循环可以复用。

### 12.4 `messages` 作为状态传递的唯一媒介

每轮迭代的结果（Observation）不是通过额外的 state 对象传递，而是直接 append 到 `messages` slice。下一轮 LLM 调用时 `messages` 已经包含了全部上下文。这保持了 ReAct 循环的无状态性。

### 12.5 Stop 序列防止 LLM 脑补

```go
callLLMAuto(ctx, messages, ..., []string{"Observation:"})
```

传 `"Observation:"` 作为 stop 序列，LLM 写完 Action Input 就停，不会自己编造 Observation。工具的真实输出只来自 `executeTool`。

### 12.6 goroutine 生命周期绑定 channel

```go
go func() {
    defer close(streamChan)
    defer cancel()
    // ... 执行逻辑
}()
```

`close(streamChan)` 和 `cancel()` 都在 goroutine 退出时触发，不会泄漏。调用方通过 `for range streamChan` 感知结束。

---

## 13. 关键代码索引

| 文件 | 行号 | 内容 |
|------|------|------|
| `aiagent/agent.go` | 110-177 | `Agent.Run()` 入口 |
| `aiagent/agent.go` | 181-203 | `runWithStream()` 流式启动器 |
| `aiagent/react.go` | 12-115 | `runReActLoop()` 核心循环 |
| `aiagent/react.go` | 118-171 | `executeReAct()` 准备阶段 |
| `aiagent/react.go` | 170-204 | `executeReActWithDone()` 收尾 |
| `aiagent/llm_caller.go` | 31-40 | `callLLM()` 非流式 |
| `aiagent/llm_caller.go` | 43-80 | `callLLMWithStreamOutput()` 流式 |
| `aiagent/llm_caller.go` | 85-88 | `drainStream()` 防泄漏 |
| `aiagent/llm_caller.go` | 93-98 | `callLLMAuto()` 自动分发 |
| `aiagent/llm/claude.go` | 195-283 | `streamResponse()` SSE 消费 |
| `aiagent/llm/claude.go` | 387-396 | `setHeaders()` 请求头 |
| `aiagent/llm/http_retry.go` | 17-69 | `doHTTPWithRetry()` 非流式重试 |
| `aiagent/llm/http_retry.go` | 73-120 | `doHTTPStreamWithRetry()` 流式重试 |
| `aiagent/llm/http_retry.go` | 122-130 | `isRetryableStatus()` 可重试判断 |
| `aiagent/llm/llm.go` | 80-86 | `StreamChunk` LLM 层类型 |
| `aiagent/llm/llm.go` | 88-98 | `LLM` 接口定义 |
| `aiagent/types.go` | 149-159 | `StreamChunk` Agent 层类型 |
| `aiagent/tool_executor.go` | - | `executeTool()` 工具执行 |
| `aiagent/prompt_builder.go` | 23-45 | `buildUserMessage()` |
| `aiagent/prompt_builder.go` | 113-143 | `buildReActSystemPrompt()` |
| `aiagent/skill.go` | - | `SkillRegistry` 技能注册表 |
| `center/router/router_ai_assistant.go` | 490-530 | Router 层 Agent 构造调用 |
| `aiagent/adapter.go` | 48-100 | Processor 适配器 |

---

> 文档版本：基于 Nightingale v6 代码库，2026-06-01
