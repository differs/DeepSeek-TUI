# DeepSeek-TUI 任务系统优化方案

> 目标：在现有 `TodoList` / `TaskManager` 基础上扩展 DAG 依赖 + 并行执行 + 健康监控
> 原则：**不新建平行系统**，所有改动在现有基础设施上增量进行
> 优先级：P0（DAG）→ P0（并行）→ P1（健康监控）
> 预估总代码量：~2000-2600 行

---

## 一、现状

### 已有基础设施

```
TaskManager（task_manager.rs, ~1913 行）
├── 持久化 Task：Queued → Running → Completed/Failed/Canceled
├── Worker pool：bounded JoinSet，默认 2 worker，最大 8
├── 执行循环：spawn_supervised + CancellationToken
├── Timeline 记录 + Artifact
└── mpsc channel 管理任务队列

TodoList（tools/todo.rs, ~630 行）
├── TodoItem：id + content + status（Pending/InProgress/Completed）
├── TodoList：items + next_id + 快照
├── Tools：checklist_write / add / update / list
└── 当前是平铺列表，无依赖关系

Tool 注册（tools/registry.rs, tool_catalog.rs）
└── 已有完备的 ToolSpec trait 注册机制
```

### 当前 Checklist 的问题

```
当前 Checklist：平铺列表，无依赖关系，串行执行

┌──────────────────────────────────────────┐
│ Checklist                                │
│ □ 任务 A（Pending）                       │
│ □ 任务 B（Pending）                       │
│ □ 任务 C（Pending）                       │
└──────────────────────────────────────────┘

问题 1：A/B/C 没有依赖关系，但 LLM 只能串行做
问题 2：做 B 的时候发现还需要查薪资 → 没有机制动态添加
问题 3：A 执行 5 分钟没响应 → 不知道是正常还是卡住了
```

**关键认知**：现有 `TaskManager` 的 worker pool 支持**不同 task** 并发执行。但 **单个 task 内部的 checklist items** 是串行的。本方案聚焦后者——让 task 内部的子任务由平铺列表变为 DAG。

---

## 二、DAG 动态子任务系统

### 改造方式：扩展现有 TodoList，不新建 TaskDag

保持 `TodoList` 的模块边界，在其上加 DAG 方法和状态集。不引入 `TaskDag` / `SubTask` / `SubTaskStatus` 等平行命名。

### 数据结构

```rust
// 在现有 TodoItem 基础上扩展，而非新建 SubTask

use std::collections::{HashMap, HashSet, VecDeque};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 扩展 TodoStatus，增加 DAG 所需的状态
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TodoStatus {
    Pending,       // 等待依赖完成
    Ready,         // 依赖已满足，等待调度（新增）
    InProgress,    // 执行中（原名 Running）
    Completed,     // 完成
    Failed,        // 失败
    Skipped,       // 跳过（依赖任务失败导致，新增）
}

/// 扩展 TodoItem
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodoItem {
    pub id: u32,                        // 已有
    pub content: String,                // 已有
    pub status: TodoStatus,             // 已有，扩状态集
    pub depends_on: Vec<u32>,           // 新增：依赖的 item id 列表
    pub created_at: DateTime<Utc>,      // 新增
    pub started_at: Option<DateTime<Utc>>,    // 新增
    pub completed_at: Option<DateTime<Utc>>,  // 新增
    pub heartbeat: Option<DateTime<Utc>>,     // 新增
    pub result: Option<String>,         // 新增
    pub error: Option<String>,          // 新增
}
```

### TodoList DAG 扩展方法

```rust
impl TodoList {
    // ── 现有方法（保持接口不变）──
    // add_item(content) -> TodoItem
    // update_status(id, status)
    // snapshot() -> TodoListSnapshot
    // remove(id)

    // ── 新增 DAG 方法 ──

    /// 添加带依赖的 item，自动检测循环依赖
    pub fn add_dag_item(
        &mut self,
        content: String,
        depends_on: Vec<u32>,
    ) -> Result<u32, String> {
        // 1. 验证所有依赖存在
        for dep_id in &depends_on {
            if !self.items.iter().any(|i| i.id == *dep_id) {
                return Err(format!("依赖的 item {} 不存在", dep_id));
            }
        }

        // 2. 环检测（BFS）
        let new_id = self.next_id; // 先分配 id 以参与检测
        if self.would_create_cycle(new_id, &depends_on) {
            return Err("添加此 item 会形成循环依赖".into());
        }

        // 3. 创建 item
        let item = TodoItem {
            id: new_id,
            content,
            status: if depends_on.is_empty() {
                TodoStatus::Ready
            } else {
                TodoStatus::Pending
            },
            depends_on,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            heartbeat: None,
            result: None,
            error: None,
        };
        self.items.push(item);
        self.next_id += 1;
        Ok(new_id)
    }

    /// 获取当前可执行的任务（所有依赖已完成）
    pub fn get_ready_items(&self) -> Vec<&TodoItem> {
        self.items.iter()
            .filter(|i| matches!(i.status, TodoStatus::Pending))
            .filter(|i| i.depends_on.iter().all(|dep_id| {
                self.items.iter()
                    .find(|other| other.id == *dep_id)
                    .map(|d| matches!(d.status, TodoStatus::Completed))
                    .unwrap_or(false)
            }))
            .collect()
    }

    /// 更新状态，自动传播到下游
    pub fn update_dag_status(
        &mut self,
        item_id: u32,
        new_status: TodoStatus,
        result: Option<String>,
        error: Option<String>,
    ) {
        let item = match self.items.iter_mut().find(|i| i.id == item_id) {
            Some(i) => i,
            None => return,
        };

        item.status = new_status;
        item.heartbeat = Some(Utc::now());

        match item.status {
            TodoStatus::Completed => {
                item.completed_at = Some(Utc::now());
                item.result = result;
                // 检查下游所有 items，如果所有依赖都完成了 → Ready
                for other in self.items.iter_mut() {
                    if other.status != TodoStatus::Pending {
                        continue;
                    }
                    if !other.depends_on.contains(&item_id) {
                        continue;
                    }
                    if other.depends_on.iter().all(|dep| {
                        self.items.iter()
                            .find(|d| d.id == *dep)
                            .map(|d| matches!(d.status, TodoStatus::Completed))
                            .unwrap_or(false)
                    }) {
                        other.status = TodoStatus::Ready;
                    }
                }
            }
            TodoStatus::Failed => {
                item.error = error;
                // 下游所有直接依赖此 item 的 → Skipped
                for other in self.items.iter_mut() {
                    if other.status != TodoStatus::Pending {
                        continue;
                    }
                    if other.depends_on.contains(&item_id) {
                        other.status = TodoStatus::Skipped;
                    }
                }
            }
            _ => {}
        }
    }

    /// 环检测（BFS，从每个依赖反向搜索）
    fn would_create_cycle(&self, new_id: u32, depends_on: &[u32]) -> bool {
        for dep_id in depends_on {
            if *dep_id == new_id {
                return true;
            }
            if let Some(dep_item) = self.items.iter().find(|i| i.id == *dep_id) {
                // 从 dep 沿 depends_on 链遍历，看是否回到 new_id
                let mut queue = VecDeque::new();
                let mut visited = HashSet::new();
                queue.push_back(dep_item.id);
                visited.insert(dep_item.id);

                while let Some(current) = queue.pop_front() {
                    if current == new_id {
                        return true;
                    }
                    if let Some(ancestor) = self.items.iter().find(|i| i.id == current) {
                        for ancestor_dep in &ancestor.depends_on {
                            if !visited.contains(ancestor_dep) {
                                visited.insert(*ancestor_dep);
                                queue.push_back(*ancestor_dep);
                            }
                        }
                    }
                }
            }
        }
        false
    }

    /// 所有 items 是否都处于终态
    pub fn is_all_terminal(&self) -> bool {
        self.items.iter().all(|i| matches!(
            i.status,
            TodoStatus::Completed | TodoStatus::Failed | TodoStatus::Skipped
        ))
    }
}
```

### 新增 Tool: `task_dynamic_add`

在现有 `tool_catalog.rs` / `registry.rs` 注册，**仅在 Worker Agent 作用域下暴露**。

```rust
struct DynamicSubtaskTool;

impl ToolSpec for DynamicSubtaskTool {
    fn name(&self) -> &'static str { "task_dynamic_add" }

    fn description(&self) -> &'static str {
        "在当前任务执行过程中动态添加新的子任务。新任务默认依赖当前任务。"
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "title":     { "type": "string", "description": "子任务标题" },
                "prompt":    { "type": "string", "description": "子任务的 Agent prompt" },
                "depends_on": {
                    "type": "array",
                    "items": { "type": "integer" },
                    "description": "依赖的 item ID 列表（可选，默认依赖当前执行中的 item）"
                }
            },
            "required": ["title", "prompt"]
        })
    }

    async fn execute(&self, input: Value, context: &ToolContext) -> Result<ToolResult, ToolError> {
        let todo_list = context.todo_list.lock().await;
        let title = required_str(&input, "title")?;
        let prompt = required_str(&input, "prompt")?;

        let depends_on: Vec<u32> = match input.get("depends_on") {
            Some(ids) => serde_json::from_value(ids.clone())
                .map_err(|_| ToolError::invalid_params("depends_on must be [{integer}]"))?,
            None => {
                // 默认依赖当前上下文中的父任务
                vec![context.current_item_id
                    .ok_or_else(|| ToolError::invalid_params("no current item context"))?]
            }
        };

        let id = todo_list.add_dag_item(format!("{}: {}", title, prompt), depends_on)
            .map_err(|e| ToolError::invalid_params(&e))?;

        Ok(ToolResult::success(json!({
            "id": id,
            "status": "pending",
            "message": format!("已添加子任务 #{}", id)
        })))
    }
}
```

### 新增 Tool: `task_dag_status`

```rust
struct DagStatusTool;

impl ToolSpec for DagStatusTool {
    fn name(&self) -> &'static str { "task_dag_status" }

    fn description(&self) -> &'static str {
        "查看当前 DAG 状态，包括每个子任务的进度和依赖关系。"
    }

    async fn execute(&self, _input: Value, context: &ToolContext) -> Result<ToolResult, ToolError> {
        let todo_list = context.todo_list.lock().await;
        let snapshot = todo_list.snapshot(); // 现有方法，加了 DAG 字段后自动扩展

        let status: Vec<Value> = todo_list.items.iter().map(|item| json!({
            "id": item.id,
            "content": item.content,
            "status": item.status,
            "depends_on": item.depends_on,
            "error": item.error,
        })).collect();

        Ok(ToolResult::success(serde_json::to_string_pretty(&json!({
            "completion_pct": snapshot.completion_pct,
            "in_progress_id": snapshot.in_progress_id,
            "tasks": status,
        }))?))
    }
}
```

### 现有 Tool 适配

现有 `checklist_write` / `add` / `update` / `list` 的接口**保持不变**（兼容现有用户），但内部储存改为 DAG-aware。调用 `checklist_add(item)` 等价于 `add_dag_item(item, [])`（无依赖），现有的旧 checklist 不会创建 DAG，升级平滑。

---

## 三、并行执行引擎

### 改造方式：嵌入式，不新建 ParallelDagExecutor

不创建独立模块。所有改动在 `task_manager.rs` 的 worker loop 内完成：

```
task_manager.rs 现有结构
┌─────────────────────────────────┐
│ spawn_workers()                 │
│   for each worker:              │
│     loop {                      │
│       task = rx.recv()          │ ← 从 mpsc channel 取 task
│       run_task(task)            │ ← 执行单个 task
│       send_result(task)         │
│     }                           │
└─────────────────────────────────┘

改动后结构
┌─────────────────────────────────┐
│ spawn_workers()                 │
│   for each worker:              │
│     loop {                      │
│       task = rx.recv()          │ ← 仍从 mpsc 取 task
│       run_task_dag(task)        │ ← 内部对 DAG items 做并发调度
│       send_result(task)         │
│     }                           │
└─────────────────────────────────┘

run_task_dag() 内部
┌─────────────────────────────────┐
│ loop {                          │
│   ready = dag.get_ready_items() │ ← 所有 Ready 状态 item
│   for item in ready:            │
│     spawn execute_item(item)    │ ← 每个 item 是一个独立协程
│   wait for any completion       │
│   update_status(item, result)   │ ← 自动释放下游
│   if dag.is_all_terminal():     │
│     break                       │
│ }                               │
└─────────────────────────────────┘
```

### 核心改动位置

#### `task_manager.rs` — worker loop

```rust
async fn run_task_dag(
    task_id: &str,
    dag: Arc<Mutex<TodoList>>,
    config: &TaskManagerConfig,
    cancel: CancellationToken,
) -> Result<String> {
    let max_parallel = config.max_workers.max(1); // Worker 内的并行 = max_workers
    let mut running: JoinSet<(u32, Result<String>)> = JoinSet::new();

    loop {
        // 1. 非阻塞检查取消
        if cancel.is_cancelled() {
            return Err(anyhow!("task cancelled"));
        }

        // 2. 拿待执行的 Ready items
        let ready_items = {
            let list = dag.lock().await;
            list.get_ready_items().cloned().collect::<Vec<_>>()
        };

        // 3. 启动新的 item 执行
        let slots = max_parallel.saturating_sub(running.len());
        for item in ready_items.into_iter().take(slots) {
            {
                let mut list = dag.lock().await;
                list.update_dag_status(
                    item.id,
                    TodoStatus::InProgress,
                    None, None,
                );
            }
            let current_dag = dag.clone();
            let current_cancel = cancel.child_token();
            running.spawn(async move {
                let result = execute_item(item.clone(), current_dag.clone(), current_cancel).await;
                (item.id, result)
            });
        }

        // 4. 等待至少一个完成
        if running.is_empty() {
            // 没有在运行的 item 且没有 Ready → 检查是否完成或死锁
            let terminal = dag.lock().await.is_all_terminal();
            if terminal { break; }

            // 有 Pending 但没有 Ready → 死锁
            let has_pending = {
                dag.lock().await.items.iter()
                    .any(|i| matches!(i.status, TodoStatus::Pending))
            };
            if has_pending && running.is_empty() {
                return Err(anyhow!("DAG deadlock: all remaining items are blocked on dependencies"));
            }
        }

        // 5. 等待一个完成，更新 DAG
        if let Some(result) = running.join_next().await {
            match result {
                Ok((item_id, Ok(summary))) => {
                    let mut list = dag.lock().await;
                    list.update_dag_status(item_id, TodoStatus::Completed, Some(summary), None);
                }
                Ok((item_id, Err(e))) => {
                    let mut list = dag.lock().await;
                    list.update_dag_status(item_id, TodoStatus::Failed, None, Some(e.to_string()));
                }
                Err(e) => {
                    tracing::error!("DAG item join error: {e}");
                }
            }
        }
    }

    // 汇总结果
    let summary = dag.lock().await.items.iter()
        .filter(|i| matches!(i.status, TodoStatus::Completed))
        .filter_map(|i| i.result.as_ref())
        .collect::<Vec<_>>()
        .join("\n---\n");

    Ok(summary)
}
```

#### `execute_item` — 将单个 item 交给现有 agent 执行循环

```rust
async fn execute_item(
    item: TodoItem,
    dag: Arc<Mutex<TodoList>>,
    cancel: CancellationToken,
) -> Result<String> {
    // 不重写 agent loop！对接现有 worker_loop / DeepSeek API 调用链
    // 1. 设置 TurnContext，注入 task_dynamic_add 和 task_heartbeat tool 的作用域
    // 2. 调用现有的 agent 轮询逻辑（runtime_api / cycle_manager）
    // 3. 返回最终结果字符串
    //
    // 参考路径：
    //   crate::runtime_api::RuntimeApi::run_turn_with_tools(...)
    //   or crate::task_manager::TaskManager::execute_agent_turn(...)

    let prompt = format!(
        "## 子任务\n\n{}\n\n{}\n\n{}",
        item.content,
        "请完成上述任务。在执行过程中可以使用 task_dynamic_add 添加子任务。",
        "每 60 秒调用一次 task_heartbeat。",
    );

    // 下面是对接已有 agent 执行逻辑的桩代码：
    // let result = runtime_api
    //     .run_turn(TurnRequest { prompt, tools: WORKER_TOOLS, cancel })
    //     .await?;

    // 暂用 mock
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(format!("item #{} completed", item.id))
}
```

### 执行流程示例

```text
用户创建 task："搜索广州的 Golang 职位并整理文档"

Step 1：LLM 拆解为 DAG（通过 checklist_write）
  task_search（无依赖）        → Ready → 调度
  task_detail（依赖 search）    → Pending

Step 2：task_search 启动（Worker 1 的 run_task_dag 内部）
  → spawn execute_item(task_search)
  → 在 item 执行过程中，Agent 调用 task_dynamic_add
    → 新增 task_compare（依赖 search）→ Pending

Step 3：search 完成
  update_dag_status(search, Completed)
  → task_detail → Ready
  → task_compare → Ready
  → 两个 item 同时被 run_task_dag 的循环 pick up

Step 4：全部完成 → 汇总结果
```

### 重要：不要踩的坑

1. ❌ **不要**在 `task_manager.rs` 外部新建 `ParallelDagExecutor` 结构体
2. ❌ **不要**重写 agent loop（`execute_agent_task` 在方案初版里的一大段代码）
3. ❌ **不要**引入新的 channel 或通知机制——`run_task_dag` 内部的 `JoinSet` 循环是自洽的
4. ✅ **Worker 总数控制**：一个 task 内并行执行多个 item，但多个 task 之间仍然由 `TaskManager` 的 mpsc channel 串行化。`max_parallel` 只在**单个 task 内部的 item 并发**有意义

---

## 四、健康监控

### 核心思路：先检查再决策，不直接杀

```
健康监控模型
┌─────────────────────────────┐
│ Agent 主动心跳（主信号）      │ ← 每 60s task_heartbeat
│     +                       │
│ 系统级兜底检测（辅助）         │ ← >5min 无心跳时触发
│     +                       │
│ Token 趋势检测（二级信号）     │ ← 有心跳但 Token 不增长时
└─────────────────────────────┘
         ↓
┌─────────────────────────────┐
│ HealthDecision               │
│ ├ Normal → 继续              │
│ ├ Restart → 重启 item        │
│ └ Escalate → 通知用户        │
└─────────────────────────────┘
```

### 健康检查逻辑（嵌入 TaskManager，不新建模块）

不创建 `health_monitor.rs`。健康检查逻辑作为 `task_manager.rs` 中 `run_task_dag` 的一部分，在等待循环的 `tokio::select!` 中作为定期 tick 加入：

```rust
// 在 run_task_dag 的 spawn 部分之后、join 之前
if check_interval.tick().await.is_ok() {
    let decision = check_item_health(&dag, &running).await;
    match decision {
        HealthDecision::Normal => {}
        HealthDecision::Restart(task_id, reason) => {
            // 取消正在运行的 JoinSet 项，重新加入
            tracing::warn!("Restarting item #{task_id}: {reason}");
            // 具体：cancel JoinSet entry，重新 add 到 DAG 下次循环 pick up
        }
        HealthDecision::Escalate(task_id, reason) => {
            notify_user(&format!("Task #{task_id} stalled: {reason}")).await;
        }
    }
}
```

```rust
/// 决策枚举
enum HealthDecision {
    Normal,
    Restart(u32, String),
    Escalate(u32, String),
}

/// 单个 item 的健康检查
fn check_item_health(
    dag: &Arc<Mutex<TodoList>>,
    running: &JoinSet<(u32, Result<String>)>,
) -> HealthDecision {
    let now = Utc::now();
    let list = dag.lock();

    for item in list.items.iter().filter(|i| matches!(i.status, TodoStatus::InProgress)) {
        let since_heartbeat = match item.heartbeat {
            Some(hb) => now - hb,
            None => continue,
        };

        if since_heartbeat < STALL_THRESHOLD {
            // 有心跳且时间短 → 正常
            continue;
        }

        if since_heartbeat > STALL_THRESHOLD * 2 {
            return HealthDecision::Escalate(
                item.id,
                format!("Item #{} 超过 {:.0?} 无心跳，需要人工确认", item.id, since_heartbeat),
            );
        }

        // 无心跳但没到两倍阈值 → 重启一次
        // 注意：需要有 restart_count 追踪，这里简化为每次超时都重启
        // 实际实现需要在 TodoItem 上加 restart_count: u32 字段
        return HealthDecision::Restart(
            item.id,
            format!("Item #{} 无心跳超时，尝试重启", item.id),
        );
    }

    HealthDecision::Normal
}
```

### Agent 心跳 Tool

```rust
struct HeartbeatTool;

impl ToolSpec for HeartbeatTool {
    fn name(&self) -> &'static str { "task_heartbeat" }

    fn description(&self) -> &'static str {
        "报告当前 item 的执行进度。每 60 秒调用一次。"
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "progress": {
                    "type": "string",
                    "description": "当前的进展描述（可选）"
                }
            }
        })
    }

    async fn execute(&self, input: Value, context: &ToolContext) -> Result<ToolResult, ToolError> {
        let progress = optional_str(&input, "progress").unwrap_or("");
        let mut list = context.todo_list.lock().await;
        let current_id = context.current_item_id
            .ok_or_else(|| ToolError::not_available("no active item"))?;

        if let Some(item) = list.items.iter_mut().find(|i| i.id == current_id) {
            item.heartbeat = Some(Utc::now());
            if !progress.is_empty() {
                item.result = Some(format!("[progress] {}", progress));
            }
        }

        Ok(ToolResult::success("ok"))
    }
}
```

### 未决项

| 问题 | 状态 | 说明 |
|------|------|------|
| Token 趋势检测 | ⚠️ 待确认 | 当前 DeepSeek API 是否暴露实时 token 消耗？如果只暴露最终消耗，趋势检测无法实现 |
| `Escalate` 的用户交互通道 | ⚠️ 待实现 | 需要一种"通知用户并等待回复"的异步通道。现有 `notify` 是单向的 |
| 重启次数限制 | ⚠️ 待配置 | `restart_count` 需要在 `TodoItem` 上增加字段，阈值通过 `config.toml` 控制 |

### 健康监控 vs 超时强杀

| 对比 | 超时强制 KILL | 健康监控 |
|------|-------------|---------|
| 态度 | "超时就杀" | "先检查是否真的卡住了" |
| 误杀 | 长任务容易被误杀 | 检查心跳 + Token 趋势（待确认），仍在运行就不杀 |
| 恢复 | 被杀 = 失败，重头开始 | 真卡了先重启（保留中间结果） |
| 用户参与 | 无 | 不确定时通知用户决策 |

---

## 五、Agent Prompt 适配

### Worker Agent 系统提示（需要新增，在现有 tool_catalog.rs 中区分作用域）

```rust
const WORKER_SYSTEM_PROMPT: &str = r#"
## 健康协议

1. 每 60 秒调用一次 task_heartbeat，报告进展
2. 如果遇到超时，先重试 3 次（指数退避）
3. 重试仍失败 → 用 task_dynamic_add 创建一个"处理错误"的子任务
4. 如果发现当前任务方向不对 → 用 task_dynamic_add 创建修正任务
5. 不要硬撑——有问题就报告，系统会根据情况重启或调整

## 动态任务

在执行过程中，如果发现：
- 当前结果需要额外的后续处理
- 出现了需要单独处理的问题
- 发现了新的有价值的分析方向

随时用 task_dynamic_add 添加新的子任务。系统会自动管理依赖关系并并行执行。
"#;
```

### Tool 作用域控制

**关键**：`task_dynamic_add`、`task_heartbeat`、`task_dag_status` 这三个 tool **不应暴露给 root agent**，只注入 Worker Agent 的 tool set。

在 `tool_catalog.rs` 中已有的 tool 分类逻辑上做扩展：

```rust
// 现有逻辑（示意）
pub fn tools_for_role(role: AgentRole) -> Vec<Box<dyn ToolSpec>> {
    match role {
        AgentRole::Root => {
            // 所有工具（排除 WORKER_ONLY）
        }
        AgentRole::Worker => {
            // Root tools + WORKER_ONLY tools
        }
    }
}

const WORKER_ONLY: &[&str] = &[
    "task_dynamic_add",
    "task_heartbeat",
    "task_dag_status",
];
```

---

## 六、改造路径

### 阶段一：DAG 数据结构 + 动态子任务

```text
改动范围：
  1. tools/todo.rs — 扩展 TodoStatus、TodoItem，加 DAG 方法
  2. tools/todo.rs — 改造现有 tool 内部调用方式（接口不变）
  3. 新增 tool: task_dynamic_add
  4. 新增 tool: task_dag_status
  5. 原有 task_list/read 等不需要改

预计代码量：~800-1000 行
```

### 阶段二：并行执行引擎

```text
改动范围：
  1. task_manager.rs — 新增 run_task_dag() 取代原有的串行 item 循环
  2. task_manager.rs — 修改 worker loop 的路由逻辑
  3. task_manager.rs — 新增 execute_item()，对接现有 agent loop
  4. tool_catalog.rs — 区分 Root/Worker tool set

预计代码量：~600-800 行
```

### 阶段三：健康监控

```text
改动范围：
  1. task_manager.rs — run_task_dag 循环中嵌入健康检查 tick
  2. TodoItem — 扩展 restart_count 字段
  3. tools/todo.rs — 新增 task_heartbeat tool
  4. tool_catalog.rs — 新增 WORKER_ONLY 作用域
  5. 改造 Worker System Prompt

预计代码量：~600-800 行
```

### 总量

| 阶段 | 方案原估算 | 实际预估 | 原因 |
|------|-----------|---------|------|
| 一：DAG | ~600 | ~800-1000 | 改 TodoItem/TodoList 接口 + DAG 工具 |
| 二：并行 | ~400 | ~600-800 | 不是新增模块，改已有 worker loop，耦合度高 |
| 三：健康 | ~500 | ~600-800 | 心跳 tool + loop 集成 + UI 反馈 |
| **总计** | **~1500** | **~2000-2600** | |

---

## 七、关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 扩展 TodoList vs 新建 TaskDag | **扩展 TodoList** | 保证后端兼容，tool 接口不变，升级平滑 |
| DAG vs Tree | **DAG** | 子任务可以依赖多个父任务（fan-in） |
| 动态添加 vs 预定义 | **动态添加** | Agent 运行中才能发现"还要做什么" |
| 并行拼接方式 | **run_task_dag 内 JoinSet** | 不新建模块，复用已有基础设施 |
| 并行原语 | **JoinSet** | Tokio 原生，比手写 channel 更安全 |
| 健康不杀 vs 超时强杀 | **健康监控** | 长任务需要时间，误杀比不杀更糟 |
| Agent 主动心跳 vs 系统检测 | **Agent 主动 + 系统兜底** | Agent 才知道自己是否卡了，系统保底 |
| 失败恢复 | **先重启 → 后升级给用户** | 自动恢复常见错误，不确定才找用户 |
| Tool 作用域 | **Root/Worker 分离** | Worker 仅能访问 DAG 相关 tool，避免混乱 |
| 代码组织 | **不新增模块 files** | DAG 在 todo.rs，并行/健康在 task_manager.rs |

### 补充约束

1. **DAG 增长上限**：一个 task 内的动态添加次数默认上限 100，可通过 `config.toml` 调整
2. **重启次数**：`TodoItem` 需要 `restart_count` 字段，默认阈值 3，可配置
3. **链式优先**：DAG 完整实现，但实际 90% 场景是链式依赖（A→B→C），接口设计应优先保证链式路径的流畅度
4. **降级兼容**：如果 task 没有 DAG items（旧格式的普通 Checklist），降级为现有串行执行逻辑
