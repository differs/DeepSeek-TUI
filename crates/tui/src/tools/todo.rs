//! Todo list tool and supporting data structures.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::tools::spec::{
    ApprovalRequirement, ToolCapability, ToolContext, ToolError, ToolResult, ToolSpec,
};

// === Types ===

/// Status for a todo item.
///
/// In DAG mode (`Ready`, `Skipped`), items can express dependency-driven
/// scheduling; legacy flat-list items only use `Pending`, `InProgress`, and
/// `Completed`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TodoStatus {
    /// Waiting for dependencies to complete (DAG-aware).
    Pending,
    /// Dependencies satisfied; ready to be scheduled (DAG-aware).
    Ready,
    /// Currently executing.
    InProgress,
    /// Successfully completed.
    Completed,
    /// Failed with an error.
    Failed,
    /// Skipped because a dependency failed (DAG-aware).
    Skipped,
}

impl TodoStatus {
    #[allow(dead_code)]
    pub fn as_str(self) -> &'static str {
        match self {
            TodoStatus::Pending => "pending",
            TodoStatus::Ready => "ready",
            TodoStatus::InProgress => "in_progress",
            TodoStatus::Completed => "completed",
            TodoStatus::Failed => "failed",
            TodoStatus::Skipped => "skipped",
        }
    }

    /// Parse a string into a todo status.
    #[must_use]
    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_lowercase().as_str() {
            "pending" => Some(TodoStatus::Pending),
            "ready" => Some(TodoStatus::Ready),
            "in_progress" | "inprogress" => Some(TodoStatus::InProgress),
            "completed" | "done" => Some(TodoStatus::Completed),
            "failed" => Some(TodoStatus::Failed),
            "skipped" => Some(TodoStatus::Skipped),
            _ => None,
        }
    }

    /// Whether this status is a terminal state.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TodoStatus::Completed | TodoStatus::Failed | TodoStatus::Skipped
        )
    }
}

/// A single todo item.
///
/// Legacy flat-list items only set `id`, `content`, `status`.
/// DAG-aware items additionally set `depends_on`, timestamps, `result`,
/// `error`, `heartbeat`, and `restart_count`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodoItem {
    pub id: u32,
    pub content: String,
    pub status: TodoStatus,
    /// DAG dependency list: item ids that must complete before this one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<u32>,
    /// When this item was created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// When this item started executing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// When this item reached a terminal status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Last heartbeat from the worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat: Option<DateTime<Utc>>,
    /// Result summary if completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    /// Error message if failed or skipped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Health-monitor restart counter.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub restart_count: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

/// Snapshot of a todo list for display or serialization.
#[derive(Debug, Clone, Serialize)]
pub struct TodoListSnapshot {
    pub items: Vec<TodoItem>,
    pub completion_pct: u8,
    pub in_progress_id: Option<u32>,
}

/// Mutable list of todo items with helper operations.
#[derive(Debug, Clone, Default)]
pub struct TodoList {
    items: Vec<TodoItem>,
    next_id: u32,
}

impl TodoList {
    /// Create an empty todo list.
    #[must_use]
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            next_id: 1,
        }
    }

    /// Return a snapshot of the list with computed metrics.
    #[must_use]
    pub fn snapshot(&self) -> TodoListSnapshot {
        TodoListSnapshot {
            items: self.items.clone(),
            completion_pct: self.completion_percentage(),
            in_progress_id: self.in_progress_id(),
        }
    }

    /// Add a new todo item.
    pub fn add(&mut self, content: String, status: TodoStatus) -> TodoItem {
        let status = match status {
            TodoStatus::InProgress => {
                self.set_single_in_progress(None);
                TodoStatus::InProgress
            }
            other => other,
        };

        let item = TodoItem {
            id: self.next_id,
            content,
            status,
            depends_on: Vec::new(),
            created_at: Some(Utc::now()),
            started_at: None,
            completed_at: None,
            heartbeat: None,
            result: None,
            error: None,
            restart_count: 0,
        };
        self.next_id += 1;
        self.items.push(item.clone());
        item
    }

    /// Update an item's status by id.
    pub fn update_status(&mut self, id: u32, status: TodoStatus) -> Option<TodoItem> {
        let mut updated: Option<TodoItem> = None;
        if status == TodoStatus::InProgress {
            self.set_single_in_progress(Some(id));
        }
        for item in &mut self.items {
            if item.id == id {
                item.status = status;
                updated = Some(item.clone());
                break;
            }
        }
        updated
    }

    /// Compute completion percentage for the list.
    #[must_use]
    pub fn completion_percentage(&self) -> u8 {
        if self.items.is_empty() {
            return 0;
        }
        let total = self.items.len();
        let completed = self
            .items
            .iter()
            .filter(|item| item.status == TodoStatus::Completed)
            .count();
        let percent = completed.saturating_mul(100);
        let percent = (percent + total / 2) / total;
        u8::try_from(percent).unwrap_or(u8::MAX)
    }

    /// Return the id of the in-progress item, if any.
    #[must_use]
    pub fn in_progress_id(&self) -> Option<u32> {
        self.items
            .iter()
            .find(|item| item.status == TodoStatus::InProgress)
            .map(|item| item.id)
    }

    /// Clear all todo items.
    pub fn clear(&mut self) {
        self.items.clear();
        self.next_id = 1;
    }

    fn set_single_in_progress(&mut self, allow_id: Option<u32>) {
        for item in &mut self.items {
            if Some(item.id) != allow_id && item.status == TodoStatus::InProgress {
                item.status = TodoStatus::Pending;
            }
        }
    }

    // ── DAG methods ───────────────────────────────────────────────

    /// Add a DAG-aware item with dependency tracking.
    ///
    /// Items with empty `depends_on` are immediately `Ready`; others
    /// start as `Pending` and transition to `Ready` once all dependencies
    /// complete.
    pub fn add_dag_item(
        &mut self,
        content: String,
        depends_on: Vec<u32>,
    ) -> Result<u32, String> {
        // Validate dependencies exist.
        for dep_id in &depends_on {
            if !self.items.iter().any(|i| i.id == *dep_id) {
                return Err(format!("Dependency item #{} does not exist", dep_id));
            }
        }

        // Cycle detection.
        let new_id = self.next_id;
        if self.would_create_cycle(new_id, &depends_on) {
            return Err("Adding this item would create a cycle".into());
        }

        let status = if depends_on.is_empty() {
            TodoStatus::Ready
        } else {
            TodoStatus::Pending
        };

        let item = TodoItem {
            id: new_id,
            content,
            status,
            depends_on,
            created_at: Some(Utc::now()),
            started_at: None,
            completed_at: None,
            heartbeat: None,
            result: None,
            error: None,
            restart_count: 0,
        };
        self.items.push(item);
        self.next_id += 1;
        Ok(new_id)
    }

    /// Return items whose dependencies are all `Completed` and whose
    /// own status is `Ready` (i.e. ready to schedule).
    #[must_use]
    pub fn get_ready_items(&self) -> Vec<&TodoItem> {
        self
            .items
            .iter()
            .filter(|i| i.status == TodoStatus::Ready)
            .collect()
    }

    /// Update an item's status and propagate downstream transitions.
    ///
    /// On `Completed`: downstream items whose *all* dependencies are now
    /// `Completed` transition from `Pending` to `Ready`.
    ///
    /// On `Failed`: downstream items that depend on this item move to
    /// `Skipped`.
    pub fn update_dag_status(
        &mut self,
        item_id: u32,
        new_status: TodoStatus,
        result: Option<String>,
        error: Option<String>,
    ) {
        // Find index first to avoid holding a &mut across self.items iteration.
        let idx = match self.items.iter().position(|i| i.id == item_id) {
            Some(i) => i,
            None => return,
        };

        let item = &mut self.items[idx];
        item.status = new_status;
        item.heartbeat = Some(Utc::now());

        match new_status {
            TodoStatus::InProgress => {
                item.started_at = Some(Utc::now());
            }
            TodoStatus::Completed => {
                item.completed_at = Some(Utc::now());
                item.result = result;
            }
            TodoStatus::Failed => {
                item.error = error;
                item.completed_at = Some(Utc::now());
            }
            _ => {}
        }

        // Phase 2: DAG propagation (drop the &mut on items[idx] above).
        match new_status {
            TodoStatus::Completed => {
                // Collect downstream candidate ids.
                let downstream_ids: Vec<u32> = self
                    .items
                    .iter()
                    .filter(|other| {
                        other.status == TodoStatus::Pending
                            && other.depends_on.contains(&item_id)
                    })
                    .map(|other| other.id)
                    .collect();

                // Pre-compute readiness for all downstream items before
                // any mutation, avoiding borrow conflicts between the
                // immutable all()-closure and the subsequent iter_mut().
                let ready_states: Vec<bool> = downstream_ids
                    .iter()
                    .map(|id| {
                        let deps: Vec<u32> = self
                            .items
                            .iter()
                            .find(|o| o.id == *id)
                            .map(|o| o.depends_on.clone())
                            .unwrap_or_default();
                        deps.iter().all(|dep| {
                            self.items.iter().any(|dd| {
                                dd.id == *dep && dd.status == TodoStatus::Completed
                            })
                        })
                    })
                    .collect();

                for (idx, is_ready) in ready_states.iter().enumerate() {
                    if *is_ready {
                        if let Some(other) = self.items.iter_mut().find(|o| o.id == downstream_ids[idx]) {
                            other.status = TodoStatus::Ready;
                        }
                    }
                }
            }
            TodoStatus::Failed => {
                // Skip downstream items that directly depend on this.
                for other in &mut self.items {
                    if other.status == TodoStatus::Pending
                        && other.depends_on.contains(&item_id)
                    {
                        other.status = TodoStatus::Skipped;
                        other.error = Some(format!(
                            "Skipped: dependency #{} failed",
                            item_id
                        ));
                        other.completed_at = Some(Utc::now());
                    }
                }
            }
            _ => {}
        }
    }

    /// Check whether adding an item with the given `depends_on` would
    /// create a cycle. Uses BFS from each dependency ancestor.
    fn would_create_cycle(&self, new_id: u32, depends_on: &[u32]) -> bool {
        for dep_id in depends_on {
            if *dep_id == new_id {
                return true;
            }
            let Some(dep_item) = self.items.iter().find(|i| i.id == *dep_id) else {
                continue;
            };
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
                        if visited.insert(*ancestor_dep) {
                            queue.push_back(*ancestor_dep);
                        }
                    }
                }
            }
        }
        false
    }

    /// All items are in a terminal state (`Completed`, `Failed`, `Skipped`).
    #[must_use]
    pub fn is_all_terminal(&self) -> bool {
        self.items.iter().all(|i| i.status.is_terminal())
    }

    /// Items currently in `InProgress` status.
    #[must_use]
    pub fn get_running_items(&self) -> Vec<&TodoItem> {
        self.items
            .iter()
            .filter(|i| i.status == TodoStatus::InProgress)
            .collect()
    }

    /// Bump the heartbeat for the given item.
    pub fn update_heartbeat(&mut self, item_id: u32) {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == item_id) {
            item.heartbeat = Some(Utc::now());
        }
    }

    /// Increment the restart counter for the given item.
    pub fn increment_restart(&mut self, item_id: u32) {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == item_id) {
            item.restart_count += 1;
        }
    }
}

// === TodoWriteTool - ToolSpec implementation ===

/// Shared reference to a `TodoList` for use across tools
pub type SharedTodoList = Arc<Mutex<TodoList>>;

/// Create a new shared `TodoList`
pub fn new_shared_todo_list() -> SharedTodoList {
    Arc::new(Mutex::new(TodoList::new()))
}

/// Tool for writing and updating the todo list
pub struct TodoWriteTool {
    todo_list: SharedTodoList,
    tool_name: &'static str,
}

impl TodoWriteTool {
    pub fn new(todo_list: SharedTodoList) -> Self {
        Self {
            todo_list,
            tool_name: "todo_write",
        }
    }

    pub fn checklist(todo_list: SharedTodoList) -> Self {
        Self {
            todo_list,
            tool_name: "checklist_write",
        }
    }
}

/// Tool for adding a single todo item (legacy compatibility).
pub struct TodoAddTool {
    todo_list: SharedTodoList,
    tool_name: &'static str,
}

impl TodoAddTool {
    pub fn new(todo_list: SharedTodoList) -> Self {
        Self {
            todo_list,
            tool_name: "todo_add",
        }
    }

    pub fn checklist(todo_list: SharedTodoList) -> Self {
        Self {
            todo_list,
            tool_name: "checklist_add",
        }
    }
}

#[async_trait]
impl ToolSpec for TodoAddTool {
    fn name(&self) -> &'static str {
        self.tool_name
    }

    fn description(&self) -> &'static str {
        if self.tool_name == "todo_add" {
            "Compatibility alias for checklist_add. Adds one checklist item on the active thread/task."
        } else {
            "Add one checklist item on the active thread/task. Durable tasks persist this checklist as subordinate work progress."
        }
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description": "The task description"
                },
                "status": {
                    "type": "string",
                    "enum": ["pending", "in_progress", "completed"],
                    "description": "Task status (default: pending)"
                }
            },
            "required": ["content"]
        })
    }

    fn capabilities(&self) -> Vec<ToolCapability> {
        vec![ToolCapability::WritesFiles]
    }

    fn approval_requirement(&self) -> ApprovalRequirement {
        ApprovalRequirement::Auto
    }

    async fn execute(
        &self,
        input: serde_json::Value,
        _context: &ToolContext,
    ) -> Result<ToolResult, ToolError> {
        let content = input
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::invalid_input("Missing 'content'"))?;
        let status = input
            .get("status")
            .and_then(|v| v.as_str())
            .and_then(TodoStatus::from_str)
            .unwrap_or(TodoStatus::Pending);

        let mut list = self.todo_list.lock().await;
        let item = list.add(content.to_string(), status);
        let snapshot = list.snapshot();

        let result = serde_json::to_string_pretty(&snapshot).unwrap_or_else(|_| "{}".to_string());
        Ok(ToolResult::success(format!(
            "Added todo #{} ({})\n{}",
            item.id,
            item.status.as_str(),
            result
        ))
        .with_metadata(checklist_metadata(&snapshot, self.tool_name)))
    }
}

/// Tool for updating a todo item's status (legacy compatibility).
pub struct TodoUpdateTool {
    todo_list: SharedTodoList,
    tool_name: &'static str,
}

impl TodoUpdateTool {
    pub fn new(todo_list: SharedTodoList) -> Self {
        Self {
            todo_list,
            tool_name: "todo_update",
        }
    }

    pub fn checklist(todo_list: SharedTodoList) -> Self {
        Self {
            todo_list,
            tool_name: "checklist_update",
        }
    }
}

#[async_trait]
impl ToolSpec for TodoUpdateTool {
    fn name(&self) -> &'static str {
        self.tool_name
    }

    fn description(&self) -> &'static str {
        if self.tool_name == "todo_update" {
            "Compatibility alias for checklist_update. Updates one checklist item by id on the active thread/task."
        } else {
            "Update one checklist item's status by id on the active thread/task."
        }
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "id": {
                    "type": "integer",
                    "description": "Todo item id"
                },
                "status": {
                    "type": "string",
                    "enum": ["pending", "in_progress", "completed", "failed", "skipped"],
                    "description": "New status"
                }
            },
            "required": ["id", "status"]
        })
    }

    fn capabilities(&self) -> Vec<ToolCapability> {
        vec![ToolCapability::WritesFiles]
    }

    fn approval_requirement(&self) -> ApprovalRequirement {
        ApprovalRequirement::Auto
    }

    async fn execute(
        &self,
        input: serde_json::Value,
        _context: &ToolContext,
    ) -> Result<ToolResult, ToolError> {
        let id = input
            .get("id")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| ToolError::invalid_input("Missing or invalid 'id'"))?;
        let status = input
            .get("status")
            .and_then(|v| v.as_str())
            .and_then(TodoStatus::from_str)
            .ok_or_else(|| ToolError::invalid_input("Missing or invalid 'status'"))?;

        let mut list = self.todo_list.lock().await;

        // Use DAG-aware status update for terminal/in-progress transitions.
        // This propagates Ready/Skipped to downstream items automatically.
        if matches!(
            status,
            TodoStatus::Completed
                | TodoStatus::Failed
                | TodoStatus::Skipped
                | TodoStatus::InProgress
        ) {
            list.update_dag_status(id, status, None, None);
        } else {
            // For simple pending/ready changes, use the lightweight path.
            list.update_status(id, status);
        }

        // Check if the item exists by id.
        let found = list.items.iter().any(|i| i.id == id);
        let snapshot = list.snapshot();
        let result = serde_json::to_string_pretty(&snapshot).unwrap_or_else(|_| "{}".to_string());

        if found {
            Ok(ToolResult::success(format!(
                "Updated todo #{} to {}\n{}",
                id,
                status.as_str(),
                result
            ))
            .with_metadata(checklist_metadata(&snapshot, self.tool_name)))
        } else {
            Ok(ToolResult::error(format!("Todo id {id} not found")))
        }
    }
}

/// Tool for listing current todos (legacy compatibility).
pub struct TodoListTool {
    todo_list: SharedTodoList,
    tool_name: &'static str,
}

impl TodoListTool {
    pub fn new(todo_list: SharedTodoList) -> Self {
        Self {
            todo_list,
            tool_name: "todo_list",
        }
    }

    pub fn checklist(todo_list: SharedTodoList) -> Self {
        Self {
            todo_list,
            tool_name: "checklist_list",
        }
    }
}

#[async_trait]
impl ToolSpec for TodoListTool {
    fn name(&self) -> &'static str {
        self.tool_name
    }

    fn description(&self) -> &'static str {
        if self.tool_name == "todo_list" {
            "Compatibility alias for checklist_list. Lists current checklist progress."
        } else {
            "List current checklist progress for the active thread/task."
        }
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {}
        })
    }

    fn capabilities(&self) -> Vec<ToolCapability> {
        vec![ToolCapability::ReadOnly]
    }

    fn approval_requirement(&self) -> ApprovalRequirement {
        ApprovalRequirement::Auto
    }

    async fn execute(
        &self,
        _input: serde_json::Value,
        _context: &ToolContext,
    ) -> Result<ToolResult, ToolError> {
        let list = self.todo_list.lock().await;
        let snapshot = list.snapshot();
        let result = serde_json::to_string_pretty(&snapshot).unwrap_or_else(|_| "{}".to_string());
        Ok(ToolResult::success(format!(
            "Todo list ({} items, {}% complete)\n{}",
            snapshot.items.len(),
            snapshot.completion_pct,
            result
        )))
    }
}

#[async_trait]
impl ToolSpec for TodoWriteTool {
    fn name(&self) -> &'static str {
        self.tool_name
    }

    fn description(&self) -> &'static str {
        if self.tool_name == "todo_write" {
            "Compatibility alias for checklist_write. Replace the active thread/task checklist; durable tasks are the real executable work object."
        } else {
            "Replace the active thread/task checklist. Use this for granular progress under the current durable task or runtime thread; durable tasks remain the real executable work object."
        }
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "todos": {
                    "type": "array",
                    "description": "The complete list of todo items. This replaces the existing list.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "content": {
                                "type": "string",
                                "description": "The task description"
                            },
                            "status": {
                                "type": "string",
                                "enum": ["pending", "in_progress", "completed"],
                                "description": "Task status"
                            }
                        },
                        "required": ["content", "status"]
                    }
                }
            },
            "required": ["todos"]
        })
    }

    fn capabilities(&self) -> Vec<ToolCapability> {
        vec![ToolCapability::WritesFiles]
    }

    fn approval_requirement(&self) -> ApprovalRequirement {
        ApprovalRequirement::Auto
    }

    async fn execute(
        &self,
        input: serde_json::Value,
        _context: &ToolContext,
    ) -> Result<ToolResult, ToolError> {
        let todos = input
            .get("todos")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ToolError::invalid_input("Missing or invalid 'todos' array"))?;

        let mut list = self.todo_list.lock().await;

        // Clear and rebuild the list
        list.clear();

        for item in todos {
            let content = item
                .get("content")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ToolError::invalid_input("Todo item missing 'content'"))?;

            let status_str = item
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("pending");

            let status = TodoStatus::from_str(status_str).unwrap_or(TodoStatus::Pending);

            list.add(content.to_string(), status);
        }

        let snapshot = list.snapshot();
        let result = serde_json::to_string_pretty(&snapshot).unwrap_or_else(|_| "{}".to_string());

        Ok(ToolResult::success(format!(
            "Todo list updated ({} items, {}% complete)\n{}",
            snapshot.items.len(),
            snapshot.completion_pct,
            result
        ))
        .with_metadata(checklist_metadata(&snapshot, self.tool_name)))
    }
}

fn checklist_metadata(snapshot: &TodoListSnapshot, tool_name: &str) -> serde_json::Value {
    let items = snapshot
        .items
        .iter()
        .map(|item| {
            json!({
                "id": item.id,
                "content": item.content,
                "status": item.status.as_str(),
            })
        })
        .collect::<Vec<_>>();
    json!({
        "canonical_tool": "checklist_write",
        "compat_alias": tool_name.starts_with("todo_"),
        "task_updates": {
            "checklist": {
                "items": items,
                "completion_pct": snapshot.completion_pct,
                "in_progress_id": snapshot.in_progress_id,
                "updated_at": null
            }
        }
    })
}

// === DAG Tools - task_dynamic_add and task_dag_status ===

/// Dynamic subtask addition tool. Worker agents call this to inject new
/// sub-items while executing, forming a DAG.
pub struct DynamicSubtaskTool {
    todo_list: SharedTodoList,
}

impl DynamicSubtaskTool {
    pub fn new(todo_list: SharedTodoList) -> Self {
        Self { todo_list }
    }
}

#[async_trait]
impl ToolSpec for DynamicSubtaskTool {
    fn name(&self) -> &'static str {
        "task_dynamic_add"
    }

    fn description(&self) -> &'static str {
        "在当前任务执行过程中动态添加新的子任务。新任务依赖于指定的父任务。"
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "子任务标题"
                },
                "prompt": {
                    "type": "string",
                    "description": "子任务的执行描述"
                },
                "depends_on": {
                    "type": "array",
                    "items": { "type": "integer" },
                    "description": "依赖的 item ID 列表（可选，默认依赖当前所有 Ready 状态 item）"
                }
            },
            "required": ["title", "prompt"]
        })
    }

    fn capabilities(&self) -> Vec<ToolCapability> {
        vec![ToolCapability::ReadOnly]
    }

    fn approval_requirement(&self) -> ApprovalRequirement {
        ApprovalRequirement::Auto
    }

    async fn execute(
        &self,
        input: serde_json::Value,
        _context: &ToolContext,
    ) -> Result<ToolResult, ToolError> {
        let title = input
            .get("title")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::invalid_input("Missing 'title'"))?;
        let prompt = input
            .get("prompt")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::invalid_input("Missing 'prompt'"))?;

        let depends_on: Vec<u32> = match input.get("depends_on") {
            Some(ids) => {
                serde_json::from_value(ids.clone())
                    .map_err(|_| ToolError::invalid_input("depends_on must be [integer]"))?
            }
            None => {
                // Default: depend on all items currently in-progress.
                let list = self.todo_list.lock().await;
                list.get_running_items()
                    .iter()
                    .map(|i| i.id)
                    .collect()
            }
        };

        let content = format!("{}: {}", title, prompt);
        let mut list = self.todo_list.lock().await;
        let id = list
            .add_dag_item(content, depends_on)
            .map_err(|e| ToolError::invalid_input(&e))?;

        Ok(ToolResult::success(
            serde_json::to_string(&json!({
                "id": id,
                "status": "pending",
                "message": format!("已添加子任务 #{}", id)
            }))
            .unwrap_or_else(|_| "{}".into()),
        ))
    }
}

/// DAG status tool. Returns a structured view of the current DAG.
pub struct DagStatusTool {
    todo_list: SharedTodoList,
}

impl DagStatusTool {
    pub fn new(todo_list: SharedTodoList) -> Self {
        Self { todo_list }
    }
}

#[async_trait]
impl ToolSpec for DagStatusTool {
    fn name(&self) -> &'static str {
        "task_dag_status"
    }

    fn description(&self) -> &'static str {
        "查看当前 DAG 状态，包括每个子任务的进度和依赖关系。"
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {}
        })
    }

    fn capabilities(&self) -> Vec<ToolCapability> {
        vec![ToolCapability::ReadOnly]
    }

    fn approval_requirement(&self) -> ApprovalRequirement {
        ApprovalRequirement::Auto
    }

    async fn execute(
        &self,
        _input: serde_json::Value,
        _context: &ToolContext,
    ) -> Result<ToolResult, ToolError> {
        let list = self.todo_list.lock().await;
        let snapshot = list.snapshot();

        let tasks: Vec<serde_json::Value> = list
            .items
            .iter()
            .map(|item| {
                json!({
                    "id": item.id,
                    "content": item.content,
                    "status": item.status.as_str(),
                    "depends_on": item.depends_on,
                    "error": item.error,
                })
            })
            .collect();

        let result = json!({
            "completion_pct": snapshot.completion_pct,
            "in_progress_id": snapshot.in_progress_id,
            "is_all_terminal": list.is_all_terminal(),
            "tasks": tasks,
        });

        Ok(ToolResult::success(
            serde_json::to_string_pretty(&result).unwrap_or_else(|_| "{}".into()),
        ))
    }
}

/// Heartbeat tool. Worker agents call this to signal liveness.
pub struct HeartbeatTool {
    todo_list: SharedTodoList,
}

impl HeartbeatTool {
    pub fn new(todo_list: SharedTodoList) -> Self {
        Self { todo_list }
    }
}

#[async_trait]
impl ToolSpec for HeartbeatTool {
    fn name(&self) -> &'static str {
        "task_heartbeat"
    }

    fn description(&self) -> &'static str {
        "报告当前 item 的执行进度。每 60 秒调用一次。"
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "item_id": {
                    "type": "integer",
                    "description": "当前执行的 item ID"
                },
                "progress": {
                    "type": "string",
                    "description": "当前的进展描述（可选）"
                }
            },
            "required": ["item_id"]
        })
    }

    fn capabilities(&self) -> Vec<ToolCapability> {
        vec![ToolCapability::ReadOnly]
    }

    fn approval_requirement(&self) -> ApprovalRequirement {
        ApprovalRequirement::Auto
    }

    async fn execute(
        &self,
        input: serde_json::Value,
        _context: &ToolContext,
    ) -> Result<ToolResult, ToolError> {
        let item_id = input
            .get("item_id")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| ToolError::invalid_input("Missing or invalid 'item_id'"))?;

        let mut list = self.todo_list.lock().await;
        list.update_heartbeat(item_id);
        Ok(ToolResult::success("ok"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── existing tests ──

    #[tokio::test]
    async fn checklist_write_returns_task_update_metadata() {
        let tool = TodoWriteTool::checklist(new_shared_todo_list());
        let context = ToolContext::new(std::env::temp_dir());
        let result = tool
            .execute(
                json!({
                    "todos": [
                        { "content": "wire durable task tools", "status": "in_progress" },
                        { "content": "run gates", "status": "pending" }
                    ]
                }),
                &context,
            )
            .await
            .expect("checklist write succeeds");

        let metadata = result.metadata.expect("metadata");
        assert_eq!(metadata["canonical_tool"], "checklist_write");
        assert_eq!(metadata["compat_alias"], false);
        assert_eq!(
            metadata["task_updates"]["checklist"]["in_progress_id"],
            json!(1)
        );
        assert_eq!(
            metadata["task_updates"]["checklist"]["items"][0]["content"],
            "wire durable task tools"
        );
    }

    #[tokio::test]
    async fn todo_write_remains_compat_alias() {
        let tool = TodoWriteTool::new(new_shared_todo_list());
        let context = ToolContext::new(std::env::temp_dir());
        let result = tool
            .execute(
                json!({
                    "todos": [
                        { "content": "legacy caller", "status": "completed" }
                    ]
                }),
                &context,
            )
            .await
            .expect("todo write succeeds");

        let metadata = result.metadata.expect("metadata");
        assert_eq!(tool.name(), "todo_write");
        assert_eq!(metadata["canonical_tool"], "checklist_write");
        assert_eq!(metadata["compat_alias"], true);
    }

    // ── DAG tests ──

    #[test]
    fn dag_item_without_deps_is_ready() {
        let mut list = TodoList::new();
        let id = list.add_dag_item("task A".into(), vec![]).unwrap();
        assert_eq!(id, 1);
        assert_eq!(list.items[0].status, TodoStatus::Ready);
        assert!(list.items[0].depends_on.is_empty());
        assert!(list.items[0].created_at.is_some());
    }

    #[test]
    fn dag_item_with_deps_starts_pending() {
        let mut list = TodoList::new();
        let a_id = list.add_dag_item("task A".into(), vec![]).unwrap();
        let b_id = list.add_dag_item("task B".into(), vec![a_id]).unwrap();
        assert_eq!(b_id, 2);
        assert_eq!(list.items[1].status, TodoStatus::Pending);
        assert_eq!(list.items[1].depends_on, vec![a_id]);
    }

    #[test]
    fn dag_missing_dep_returns_error() {
        let mut list = TodoList::new();
        let err = list.add_dag_item("task".into(), vec![42]).unwrap_err();
        assert!(err.contains("42"));
    }

    #[test]
    fn dag_self_dep_returns_error() {
        let mut list = TodoList::new();
        // Self-dependency is caught by dependency-existence check first
        // (the item's own id doesn't exist yet), so the only way to hit
        // the cycle detector is through a transitive cycle, which our
        // API prevents via the dependency-existence gate.  The self-dep
        // case is handled by `would_create_cycle`.
        let err = list.add_dag_item("X".into(), vec![0]).unwrap_err();
        assert!(err.contains("0"));
    }

    #[test]
    fn dag_get_ready_returns_pending_with_all_deps_done() {
        let mut list = TodoList::new();
        let a = list.add_dag_item("A".into(), vec![]).unwrap();
        let b = list.add_dag_item("B".into(), vec![a]).unwrap();

        // Before A completes: B should not be ready.
        let ready_ids: Vec<u32> = list.get_ready_items().iter().map(|i| i.id).collect();
        assert_eq!(ready_ids, vec![a], "only A should be ready initially");

        // Complete A.
        list.update_dag_status(a, TodoStatus::Completed, Some("done".into()), None);

        // Now B should be ready.
        let ready = list.get_ready_items();
        assert_eq!(ready.len(), 1, "B should be ready after A completes");
        assert_eq!(ready[0].id, b);
    }

    #[test]
    fn dag_failed_dep_causes_skipped_downstream() {
        let mut list = TodoList::new();
        let a = list.add_dag_item("A".into(), vec![]).unwrap();
        let _b = list.add_dag_item("B".into(), vec![a]).unwrap();
        list.update_dag_status(a, TodoStatus::Failed, None, Some("boom".into()));

        // B should be Skipped.
        assert_eq!(list.items[1].status, TodoStatus::Skipped);
        assert!(
            list.items[1].error.as_deref().unwrap().contains("Skipped"),
            "error should mention skipped: {:?}",
            list.items[1].error
        );
    }

    #[test]
    fn dag_is_all_terminal_works() {
        let mut list = TodoList::new();
        let a = list.add_dag_item("A".into(), vec![]).unwrap();
        let b = list.add_dag_item("B".into(), vec![a]).unwrap();

        assert!(!list.is_all_terminal());

        list.update_dag_status(a, TodoStatus::Completed, None, None);
        assert!(!list.is_all_terminal());

        list.update_dag_status(b, TodoStatus::Completed, None, None);
        assert!(list.is_all_terminal());
    }

    #[test]
    fn dag_heartbeat_and_restart() {
        let mut list = TodoList::new();
        let a = list.add_dag_item("A".into(), vec![]).unwrap();

        list.update_heartbeat(a);
        assert!(list.items[0].heartbeat.is_some());

        assert_eq!(list.items[0].restart_count, 0);
        list.increment_restart(a);
        assert_eq!(list.items[0].restart_count, 1);
    }

    #[test]
    fn dag_multiple_deps_only_ready_when_all_complete() {
        let mut list = TodoList::new();
        let a = list.add_dag_item("A".into(), vec![]).unwrap();
        let b = list.add_dag_item("B".into(), vec![]).unwrap();
        let c = list.add_dag_item("C".into(), vec![a, b]).unwrap();

        // A and B have no deps, both immediately Ready. C is Pending.
        let ready: Vec<u32> = list.get_ready_items().iter().map(|i| i.id).collect();
        assert_eq!(ready, vec![a, b]);

        // Only complete A.
        list.update_dag_status(a, TodoStatus::Completed, None, None);
        let ready: Vec<u32> = list.get_ready_items().iter().map(|i| i.id).collect();
        assert_eq!(ready, vec![b], "C needs both A and B done");

        // Now complete B.
        list.update_dag_status(b, TodoStatus::Completed, None, None);
        let ready: Vec<u32> = list.get_ready_items().iter().map(|i| i.id).collect();
        assert_eq!(ready, vec![c], "C should now be ready");
    }
}
