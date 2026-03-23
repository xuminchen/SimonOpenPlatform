import { ListPanel } from "../ui/Primitives"

function MonitorTasksPanel({
  taskSearch,
  onTaskSearchChange,
  taskStatus,
  onTaskStatusChange,
  filteredTasks,
  statusClass,
  onTaskDetail,
  detailText,
}) {
  return (
    <section className="space-y-3">
      <ListPanel
        title="调用任务与错误追踪"
        actions={
          <div className="grid gap-2 sm:grid-cols-2">
            <input className="input-base" placeholder="搜索任务类型/账号ID" value={taskSearch} onChange={onTaskSearchChange} />
            <select className="input-base" value={taskStatus} onChange={onTaskStatusChange}>
              <option value="all">全部状态</option>
              <option value="pending">pending</option>
              <option value="running">running</option>
              <option value="success">success</option>
              <option value="failed">failed</option>
            </select>
          </div>
        }
      >
        <table className="table-shell min-w-[760px]">
          <thead>
            <tr className="table-head-row">
              <th className="table-head-cell">ID</th>
              <th className="table-head-cell">账号ID</th>
              <th className="table-head-cell">类型</th>
              <th className="table-head-cell">状态</th>
              <th className="table-head-cell">创建时间</th>
              <th className="table-head-cell">操作</th>
            </tr>
          </thead>
          <tbody>
            {filteredTasks.map((item) => (
              <tr key={item.id}>
                <td className="table-cell">{item.id}</td>
                <td className="table-cell">{item.account_id}</td>
                <td className="table-cell">{item.task_type}</td>
                <td className="table-cell">
                  <span className={statusClass(item.status)}>{item.status}</span>
                </td>
                <td className="table-cell">{item.created_at || "-"}</td>
                <td className="table-cell">
                  <button className="btn-subtle" onClick={() => onTaskDetail(item.id)}>
                    查看
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </ListPanel>

      <article className="section-block">
        <h3 className="panel-title mb-3">详情面板</h3>
        <pre className="max-h-[360px] overflow-auto rounded-sm border border-slate-300 bg-[#F0EFEC] p-3 text-xs text-slate-700 mono-ui">
          {detailText}
        </pre>
      </article>
    </section>
  )
}

export default MonitorTasksPanel
