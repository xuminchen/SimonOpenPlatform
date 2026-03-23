import { ListPanel } from "../ui/Primitives"

function PlatformConfigsTablePanel({
  platformSearch,
  onPlatformSearchChange,
  onRefresh,
  onCreate,
  filteredPlatformConfigs,
  platformLoading,
  platformsInUse,
  normalizePlatformCode,
  onEdit,
  onDelete,
}) {
  return (
    <ListPanel
      actions={
        <div className="flex flex-wrap items-center gap-2">
          <input
            className="input-base max-w-md"
            placeholder="搜索平台名称或编码..."
            value={platformSearch}
            onChange={onPlatformSearchChange}
          />
          <div className="flex items-center gap-2">
            <button className="btn-subtle" type="button" onClick={onRefresh}>
              ↻
            </button>
            <button className="btn-brand" type="button" onClick={onCreate}>
              + 注册平台
            </button>
          </div>
        </div>
      }
      headerClassName="mb-4"
    >
      <table className="table-shell min-w-[760px]">
        <thead>
          <tr className="table-head-row">
            <th className="table-head-cell">平台编码</th>
            <th className="table-head-cell">平台名称</th>
            <th className="table-head-cell">来源</th>
            <th className="table-head-cell">说明</th>
            <th className="table-head-cell">文档</th>
            <th className="table-head-cell">操作</th>
          </tr>
        </thead>
        <tbody>
          {filteredPlatformConfigs.map((item) => (
            <tr key={item.platform}>
              <td className="table-cell mono-ui">{item.platform}</td>
              <td className="table-cell">
                <span>{item.label || item.platform}</span>
              </td>
              <td className="table-cell">
                {item.mutable ? (
                  <span className="rounded-sm bg-[#E6E6FC] px-2 py-0.5 text-xs text-[#0000E1]">自定义</span>
                ) : (
                  <span className="rounded-sm bg-[#F0EFEC] px-2 py-0.5 text-xs text-slate-600">系统内置</span>
                )}
              </td>
              <td className="table-cell">{item.helper || "-"}</td>
              <td className="table-cell">
                {item.docs_url ? (
                  <a href={item.docs_url} target="_blank" rel="noreferrer" className="text-[#0000E1] hover:underline">
                    查看文档
                  </a>
                ) : (
                  "-"
                )}
              </td>
              <td className="table-cell">
                {item.mutable ? (
                  <div className="inline-flex items-center gap-2">
                    <button className="btn-subtle" type="button" onClick={() => onEdit(item)}>
                      编辑
                    </button>
                    <button
                      className="btn-subtle"
                      type="button"
                      disabled={platformsInUse.has(normalizePlatformCode(item.platform))}
                      title={platformsInUse.has(normalizePlatformCode(item.platform)) ? "该平台存在关联应用，暂不可删除" : "删除平台"}
                      onClick={() => onDelete(item)}
                    >
                      删除
                    </button>
                  </div>
                ) : (
                  <span className="text-slate-400" title="系统内置平台，不可更改">
                    🔒
                  </span>
                )}
              </td>
            </tr>
          ))}
          {!platformLoading && !filteredPlatformConfigs.length && (
            <tr>
              <td className="table-cell" colSpan={6}>
                暂无平台配置
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </ListPanel>
  )
}

export default PlatformConfigsTablePanel
