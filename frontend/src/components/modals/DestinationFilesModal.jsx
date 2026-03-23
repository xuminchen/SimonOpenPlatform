import { createPortal } from "react-dom"

function DestinationFilesModal({
  portalRoot,
  dialog,
  loading,
  onClose,
  formatBytes,
  formatTimeText,
}) {
  if (!dialog || !portalRoot) return null

  return createPortal(
    <div className="fixed inset-0 z-[80] grid place-items-center bg-black/30 p-4" onClick={onClose}>
      <article
        className="w-full max-w-3xl rounded-xl bg-[#F0EFEC] p-5 shadow-[0_18px_40px_rgba(15,23,42,0.2)]"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="mb-3 flex items-center justify-between">
          <div>
            <h4 className="m-0 text-base font-semibold text-slate-900">已同步文件</h4>
            <p className="mt-1 text-xs text-slate-500">
              {dialog.profile?.name} · {dialog.relative_path}
            </p>
          </div>
          <button className="btn-subtle px-2 py-1 text-xs" onClick={onClose}>关闭</button>
        </div>
        {loading ? (
          <p className="text-sm text-slate-500">加载中...</p>
        ) : (
          <div className="overflow-auto">
            <table className="table-shell min-w-[680px]">
              <thead>
                <tr className="table-head-row">
                  <th className="table-head-cell">文件名</th>
                  <th className="table-head-cell">大小</th>
                  <th className="table-head-cell">生成时间</th>
                  <th className="table-head-cell">下载</th>
                </tr>
              </thead>
              <tbody>
                {dialog.files.map((file) => (
                  <tr key={file.name}>
                    <td className="table-cell mono-ui">{file.name}</td>
                    <td className="table-cell">{formatBytes(file.size_bytes)}</td>
                    <td className="table-cell mono-ui">{formatTimeText(file.modified_at)}</td>
                    <td className="table-cell">
                      <a
                        href={`/api/v1/destinations/${dialog.profile.id}/files/download?name=${encodeURIComponent(file.name)}`}
                        className="btn-subtle px-2 py-1 text-xs"
                      >
                        ⬇️ 下载
                      </a>
                    </td>
                  </tr>
                ))}
                {dialog.files.length === 0 && (
                  <tr>
                    <td className="table-cell text-slate-400" colSpan={4}>暂无可下载文件</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </article>
    </div>,
    portalRoot
  )
}

export default DestinationFilesModal
