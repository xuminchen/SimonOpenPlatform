import { createPortal } from "react-dom"

function DestinationDeleteModal({
  portalRoot,
  dialog,
  deleting,
  onClose,
  onTogglePurgeFiles,
  onConfirmDelete,
}) {
  if (!dialog || !portalRoot) return null

  return createPortal(
    <div className="fixed inset-0 z-[85] grid place-items-center bg-black/35 p-4" onClick={() => !deleting && onClose()}>
      <article className="w-full max-w-lg rounded-xl bg-[#F0EFEC] p-5 shadow-[0_18px_40px_rgba(15,23,42,0.24)]" onClick={(e) => e.stopPropagation()}>
        <h4 className="m-0 text-base font-semibold text-slate-900">删除目标库</h4>
        <p className="mt-2 text-sm text-slate-600">
          您确定要删除目标库 <span className="font-semibold">{dialog.profile.name}</span> 吗？此操作将解除系统的关联配置。
        </p>
        <label className="mt-3 inline-flex items-center gap-2 text-sm text-slate-700">
          <input
            type="checkbox"
            className="h-4 w-4 rounded border-slate-300 text-rose-600 focus:ring-rose-500"
            checked={!!dialog.purgeFiles}
            onChange={onTogglePurgeFiles}
          />
          同时删除本地硬盘上的底层文件（危险）
        </label>
        <div className="mt-4 flex items-center justify-end gap-2">
          <button className="btn-subtle px-3 py-1.5 text-xs" onClick={onClose} disabled={deleting}>
            取消
          </button>
          <button className="cxm-ok px-3 py-1.5 text-xs" onClick={onConfirmDelete} disabled={deleting}>
            {deleting ? "删除中..." : "确认删除"}
          </button>
        </div>
      </article>
    </div>,
    portalRoot
  )
}

export default DestinationDeleteModal
