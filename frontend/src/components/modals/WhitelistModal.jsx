import { createPortal } from "react-dom"

function WhitelistModal({
  portalRoot,
  dialog,
  value,
  saving,
  onValueChange,
  onClose,
  onSave,
}) {
  if (!dialog || !portalRoot) return null

  return createPortal(
    <div className="fixed inset-0 z-[60] grid place-items-center bg-black/30 p-4" onClick={onClose}>
      <article
        className="w-full max-w-xl rounded-sm border border-slate-200 bg-[#F0EFEC] p-5 shadow-[0_10px_30px_rgba(15,23,42,0.12)]"
        onClick={(e) => e.stopPropagation()}
      >
        <h4 className="m-0 text-base font-semibold text-slate-900">编辑 IP 白名单</h4>
        <p className="mt-3 text-sm text-slate-600">
          当前应用：{dialog.name}。请输入允许访问的 IP，支持英文逗号或换行分隔。
        </p>
        <label className="field-label mt-3 block">
          IP 列表
          <textarea
            className="input-base mt-1 min-h-[140px]"
            value={value}
            onChange={onValueChange}
            placeholder={"例如:\n1.1.1.1\n2.2.2.2"}
          />
        </label>
        <div className="mt-4 grid grid-cols-2 gap-2">
          <button className="btn-subtle" type="button" onClick={onClose} disabled={saving}>
            取消
          </button>
          <button className="btn-brand" type="button" onClick={onSave} disabled={saving}>
            {saving ? "保存中..." : "保存白名单"}
          </button>
        </div>
      </article>
    </div>,
    portalRoot
  )
}

export default WhitelistModal
