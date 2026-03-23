import { createPortal } from "react-dom"

function ConfirmModal({
  isOpen,
  onClose,
  onConfirm,
  title,
  content,
  confirmText = "确定",
  cancelText = "取消",
  isDanger: _isDanger = false,
  isLoading = false,
}) {
  if (!isOpen) return null
  const portalRoot = typeof document !== "undefined" ? document.body : null
  if (!portalRoot) return null

  return createPortal(
    <div className="fixed inset-0 z-[1000] flex items-center justify-center p-4">
      <div
        className="absolute inset-0 bg-slate-900/40 backdrop-blur-[2px] transition-opacity duration-200"
        onClick={isLoading ? undefined : onClose}
      />
      <div className="relative w-full max-w-md rounded-2xl bg-[#F0EFEC] p-6 shadow-[0_30px_80px_rgba(15,23,42,0.24)]">
        <h3 className="text-lg font-semibold text-slate-900">{title}</h3>
        <p className="mt-2 text-sm leading-relaxed text-slate-500">{content}</p>
        <div className="mt-8 flex justify-end gap-3">
          <button
            type="button"
            onClick={onClose}
            disabled={isLoading}
            className="rounded-lg bg-transparent px-4 py-2 text-sm font-medium text-slate-600 transition-colors hover:bg-[#E7E6E2] disabled:opacity-50"
          >
            {cancelText}
          </button>
          <button
            type="button"
            onClick={onConfirm}
            disabled={isLoading}
            className="cxm-ok"
          >
            {isLoading ? (
              <svg className="-ml-1 mr-2 h-4 w-4 animate-spin text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
            ) : null}
            {confirmText}
          </button>
        </div>
      </div>
    </div>,
    portalRoot
  )
}

export default ConfirmModal
