import { createPortal } from "react-dom"

function ConnectionWizardModal({
  portalRoot,
  isOpen,
  visible,
  mode,
  onClose,
  children,
}) {
  if (!isOpen || !portalRoot) return null

  return createPortal(
    <div className="fixed inset-0 z-[70] flex items-stretch justify-end bg-black/25" onClick={onClose}>
      <article
        className="flex h-full w-full max-w-[560px] flex-col bg-[#F0EFEC] shadow-[0_20px_50px_rgba(15,23,42,0.16)]"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="shrink-0 border-b border-slate-100 px-6 py-4">
          <div>
            <h3 className="m-0 text-lg font-semibold text-slate-900">
              {mode === "setup" ? "新建 Connection Project" : "编辑 Connection Project"}
            </h3>
            <p className="mt-1 text-xs text-slate-500">
              {mode === "setup" ? "先完成项目级配置与连接测试，再保存。" : "在此编辑授权账号并管理接口任务配置。"}
            </p>
          </div>
          <button className="mt-2 rounded-md px-2 py-1 text-xs text-slate-500 transition hover:bg-[#E7E6E2] hover:text-slate-700" onClick={onClose}>关闭</button>
        </div>
        {children}
      </article>
    </div>,
    portalRoot
  )
}

export default ConnectionWizardModal
