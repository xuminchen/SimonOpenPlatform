import { createPortal } from "react-dom"
import { ActionButtonGroup, DrawerHeader, FormField } from "../ui/Primitives"

function PlatformDrawerModal({
  portalRoot,
  open,
  visible,
  mode,
  form,
  codeError,
  submitting,
  onClose,
  onSubmit,
  onFieldChange,
  onCodeBlur,
}) {
  if (!open || !portalRoot) return null

  return createPortal(
    <div
      className={`fixed inset-0 -top-px z-50 flex items-stretch justify-end p-0 transition-opacity duration-200 ${
        visible ? "bg-black/30 opacity-100" : "bg-black/0 opacity-0"
      }`}
      onClick={onClose}
    >
      <article
        className={`h-[calc(100vh+1px)] w-full max-w-[480px] overflow-y-auto border-l border-slate-200 bg-[#F0EFEC] p-6 shadow-[0_10px_30px_rgba(15,23,42,0.12)] transition-transform duration-300 ease-[cubic-bezier(0.22,1,0.36,1)] ${
          visible ? "translate-x-0" : "translate-x-full"
        }`}
        onClick={(e) => e.stopPropagation()}
      >
        <DrawerHeader title={mode === "add" ? "注册平台" : "编辑平台"} onClose={onClose} />
        <form className="grid gap-3" onSubmit={onSubmit}>
          <FormField label="平台编码" required>
            <input
              className={`input-base mt-1 mono-ui ${mode === "edit" ? "bg-[#E7E6E2] text-slate-400 cursor-not-allowed" : ""}`}
              value={form.platform}
              onChange={(e) => onFieldChange("platform", e.target.value)}
              onBlur={onCodeBlur}
              placeholder="例如：my_platform"
              disabled={mode === "edit"}
              required
            />
            {codeError ? <p className="mt-1 text-xs text-rose-500">{codeError}</p> : null}
          </FormField>
          <FormField label="平台名称" required>
            <input
              className="input-base mt-1"
              value={form.label}
              onChange={(e) => onFieldChange("label", e.target.value)}
              placeholder="请输入外部平台名称"
              required
            />
          </FormField>
          <FormField label="平台说明">
            <textarea
              className="input-base mt-1 min-h-[96px]"
              maxLength={200}
              value={form.helper}
              onChange={(e) => onFieldChange("helper", e.target.value)}
              placeholder="请简要描述该平台的用途（选填）"
            />
            <p className="mt-1 text-right text-xs text-slate-400">{form.helper.length}/200</p>
          </FormField>
          <FormField label="文档链接">
            <input
              className="input-base mt-1"
              value={form.docs_url}
              onChange={(e) => onFieldChange("docs_url", e.target.value)}
              placeholder="请输入 OpenAPI 对接文档地址（选填）"
            />
          </FormField>
          <ActionButtonGroup>
            <button className="btn-subtle" type="button" onClick={onClose} disabled={submitting}>
              取消
            </button>
            <button className="btn-brand" type="submit" disabled={submitting}>
              {submitting ? "提交中..." : mode === "add" ? "确定注册" : "保存更改"}
            </button>
          </ActionButtonGroup>
        </form>
      </article>
    </div>,
    portalRoot
  )
}

export default PlatformDrawerModal
