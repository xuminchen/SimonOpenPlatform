import { createPortal } from "react-dom"
import { ActionButtonGroup, DrawerHeader, FormField } from "../ui/Primitives"

function CreateCredentialDrawerModal({
  portalRoot,
  open,
  visible,
  form,
  availablePlatformConfigs,
  currentPlatformSchema,
  accountConfigPreview,
  onClose,
  onSubmit,
  onFieldChange,
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
        className={`h-[calc(100vh+1px)] w-full max-w-[520px] overflow-y-auto border-l border-slate-200 bg-[#F0EFEC] p-6 shadow-[0_10px_30px_rgba(15,23,42,0.12)] transition-transform duration-300 ease-[cubic-bezier(0.22,1,0.36,1)] ${
          visible ? "translate-x-0" : "translate-x-full"
        }`}
        onClick={(e) => e.stopPropagation()}
      >
        <DrawerHeader title="新建应用凭证" onClose={onClose} />
        <p className="mb-4 text-sm text-slate-500">
          只需要填写第三方平台基础凭证（app_id / secret_key / auth_code）。`access_token` 与 `refresh_token` 由系统自动维护并从数据库/凭证源读取，不需要人工填写。
        </p>
        <form className="grid gap-3" onSubmit={onSubmit}>
          <h4 className="text-xs font-bold uppercase tracking-wide text-slate-500 mono-ui">基础信息</h4>
          <FormField label="应用名称" required>
            <input className="input-base mt-1" value={form.name} onChange={(e) => onFieldChange("name", e.target.value)} required />
          </FormField>
          <FormField label="平台" required>
            <select className="input-base mt-1" value={form.platform} onChange={(e) => onFieldChange("platform", e.target.value)} required>
              <option value="" disabled>{availablePlatformConfigs.length ? "请选择平台" : "请先注册平台"}</option>
              {availablePlatformConfigs.map((item) => (
                <option key={item.platform} value={item.platform}>{item.label || item.platform}</option>
              ))}
            </select>
          </FormField>
          <FormField label="状态" required>
            <select className="input-base mt-1" value={form.status} onChange={(e) => onFieldChange("status", e.target.value)} required>
              <option value="active">active</option>
              <option value="disabled">disabled</option>
            </select>
          </FormField>
          <p className="rounded-sm border border-slate-200 bg-[#F0EFEC] px-3 py-2 text-xs text-slate-600">
            {currentPlatformSchema.helper}
            {currentPlatformSchema.docsUrl ? (
              <>
                {" "}
                <a href={currentPlatformSchema.docsUrl} target="_blank" rel="noreferrer" className="text-[#0000E1] hover:underline">
                  查看文档
                </a>
              </>
            ) : null}
          </p>

          <h4 className="mt-2 text-xs font-bold uppercase tracking-wide text-slate-500 mono-ui">应用凭证</h4>
          <FormField label="app_id" required>
            <input
              className="input-base mt-1 mono-ui"
              value={form.app_id}
              onChange={(e) => onFieldChange("app_id", e.target.value)}
              placeholder="输入平台 app_id"
              required
            />
          </FormField>
          <FormField label="secret_key" required>
            <input
              className="input-base mt-1 mono-ui"
              type="password"
              value={form.secret_key}
              onChange={(e) => onFieldChange("secret_key", e.target.value)}
              placeholder="输入平台 secret_key / secret"
              required
            />
          </FormField>

          <h4 className="mt-2 text-xs font-bold uppercase tracking-wide text-slate-500 mono-ui">授权信息</h4>
          <FormField label="auth_code（可选）">
            <input
              className="input-base mt-1 mono-ui"
              value={form.auth_code}
              onChange={(e) => onFieldChange("auth_code", e.target.value)}
              placeholder="首次授权可填写 auth_code"
            />
          </FormField>
          <FormField label="token 提前刷新（分钟）">
            <input
              type="number"
              min="5"
              max="180"
              className="input-base mt-1"
              value={form.token_expire_advance_minutes}
              onChange={(e) => onFieldChange("token_expire_advance_minutes", e.target.value)}
            />
          </FormField>
          <label className="field-label flex items-center justify-between gap-3 rounded-sm border border-slate-200 bg-[#F0EFEC] px-3 py-2">
            <span>自动刷新 token</span>
            <input
              type="checkbox"
              checked={form.auto_refresh_token}
              onChange={(e) => onFieldChange("auto_refresh_token", e.target.checked)}
            />
          </label>
          <FormField label="备注">
            <textarea
              className="input-base mt-1 min-h-[72px]"
              value={form.remark}
              onChange={(e) => onFieldChange("remark", e.target.value)}
              placeholder="可填写业务归属、用途说明"
            />
          </FormField>

          <FormField label="配置预览（自动生成）">
            <textarea className="input-base mt-1 min-h-[150px] mono-ui" value={JSON.stringify(accountConfigPreview, null, 2)} readOnly />
          </FormField>
          <p className="text-xs text-slate-500">
            提示：token 会在后台自动刷新并写入系统托管字段，前端表单不会保存你手填的 token。
          </p>
          <ActionButtonGroup columns={3}>
            <button className="btn-subtle" type="button" onClick={onClose}>取消</button>
            <button className="btn-ghost-brand" type="submit" data-action="continue">保存并继续</button>
            <button className="btn-brand" type="submit" data-action="create">确定创建</button>
          </ActionButtonGroup>
        </form>
      </article>
    </div>,
    portalRoot
  )
}

export default CreateCredentialDrawerModal
