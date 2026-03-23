import { createPortal } from "react-dom"
import { ActionButtonGroup, FormField } from "../ui/Primitives"

function CredentialEditModal({
  portalRoot,
  dialog,
  loading,
  saving,
  form,
  availablePlatformConfigs,
  onChangeField,
  onClose,
  onSave,
}) {
  if (!dialog || !portalRoot) return null

  return createPortal(
    <div className="fixed inset-0 z-[60] grid place-items-center bg-black/30 p-4" onClick={onClose}>
      <article
        className="w-full max-w-2xl rounded-sm border border-slate-200 bg-[#F0EFEC] p-5 shadow-[0_10px_30px_rgba(15,23,42,0.12)]"
        onClick={(e) => e.stopPropagation()}
      >
        <h4 className="m-0 text-base font-semibold text-slate-900">编辑应用凭证</h4>
        {loading ? (
          <p className="mt-3 text-sm text-slate-600">加载中...</p>
        ) : (
          <div className="mt-3 grid gap-3 md:grid-cols-2">
            <FormField label="名称">
              <input className="input-base mt-1" value={form.name} onChange={(e) => onChangeField("name", e.target.value)} />
            </FormField>
            <FormField label="平台">
              <select className="input-base mt-1" value={form.platform} onChange={(e) => onChangeField("platform", e.target.value)}>
                {!availablePlatformConfigs.some((item) => item.platform === form.platform) && form.platform && (
                  <option value={form.platform}>{form.platform}</option>
                )}
                {availablePlatformConfigs.map((item) => (
                  <option key={item.platform} value={item.platform}>{item.label || item.platform}</option>
                ))}
              </select>
            </FormField>
            <FormField label="app_id">
              <input className="input-base mt-1 mono-ui" value={form.app_id} onChange={(e) => onChangeField("app_id", e.target.value)} />
            </FormField>
            <FormField label="secret_key">
              <input className="input-base mt-1 mono-ui" type="password" value={form.secret_key} onChange={(e) => onChangeField("secret_key", e.target.value)} />
            </FormField>
            <FormField label="auth_code">
              <input className="input-base mt-1 mono-ui" value={form.auth_code} onChange={(e) => onChangeField("auth_code", e.target.value)} />
            </FormField>
            <FormField label="refresh_token">
              <input className="input-base mt-1 mono-ui" value={form.refresh_token} onChange={(e) => onChangeField("refresh_token", e.target.value)} />
            </FormField>
            <FormField label="token 提前刷新（分钟）">
              <input
                type="number"
                min="5"
                max="180"
                className="input-base mt-1"
                value={form.token_expire_advance_minutes}
                onChange={(e) => onChangeField("token_expire_advance_minutes", e.target.value)}
              />
            </FormField>
            <label className="field-label flex items-center justify-between gap-3 rounded-sm border border-slate-200 bg-[#F0EFEC] px-3 py-2">
              <span>自动刷新 token</span>
              <input type="checkbox" checked={form.auto_refresh_token} onChange={(e) => onChangeField("auto_refresh_token", e.target.checked)} />
            </label>
            <FormField label="备注" className="md:col-span-2">
              <textarea className="input-base mt-1 min-h-[72px]" value={form.remark} onChange={(e) => onChangeField("remark", e.target.value)} />
            </FormField>
          </div>
        )}
        <ActionButtonGroup className="mt-4">
          <button className="btn-subtle" type="button" onClick={onClose} disabled={saving}>
            取消
          </button>
          <button className="btn-brand" type="button" onClick={onSave} disabled={loading || saving}>
            {saving ? "保存中..." : "保存修改"}
          </button>
        </ActionButtonGroup>
      </article>
    </div>,
    portalRoot
  )
}

export default CredentialEditModal
