import { Button, Input, Modal, Select, Space, Spin, Switch } from "antd"

function CredentialEditModal({
  dialog,
  loading,
  saving,
  form,
  availablePlatformConfigs,
  onChangeField,
  onClose,
  onSave,
}) {
  return (
    <Modal
      open={!!dialog}
      title="编辑应用凭证"
      onCancel={onClose}
      footer={
        <Space>
          <Button onClick={onClose} disabled={saving}>取消</Button>
          <Button type="primary" onClick={onSave} disabled={loading || saving} loading={saving}>
            保存修改
          </Button>
        </Space>
      }
      destroyOnHidden
      width={860}
    >
      {loading ? (
        <div className="py-10 text-center">
          <Spin />
        </div>
      ) : (
        <div className="grid gap-3 md:grid-cols-2">
          <label className="field-label">
            名称
            <Input className="mt-1" value={form.name} onChange={(e) => onChangeField("name", e.target.value)} />
          </label>

          <label className="field-label">
            平台
            <Select
              className="mt-1"
              value={form.platform || undefined}
              onChange={(value) => onChangeField("platform", value)}
              options={[
                ...(!availablePlatformConfigs.some((item) => item.platform === form.platform) && form.platform
                  ? [{ value: form.platform, label: form.platform }]
                  : []),
                ...availablePlatformConfigs.map((item) => ({
                  value: item.platform,
                  label: item.label || item.platform,
                })),
              ]}
            />
          </label>

          <label className="field-label">
            app_id
            <Input className="mt-1 mono-ui" value={form.app_id} onChange={(e) => onChangeField("app_id", e.target.value)} />
          </label>

          <label className="field-label">
            secret_key
            <Input.Password className="mt-1 mono-ui" value={form.secret_key} onChange={(e) => onChangeField("secret_key", e.target.value)} />
          </label>

          <label className="field-label">
            auth_code
            <Input className="mt-1 mono-ui" value={form.auth_code} onChange={(e) => onChangeField("auth_code", e.target.value)} />
          </label>

          <label className="field-label">
            refresh_token
            <Input className="mt-1 mono-ui" value={form.refresh_token} onChange={(e) => onChangeField("refresh_token", e.target.value)} />
          </label>

          <label className="field-label">
            token 提前刷新（分钟）
            <Input
              className="mt-1"
              type="number"
              min={5}
              max={180}
              value={form.token_expire_advance_minutes}
              onChange={(e) => onChangeField("token_expire_advance_minutes", e.target.value)}
            />
          </label>

          <div className="field-label flex items-center justify-between rounded-sm border border-slate-200 bg-[#F0EFEC] px-3 py-2">
            <span>自动刷新 token</span>
            <Switch checked={!!form.auto_refresh_token} onChange={(checked) => onChangeField("auto_refresh_token", checked)} />
          </div>

          <label className="field-label md:col-span-2">
            备注
            <Input.TextArea className="mt-1" value={form.remark} onChange={(e) => onChangeField("remark", e.target.value)} autoSize={{ minRows: 3 }} />
          </label>
        </div>
      )}
    </Modal>
  )
}

export default CredentialEditModal
