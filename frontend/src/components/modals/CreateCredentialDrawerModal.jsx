import { Button, Drawer, Input, Select, Space, Switch, Typography } from "antd"

function CreateCredentialDrawerModal({
  open,
  form,
  availablePlatformConfigs,
  currentPlatformSchema,
  accountConfigPreview,
  onClose,
  onSubmit,
  onFieldChange,
}) {
  return (
    <Drawer
      open={open}
      onClose={onClose}
      placement="right"
      width={520}
      destroyOnHidden={false}
      title="新建应用凭证"
      styles={{
        body: { background: "#F0EFEC", padding: 20 },
        header: { background: "#F0EFEC", borderBottom: "1px solid #f1f5f9" },
      }}
    >
      <Typography.Paragraph type="secondary">
        只需要填写第三方平台基础凭证（app_id / secret_key / auth_code）。`access_token` 与 `refresh_token`
        由系统自动维护并从数据库/凭证源读取，不需要人工填写。
      </Typography.Paragraph>

      <form onSubmit={onSubmit} className="grid gap-3">
        <label className="field-label">
          应用名称
          <Input
            className="mt-1"
            value={form.name}
            onChange={(e) => onFieldChange("name", e.target.value)}
            required
          />
        </label>

        <label className="field-label">
          平台
          <Select
            className="mt-1"
            value={form.platform || undefined}
            onChange={(value) => onFieldChange("platform", value)}
            options={[
              ...(availablePlatformConfigs.length
                ? []
                : [{ value: "", label: "请先注册平台", disabled: true }]),
              ...availablePlatformConfigs.map((item) => ({
                value: item.platform,
                label: item.label || item.platform,
              })),
            ]}
          />
        </label>

        <label className="field-label">
          状态
          <Select
            className="mt-1"
            value={form.status}
            onChange={(value) => onFieldChange("status", value)}
            options={[
              { value: "active", label: "active" },
              { value: "disabled", label: "disabled" },
            ]}
          />
        </label>

        <Typography.Paragraph className="rounded-sm border border-slate-200 bg-[#F0EFEC] px-3 py-2 text-xs !mb-0">
          {currentPlatformSchema.helper}
          {currentPlatformSchema.docsUrl ? (
            <>
              {" "}
              <a href={currentPlatformSchema.docsUrl} target="_blank" rel="noreferrer" className="text-[#0000E1] hover:underline">
                查看文档
              </a>
            </>
          ) : null}
        </Typography.Paragraph>

        <label className="field-label">
          app_id
          <Input
            className="mt-1 mono-ui"
            value={form.app_id}
            onChange={(e) => onFieldChange("app_id", e.target.value)}
            placeholder="输入平台 app_id"
            required
          />
        </label>

        <label className="field-label">
          secret_key
          <Input.Password
            className="mt-1 mono-ui"
            value={form.secret_key}
            onChange={(e) => onFieldChange("secret_key", e.target.value)}
            placeholder="输入平台 secret_key / secret"
            required
          />
        </label>

        <label className="field-label">
          auth_code（可选）
          <Input
            className="mt-1 mono-ui"
            value={form.auth_code}
            onChange={(e) => onFieldChange("auth_code", e.target.value)}
            placeholder="首次授权可填写 auth_code"
          />
        </label>

        <label className="field-label">
          token 提前刷新（分钟）
          <Input
            className="mt-1"
            type="number"
            min={5}
            max={180}
            value={form.token_expire_advance_minutes}
            onChange={(e) => onFieldChange("token_expire_advance_minutes", e.target.value)}
          />
        </label>

        <div className="field-label flex items-center justify-between rounded-sm border border-slate-200 bg-[#F0EFEC] px-3 py-2">
          <span>自动刷新 token</span>
          <Switch checked={!!form.auto_refresh_token} onChange={(checked) => onFieldChange("auto_refresh_token", checked)} />
        </div>

        <label className="field-label">
          备注
          <Input.TextArea
            className="mt-1"
            value={form.remark}
            onChange={(e) => onFieldChange("remark", e.target.value)}
            placeholder="可填写业务归属、用途说明"
            autoSize={{ minRows: 3 }}
          />
        </label>

        <label className="field-label">
          配置预览（自动生成）
          <Input.TextArea
            className="mt-1 mono-ui"
            value={JSON.stringify(accountConfigPreview, null, 2)}
            autoSize={{ minRows: 6 }}
            readOnly
          />
        </label>

        <Typography.Text type="secondary" className="text-xs">
          提示：token 会在后台自动刷新并写入系统托管字段，前端表单不会保存你手填的 token。
        </Typography.Text>

        <Space size={8}>
          <Button onClick={onClose}>取消</Button>
          <Button htmlType="submit" data-action="continue">
            保存并继续
          </Button>
          <Button type="primary" htmlType="submit" data-action="create">
            确定创建
          </Button>
        </Space>
      </form>
    </Drawer>
  )
}

export default CreateCredentialDrawerModal
