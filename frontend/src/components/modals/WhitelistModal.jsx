import { Button, Input, Modal, Space, Typography } from "antd"

function WhitelistModal({
  dialog,
  value,
  saving,
  onValueChange,
  onClose,
  onSave,
}) {
  return (
    <Modal
      open={!!dialog}
      title="编辑 IP 白名单"
      onCancel={onClose}
      footer={
        <Space>
          <Button onClick={onClose} disabled={saving}>取消</Button>
          <Button type="primary" onClick={onSave} loading={saving}>
            保存白名单
          </Button>
        </Space>
      }
      destroyOnHidden
      width={720}
    >
      <Typography.Paragraph type="secondary">
        当前应用：{dialog?.name || "-"}。请输入允许访问的 IP，支持英文逗号或换行分隔。
      </Typography.Paragraph>
      <label className="field-label block">
        IP 列表
        <Input.TextArea
          className="mt-1"
          value={value}
          onChange={onValueChange}
          placeholder={"例如:\n1.1.1.1\n2.2.2.2"}
          autoSize={{ minRows: 6 }}
        />
      </label>
    </Modal>
  )
}

export default WhitelistModal
