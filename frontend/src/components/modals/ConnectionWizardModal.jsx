import { Button, Drawer } from "antd"

function ConnectionWizardModal({
  isOpen,
  mode,
  onClose,
  children,
}) {
  return (
    <Drawer
      open={isOpen}
      onClose={onClose}
      placement="right"
      width={560}
      destroyOnHidden={false}
      title={
        <div>
          <h3 className="m-0 text-lg font-semibold text-slate-900">
            {mode === "setup" ? "新建 Connection Project" : "编辑 Connection Project"}
          </h3>
          <p className="mt-1 text-xs text-slate-500">
            {mode === "setup" ? "先完成项目级配置与连接测试，再保存。" : "在此编辑授权账号并管理接口任务配置。"}
          </p>
        </div>
      }
      extra={<Button onClick={onClose}>关闭</Button>}
      styles={{
        body: { background: "#F0EFEC", padding: 0 },
        header: { background: "#F0EFEC", borderBottom: "1px solid #f1f5f9" },
      }}
    >
      {children}
    </Drawer>
  )
}

export default ConnectionWizardModal
