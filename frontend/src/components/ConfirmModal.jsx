import { Modal } from "antd"

function ConfirmModal({
  isOpen,
  onClose,
  onConfirm,
  title,
  content,
  confirmText = "确定",
  cancelText = "取消",
  isDanger = false,
  isLoading = false,
}) {
  return (
    <Modal
      open={isOpen}
      title={title}
      okText={confirmText}
      cancelText={cancelText}
      onOk={onConfirm}
      onCancel={onClose}
      confirmLoading={isLoading}
      okButtonProps={isDanger ? { danger: true } : undefined}
      maskClosable={!isLoading}
      centered
      destroyOnHidden
    >
      <p className="mt-1 text-sm leading-relaxed text-slate-500">{content}</p>
    </Modal>
  )
}

export default ConfirmModal
