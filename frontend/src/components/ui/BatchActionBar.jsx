function BatchActionBar({ visible, selectedCount, actions, onClose }) {
  return (
    <div className={`batch-action-bar ${visible ? "is-visible" : ""}`} role="status" aria-live="polite">
      <div className="selected-count">
        已选择 <span key={selectedCount} className="count context-action-count-number">{selectedCount}</span> 项
      </div>
      <div className="context-action-right">
        {actions.map((action, index) => (
          <span key={action.key} className="inline-flex items-center">
            <button
              className={action.tone === "delete" ? "batch-delete-btn" : "batch-update-btn"}
              type="button"
              onClick={action.onClick}
              disabled={action.disabled}
            >
              {action.label}
            </button>
            {index < actions.length - 1 ? <span className="action-divider" /> : null}
          </span>
        ))}
        <span className="action-divider" />
        <button className="context-action-close" type="button" aria-label="取消选择" onClick={onClose}>
          ✕
        </button>
      </div>
    </div>
  )
}

export default BatchActionBar
