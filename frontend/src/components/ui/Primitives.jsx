function cx(...parts) {
  return parts.filter(Boolean).join(" ")
}

export function FormField({ label, required = false, className = "", labelClassName = "", children }) {
  return (
    <label className={cx("field-label", className)}>
      <span className={labelClassName}>
        {label}
        {required ? <span className="required-mark">*</span> : null}
      </span>
      {children}
    </label>
  )
}

export function DrawerHeader({ title, onClose }) {
  return (
    <div className="mb-4 flex items-center justify-between">
      <h3 className="panel-title">{title}</h3>
      <button className="btn-subtle px-2 py-1 text-xs" onClick={onClose}>
        关闭
      </button>
    </div>
  )
}

export function ActionButtonGroup({ columns = 2, className = "", children }) {
  const columnsClass = columns === 3 ? "grid-cols-3" : "grid-cols-2"
  return <div className={cx("mt-1 grid gap-2", columnsClass, className)}>{children}</div>
}

export function ListPanel({ title, actions, className = "", headerClassName = "", children }) {
  return (
    <article className={cx("section-block overflow-auto", className)}>
      {(title || actions) ? (
        <div className={cx("mb-3 flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between", headerClassName)}>
          {title ? <h3 className="panel-title">{title}</h3> : <div />}
          {actions ? <div>{actions}</div> : null}
        </div>
      ) : null}
      {children}
    </article>
  )
}
