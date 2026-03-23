import { useCallback, useEffect, useMemo, useState } from "react"
import { useNavigate } from "react-router-dom"
import { apiFetch as apiRequest } from "../api/client"

const PANEL_KEYS = {
  request: "request",
  auth: "auth",
  pagination: "pagination",
  extract: "extract",
  vars: "vars",
}

const CONSOLE_TABS = [
  { key: "request", label: "HTTP Request" },
  { key: "raw", label: "Raw Response" },
  { key: "extracted", label: "Extracted Records" },
  { key: "schema", label: "Inferred Schema" },
]

const EMPTY_TEST_OUTPUT = {
  request_preview: {},
  raw_response: {},
  extracted_records: [],
  inferred_schema: [],
}

const SYNC_MODE_OPTIONS = ["full_refresh", "incremental"]

function parseLooseValue(rawValue) {
  const text = String(rawValue ?? "").trim()
  if (!text) return ""
  if (text === "true") return true
  if (text === "false") return false
  if (text === "null") return null
  if (/^-?\d+(\.\d+)?$/.test(text)) return Number(text)
  if ((text.startsWith("{") && text.endsWith("}")) || (text.startsWith("[") && text.endsWith("]"))) {
    try {
      return JSON.parse(text)
    } catch {
      return rawValue
    }
  }
  return rawValue
}

function rowsToObject(rows, parseValue = true) {
  return rows.reduce((acc, row) => {
    const key = String(row.key || "").trim()
    if (!key) return acc
    const value = parseValue ? parseLooseValue(row.value) : String(row.value || "")
    acc[key] = value
    return acc
  }, {})
}

function objectToRows(payload) {
  const entries = Object.entries(payload || {})
  if (!entries.length) {
    return [{ id: `row-${Date.now()}`, key: "", value: "" }]
  }
  return entries.map(([key, value], index) => ({
    id: `row-${Date.now()}-${index}`,
    key,
    value: typeof value === "string" ? value : JSON.stringify(value),
  }))
}

function withOneRow(rows) {
  return rows.length ? rows : [{ id: `row-${Date.now()}`, key: "", value: "" }]
}

function buildRow() {
  return { id: `row-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`, key: "", value: "" }
}

function safeJson(value) {
  try {
    return JSON.stringify(value ?? {}, null, 2)
  } catch {
    return String(value)
  }
}

function colorizeJson(value) {
  const json = safeJson(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")

  return json.replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\btrue\b|\bfalse\b|\bnull\b|-?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?)/g,
    (token) => {
      let cls = "text-amber-200"
      if (token.startsWith('"')) {
        cls = token.endsWith(":") ? "text-sky-300" : "text-emerald-300"
      } else if (token === "true" || token === "false") {
        cls = "text-violet-300"
      } else if (token === "null") {
        cls = "text-rose-300"
      }
      return `<span class="${cls}">${token}</span>`
    }
  )
}

function JsonBlock({ value }) {
  const html = useMemo(() => colorizeJson(value), [value])
  return (
    <pre
      className="mono-ui m-0 min-h-full whitespace-pre-wrap break-words text-xs leading-relaxed"
      dangerouslySetInnerHTML={{ __html: html }}
    />
  )
}

function KeyValueEditor({ rows, onChange, keyPlaceholder, valuePlaceholder, parseHint }) {
  const updateRow = useCallback(
    (rowId, field, nextValue) => {
      onChange((prev) => prev.map((row) => (row.id === rowId ? { ...row, [field]: nextValue } : row)))
    },
    [onChange]
  )

  const removeRow = useCallback(
    (rowId) => {
      onChange((prev) => {
        const next = prev.filter((row) => row.id !== rowId)
        return withOneRow(next)
      })
    },
    [onChange]
  )

  return (
    <div className="grid gap-2">
      {rows.map((row) => (
        <div className="grid gap-2 md:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto]" key={row.id}>
          <input
            className="input-base"
            placeholder={keyPlaceholder}
            value={row.key}
            onChange={(e) => updateRow(row.id, "key", e.target.value)}
          />
          <input
            className="input-base"
            placeholder={valuePlaceholder}
            value={row.value}
            onChange={(e) => updateRow(row.id, "value", e.target.value)}
          />
          <button type="button" className="btn-subtle border-rose-200 bg-rose-50 text-rose-700" onClick={() => removeRow(row.id)}>
            删除
          </button>
        </div>
      ))}
      <div className="flex flex-wrap items-center justify-between gap-2">
        <button type="button" className="btn-subtle" onClick={() => onChange((prev) => [...prev, buildRow()])}>
          + 添加变量
        </button>
        {parseHint ? <span className="panel-subtitle">支持 `true / false / number / JSON` 自动识别</span> : null}
      </div>
    </div>
  )
}

function Panel({ title, subtitle, open, onToggle, children }) {
  return (
    <section className="section-block mb-3 p-0">
      <button type="button" className="flex w-full items-center justify-between bg-transparent px-4 py-3 text-left" onClick={onToggle}>
        <div>
          <h2 className="panel-title">{title}</h2>
          <p className="panel-subtitle">{subtitle}</p>
        </div>
        <span
          className={`inline-flex h-6 w-6 items-center justify-center text-[var(--brand)] transition-transform ${
            open ? "rotate-180" : ""
          }`}
        >
          ⌄
        </span>
      </button>
      {open ? <div className="border-t border-slate-200 px-4 py-4">{children}</div> : null}
    </section>
  )
}

function ConnectorBuilderPage({ embedded = false }) {
  const navigate = useNavigate()

  const [platformOptions, setPlatformOptions] = useState([])
  const [platformOptionsLoading, setPlatformOptionsLoading] = useState(false)
  const [platformOptionsError, setPlatformOptionsError] = useState("")
  const [platformCode, setPlatformCode] = useState("")
  const [streamName, setStreamName] = useState("")
  const [displayName, setDisplayName] = useState("")
  const [docUrl, setDocUrl] = useState("")

  const [requestMethod, setRequestMethod] = useState("GET")
  const [urlBase, setUrlBase] = useState("")
  const [urlPath, setUrlPath] = useState("")
  const [headersRows, setHeadersRows] = useState([{ id: "header-0", key: "", value: "" }])
  const [queryRows, setQueryRows] = useState([{ id: "query-0", key: "", value: "" }])
  const [bodyRows, setBodyRows] = useState([{ id: "body-0", key: "", value: "" }])

  const [authType, setAuthType] = useState("None")
  const [authInjectInto, setAuthInjectInto] = useState("header")
  const [authKeyName, setAuthKeyName] = useState("Authorization")
  const [authTestVariable, setAuthTestVariable] = useState("token")

  const [paginationType, setPaginationType] = useState("none")
  const [paginationCursorPath, setPaginationCursorPath] = useState("")
  const [paginationInjectParam, setPaginationInjectParam] = useState("")

  const [recordSelector, setRecordSelector] = useState("$.data.list")
  const [testVarRows, setTestVarRows] = useState([{ id: "testvar-0", key: "token", value: "" }])
  const [syncModes, setSyncModes] = useState([...SYNC_MODE_OPTIONS])

  const [consoleTab, setConsoleTab] = useState("request")
  const [testOutput, setTestOutput] = useState(EMPTY_TEST_OUTPUT)
  const [testMessage, setTestMessage] = useState("请先在左侧配置规则，再点击“测试抓取”。")
  const [testError, setTestError] = useState("")
  const [testing, setTesting] = useState(false)
  const [saving, setSaving] = useState(false)
  const [publishMessage, setPublishMessage] = useState("")
  const [publishError, setPublishError] = useState("")

  const [streams, setStreams] = useState([])
  const [streamsLoading, setStreamsLoading] = useState(false)
  const [streamsError, setStreamsError] = useState("")
  const [selectedStreamId, setSelectedStreamId] = useState(null)

  const [lastTestConfigDigest, setLastTestConfigDigest] = useState("")

  const [openPanels, setOpenPanels] = useState({
    [PANEL_KEYS.request]: true,
    [PANEL_KEYS.auth]: true,
    [PANEL_KEYS.pagination]: true,
    [PANEL_KEYS.extract]: true,
    [PANEL_KEYS.vars]: true,
  })

  const platformOptionMap = useMemo(() => {
    const map = {}
    platformOptions.forEach((item) => {
      const code = String(item?.platform || "").trim()
      if (!code) return
      map[code] = item
    })
    return map
  }, [platformOptions])

  const platformSelectOptions = useMemo(() => {
    const base = [...platformOptions]
    const current = String(platformCode || "").trim()
    if (current && !base.some((x) => String(x?.platform || "").trim() === current)) {
      base.push({
        platform: current,
        label: `${current}（未在平台管理中注册）`,
        docs_url: "",
      })
    }
    return base
  }, [platformCode, platformOptions])

  const requestConfigPayload = useMemo(
    () => ({
      url_base: String(urlBase || "").trim(),
      url_path: String(urlPath || "").trim(),
      method: requestMethod,
      headers: rowsToObject(headersRows, false),
      query_params: rowsToObject(queryRows, true),
      body: rowsToObject(bodyRows, true),
    }),
    [urlBase, urlPath, requestMethod, headersRows, queryRows, bodyRows]
  )

  const authStrategyPayload = useMemo(
    () => ({
      type: authType,
      inject_into: authInjectInto,
      key_name: String(authKeyName || "").trim(),
      test_variable: String(authTestVariable || "").trim() || "token",
    }),
    [authType, authInjectInto, authKeyName, authTestVariable]
  )

  const paginationPayload = useMemo(
    () => ({
      type: paginationType,
      cursor_path: String(paginationCursorPath || "").trim(),
      inject_param: String(paginationInjectParam || "").trim(),
    }),
    [paginationType, paginationCursorPath, paginationInjectParam]
  )

  const extractionPayload = useMemo(
    () => ({
      record_selector: String(recordSelector || "").trim() || "$.data.list",
    }),
    [recordSelector]
  )

  const publishPayload = useMemo(
    () => ({
      platform_code: String(platformCode || "").trim(),
      stream_name: String(streamName || "").trim(),
      display_name: String(displayName || "").trim(),
      doc_url: String(docUrl || "").trim(),
      request_config: requestConfigPayload,
      auth_strategy: authStrategyPayload,
      pagination_strategy: paginationPayload,
      extraction_strategy: extractionPayload,
      supported_sync_modes: syncModes,
    }),
    [
      platformCode,
      streamName,
      displayName,
      docUrl,
      requestConfigPayload,
      authStrategyPayload,
      paginationPayload,
      extractionPayload,
      syncModes,
    ]
  )

  const publishDigest = useMemo(() => safeJson(publishPayload), [publishPayload])

  const testPayload = useMemo(
    () => ({
      ...publishPayload,
      test_variables: rowsToObject(testVarRows, true),
    }),
    [publishPayload, testVarRows]
  )

  const canPublish = Boolean(
    !testing &&
      !saving &&
      lastTestConfigDigest &&
      lastTestConfigDigest === publishDigest &&
      publishPayload.platform_code &&
      publishPayload.stream_name &&
      publishPayload.request_config.url_base
  )

  const activeConsolePayload = useMemo(() => {
    if (consoleTab === "request") return testOutput.request_preview || {}
    if (consoleTab === "raw") return testOutput.raw_response || {}
    if (consoleTab === "extracted") return testOutput.extracted_records || []
    return testOutput.inferred_schema || []
  }, [consoleTab, testOutput])

  const streamIdentity = useMemo(() => {
    const label = displayName.trim() || streamName.trim() || "未命名 Stream"
    const platform = platformCode.trim() || "未指定平台"
    const stream = streamName.trim() || "-"
    return `${platform} - ${label} (${stream})`
  }, [platformCode, displayName, streamName])

  const toggleMode = useCallback((mode) => {
    setSyncModes((prev) => {
      if (prev.includes(mode)) {
        const next = prev.filter((item) => item !== mode)
        return next.length ? next : [mode]
      }
      return [...prev, mode]
    })
  }, [])

  const loadStreams = useCallback(
    async (targetPlatform = "") => {
      setStreamsLoading(true)
      setStreamsError("")
      try {
        const code = String(targetPlatform || "").trim()
        const suffix = code ? `?platform_code=${encodeURIComponent(code)}` : ""
        const data = await apiRequest(`/api/v1/builder/streams${suffix}`)
        setStreams(Array.isArray(data) ? data : [])
      } catch (err) {
        setStreamsError(err.message)
      } finally {
        setStreamsLoading(false)
      }
    },
    []
  )

  useEffect(() => {
    loadStreams("")
  }, [loadStreams])

  const loadPlatformOptions = useCallback(async () => {
    setPlatformOptionsLoading(true)
    setPlatformOptionsError("")
    try {
      const data = await apiRequest("/api/v1/platform-configs")
      const rows = (Array.isArray(data) ? data : []).map((item) => ({
        platform: String(item?.platform || "").trim(),
        label: String(item?.label || item?.platform || "").trim(),
        docs_url: String(item?.docs_url || "").trim(),
      })).filter((item) => item.platform)
      setPlatformOptions(rows)
    } catch (err) {
      setPlatformOptionsError(err.message)
      setPlatformOptions([])
    } finally {
      setPlatformOptionsLoading(false)
    }
  }, [])

  useEffect(() => {
    loadPlatformOptions()
  }, [loadPlatformOptions])

  const loadStreamToForm = useCallback((item) => {
    setSelectedStreamId(item.id)

    setPlatformCode(item.platform_code || "")
    setStreamName(item.stream_name || "")
    setDisplayName(item.display_name || "")
    setDocUrl(item.doc_url || "")

    const request = item.request_config || {}
    setRequestMethod(String(request.method || "GET").toUpperCase())
    setUrlBase(String(request.url_base || ""))
    setUrlPath(String(request.url_path || ""))
    setHeadersRows(withOneRow(objectToRows(request.headers || {})))
    setQueryRows(withOneRow(objectToRows(request.query_params || {})))
    setBodyRows(withOneRow(objectToRows(request.body || {})))

    const auth = item.auth_strategy || {}
    setAuthType(String(auth.type || "None"))
    setAuthInjectInto(String(auth.inject_into || "header"))
    setAuthKeyName(String(auth.key_name || "Authorization"))
    setAuthTestVariable(String(auth.test_variable || "token"))

    const pagination = item.pagination_strategy || {}
    setPaginationType(String(pagination.type || "none"))
    setPaginationCursorPath(String(pagination.cursor_path || ""))
    setPaginationInjectParam(String(pagination.inject_param || ""))

    const extraction = item.extraction_strategy || {}
    setRecordSelector(String(extraction.record_selector || "$.data.list"))

    const streamModes = Array.isArray(item.supported_sync_modes) && item.supported_sync_modes.length
      ? item.supported_sync_modes
      : [...SYNC_MODE_OPTIONS]
    setSyncModes(streamModes)

    setTestMessage("已载入发布配置，可直接测试抓取。")
    setTestError("")
    setPublishMessage("")
    setPublishError("")
    setLastTestConfigDigest("")
    setTestOutput(EMPTY_TEST_OUTPUT)
    setConsoleTab("request")
  }, [])

  const handleTest = useCallback(async () => {
    setPublishMessage("")
    setPublishError("")

    if (!publishPayload.stream_name) {
      setTestError("请先填写 Stream Name。")
      return
    }
    if (!publishPayload.request_config.url_base) {
      setTestError("请先填写 API URL（url_base）。")
      return
    }

    setTesting(true)
    setTestError("")
    setTestMessage("测试抓取中，后端正在进行代理请求与 JSONPath 剥离...")

    try {
      const data = await apiRequest("/api/v1/builder/test", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(testPayload),
      })
      setTestOutput({ ...EMPTY_TEST_OUTPUT, ...data })
      setLastTestConfigDigest(publishDigest)
      const rows = Array.isArray(data.extracted_records) ? data.extracted_records.length : 0
      const fields = Array.isArray(data.inferred_schema) ? data.inferred_schema.length : 0
      setTestMessage(`测试成功：提取到 ${rows} 条记录，推断 ${fields} 个字段。`)
      setConsoleTab("extracted")
    } catch (err) {
      setTestError(err.message)
      setLastTestConfigDigest("")
      setTestMessage("测试失败，请检查左侧规则与测试变量。")
    } finally {
      setTesting(false)
    }
  }, [publishPayload, testPayload, publishDigest])

  const handlePublish = useCallback(async () => {
    if (!canPublish) return

    setSaving(true)
    setPublishError("")
    setPublishMessage("发布中...")

    try {
      const data = await apiRequest("/api/v1/builder/streams", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(publishPayload),
      })
      setPublishMessage(`发布成功：${data.platform_code} / ${data.stream_name}（ID: ${data.id}）`)
      setSelectedStreamId(data.id)
      await loadStreams(publishPayload.platform_code)
    } catch (err) {
      setPublishError(err.message)
      setPublishMessage("")
    } finally {
      setSaving(false)
    }
  }, [canPublish, publishPayload, loadStreams])

  return (
    <div className={embedded ? "space-y-3" : "min-h-screen bg-[#f4f6f8] text-slate-900 flex flex-col"}>
      <header
        className={
          embedded
            ? "section-block flex flex-wrap items-center justify-between gap-3 p-4"
            : "sticky top-0 z-30 flex items-center justify-between gap-3 border-b border-slate-200 bg-[#f4f6f8]/95 px-4 py-3 backdrop-blur"
        }
      >
        <div className="flex min-w-0 items-center gap-3">
          {!embedded ? (
            <button type="button" className="btn-subtle" onClick={() => navigate(-1)}>
              返回
            </button>
          ) : null}
          <div className="min-w-0">
            <p className="mono-ui m-0 text-[11px] uppercase tracking-[0.08em] text-slate-500">SimonOpenPlatform · Connector Builder</p>
            <h1 className="panel-title m-0 mt-1 truncate">{streamIdentity}</h1>
          </div>
        </div>
        <div className="flex shrink-0 flex-wrap items-center gap-2">
          <a
            className={docUrl.trim() ? "btn-subtle" : "btn-subtle opacity-45 cursor-not-allowed"}
            href={docUrl.trim() || undefined}
            target="_blank"
            rel="noreferrer"
            onClick={(e) => {
              if (!docUrl.trim()) e.preventDefault()
            }}
          >
            查看官方 API 文档
          </a>
          <button type="button" className="btn-ghost-brand" onClick={handleTest} disabled={testing || saving}>
            {testing ? "测试中..." : "测试抓取"}
          </button>
          <button type="button" className="btn-brand" onClick={handlePublish} disabled={!canPublish}>
            {saving ? "发布中..." : "保存并发布"}
          </button>
        </div>
      </header>

      <div className="grid gap-1 px-4 pt-2 text-[13px]">
        <p>{testMessage}</p>
        {testError ? <p className="text-rose-700">测试错误：{testError}</p> : null}
        {publishError ? <p className="text-rose-700">发布错误：{publishError}</p> : null}
        {publishMessage ? <p className="text-emerald-700">{publishMessage}</p> : null}
        {!canPublish ? (
          <p className="panel-subtitle">保存并发布仅在“当前配置已测试通过”时可用；修改规则后需重新测试。</p>
        ) : null}
      </div>

      <main
        className={
          embedded
            ? "grid min-h-[72vh] gap-3 xl:grid-cols-[minmax(420px,48%)_minmax(380px,52%)]"
            : "grid flex-1 min-h-0 gap-3 p-3 xl:grid-cols-[minmax(420px,48%)_minmax(380px,52%)]"
        }
      >
        <section className="min-h-0 overflow-auto pr-1">
          <Panel
            title="1. 基础通信引擎 (HTTP Request)"
            subtitle="定义 URL、方法和请求参数。"
            open={openPanels[PANEL_KEYS.request]}
            onToggle={() => setOpenPanels((prev) => ({ ...prev, [PANEL_KEYS.request]: !prev[PANEL_KEYS.request] }))}
          >
            <div className="grid gap-3 md:grid-cols-2">
              <label className="field-label">
                Platform Code
                <select
                  className="input-base"
                  value={platformCode}
                  onChange={(e) => {
                    const nextCode = e.target.value
                    setPlatformCode(nextCode)
                    const matched = platformOptionMap[nextCode]
                    if (matched && !String(docUrl || "").trim() && String(matched.docs_url || "").trim()) {
                      setDocUrl(String(matched.docs_url || "").trim())
                    }
                  }}
                >
                  <option value="">
                    {platformOptionsLoading ? "平台加载中..." : "请选择平台编码"}
                  </option>
                  {platformSelectOptions.map((item) => (
                    <option key={item.platform} value={item.platform}>
                      {item.label} ({item.platform})
                    </option>
                  ))}
                </select>
                {platformOptionsError ? (
                  <span className="panel-subtitle text-rose-600">平台列表加载失败：{platformOptionsError}</span>
                ) : (
                  <span className="panel-subtitle">选项来自平台管理（`platform_configs.json`）</span>
                )}
              </label>
              <label className="field-label">
                Stream Name *
                <input
                  className="input-base"
                  value={streamName}
                  onChange={(e) => setStreamName(e.target.value)}
                  placeholder="例如：offline_campaign"
                  maxLength={128}
                  required
                />
              </label>
              <label className="field-label">
                Display Name
                <input
                  className="input-base"
                  value={displayName}
                  onChange={(e) => setDisplayName(e.target.value)}
                  placeholder="例如：离线计划报表"
                />
              </label>
              <label className="field-label">
                HTTP Method
                <select className="input-base" value={requestMethod} onChange={(e) => setRequestMethod(e.target.value)}>
                  <option value="GET">GET</option>
                  <option value="POST">POST</option>
                  <option value="PUT">PUT</option>
                  <option value="PATCH">PATCH</option>
                </select>
              </label>
              <label className="field-label md:col-span-2">
                API URL Base *
                <input
                  className="input-base"
                  value={urlBase}
                  onChange={(e) => setUrlBase(e.target.value)}
                  placeholder="例如：https://api.xiaohongshu.com"
                  required
                />
              </label>
              <label className="field-label md:col-span-2">
                URL Path
                <input
                  className="input-base"
                  value={urlPath}
                  onChange={(e) => setUrlPath(e.target.value)}
                  placeholder="例如：/v1/reports/campaign"
                />
              </label>
              <label className="field-label md:col-span-2">
                文档 URL
                <input
                  className="input-base"
                  value={docUrl}
                  onChange={(e) => setDocUrl(e.target.value)}
                  placeholder="例如：https://open.xiaohongshu.com/docs/..."
                />
              </label>
            </div>

            <div className="mt-3.5">
              <h3 className="mb-2 mt-0 text-[13px] text-slate-700">Headers</h3>
              <KeyValueEditor
                rows={headersRows}
                onChange={setHeadersRows}
                keyPlaceholder="Header Key"
                valuePlaceholder="Header Value"
              />
            </div>

            <div className="mt-3.5">
              <h3 className="mb-2 mt-0 text-[13px] text-slate-700">Query Params</h3>
              <KeyValueEditor
                rows={queryRows}
                onChange={setQueryRows}
                keyPlaceholder="Param Key"
                valuePlaceholder="Param Value"
                parseHint
              />
            </div>

            <div className="mt-3.5">
              <h3 className="mb-2 mt-0 text-[13px] text-slate-700">Body</h3>
              <KeyValueEditor
                rows={bodyRows}
                onChange={setBodyRows}
                keyPlaceholder="Body Key"
                valuePlaceholder="Body Value"
                parseHint
              />
            </div>
          </Panel>

          <Panel
            title="2. 鉴权注入规则 (Authentication Strategy)"
            subtitle="定义运行时 Token 注入行为。"
            open={openPanels[PANEL_KEYS.auth]}
            onToggle={() => setOpenPanels((prev) => ({ ...prev, [PANEL_KEYS.auth]: !prev[PANEL_KEYS.auth] }))}
          >
            <div className="grid gap-3 md:grid-cols-2">
              <label className="field-label">
                鉴权类型
                <select className="input-base" value={authType} onChange={(e) => setAuthType(e.target.value)}>
                  <option value="None">None</option>
                  <option value="Bearer Token">Bearer Token</option>
                  <option value="API Key">API Key</option>
                  <option value="OAuth2.0">OAuth2.0</option>
                </select>
              </label>
              <label className="field-label">
                注入位置
                <select className="input-base" value={authInjectInto} onChange={(e) => setAuthInjectInto(e.target.value)}>
                  <option value="header">Header</option>
                  <option value="query">Query Params</option>
                  <option value="body">Body</option>
                </select>
              </label>
              <label className="field-label">
                参数键名
                <input
                  className="input-base"
                  value={authKeyName}
                  onChange={(e) => setAuthKeyName(e.target.value)}
                  placeholder="例如：Authorization"
                />
              </label>
              <label className="field-label">
                测试变量键
                <input
                  className="input-base"
                  value={authTestVariable}
                  onChange={(e) => setAuthTestVariable(e.target.value)}
                  placeholder="例如：token"
                />
              </label>
            </div>
          </Panel>

          <Panel
            title="3. 自动分页引擎 (Pagination Strategy)"
            subtitle="支持无分页、偏移量、游标模式。"
            open={openPanels[PANEL_KEYS.pagination]}
            onToggle={() => setOpenPanels((prev) => ({ ...prev, [PANEL_KEYS.pagination]: !prev[PANEL_KEYS.pagination] }))}
          >
            <div className="grid gap-3 md:grid-cols-2">
              <label className="field-label">
                分页模式
                <select className="input-base" value={paginationType} onChange={(e) => setPaginationType(e.target.value)}>
                  <option value="none">No Pagination</option>
                  <option value="offset_limit">Offset / Limit</option>
                  <option value="cursor">Cursor Pagination</option>
                </select>
              </label>
              <label className="field-label">
                游标注入参数
                <input
                  className="input-base"
                  value={paginationInjectParam}
                  onChange={(e) => setPaginationInjectParam(e.target.value)}
                  placeholder="例如：page_token"
                />
              </label>
              <label className="field-label md:col-span-2">
                游标提取路径 (Cursor JSONPath)
                <input
                  className="input-base"
                  value={paginationCursorPath}
                  onChange={(e) => setPaginationCursorPath(e.target.value)}
                  placeholder="例如：$.page_info.next_token"
                />
              </label>
            </div>
          </Panel>

          <Panel
            title="4. 数据剥离器 (Record Extractor)"
            subtitle="配置 JSONPath 抽取目标记录。"
            open={openPanels[PANEL_KEYS.extract]}
            onToggle={() => setOpenPanels((prev) => ({ ...prev, [PANEL_KEYS.extract]: !prev[PANEL_KEYS.extract] }))}
          >
            <div className="grid gap-3">
              <label className="field-label">
                JSONPath Selector
                <input
                  className="input-base"
                  value={recordSelector}
                  onChange={(e) => setRecordSelector(e.target.value)}
                  placeholder="例如：$.data.list"
                />
              </label>
              <div className="grid gap-2 text-[13px]">
                <span>支持同步模式</span>
                <div className="flex flex-wrap gap-2">
                  {SYNC_MODE_OPTIONS.map((mode) => (
                    <label
                      key={mode}
                      className="inline-flex items-center gap-1.5 rounded-full border border-slate-300 bg-[#F0EFEC] px-2.5 py-1 text-xs"
                    >
                      <input
                        type="checkbox"
                        className="accent-[var(--brand)]"
                        checked={syncModes.includes(mode)}
                        onChange={() => toggleMode(mode)}
                      />
                      {mode}
                    </label>
                  ))}
                </div>
              </div>
            </div>
          </Panel>

          <Panel
            title="5. 临时测试变量 (Test Variables)"
            subtitle="仅用于测试抓取，不落库存储。"
            open={openPanels[PANEL_KEYS.vars]}
            onToggle={() => setOpenPanels((prev) => ({ ...prev, [PANEL_KEYS.vars]: !prev[PANEL_KEYS.vars] }))}
          >
            <KeyValueEditor
              rows={testVarRows}
              onChange={setTestVarRows}
              keyPlaceholder="变量名，例如 token"
              valuePlaceholder="变量值"
              parseHint
            />
          </Panel>

          <section className="section-block p-4">
            <div className="flex items-center justify-between gap-2">
              <div>
                <h2 className="panel-title">已发布 Streams</h2>
                <p className="panel-subtitle">来自 `GET /api/v1/builder/streams`</p>
              </div>
              <div className="flex gap-1.5">
                <button type="button" className="btn-subtle" onClick={() => loadStreams(platformCode)} disabled={streamsLoading}>
                  {streamsLoading ? "刷新中..." : "按平台刷新"}
                </button>
                <button type="button" className="btn-subtle" onClick={() => loadStreams("")} disabled={streamsLoading}>
                  全部
                </button>
              </div>
            </div>
            {streamsError ? <p className="mt-2 text-sm text-rose-700">{streamsError}</p> : null}
            <div className="mt-2.5 grid max-h-[260px] gap-2 overflow-auto">
              {streams.map((item) => {
                const active = selectedStreamId === item.id
                const title = item.display_name || item.stream_name
                const updated = item.updated_at ? new Date(item.updated_at).toLocaleString("zh-CN") : "-"
                return (
                  <button
                    key={item.id}
                    type="button"
                    className={
                      active
                        ? "flex w-full flex-col items-start justify-between gap-2 rounded-sm border border-[var(--brand)] bg-[var(--brand-soft)] px-2.5 py-2 text-left md:flex-row md:items-center"
                        : "flex w-full flex-col items-start justify-between gap-2 rounded-sm border border-slate-300 bg-[#F0EFEC] px-2.5 py-2 text-left md:flex-row md:items-center"
                    }
                    onClick={() => loadStreamToForm(item)}
                  >
                    <div className="mono-ui">
                      <strong className="block text-[13px] text-slate-900">{title}</strong>
                      <p className="mb-0 mt-1 text-xs text-slate-500">{item.platform_code} / {item.stream_name}</p>
                    </div>
                    <span className="mono-ui text-[11px] text-slate-500">{updated}</span>
                  </button>
                )
              })}
              {!streamsLoading && streams.length === 0 ? <p className="panel-subtitle">暂无已发布流配置。</p> : null}
            </div>
          </section>
        </section>

        <section className="min-h-0 flex flex-col rounded-sm border border-slate-800 bg-gradient-to-b from-slate-900 to-slate-800 text-slate-200 shadow-[0_8px_24px_rgba(15,23,42,0.14)]">
          <div className="flex flex-wrap gap-2 border-b border-slate-600/50 p-3">
            {CONSOLE_TABS.map((tab) => {
              const isActive = tab.key === consoleTab
              return (
                <button
                  key={tab.key}
                  type="button"
                  className={
                    isActive
                      ? "tab-minimal !border-b-[var(--brand)] !pb-1 !text-slate-100"
                      : "tab-minimal !border-b-transparent !pb-1 !text-slate-400"
                  }
                  onClick={() => setConsoleTab(tab.key)}
                >
                  {tab.label}
                </button>
              )
            })}
          </div>
          <div className="mono-ui flex flex-wrap gap-3 border-b border-slate-600/50 px-3 pb-2 text-xs text-slate-400">
            <span>records: {(testOutput.extracted_records || []).length}</span>
            <span>fields: {(testOutput.inferred_schema || []).length}</span>
            <span>status: {testError ? "failed" : lastTestConfigDigest === publishDigest ? "passed" : "pending"}</span>
          </div>
          <div className="min-h-0 flex-1 overflow-auto p-3">
            <JsonBlock value={activeConsolePayload} />
          </div>
        </section>
      </main>
    </div>
  )
}

export default ConnectorBuilderPage
