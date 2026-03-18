import { useCallback, useEffect, useMemo, useState } from "react"
import { createPortal } from "react-dom"
import { useLocation, useNavigate } from "react-router-dom"

const moduleMeta = {
  dashboard: { title: "系统概览", desc: "查看平台总体状态、账号规模和任务执行健康度。" },
  iam: { title: "账号与权限", desc: "管理用户、角色权限与关键操作审计（RBAC 预留区）。" },
  appauth: { title: "平台和应用管理", desc: "先注册平台，再管理应用凭证与授权生命周期。" },
  apihub: { title: "API 接口管理", desc: "按业务模块管理定义、发布、策略与沙箱调试。" },
  monitor: { title: "日志与监控", desc: "查看任务执行状态、错误明细与可观测信息。" },
}

const navItems = [
  { key: "dashboard", label: "系统概览", path: "/dashboard" },
  { key: "iam", label: "账号与权限", path: "/iam" },
  { key: "appauth", label: "平台和应用管理", path: "/appauth" },
  { key: "apihub", label: "API 接口管理", path: "/apihub" },
  { key: "monitor", label: "日志与监控", path: "/monitor" },
]

const appauthTabs = [
  { key: "app_management", label: "应用管理" },
  { key: "platform_management", label: "平台管理" },
]

const apiTabs = [
  { key: "definition", label: "接口定义" },
  { key: "publish", label: "发布与路由" },
  { key: "policies", label: "流量与安全策略" },
  { key: "sandbox", label: "在线调试沙箱" },
]

const appPlatformSchemas = {
  oceanengine: {
    label: "千川 (OceanEngine)",
    helper: "用于千川授权：通过 auth_code 换取 access_token，并由系统自动刷新 refresh_token。",
    docsUrl: "https://open.oceanengine.com/labels/12/docs/1697468248190020?origin=left_nav",
  },
  red_juguang: {
    label: "Red_JuGuang",
    helper: "用于聚光授权：返回 approval_advertisers，可同步 advertiser_id / advertiser_name。",
    docsUrl: "https://ad-market.xiaohongshu.com/docs-center?bizType=943&articleId=2605",
  },
  red_chengfeng: {
    label: "Red_ChengFeng",
    helper: "用于乘风授权：token 字段与聚光一致，业务域不同。",
    docsUrl: "https://ad-market.xiaohongshu.com/docs-center?bizType=1084&articleId=2605",
  },
  wechat_shop: {
    label: "微信小店",
    helper: "用于微信小店 API 调用配置。",
    docsUrl: "",
  },
  meta_ads: {
    label: "Meta Ads",
    helper: "用于 Meta 广告报表 API 调用配置。",
    docsUrl: "",
  },
}

const initialAccountForm = {
  name: "",
  platform: "oceanengine",
  status: "active",
  app_id: "",
  secret_key: "",
  auth_code: "",
  auto_refresh_token: true,
  token_expire_advance_minutes: "30",
  remark: "",
}

const initialEditForm = {
  name: "",
  platform: "oceanengine",
  app_id: "",
  secret_key: "",
  auth_code: "",
  refresh_token: "",
  auto_refresh_token: true,
  token_expire_advance_minutes: "30",
  remark: "",
}

const initialPlatformForm = {
  platform: "",
  label: "",
  helper: "",
  docs_url: "",
  status: "active",
}

function statusClass(status) {
  const key = String(status || "").trim().toLowerCase()
  if (key === "ready" || key === "active" || key === "success") return "status-pill bg-emerald-600"
  if (key === "missing" || key === "disabled") return "status-pill bg-slate-500"
  if (key === "partial") return "status-pill bg-amber-500"
  if (key === "failed" || key === "error" || key === "exception" || key === "refresh_failed" || key === "abnormal") {
    return "status-pill bg-rose-600"
  }
  if (key === "running" || key === "pending") return "status-pill bg-sky-600"
  return "status-pill bg-slate-400"
}

function platformMappingLabel(platform) {
  const key = String(platform || "").trim().toLowerCase()
  const normalized =
    key === "xhs_juguang" || key === "小红书聚光"
      ? "red_juguang"
      : key === "xhs_chengfeng" || key === "小红书乘风"
        ? "red_chengfeng"
        : key
  return appPlatformSchemas[normalized]?.label || platform || "-"
}

async function apiFetch(url, options = {}) {
  const resp = await fetch(url, options)
  const data = await resp.json()
  if (!resp.ok) {
    throw new Error(data.detail || JSON.stringify(data))
  }
  return data
}

function AnimatedNumber({ value }) {
  const [display, setDisplay] = useState(value)

  useEffect(() => {
    const start = display
    const end = value
    if (start === end) return undefined

    const duration = 420
    const startAt = performance.now()
    let rafId = 0

    const step = (now) => {
      const progress = Math.min((now - startAt) / duration, 1)
      const eased = 1 - (1 - progress) ** 3
      const next = Math.round(start + (end - start) * eased)
      setDisplay(next)
      if (progress < 1) {
        rafId = requestAnimationFrame(step)
      }
    }

    rafId = requestAnimationFrame(step)
    return () => cancelAnimationFrame(rafId)
  }, [value, display])

  return <>{display.toLocaleString("zh-CN")}</>
}

function App() {
  const location = useLocation()
  const navigate = useNavigate()
  const [activeApiTab, setActiveApiTab] = useState("definition")
  const [activeAppauthTab, setActiveAppauthTab] = useState("app_management")
  const [healthText, setHealthText] = useState("健康检查中...")

  const [accounts, setAccounts] = useState([])
  const [tasks, setTasks] = useState([])
  const [detailText, setDetailText] = useState("点击“查看”后显示任务或账号详情")

  const [accountSearch, setAccountSearch] = useState("")
  const [accountPlatform, setAccountPlatform] = useState("all")
  const [accountStatus, setAccountStatus] = useState("all")
  const [sourceAccounts, setSourceAccounts] = useState([])
  const [sourcePlatforms, setSourcePlatforms] = useState([])
  const [sourcePage, setSourcePage] = useState(1)
  const [sourcePageSize] = useState(20)
  const [sourceTotal, setSourceTotal] = useState(0)
  const [sourceTotalPages, setSourceTotalPages] = useState(1)
  const [sourceJumpPage, setSourceJumpPage] = useState("1")
  const [selectedSourceAppIds, setSelectedSourceAppIds] = useState([])
  const [batchDeleting, setBatchDeleting] = useState(false)
  const [batchBarMounted, setBatchBarMounted] = useState(false)
  const [batchBarVisible, setBatchBarVisible] = useState(false)
  const [tokenConfirmAppId, setTokenConfirmAppId] = useState("")
  const [tokenRefreshingByAppId, setTokenRefreshingByAppId] = useState({})
  const [moreMenuAppId, setMoreMenuAppId] = useState("")
  const [editDialog, setEditDialog] = useState(null)
  const [editForm, setEditForm] = useState(initialEditForm)
  const [editLoading, setEditLoading] = useState(false)
  const [editSaving, setEditSaving] = useState(false)
  const [appDrawerOpen, setAppDrawerOpen] = useState(false)
  const [appDrawerVisible, setAppDrawerVisible] = useState(false)
  const [confirmDialog, setConfirmDialog] = useState(null)
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [whitelistDialog, setWhitelistDialog] = useState(null)
  const [whitelistValue, setWhitelistValue] = useState("")
  const [whitelistSaving, setWhitelistSaving] = useState(false)
  const [taskSearch, setTaskSearch] = useState("")
  const [taskStatus, setTaskStatus] = useState("all")
  const [platformConfigs, setPlatformConfigs] = useState([])
  const [platformForm, setPlatformForm] = useState(initialPlatformForm)
  const [platformLoading, setPlatformLoading] = useState(false)
  const [platformSubmitting, setPlatformSubmitting] = useState(false)

  const [autoRefresh, setAutoRefresh] = useState(false)
  const [toast, setToast] = useState("")

  const [accountForm, setAccountForm] = useState(initialAccountForm)

  const [wechatForm, setWechatForm] = useState({
    account_id: "",
    start_date: "",
    end_date: "",
    time_type: "create_time",
    page_size: "50",
  })

  const [metaForm, setMetaForm] = useState({
    account_id: "",
    start_date: "",
    end_date: "",
    level: "ad",
    dry_run: "true",
  })
  const selectedCount = selectedSourceAppIds.length

  const activeModule = useMemo(() => {
    const hit = navItems.find((item) => item.path === location.pathname)
    return hit ? hit.key : "dashboard"
  }, [location.pathname])

  function gotoModule(moduleKey) {
    const hit = navItems.find((item) => item.key === moduleKey)
    if (!hit) {
      navigate("/dashboard")
      return
    }
    if (moduleKey === "appauth") {
      setActiveAppauthTab("app_management")
    }
    navigate(hit.path)
  }

  const currentMeta = moduleMeta[activeModule] || moduleMeta.dashboard
  const portalRoot = typeof document !== "undefined" ? document.body : null

  function showToast(message) {
    setToast(message)
    window.setTimeout(() => setToast(""), 2400)
  }

  function closeTokenConfirm() {
    setTokenConfirmAppId("")
  }

  function formatTimeText(value) {
    const raw = String(value || "").trim()
    if (!raw) return "-"
    const normalized = raw.replace("T", " ").replace("Z", "")
    const noMillis = normalized.split(".")[0]
    const noOffset = noMillis.split("+")[0]
    return noOffset
  }

  function openAppDrawer() {
    setAppDrawerOpen(true)
    window.requestAnimationFrame(() => setAppDrawerVisible(true))
  }

  function closeAppDrawer() {
    setAppDrawerVisible(false)
    window.setTimeout(() => setAppDrawerOpen(false), 220)
    setAccountForm(initialAccountForm)
  }

  async function loadHealth() {
    try {
      const data = await apiFetch("/api/v1/health")
      setHealthText(`服务状态: ${data.status}`)
    } catch (err) {
      setHealthText(`服务异常: ${err.message}`)
    }
  }

  async function loadAccounts() {
    const data = await apiFetch("/api/v1/accounts")
    setAccounts(data)
  }

  async function loadTasks() {
    const data = await apiFetch("/api/v1/tasks")
    setTasks(data)
  }

  const loadPlatformConfigs = useCallback(async () => {
    setPlatformLoading(true)
    try {
      const data = await apiFetch("/api/v1/platform-configs")
      setPlatformConfigs(Array.isArray(data) ? data : [])
    } finally {
      setPlatformLoading(false)
    }
  }, [])

  const loadCredentialSource = useCallback(async (page = 1) => {
    const params = new URLSearchParams()
    params.set("page", String(page))
    params.set("page_size", String(sourcePageSize))
    if (accountSearch.trim()) params.set("keyword", accountSearch.trim())
    if (accountPlatform !== "all") params.set("platform", accountPlatform)
    if (accountStatus !== "all") params.set("status", accountStatus)
    const data = await apiFetch(`/api/v1/accounts/credentials/source?${params.toString()}`)
    const nextEntries = data.entries || []
    setSourceAccounts(nextEntries)
    setSourcePlatforms(data.platforms || [])
    setSourceTotal(data.total || 0)
    setSourceTotalPages(data.total_pages || 1)
    setSourcePage(data.page || 1)
    setSourceJumpPage(String(data.page || 1))
    setSelectedSourceAppIds((prev) => {
      const valid = new Set(
        nextEntries
          .map((item) => String(item.app_id || "").trim())
          .filter(Boolean)
      )
      return prev.filter((appId) => valid.has(appId))
    })
  }, [accountPlatform, accountSearch, accountStatus, sourcePageSize])

  useEffect(() => {
    const timer = window.setTimeout(() => {
      loadHealth()
      Promise.all([loadAccounts(), loadTasks(), loadPlatformConfigs()]).catch((err) => showToast(err.message))
    }, 0)
    return () => window.clearTimeout(timer)
  }, [loadPlatformConfigs])

  useEffect(() => {
    loadCredentialSource(1).catch((err) => showToast(err.message))
  }, [loadCredentialSource])

  useEffect(() => {
    if (selectedCount > 0) {
      if (!batchBarMounted) {
        setBatchBarMounted(true)
        const rafId = window.requestAnimationFrame(() => setBatchBarVisible(true))
        return () => window.cancelAnimationFrame(rafId)
      }
      setBatchBarVisible(true)
      return undefined
    }

    if (!batchBarMounted) return undefined
    setBatchBarVisible(false)
    const timer = window.setTimeout(() => setBatchBarMounted(false), 200)
    return () => window.clearTimeout(timer)
  }, [selectedCount, batchBarMounted])

  useEffect(() => {
    if (!autoRefresh) return undefined
    const timer = window.setInterval(() => {
      loadTasks().catch(() => {})
    }, 3000)
    return () => window.clearInterval(timer)
  }, [autoRefresh])

  useEffect(() => {
    function handleEsc(e) {
      if (e.key !== "Escape") return
      closeAppDrawer()
      setConfirmDialog(null)
      setWhitelistDialog(null)
      setEditDialog(null)
      closeTokenConfirm()
      setMoreMenuAppId("")
    }

    window.addEventListener("keydown", handleEsc)
    return () => {
      window.removeEventListener("keydown", handleEsc)
    }
  }, [])

  useEffect(() => {
    function handleOutsideClick(e) {
      const target = e.target
      if (!(target instanceof Element)) return
      if (target.closest(".token-popconfirm")) return
      if (target.closest(".token-btn")) return
      closeTokenConfirm()
    }
    window.addEventListener("mousedown", handleOutsideClick)
    return () => window.removeEventListener("mousedown", handleOutsideClick)
  }, [])

  const filteredTasks = useMemo(() => {
    return tasks.filter((item) => {
      const keyword = taskSearch.trim().toLowerCase()
      const hitKeyword =
        !keyword ||
        String(item.account_id || "").includes(keyword) ||
        String(item.task_type || "").toLowerCase().includes(keyword)
      const hitStatus = taskStatus === "all" || item.status === taskStatus
      return hitKeyword && hitStatus
    })
  }, [tasks, taskSearch, taskStatus])

  const selectableSourceAppIds = useMemo(
    () => sourceAccounts.map((item) => String(item.app_id || "").trim()).filter(Boolean),
    [sourceAccounts]
  )

  const allSourceRowsSelected = useMemo(() => {
    if (!selectableSourceAppIds.length) return false
    return selectableSourceAppIds.every((appId) => selectedSourceAppIds.includes(appId))
  }, [selectableSourceAppIds, selectedSourceAppIds])

  const stats = useMemo(() => {
    const success = tasks.filter((x) => x.status === "success").length
    const failed = tasks.filter((x) => x.status === "failed").length
    return {
      accountCount: accounts.length,
      taskCount: tasks.length,
      success,
      failed,
    }
  }, [accounts, tasks])

  const statsWithTrend = useMemo(() => {
    const safeRate = (n) => (stats.taskCount ? `${Math.round((n / stats.taskCount) * 100)}%` : "0%")
    return [
      {
        key: "accounts",
        label: "账号总数",
        value: stats.accountCount,
        diff: 0,
        helper: "已接入平台账号",
        icon: "◧",
      },
      {
        key: "tasks",
        label: "任务总数",
        value: stats.taskCount,
        diff: 0,
        helper: "累计调度任务",
        icon: "◎",
      },
      {
        key: "success",
        label: "成功任务",
        value: stats.success,
        diff: Number.parseInt(safeRate(stats.success), 10),
        helper: `成功率 ${safeRate(stats.success)}`,
        icon: "↗",
      },
      {
        key: "failed",
        label: "失败任务",
        value: stats.failed,
        diff: Number.parseInt(safeRate(stats.failed), 10),
        helper: `失败率 ${safeRate(stats.failed)}`,
        icon: "↘",
      },
    ]
  }, [stats])

  const fallbackPlatformConfigs = useMemo(
    () =>
      Object.entries(appPlatformSchemas).map(([platform, schema]) => ({
        platform,
        label: schema.label,
        helper: schema.helper,
        docs_url: schema.docsUrl || "",
        status: "active",
        mutable: false,
      })),
    []
  )

  const availablePlatformConfigs = useMemo(() => {
    if (platformConfigs.length) return platformConfigs
    return fallbackPlatformConfigs
  }, [platformConfigs, fallbackPlatformConfigs])

  const currentPlatformSchema = useMemo(() => {
    const current = availablePlatformConfigs.find((item) => item.platform === accountForm.platform)
    if (current) {
      return {
        label: current.label || current.platform,
        helper: current.helper || "",
        docsUrl: current.docs_url || "",
      }
    }
    return appPlatformSchemas.oceanengine
  }, [accountForm.platform, availablePlatformConfigs])

  useEffect(() => {
    if (!availablePlatformConfigs.length) return
    const available = new Set(availablePlatformConfigs.map((item) => item.platform))
    setAccountForm((prev) => {
      if (available.has(prev.platform)) return prev
      return { ...prev, platform: availablePlatformConfigs[0].platform }
    })
  }, [availablePlatformConfigs])

  const accountConfigPreview = useMemo(() => {
    const appId = accountForm.app_id.trim()
    const secret = accountForm.secret_key.trim()
    const authCode = accountForm.auth_code.trim()
    const advanceMinutes = Number.parseInt(accountForm.token_expire_advance_minutes, 10)
    return {
      app_id: appId,
      secret_key: secret,
      secret,
      auth_code: authCode,
      token_policy: {
        auto_refresh_token: accountForm.auto_refresh_token,
        token_expire_advance_minutes: Number.isFinite(advanceMinutes) ? advanceMinutes : 30,
      },
      remark: accountForm.remark.trim(),
    }
  }, [accountForm])

  async function handleCreateAccount(e) {
    e.preventDefault()
    try {
      const action = e?.nativeEvent?.submitter?.getAttribute?.("data-action") || "create"
      const keepOpen = action === "continue"
      const appId = accountForm.app_id.trim()
      const secret = accountForm.secret_key.trim()
      const authCode = accountForm.auth_code.trim()
      if (!appId) {
        showToast("请填写 app_id")
        return
      }
      if (!secret) {
        showToast("请填写 secret_key")
        return
      }
      const advanceMinutes = Number.parseInt(accountForm.token_expire_advance_minutes, 10)
      const payload = {
        name: accountForm.name.trim(),
        platform: accountForm.platform,
        status: accountForm.status,
        config: {
          app_id: appId,
          secret_key: secret,
          secret,
          auth_code: authCode,
          token_policy: {
            auto_refresh_token: accountForm.auto_refresh_token,
            token_expire_advance_minutes: Number.isFinite(advanceMinutes) ? advanceMinutes : 30,
          },
          remark: accountForm.remark.trim(),
        },
      }
      await apiFetch("/api/v1/accounts/credentials/source/upsert", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })
      showToast("应用凭证已写入 api_credentials.json")
      if (keepOpen) {
        setAccountForm(initialAccountForm)
      } else {
        closeAppDrawer()
      }
      await loadCredentialSource(1)
    } catch (err) {
      showToast(`创建失败: ${err.message}`)
    }
  }

  async function handleResetCredentials(id) {
    try {
      const data = await apiFetch(`/api/v1/accounts/${id}/credentials/reset`, { method: "POST" })
      setDetailText(
        JSON.stringify(
          {
            account_id: id,
            app_id: data.app_id,
            issued_secret_key: data.issued_secret_key,
            credential_updated_at: data.credential_updated_at,
            note: "已重置密钥，请立即保存。",
          },
          null,
          2
        )
      )
      showToast(`账号 ${id} 密钥已重置`)
    } catch (err) {
      showToast(err.message)
    }
  }

  async function handleSaveWhitelist() {
    if (!whitelistDialog) return
    const ip_whitelist = whitelistValue
      .split(/[\n,]/)
      .map((x) => x.trim())
      .filter(Boolean)

    setWhitelistSaving(true)
    try {
      const data = await apiFetch(`/api/v1/accounts/${whitelistDialog.id}/ip-whitelist`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ip_whitelist }),
      })
      setDetailText(JSON.stringify(data, null, 2))
      showToast(`账号 ${whitelistDialog.id} 白名单已更新`)
      setWhitelistDialog(null)
      setWhitelistValue("")
      await loadAccounts()
    } catch (err) {
      showToast(err.message)
    } finally {
      setWhitelistSaving(false)
    }
  }

  async function handleDisableAccount(id) {
    try {
      await apiFetch(`/api/v1/accounts/${id}/disable`, { method: "POST" })
      showToast(`账号 ${id} 已禁用`)
      await loadAccounts()
    } catch (err) {
      showToast(err.message)
    }
  }

  async function handleConfirmAction() {
    if (!confirmDialog) return
    setConfirmLoading(true)
    try {
      if (confirmDialog.type === "reset_secret") {
        await handleResetCredentials(confirmDialog.id)
      }
      if (confirmDialog.type === "disable_account") {
        await handleDisableAccount(confirmDialog.id)
      }
      setConfirmDialog(null)
    } finally {
      setConfirmLoading(false)
    }
  }

  async function handleSyncCredentialSource() {
    try {
      const data = await apiFetch("/api/v1/accounts/credentials/source/sync", {
        method: "POST",
      })
      showToast(`同步完成：新增 ${data.created}，更新 ${data.updated}`)
      setDetailText(JSON.stringify(data, null, 2))
      await loadAccounts()
      await loadCredentialSource(1)
    } catch (err) {
      showToast(`同步失败: ${err.message}`)
    }
  }

  async function handleCreatePlatform(e) {
    e.preventDefault()
    const platform = platformForm.platform.trim()
    const label = platformForm.label.trim()
    if (!platform) {
      showToast("请填写平台编码")
      return
    }
    if (!label) {
      showToast("请填写平台名称")
      return
    }
    setPlatformSubmitting(true)
    try {
      await apiFetch("/api/v1/platform-configs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          platform,
          label,
          helper: platformForm.helper.trim(),
          docs_url: platformForm.docs_url.trim(),
          status: platformForm.status,
        }),
      })
      showToast("平台注册成功")
      setPlatformForm(initialPlatformForm)
      await loadPlatformConfigs()
    } catch (err) {
      showToast(`平台注册失败: ${err.message}`)
    } finally {
      setPlatformSubmitting(false)
    }
  }

  async function handleDeletePlatform(item) {
    const platform = String(item?.platform || "").trim()
    if (!platform) return
    if (!window.confirm(`确认删除平台 ${platform} 吗？`)) {
      return
    }
    try {
      await apiFetch(`/api/v1/platform-configs/${encodeURIComponent(platform)}`, {
        method: "DELETE",
      })
      showToast("平台已删除")
      await loadPlatformConfigs()
    } catch (err) {
      showToast(`删除失败: ${err.message}`)
    }
  }

  function toggleSelectAllSourceRows(checked) {
    if (!checked) {
      setSelectedSourceAppIds([])
      return
    }
    setSelectedSourceAppIds(selectableSourceAppIds)
  }

  function toggleSelectSourceRow(appId, checked) {
    if (!appId) return
    setSelectedSourceAppIds((prev) => {
      if (checked) {
        if (prev.includes(appId)) return prev
        return [...prev, appId]
      }
      return prev.filter((x) => x !== appId)
    })
  }

  function clearSourceSelection() {
    setSelectedSourceAppIds([])
  }

  async function openEditCredentialDialog(item) {
    closeTokenConfirm()
    const appId = String(item?.app_id || "").trim()
    if (!appId) {
      showToast("当前记录缺少 app_id，无法编辑")
      return
    }
    setEditDialog({ app_id: appId, original_app_id: appId })
    setEditLoading(true)
    try {
      const data = await apiFetch(`/api/v1/accounts/credentials/source/item?app_id=${encodeURIComponent(appId)}&refresh=true`)
      const config = data.config || {}
      const tokenPolicy = config.token_policy || {}
      const token = config.token || {}
      setEditForm({
        name: data.name || "",
        platform: data.platform || item.platform || "oceanengine",
        app_id: String(config.app_id || data.app_id || appId),
        secret_key: String(config.secret_key || config.secret || ""),
        auth_code: String(config.auth_code || ""),
        refresh_token: String(token.refresh_token || config.refresh_token || ""),
        auto_refresh_token: tokenPolicy.auto_refresh_token !== false,
        token_expire_advance_minutes: String(tokenPolicy.token_expire_advance_minutes || 30),
        remark: String(config.remark || ""),
      })
    } catch (err) {
      setEditDialog(null)
      showToast(`加载失败: ${err.message}`)
    } finally {
      setEditLoading(false)
    }
  }

  async function handleSaveCredentialEdit() {
    const appId = editForm.app_id.trim()
    const secret = editForm.secret_key.trim()
    if (!appId) {
      showToast("请填写 app_id")
      return
    }
    if (!secret) {
      showToast("请填写 secret_key")
      return
    }
    setEditSaving(true)
    try {
      const advanceMinutes = Number.parseInt(editForm.token_expire_advance_minutes, 10)
      const payload = {
        name: editForm.name.trim() || appId,
        platform: editForm.platform,
        status: "active",
        previous_app_id: String(editDialog?.original_app_id || "").trim() || null,
        config: {
          app_id: appId,
          secret_key: secret,
          secret,
          auth_code: editForm.auth_code.trim(),
          refresh_token: editForm.refresh_token.trim(),
          token_policy: {
            auto_refresh_token: !!editForm.auto_refresh_token,
            token_expire_advance_minutes: Number.isFinite(advanceMinutes) ? advanceMinutes : 30,
          },
          remark: editForm.remark.trim(),
        },
      }
      await apiFetch("/api/v1/accounts/credentials/source/upsert", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })
      showToast("应用凭证已更新")
      setEditDialog(null)
      await loadCredentialSource(sourcePage)
    } catch (err) {
      showToast(`保存失败: ${err.message}`)
    } finally {
      setEditSaving(false)
    }
  }

  async function handleBatchDeleteCredentials() {
    closeTokenConfirm()
    if (!selectedCount) {
      showToast("请先选择要删除的应用")
      return
    }
    if (!window.confirm(`确认删除选中的 ${selectedCount} 个应用凭证吗？`)) {
      return
    }
    setBatchDeleting(true)
    try {
      const data = await apiFetch("/api/v1/accounts/credentials/source/delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ app_ids: selectedSourceAppIds }),
      })
      showToast(`批量删除完成：请求 ${data.total}，删除 ${data.deleted}`)
      clearSourceSelection()
      await loadCredentialSource(sourcePage)
    } catch (err) {
      showToast(`批量删除失败: ${err.message}`)
    } finally {
      setBatchDeleting(false)
    }
  }

  function isRowTokenRefreshing(appId) {
    return !!tokenRefreshingByAppId[String(appId || "")]
  }

  function openTokenPopconfirm(appId) {
    const normalized = String(appId || "").trim()
    if (!normalized) return
    setMoreMenuAppId("")
    setTokenConfirmAppId((prev) => (prev === normalized ? "" : normalized))
  }

  async function handleRefreshToken(item) {
    const appId = String(item?.app_id || "").trim()
    if (!appId) {
      showToast("当前记录缺少 app_id，无法更新 Token")
      return
    }
    closeTokenConfirm()
    setTokenRefreshingByAppId((prev) => ({ ...prev, [appId]: true }))
    try {
      const data = await apiFetch("/api/v1/accounts/credentials/source/token/refresh", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ app_id: appId }),
      })
      setSourceAccounts((prev) =>
        prev.map((row) =>
          String(row.app_id || "") === appId
            ? {
                ...row,
                has_access_token: !!data.has_access_token,
                access_token: data.access_token || row.access_token || null,
                refresh_token: data.refresh_token || row.refresh_token || null,
                token_status: data.token_status || (data.has_access_token ? "ready" : "missing"),
                token_updated_at: data.token_updated_at || row.token_updated_at,
              }
            : row
        )
      )
      showToast("Token 更新成功")
    } catch (err) {
      showToast(err.message || "更新失败：网络超时，请重试")
    } finally {
      setTokenRefreshingByAppId((prev) => ({ ...prev, [appId]: false }))
    }
  }

  async function handleDeleteSingleCredential(item) {
    closeTokenConfirm()
    const appId = String(item?.app_id || "").trim()
    if (!appId) {
      showToast("当前记录缺少 app_id，无法删除")
      return
    }
    if (!window.confirm(`确认删除应用 ${appId} 吗？`)) {
      return
    }
    try {
      const data = await apiFetch("/api/v1/accounts/credentials/source/delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ app_ids: [appId] }),
      })
      showToast(`删除完成：删除 ${data.deleted} 条`)
      setMoreMenuAppId("")
      setSelectedSourceAppIds((prev) => prev.filter((x) => x !== appId))
      await loadCredentialSource(sourcePage)
    } catch (err) {
      showToast(`删除失败: ${err.message}`)
    }
  }

  function handleJumpPage() {
    closeTokenConfirm()
    const parsed = Number.parseInt(sourceJumpPage, 10)
    if (!Number.isFinite(parsed)) return
    const target = Math.max(1, Math.min(sourceTotalPages, parsed))
    loadCredentialSource(target).catch((err) => showToast(err.message))
  }

  async function handleTaskDetail(id) {
    try {
      const data = await apiFetch(`/api/v1/tasks/${id}`)
      setDetailText(JSON.stringify(data, null, 2))
    } catch (err) {
      showToast(err.message)
    }
  }

  async function handleSubmitWechat(e) {
    e.preventDefault()
    try {
      const payload = {
        account_id: Number(wechatForm.account_id),
        start_date: wechatForm.start_date || null,
        end_date: wechatForm.end_date || null,
        time_type: wechatForm.time_type,
        page_size: Number(wechatForm.page_size || 50),
      }
      const data = await apiFetch("/api/v1/tasks/wechat-orders/submit", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })
      showToast(`微信任务已提交 ID=${data.id}`)
      gotoModule("monitor")
      await loadTasks()
    } catch (err) {
      showToast(`微信任务失败: ${err.message}`)
    }
  }

  async function handleSubmitMeta(e) {
    e.preventDefault()
    try {
      const payload = {
        account_id: Number(metaForm.account_id),
        start_date: metaForm.start_date || null,
        end_date: metaForm.end_date || null,
        level: metaForm.level,
        dry_run: metaForm.dry_run === "true",
      }
      const data = await apiFetch("/api/v1/tasks/meta-report/submit", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })
      showToast(`Meta 任务已提交 ID=${data.id}`)
      gotoModule("monitor")
      await loadTasks()
    } catch (err) {
      showToast(`Meta 任务失败: ${err.message}`)
    }
  }

  return (
    <div className="min-h-screen bg-white lg:grid lg:grid-cols-[260px_1fr]">
      <aside className="border-r border-slate-200 bg-white/80 px-4 py-6 text-slate-700 backdrop-blur-sm">
        <div className="mb-8 flex items-center gap-3 rounded-sm bg-white p-3">
          <div className="grid h-10 w-10 place-items-center overflow-hidden rounded-sm border border-slate-200 bg-white">
            <img src="/static/brand-logo.png" alt="Simon Logo" className="h-full w-full object-cover" />
          </div>
          <div>
            <p className="m-0 text-sm font-extrabold mono-ui">SimonOpenPlatfrom</p>
            <p className="m-0 text-xs text-slate-500 mono-ui">Management Console</p>
          </div>
        </div>

        <nav className="-mx-4 grid gap-0">
          {navItems.map((item) => (
            <button
              key={item.key}
              onClick={() => gotoModule(item.key)}
              className={`nav-item w-full rounded-none ${activeModule === item.key ? "active" : ""}`}
            >
              {item.label}
            </button>
          ))}
        </nav>

        <div className="mt-8 rounded-sm bg-white p-3 text-xs">
          <p className="mb-2 text-slate-500 mono-ui">服务状态</p>
          <p className="inline-flex rounded-sm border border-slate-200 bg-white px-2 py-1 text-slate-700 mono-ui">{healthText}</p>
          <div className="mt-3">
            <a href="/docs" target="_blank" rel="noreferrer" className="text-[#0000E1] mono-ui hover:underline">
              打开 OpenAPI 文档
            </a>
          </div>
        </div>
      </aside>

      <main className="p-6 md:p-8">
        <header className="card mb-6 flex flex-col gap-2 p-4 md:flex-row md:items-center md:justify-between">
          <div className="flex flex-wrap items-center gap-2 text-xs text-slate-500">
            <span>SimonOpenPlatfrom</span>
            <span>/</span>
            <span className="font-bold text-slate-700">{currentMeta.title}</span>
          </div>
          <div className="flex flex-wrap gap-2 text-xs">
            <span className="rounded-sm border border-slate-300 bg-white px-2 py-1 text-slate-700 mono-ui">ENV: DEV</span>
            <span className="rounded-sm border border-slate-200 bg-white px-2 py-1 text-slate-700 mono-ui">登录账号: SimonOpenPlatfrom</span>
          </div>
        </header>

        <header className="mb-6 flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
          <div>
            <h1 className="m-0 text-2xl font-extrabold text-slate-900 mono-ui">{currentMeta.title}</h1>
            <p className="mt-2 text-sm text-slate-500">{currentMeta.desc}</p>
          </div>
          <div className="flex flex-wrap gap-2">
            <button className="btn-subtle" onClick={() => loadAccounts().catch((e) => showToast(e.message))}>刷新账号</button>
            <button className="btn-subtle" onClick={() => loadTasks().catch((e) => showToast(e.message))}>刷新任务</button>
            <button className="btn-brand" onClick={() => setAutoRefresh((v) => !v)}>{`自动刷新: ${autoRefresh ? "开" : "关"}`}</button>
          </div>
        </header>

        {activeModule === "dashboard" && (
          <section className="space-y-3">
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              {statsWithTrend.map((card) => {
                const up = card.key === "success"
                const down = card.key === "failed"
                return (
                  <article key={card.key} className="card p-5">
                    <div className="flex items-start justify-between">
                      <p className="text-xs text-slate-500">{card.label}</p>
                      <span className="inline-flex h-6 min-w-6 items-center justify-center rounded-full bg-slate-100 px-2 text-xs text-slate-600">
                        {card.icon}
                      </span>
                    </div>
                    <h3 className="mt-2 text-4xl font-extrabold tracking-tight text-slate-900">
                      <AnimatedNumber value={card.value} />
                    </h3>
                    <div className="mt-2 flex items-center justify-between">
                      <span className="text-xs text-slate-500">{card.helper}</span>
                      <span
                        className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs ${
                          up
                            ? "bg-blue-50 text-[#0000E1]"
                            : down
                              ? "bg-slate-200 text-slate-700"
                              : "bg-slate-100 text-slate-500"
                        }`}
                      >
                        {up ? "▲" : down ? "▼" : "•"} {card.key === "accounts" || card.key === "tasks" ? "实时" : `${Math.abs(card.diff)}%`}
                      </span>
                    </div>
                  </article>
                )
              })}
            </div>

            <div className="grid gap-3 lg:grid-cols-2">
              <article className="section-block">
                <div className="mb-3 flex items-center justify-between">
                  <h3 className="panel-title">调用概览</h3>
                  <span className="rounded-full bg-slate-100 px-2 py-1 text-xs text-slate-600">24h</span>
                </div>
                <div className="h-24 rounded-sm border border-slate-300 bg-slate-50 p-2">
                  <div className="grid h-full grid-cols-12 items-end gap-1">
                    {[28, 34, 18, 46, 62, 42, 58, 49, 66, 52, 75, 64].map((v, i) => (
                      <div key={i} className="rounded-t-sm bg-gradient-to-t from-[#0000E1]/45 to-[#0000E1]/18" style={{ height: `${v}%` }} />
                    ))}
                  </div>
                </div>
                <p className="mt-2 text-xs text-slate-500">当前为占位图，下一阶段可接入真实监控指标。</p>
              </article>

              <article className="section-block">
                <h3 className="panel-title mb-3">快速入口</h3>
                <div className="grid gap-2 sm:grid-cols-3">
                  <button className="btn-brand" onClick={() => gotoModule("appauth")}>创建应用账号</button>
                  <button className="btn-ghost-brand" onClick={() => gotoModule("apihub")}>管理 API 定义</button>
                  <button className="btn-ghost-brand" onClick={() => gotoModule("monitor")}>查看调用任务</button>
                </div>
              </article>
            </div>
          </section>
        )}

        {activeModule === "iam" && (
          <section className="section-block overflow-auto">
            <div className="mb-3 flex items-center justify-between">
              <h3 className="panel-title">用户管理（Phase 2）</h3>
              <span className="rounded-sm border border-slate-900 bg-slate-900 px-2 py-1 text-xs text-white mono-ui">RBAC</span>
            </div>
            <p className="mb-3 text-sm text-slate-500">已预留用户管理、角色权限矩阵、审计日志信息架构，后续接入真实接口即可启用。</p>
            <table className="table-shell min-w-[640px]">
              <thead>
                <tr className="table-head-row"><th className="table-head-cell">模块</th><th className="table-head-cell">状态</th><th className="table-head-cell">说明</th></tr>
              </thead>
              <tbody>
                <tr><td className="table-cell">用户管理</td><td className="table-cell"><span className="status-pill bg-slate-500">planned</span></td><td className="table-cell">注册 / 审核 / 禁用</td></tr>
                <tr><td className="table-cell">角色权限（RBAC）</td><td className="table-cell"><span className="status-pill bg-slate-500">planned</span></td><td className="table-cell">菜单可见性 + 接口粒度权限</td></tr>
                <tr><td className="table-cell">审计日志</td><td className="table-cell"><span className="status-pill bg-slate-500">planned</span></td><td className="table-cell">登录与关键配置变更记录</td></tr>
              </tbody>
            </table>
          </section>
        )}

        {activeModule === "appauth" && (
          <section className="space-y-3">
            <article className="section-block">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <h3 className="panel-title">子列表</h3>
                <div className="flex flex-wrap gap-2">
                  {appauthTabs.map((tab) => (
                    <button
                      key={tab.key}
                      className={activeAppauthTab === tab.key ? "tab-minimal active" : "tab-minimal"}
                      onClick={() => setActiveAppauthTab(tab.key)}
                    >
                      {tab.label}
                    </button>
                  ))}
                </div>
              </div>
            </article>

            {activeAppauthTab === "app_management" && (
              <>
            <article className="section-block overflow-auto">
              <div className="mb-4 flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                <div className="flex items-center gap-3">
                  <h3 className="panel-title">应用管理列表</h3>
                  <a href="/docs" target="_blank" rel="noreferrer" className="text-xs font-medium text-slate-500 hover:text-[#0000E1]">
                    查看安全最佳实践
                  </a>
                </div>
                <div className="flex items-center gap-2">
                  <button className="btn-ghost-brand" onClick={handleSyncCredentialSource}>
                    同步凭证 JSON
                  </button>
                  <button
                    className="btn-brand"
                    onClick={() => {
                      openAppDrawer()
                    }}
                  >
                    + 新建应用凭证
                  </button>
                </div>
              </div>

              <div className="mb-4 grid gap-2 sm:grid-cols-3">
                <input className="input-base" placeholder="搜索应用名称" value={accountSearch} onChange={(e) => setAccountSearch(e.target.value)} />
                <select className="input-base" value={accountPlatform} onChange={(e) => setAccountPlatform(e.target.value)}>
                  <option value="all">全部平台</option>
                  {sourcePlatforms.map((platform) => (
                    <option key={platform} value={platform}>{platformMappingLabel(platform)}</option>
                  ))}
                </select>
                <select className="input-base" value={accountStatus} onChange={(e) => setAccountStatus(e.target.value)}>
                  <option value="all">全部状态</option>
                  <option value="ready">凭证完整</option>
                  <option value="partial">凭证缺失</option>
                </select>
              </div>

              <table className="table-shell appauth-table min-w-[760px]">
                <thead>
                  <tr className="table-head-row">
                    <th className="table-head-cell">
                      <input
                        type="checkbox"
                        checked={allSourceRowsSelected}
                        onChange={(e) => toggleSelectAllSourceRows(e.target.checked)}
                      />
                    </th>
                    <th className="table-head-cell">序号</th>
                    <th className="table-head-cell">名称</th>
                    <th className="table-head-cell">App_ID</th>
                    <th className="table-head-cell">平台</th>
                    <th className="table-head-cell">状态</th>
                    <th className="table-head-cell">Token 状态</th>
                    <th className="table-head-cell">更新时间</th>
                    <th className="table-head-cell">access_token</th>
                    <th className="table-head-cell">操作</th>
                  </tr>
                </thead>
                <tbody>
                  {sourceAccounts.map((item) => (
                    <tr key={item.source_path}>
                      <td className="table-cell">
                        <input
                          type="checkbox"
                          disabled={!item.app_id || isRowTokenRefreshing(item.app_id)}
                          checked={item.app_id ? selectedSourceAppIds.includes(String(item.app_id)) : false}
                          onChange={(e) => toggleSelectSourceRow(String(item.app_id || ""), e.target.checked)}
                        />
                      </td>
                      <td className="table-cell">{item.row_no}</td>
                      <td className="table-cell">{item.name}</td>
                      <td className="table-cell mono-ui">{item.app_id || "-"}</td>
                      <td className="table-cell">{platformMappingLabel(item.platform)}</td>
                      <td className="table-cell"><span className={statusClass(item.status)}>{item.status}</span></td>
                      <td className="table-cell">
                        <span className={statusClass(item.token_status || (item.has_access_token ? "ready" : "missing"))}>
                          {item.token_status || (item.has_access_token ? "ready" : "missing")}
                        </span>
                      </td>
                      <td className="table-cell mono-ui">{formatTimeText(item.token_updated_at)}</td>
                      <td className="table-cell mono-ui">{item.access_token || "-"}</td>
                      <td className="table-cell">
                        <div className="relative action-cell-group">
                          <button
                            className="action-btn edit-btn"
                            disabled={isRowTokenRefreshing(item.app_id)}
                            onClick={() => openEditCredentialDialog(item)}
                          >
                            编辑
                          </button>
                          <button
                            className="action-btn token-btn"
                            disabled={!item.app_id || isRowTokenRefreshing(item.app_id)}
                            onClick={() => openTokenPopconfirm(item.app_id)}
                          >
                            <span className={isRowTokenRefreshing(item.app_id) ? "sync-icon spinning" : "sync-icon"}>↻</span>
                            {isRowTokenRefreshing(item.app_id) ? "更新中..." : "更新 Token"}
                          </button>
                          <button
                            className="action-btn more-btn dropdown-trigger"
                            disabled={isRowTokenRefreshing(item.app_id)}
                            onClick={() => {
                              closeTokenConfirm()
                              setMoreMenuAppId((prev) =>
                                prev === String(item.app_id || item.source_path) ? "" : String(item.app_id || item.source_path)
                              )
                            }}
                          >
                            更多 ▼
                          </button>

                          {tokenConfirmAppId === String(item.app_id || "") && (
                            <div className="token-popconfirm" onClick={(e) => e.stopPropagation()}>
                              <p className="m-0 text-xs text-slate-700">确定要强制更新此凭证的 Token 吗？原 Token 将立即失效。</p>
                              <div className="mt-2 flex items-center justify-end gap-2">
                                <button
                                  type="button"
                                  className="text-xs text-slate-500 hover:text-slate-700"
                                  onClick={closeTokenConfirm}
                                >
                                  取消
                                </button>
                                <button type="button" className="text-xs text-slate-500 hover:text-slate-700" onClick={closeTokenConfirm}>
                                  ✕
                                </button>
                                <button
                                  type="button"
                                  className="rounded-sm bg-[#0000E1] px-2 py-1 text-xs font-semibold text-white"
                                  onClick={() => handleRefreshToken(item)}
                                >
                                  确定
                                </button>
                              </div>
                            </div>
                          )}

                          {moreMenuAppId === String(item.app_id || item.source_path) && (
                            <div className="more-action-menu">
                              <button
                                className="more-action-item"
                                onClick={() => {
                                  setDetailText(JSON.stringify(item, null, 2))
                                  setMoreMenuAppId("")
                                }}
                              >
                                查看
                              </button>
                              <button className="more-action-item text-rose-500" onClick={() => handleDeleteSingleCredential(item)}>
                                删除
                              </button>
                            </div>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <div className="appauth-pagination mt-4 flex flex-wrap items-center justify-center gap-3 text-xs text-slate-500">
                <span>共 {sourceTotal} 条，每页 {sourcePageSize} 条</span>
                <div className="flex items-center gap-2">
                  <button
                    className="btn-subtle px-2 py-1 text-xs disabled:opacity-50"
                    type="button"
                    disabled={sourcePage <= 1}
                    onClick={() => loadCredentialSource(sourcePage - 1).catch((err) => showToast(err.message))}
                  >
                    上一页
                  </button>
                  <span className="mono-ui">第 {sourcePage} / {sourceTotalPages} 页</span>
                  <input
                    className="input-base w-20 px-2 py-1 text-xs"
                    value={sourceJumpPage}
                    onChange={(e) => setSourceJumpPage(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") handleJumpPage()
                    }}
                    placeholder="页码"
                  />
                  <button
                    className="btn-subtle px-2 py-1 text-xs"
                    type="button"
                    onClick={handleJumpPage}
                  >
                    跳转
                  </button>
                  <button
                    className="btn-subtle px-2 py-1 text-xs disabled:opacity-50"
                    type="button"
                    disabled={sourcePage >= sourceTotalPages}
                    onClick={() => loadCredentialSource(sourcePage + 1).catch((err) => showToast(err.message))}
                  >
                    下一页
                  </button>
                </div>
              </div>
            </article>

            {appDrawerOpen && portalRoot && createPortal(
              <div
                className={`fixed inset-0 -top-px z-50 flex items-stretch justify-end p-0 transition-opacity duration-200 ${
                  appDrawerVisible ? "bg-black/30 opacity-100" : "bg-black/0 opacity-0"
                }`}
                onClick={closeAppDrawer}
              >
                <article
                  className={`h-[calc(100vh+1px)] w-full max-w-[520px] overflow-y-auto border-l border-slate-200 bg-white p-6 shadow-[0_10px_30px_rgba(15,23,42,0.12)] transition-transform duration-300 ease-[cubic-bezier(0.22,1,0.36,1)] ${
                    appDrawerVisible ? "translate-x-0" : "translate-x-full"
                  }`}
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="mb-4 flex items-center justify-between">
                    <h3 className="panel-title">新建应用凭证</h3>
                    <button className="btn-subtle px-2 py-1 text-xs" onClick={closeAppDrawer}>关闭</button>
                  </div>
                  <p className="mb-4 text-sm text-slate-500">
                    只需要填写第三方平台基础凭证（app_id / secret_key / auth_code）。`access_token` 与 `refresh_token` 由系统自动维护并从数据库/凭证源读取，不需要人工填写。
                  </p>
                  <form className="grid gap-3" onSubmit={handleCreateAccount}>
                    <h4 className="text-xs font-bold uppercase tracking-wide text-slate-500 mono-ui">基础信息</h4>
                    <label className="field-label">应用名称
                      <input className="input-base mt-1" value={accountForm.name} onChange={(e) => setAccountForm((x) => ({ ...x, name: e.target.value }))} required />
                    </label>
                    <label className="field-label">平台
                      <select className="input-base mt-1" value={accountForm.platform} onChange={(e) => setAccountForm((x) => ({ ...x, platform: e.target.value }))}>
                        {availablePlatformConfigs.map((item) => (
                          <option key={item.platform} value={item.platform}>{item.label || item.platform}</option>
                        ))}
                      </select>
                    </label>
                    <label className="field-label">状态
                      <select className="input-base mt-1" value={accountForm.status} onChange={(e) => setAccountForm((x) => ({ ...x, status: e.target.value }))}>
                        <option value="active">active</option>
                        <option value="disabled">disabled</option>
                      </select>
                    </label>
                    <p className="rounded-sm border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
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
                    <label className="field-label">app_id
                      <input
                        className="input-base mt-1 mono-ui"
                        value={accountForm.app_id}
                        onChange={(e) => setAccountForm((x) => ({ ...x, app_id: e.target.value }))}
                        placeholder="输入平台 app_id"
                        required
                      />
                    </label>
                    <label className="field-label">secret_key
                      <input
                        className="input-base mt-1 mono-ui"
                        type="password"
                        value={accountForm.secret_key}
                        onChange={(e) => setAccountForm((x) => ({ ...x, secret_key: e.target.value }))}
                        placeholder="输入平台 secret_key / secret"
                        required
                      />
                    </label>

                    <h4 className="mt-2 text-xs font-bold uppercase tracking-wide text-slate-500 mono-ui">授权信息</h4>
                    <label className="field-label">auth_code（可选）
                      <input
                        className="input-base mt-1 mono-ui"
                        value={accountForm.auth_code}
                        onChange={(e) => setAccountForm((x) => ({ ...x, auth_code: e.target.value }))}
                        placeholder="首次授权可填写 auth_code"
                      />
                    </label>
                    <label className="field-label">token 提前刷新（分钟）
                      <input
                        type="number"
                        min="5"
                        max="180"
                        className="input-base mt-1"
                        value={accountForm.token_expire_advance_minutes}
                        onChange={(e) => setAccountForm((x) => ({ ...x, token_expire_advance_minutes: e.target.value }))}
                      />
                    </label>
                    <label className="field-label flex items-center justify-between gap-3 rounded-sm border border-slate-200 bg-slate-50 px-3 py-2">
                      <span>自动刷新 token</span>
                      <input
                        type="checkbox"
                        checked={accountForm.auto_refresh_token}
                        onChange={(e) => setAccountForm((x) => ({ ...x, auto_refresh_token: e.target.checked }))}
                      />
                    </label>
                    <label className="field-label">备注
                      <textarea
                        className="input-base mt-1 min-h-[72px]"
                        value={accountForm.remark}
                        onChange={(e) => setAccountForm((x) => ({ ...x, remark: e.target.value }))}
                        placeholder="可填写业务归属、用途说明"
                      />
                    </label>

                    <label className="field-label">配置预览（自动生成）
                      <textarea className="input-base mt-1 min-h-[150px] mono-ui" value={JSON.stringify(accountConfigPreview, null, 2)} readOnly />
                    </label>
                    <p className="text-xs text-slate-500">
                      提示：token 会在后台自动刷新并写入系统托管字段，前端表单不会保存你手填的 token。
                    </p>
                    <div className="mt-1 grid grid-cols-3 gap-2">
                      <button className="btn-subtle" type="button" onClick={closeAppDrawer}>取消</button>
                      <button className="btn-ghost-brand" type="submit" data-action="continue">保存并继续</button>
                      <button className="btn-brand" type="submit" data-action="create">确定创建</button>
                    </div>
                  </form>
                </article>
              </div>,
              portalRoot
            )}

            {editDialog && portalRoot && createPortal(
              <div className="fixed inset-0 z-[60] grid place-items-center bg-black/30 p-4" onClick={() => setEditDialog(null)}>
                <article
                  className="w-full max-w-2xl rounded-sm border border-slate-200 bg-white p-5 shadow-[0_10px_30px_rgba(15,23,42,0.12)]"
                  onClick={(e) => e.stopPropagation()}
                >
                  <h4 className="m-0 text-base font-semibold text-slate-900">编辑应用凭证</h4>
                  {editLoading ? (
                    <p className="mt-3 text-sm text-slate-600">加载中...</p>
                  ) : (
                    <div className="mt-3 grid gap-3 md:grid-cols-2">
                      <label className="field-label">名称
                        <input className="input-base mt-1" value={editForm.name} onChange={(e) => setEditForm((x) => ({ ...x, name: e.target.value }))} />
                      </label>
                      <label className="field-label">平台
                        <select className="input-base mt-1" value={editForm.platform} onChange={(e) => setEditForm((x) => ({ ...x, platform: e.target.value }))}>
                          {availablePlatformConfigs.map((item) => (
                            <option key={item.platform} value={item.platform}>{item.label || item.platform}</option>
                          ))}
                        </select>
                      </label>
                      <label className="field-label">app_id
                        <input className="input-base mt-1 mono-ui" value={editForm.app_id} onChange={(e) => setEditForm((x) => ({ ...x, app_id: e.target.value }))} />
                      </label>
                      <label className="field-label">secret_key
                        <input className="input-base mt-1 mono-ui" type="password" value={editForm.secret_key} onChange={(e) => setEditForm((x) => ({ ...x, secret_key: e.target.value }))} />
                      </label>
                      <label className="field-label">auth_code
                        <input className="input-base mt-1 mono-ui" value={editForm.auth_code} onChange={(e) => setEditForm((x) => ({ ...x, auth_code: e.target.value }))} />
                      </label>
                      <label className="field-label">refresh_token
                        <input className="input-base mt-1 mono-ui" value={editForm.refresh_token} onChange={(e) => setEditForm((x) => ({ ...x, refresh_token: e.target.value }))} />
                      </label>
                      <label className="field-label">token 提前刷新（分钟）
                        <input
                          type="number"
                          min="5"
                          max="180"
                          className="input-base mt-1"
                          value={editForm.token_expire_advance_minutes}
                          onChange={(e) => setEditForm((x) => ({ ...x, token_expire_advance_minutes: e.target.value }))}
                        />
                      </label>
                      <label className="field-label flex items-center justify-between gap-3 rounded-sm border border-slate-200 bg-slate-50 px-3 py-2">
                        <span>自动刷新 token</span>
                        <input type="checkbox" checked={editForm.auto_refresh_token} onChange={(e) => setEditForm((x) => ({ ...x, auto_refresh_token: e.target.checked }))} />
                      </label>
                      <label className="field-label md:col-span-2">备注
                        <textarea className="input-base mt-1 min-h-[72px]" value={editForm.remark} onChange={(e) => setEditForm((x) => ({ ...x, remark: e.target.value }))} />
                      </label>
                    </div>
                  )}
                  <div className="mt-4 grid grid-cols-2 gap-2">
                    <button className="btn-subtle" type="button" onClick={() => setEditDialog(null)} disabled={editSaving}>
                      取消
                    </button>
                    <button className="btn-brand" type="button" onClick={handleSaveCredentialEdit} disabled={editLoading || editSaving}>
                      {editSaving ? "保存中..." : "保存修改"}
                    </button>
                  </div>
                </article>
              </div>,
              portalRoot
            )}

            {confirmDialog && portalRoot && createPortal(
              <div className="fixed inset-0 z-[60] grid place-items-center bg-black/30 p-4" onClick={() => setConfirmDialog(null)}>
                <article
                  className="w-full max-w-md rounded-sm border border-slate-200 bg-white p-5 shadow-[0_10px_30px_rgba(15,23,42,0.12)]"
                  onClick={(e) => e.stopPropagation()}
                >
                  <h4 className="m-0 text-base font-semibold text-slate-900">确认操作</h4>
                  <p className="mt-3 text-sm text-slate-600">
                    {confirmDialog.type === "reset_secret"
                      ? `将重置应用“${confirmDialog.name || "-"}”的 Secret_Key，旧密钥会立即失效。是否继续？`
                      : `将禁用应用“${confirmDialog.name || "-"}”。禁用后该应用无法继续调用 API。是否继续？`}
                  </p>
                  <div className="mt-4 grid grid-cols-2 gap-2">
                    <button className="btn-subtle" type="button" onClick={() => setConfirmDialog(null)} disabled={confirmLoading}>
                      取消
                    </button>
                    <button className="btn-brand" type="button" onClick={handleConfirmAction} disabled={confirmLoading}>
                      {confirmLoading ? "处理中..." : "确认"}
                    </button>
                  </div>
                </article>
              </div>,
              portalRoot
            )}

            {whitelistDialog && portalRoot && createPortal(
              <div className="fixed inset-0 z-[60] grid place-items-center bg-black/30 p-4" onClick={() => setWhitelistDialog(null)}>
                <article
                  className="w-full max-w-xl rounded-sm border border-slate-200 bg-white p-5 shadow-[0_10px_30px_rgba(15,23,42,0.12)]"
                  onClick={(e) => e.stopPropagation()}
                >
                  <h4 className="m-0 text-base font-semibold text-slate-900">编辑 IP 白名单</h4>
                  <p className="mt-3 text-sm text-slate-600">
                    当前应用：{whitelistDialog.name}。请输入允许访问的 IP，支持英文逗号或换行分隔。
                  </p>
                  <label className="field-label mt-3 block">
                    IP 列表
                    <textarea
                      className="input-base mt-1 min-h-[140px]"
                      value={whitelistValue}
                      onChange={(e) => setWhitelistValue(e.target.value)}
                      placeholder={"例如:\n1.1.1.1\n2.2.2.2"}
                    />
                  </label>
                  <div className="mt-4 grid grid-cols-2 gap-2">
                    <button
                      className="btn-subtle"
                      type="button"
                      onClick={() => setWhitelistDialog(null)}
                      disabled={whitelistSaving}
                    >
                      取消
                    </button>
                    <button
                      className="btn-brand"
                      type="button"
                      onClick={handleSaveWhitelist}
                      disabled={whitelistSaving}
                    >
                      {whitelistSaving ? "保存中..." : "保存白名单"}
                    </button>
                  </div>
                </article>
              </div>,
              portalRoot
            )}
              </>
            )}

            {activeAppauthTab === "platform_management" && (
              <article className="section-block overflow-auto">
                <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
                  <h3 className="panel-title">平台注册列表</h3>
                  <button className="btn-subtle" type="button" onClick={() => loadPlatformConfigs().catch((err) => showToast(err.message))}>
                    刷新平台
                  </button>
                </div>

                <form className="mb-4 grid gap-2 rounded-sm border border-slate-200 bg-slate-50 p-3 md:grid-cols-2 xl:grid-cols-5" onSubmit={handleCreatePlatform}>
                  <input
                    className="input-base"
                    placeholder="平台编码（如：my_platform）"
                    value={platformForm.platform}
                    onChange={(e) => setPlatformForm((x) => ({ ...x, platform: e.target.value }))}
                    required
                  />
                  <input
                    className="input-base"
                    placeholder="平台名称"
                    value={platformForm.label}
                    onChange={(e) => setPlatformForm((x) => ({ ...x, label: e.target.value }))}
                    required
                  />
                  <input
                    className="input-base"
                    placeholder="平台说明（可选）"
                    value={platformForm.helper}
                    onChange={(e) => setPlatformForm((x) => ({ ...x, helper: e.target.value }))}
                  />
                  <input
                    className="input-base"
                    placeholder="文档链接（可选）"
                    value={platformForm.docs_url}
                    onChange={(e) => setPlatformForm((x) => ({ ...x, docs_url: e.target.value }))}
                  />
                  <button className="btn-brand" type="submit" disabled={platformSubmitting}>
                    {platformSubmitting ? "注册中..." : "+ 注册平台"}
                  </button>
                </form>

                <table className="table-shell min-w-[760px]">
                  <thead>
                    <tr className="table-head-row">
                      <th className="table-head-cell">平台编码</th>
                      <th className="table-head-cell">平台名称</th>
                      <th className="table-head-cell">说明</th>
                      <th className="table-head-cell">文档</th>
                      <th className="table-head-cell">状态</th>
                      <th className="table-head-cell">操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {availablePlatformConfigs.map((item) => (
                      <tr key={item.platform}>
                        <td className="table-cell mono-ui">{item.platform}</td>
                        <td className="table-cell">{item.label || item.platform}</td>
                        <td className="table-cell">{item.helper || "-"}</td>
                        <td className="table-cell">
                          {item.docs_url ? (
                            <a href={item.docs_url} target="_blank" rel="noreferrer" className="text-[#0000E1] hover:underline">
                              查看文档
                            </a>
                          ) : (
                            "-"
                          )}
                        </td>
                        <td className="table-cell">
                          <span className={statusClass(item.status || "active")}>{item.status || "active"}</span>
                        </td>
                        <td className="table-cell">
                          <button
                            className="btn-subtle"
                            type="button"
                            disabled={!item.mutable}
                            onClick={() => handleDeletePlatform(item)}
                            title={item.mutable ? "删除平台" : "系统平台不可删除"}
                          >
                            {item.mutable ? "删除" : "系统内置"}
                          </button>
                        </td>
                      </tr>
                    ))}
                    {!platformLoading && !availablePlatformConfigs.length && (
                      <tr>
                        <td className="table-cell" colSpan={6}>暂无平台配置</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </article>
            )}
          </section>
        )}

        {activeModule === "apihub" && (
          <section className="section-block p-6">
            <div className="mb-6 flex items-center justify-between border-b border-slate-200 pb-4">
              <h3 className="panel-title">API 接口管理</h3>
              <span className="rounded-sm border border-slate-300 bg-white px-2 py-1 text-xs text-slate-600 mono-ui">Definition / Publish / Policies / Sandbox</span>
            </div>

            <div className="grid gap-6 xl:grid-cols-[240px_1fr]">
              <aside className="border-r border-slate-200 pr-4">
                <h4 className="mb-3 mt-0 text-xs font-bold uppercase tracking-wide text-slate-500 mono-ui">功能模块</h4>
                <ul className="space-y-2 text-sm">
                  <li className="border-l-2 border-[#0000E1] bg-slate-50 px-3 py-2 text-slate-900 mono-ui">主动数据服务</li>
                  <li className="border-l-2 border-transparent px-3 py-2 text-slate-600 mono-ui">被动数据服务</li>
                  <li className="border-l-2 border-transparent px-3 py-2 text-slate-600 mono-ui">公共基础服务</li>
                </ul>
              </aside>

              <div className="min-w-0">
                <div className="mb-5 flex flex-wrap gap-6 border-b border-slate-200">
                  {apiTabs.map((tab) => (
                    <button
                      key={tab.key}
                      className={activeApiTab === tab.key ? "tab-minimal active" : "tab-minimal"}
                      onClick={() => setActiveApiTab(tab.key)}
                    >
                      {tab.label}
                    </button>
                  ))}
                </div>

                {activeApiTab === "definition" && (
                  <div className="grid gap-4 md:grid-cols-2">
                    <label className="field-label">接口名称<input className="input-base mt-1" defaultValue="获取订单列表" /></label>
                    <label className="field-label">请求路径<input className="input-base mt-1" defaultValue="/api/v1/orders/list" /></label>
                    <label className="field-label">请求方法<select className="input-base mt-1"><option>GET</option><option>POST</option></select></label>
                    <label className="field-label">协议类型<select className="input-base mt-1"><option>HTTPS</option></select></label>
                    <label className="field-label md:col-span-2">入参结构<textarea className="input-base mt-1 min-h-28" defaultValue='{"start_date":"string","end_date":"string","page_size":"int"}' /></label>
                    <label className="field-label md:col-span-2">出参结构<textarea className="input-base mt-1 min-h-28" defaultValue='{"code":0,"data":[...],"message":"ok"}' /></label>
                  </div>
                )}

                {activeApiTab === "publish" && (
                  <div className="grid gap-4 md:grid-cols-2">
                    <label className="field-label">后端转发地址<input className="input-base mt-1" defaultValue="https://backend.internal/orders/list" /></label>
                    <label className="field-label">当前版本<select className="input-base mt-1"><option>v1.0</option><option>v2.0</option></select></label>
                    <label className="field-label">发布策略<select className="input-base mt-1"><option>全量发布</option><option>灰度发布</option></select></label>
                    <label className="field-label">灰度比例<input className="input-base mt-1" defaultValue="10%" /></label>
                  </div>
                )}

                {activeApiTab === "policies" && (
                  <div className="grid gap-4 md:grid-cols-2">
                    <label className="field-label">限流（QPS / App_ID）<input className="input-base mt-1" defaultValue="50" /></label>
                    <label className="field-label">突发峰值<input className="input-base mt-1" defaultValue="100" /></label>
                    <label className="field-label">鉴权校验<select className="input-base mt-1"><option>启用</option><option>关闭</option></select></label>
                    <label className="field-label">签名算法<select className="input-base mt-1"><option>HMAC-SHA256</option></select></label>
                  </div>
                )}

                {activeApiTab === "sandbox" && (
                  <div>
                    <p className="mb-4 text-sm text-slate-500">沙箱已接入真实异步任务，可直接提交微信或 Meta 任务验证链路。</p>
                    <div className="grid gap-4 xl:grid-cols-2">
                      <form className="rounded-sm border border-slate-200 bg-white p-4" onSubmit={handleSubmitWechat}>
                        <h4 className="mb-3 mt-0 text-sm font-bold text-slate-700">微信订单任务（异步）</h4>
                        <div className="grid gap-2">
                          <label className="field-label">账号ID<input type="number" className="input-base mt-1" value={wechatForm.account_id} onChange={(e) => setWechatForm((x) => ({ ...x, account_id: e.target.value }))} required /></label>
                          <label className="field-label">开始日期<input type="date" className="input-base mt-1" value={wechatForm.start_date} onChange={(e) => setWechatForm((x) => ({ ...x, start_date: e.target.value }))} /></label>
                          <label className="field-label">结束日期<input type="date" className="input-base mt-1" value={wechatForm.end_date} onChange={(e) => setWechatForm((x) => ({ ...x, end_date: e.target.value }))} /></label>
                          <label className="field-label">时间类型<select className="input-base mt-1" value={wechatForm.time_type} onChange={(e) => setWechatForm((x) => ({ ...x, time_type: e.target.value }))}><option value="create_time">create_time</option><option value="update_time">update_time</option></select></label>
                          <label className="field-label">page_size<input type="number" min="1" max="100" className="input-base mt-1" value={wechatForm.page_size} onChange={(e) => setWechatForm((x) => ({ ...x, page_size: e.target.value }))} /></label>
                          <button className="btn-brand" type="submit">提交微信任务</button>
                        </div>
                      </form>

                      <form className="rounded-sm border border-slate-200 bg-white p-4" onSubmit={handleSubmitMeta}>
                        <h4 className="mb-3 mt-0 text-sm font-bold text-slate-700">Meta 报表任务（异步）</h4>
                        <div className="grid gap-2">
                          <label className="field-label">账号ID<input type="number" className="input-base mt-1" value={metaForm.account_id} onChange={(e) => setMetaForm((x) => ({ ...x, account_id: e.target.value }))} required /></label>
                          <label className="field-label">开始日期<input type="date" className="input-base mt-1" value={metaForm.start_date} onChange={(e) => setMetaForm((x) => ({ ...x, start_date: e.target.value }))} /></label>
                          <label className="field-label">结束日期<input type="date" className="input-base mt-1" value={metaForm.end_date} onChange={(e) => setMetaForm((x) => ({ ...x, end_date: e.target.value }))} /></label>
                          <label className="field-label">层级<select className="input-base mt-1" value={metaForm.level} onChange={(e) => setMetaForm((x) => ({ ...x, level: e.target.value }))}><option value="ad">ad</option><option value="adset">adset</option><option value="campaign">campaign</option></select></label>
                          <label className="field-label">dry_run<select className="input-base mt-1" value={metaForm.dry_run} onChange={(e) => setMetaForm((x) => ({ ...x, dry_run: e.target.value }))}><option value="true">true</option><option value="false">false</option></select></label>
                          <button className="btn-brand" type="submit">提交 Meta 任务</button>
                        </div>
                      </form>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </section>
        )}

        {activeModule === "monitor" && (
          <section className="space-y-3">
            <article className="section-block overflow-auto">
              <div className="mb-3 flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
                <h3 className="panel-title">调用任务与错误追踪</h3>
                <div className="grid gap-2 sm:grid-cols-2">
                  <input className="input-base" placeholder="搜索任务类型/账号ID" value={taskSearch} onChange={(e) => setTaskSearch(e.target.value)} />
                  <select className="input-base" value={taskStatus} onChange={(e) => setTaskStatus(e.target.value)}>
                    <option value="all">全部状态</option>
                    <option value="pending">pending</option>
                    <option value="running">running</option>
                    <option value="success">success</option>
                    <option value="failed">failed</option>
                  </select>
                </div>
              </div>

              <table className="table-shell min-w-[760px]">
                <thead>
                  <tr className="table-head-row">
                    <th className="table-head-cell">ID</th><th className="table-head-cell">账号ID</th><th className="table-head-cell">类型</th><th className="table-head-cell">状态</th><th className="table-head-cell">创建时间</th><th className="table-head-cell">操作</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredTasks.map((item) => (
                    <tr key={item.id}>
                      <td className="table-cell">{item.id}</td>
                      <td className="table-cell">{item.account_id}</td>
                      <td className="table-cell">{item.task_type}</td>
                      <td className="table-cell"><span className={statusClass(item.status)}>{item.status}</span></td>
                      <td className="table-cell">{item.created_at || "-"}</td>
                      <td className="table-cell"><button className="btn-subtle" onClick={() => handleTaskDetail(item.id)}>查看</button></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </article>

            <article className="section-block">
              <h3 className="panel-title mb-3">详情面板</h3>
              <pre className="max-h-[360px] overflow-auto rounded-sm border border-slate-300 bg-slate-50 p-3 text-xs text-slate-700 mono-ui">{detailText}</pre>
            </article>
          </section>
        )}

        {activeModule === "appauth" && activeAppauthTab === "app_management" && batchBarMounted && (
          <div className={`batch-action-bar ${batchBarVisible ? "is-visible" : ""}`} role="status" aria-live="polite">
            <div className="selected-count">
              已选择 <span key={selectedCount} className="count context-action-count-number">{selectedCount}</span> 项
            </div>
            <div className="context-action-right">
              <button className="batch-delete-btn" type="button" onClick={handleBatchDeleteCredentials} disabled={batchDeleting}>
                {batchDeleting ? "批量删除中..." : "批量删除"}
              </button>
              <span className="action-divider" />
              <button className="context-action-close" type="button" aria-label="取消选择" onClick={clearSourceSelection}>
                ✕
              </button>
            </div>
          </div>
        )}
      </main>

      {toast && <div className="fixed bottom-4 right-4 rounded-sm border border-slate-900 bg-white px-4 py-2 text-sm text-slate-700 mono-ui">{toast}</div>}
    </div>
  )
}

export default App
