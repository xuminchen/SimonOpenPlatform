import { useCallback, useEffect, useMemo, useState } from "react"
import { createPortal } from "react-dom"
import { useLocation, useNavigate } from "react-router-dom"
import { Button, Checkbox, Dropdown, Input, Modal, Select, Segmented, Space, Switch, Table, Tag } from "antd"
import { apiFetch } from "./api/client"
import ConfirmModal from "./components/ConfirmModal"
import ConnectionWizardModal from "./components/modals/ConnectionWizardModal"
import CreateCredentialDrawerModal from "./components/modals/CreateCredentialDrawerModal"
import CredentialEditModal from "./components/modals/CredentialEditModal"
import DestinationDeleteModal from "./components/modals/DestinationDeleteModal"
import DestinationFilesModal from "./components/modals/DestinationFilesModal"
import PlatformDrawerModal from "./components/modals/PlatformDrawerModal"
import WhitelistModal from "./components/modals/WhitelistModal"
import MonitorTasksPanel from "./components/panels/MonitorTasksPanel"
import PlatformConfigsTablePanel from "./components/panels/PlatformConfigsTablePanel"
import BatchActionBar from "./components/ui/BatchActionBar"
import ConnectorBuilderPage from "./pages/ConnectorBuilderPage"

const moduleMeta = {
  dashboard: { title: "系统概览", desc: "查看平台总体状态、账号规模和任务执行健康度。" },
  iam: { title: "账号与权限", desc: "管理用户、角色权限与关键操作审计（RBAC 预留区）。" },
  platform_management: { title: "平台管理", desc: "维护平台注册信息，确保应用创建前完成平台准入配置。" },
  application_credentials: { title: "应用与凭证", desc: "管理应用凭证、授权状态和 Token 生命周期。" },
  application_connection: { title: "Application / Connection", desc: "管理数据连接来源与认证配置。" },
  application_transformation: { title: "Application / Transformation", desc: "管理数据转换规则与流程编排。" },
  application_destination: { title: "Application / Destination", desc: "管理数据落地目标与投递策略。" },
  apihub: { title: "Connector Builder", desc: "声明式配置并发布 API 数据流，进入全屏双栏构建页面。" },
  monitor: { title: "日志与监控", desc: "查看任务执行状态、错误明细与可观测信息。" },
  settings: { title: "系统设置", desc: "管理系统运行配置与环境参数（包含 DB 配置）。" },
}

const navItems = [
  { key: "dashboard", label: "系统概览", path: "/dashboard" },
  {
    key: "application_group",
    label: "Application",
    children: [
      { key: "application_connection", label: "Connection", path: "/application/connection" },
      { key: "application_transformation", label: "Transformation", path: "/application/transformation" },
      { key: "application_destination", label: "Destination", path: "/application/destination" },
    ],
  },
  { key: "apihub", label: "Connector Builder", path: "/apihub" },
  {
    key: "platform_and_app",
    label: "平台和应用管理",
    children: [
      { key: "application_credentials", label: "应用与凭证", path: "/application/credentials" },
      { key: "platform_management", label: "平台管理", path: "/platform/management" },
    ],
  },
  { key: "monitor", label: "日志与监控", path: "/monitor" },
  { key: "settings", label: "系统设置", path: "/settings" },
]

const initialAccountForm = {
  name: "",
  platform: "",
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
  platform: "",
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

const CONNECTION_SCHEMA_PRESETS = {
  red_juguang: [
    {
      id: "reporting",
      label: "Reporting",
      items: [
        {
          id: "offline_campaign",
          label: "offline_campaign",
          description: "计划层级离线报表数据",
          columns: [
            { name: "campaign_id", type: "STRING", role: "primary" },
            { name: "campaign_name", type: "STRING" },
            { name: "time", type: "DATE", role: "cursor" },
            { name: "impression", type: "INTEGER" },
            { name: "click", type: "INTEGER" },
            { name: "fee", type: "DECIMAL" },
          ],
        },
        {
          id: "offline_creative",
          label: "offline_creative",
          description: "创意层级离线报表数据",
          columns: [
            { name: "creativity_id", type: "STRING", role: "primary" },
            { name: "campaign_id", type: "STRING" },
            { name: "time", type: "DATE", role: "cursor" },
            { name: "click", type: "INTEGER" },
            { name: "fee", type: "DECIMAL" },
          ],
        },
        {
          id: "offline_keyword",
          label: "offline_keyword",
          description: "关键词层级离线报表数据",
          columns: [
            { name: "keyword_id", type: "BIGINT", role: "primary" },
            { name: "keyword", type: "STRING" },
            { name: "time", type: "DATE", role: "cursor" },
            { name: "click", type: "INTEGER" },
            { name: "fee", type: "DECIMAL" },
          ],
        },
      ],
    },
    {
      id: "master_data",
      label: "Master Data",
      items: [
        {
          id: "campaign_group_base_list",
          label: "campaign_group_base_list",
          description: "查询广告组列表（未删除）",
          columns: [
            { name: "campaign_group_id", type: "BIGINT", role: "primary" },
            { name: "campaign_group_name", type: "STRING" },
            { name: "enable", type: "INTEGER" },
            { name: "create_time", type: "TIMESTAMP", role: "cursor" },
          ],
        },
        {
          id: "ube_extra_query",
          label: "ube_extra_query",
          description: "批量查询简单投标的ID",
          columns: [
            { name: "campaign_group_id", type: "BIGINT", role: "primary" },
            { name: "ube_view_id", type: "STRING" },
            { name: "group_type", type: "INTEGER" },
          ],
        },
      ],
    },
  ],
  wechat_shop: [
    {
      id: "trade",
      label: "Trade",
      items: [
        {
          id: "orders",
          label: "orders",
          description: "微信小店订单列表",
          columns: [
            { name: "order_id", type: "STRING", role: "primary" },
            { name: "create_time", type: "TIMESTAMP", role: "cursor" },
            { name: "status", type: "STRING" },
            { name: "total_amount", type: "DECIMAL" },
          ],
        },
        {
          id: "refunds",
          label: "refunds",
          description: "微信小店退款流水",
          columns: [
            { name: "refund_id", type: "STRING", role: "primary" },
            { name: "update_time", type: "TIMESTAMP", role: "cursor" },
            { name: "order_id", type: "STRING" },
            { name: "refund_amount", type: "DECIMAL" },
          ],
        },
      ],
    },
  ],
  oceanengine: [
    {
      id: "marketing",
      label: "Marketing",
      items: [
        {
          id: "ad_report",
          label: "ad_report",
          description: "广告报表明细",
          columns: [
            { name: "ad_id", type: "STRING", role: "primary" },
            { name: "stat_time", type: "DATE", role: "cursor" },
            { name: "impressions", type: "INTEGER" },
            { name: "cost", type: "DECIMAL" },
          ],
        },
      ],
    },
  ],
}

const SUPPORTED_STREAMS = {
  red_juguang: [
    { key: "offline_campaign", label: "离线计划报表", desc: "包含广告计划层级的花费、曝光、点击等核心转化数据。" },
    { key: "offline_creative", label: "离线创意报表", desc: "创意层级消耗与效果数据。" },
    { key: "offline_account", label: "离线账户报表", desc: "账户维度汇总数据，适合总体监控。" },
    { key: "offline_unit", label: "离线单元报表", desc: "广告单元层级数据，用于优化分析。" },
    { key: "offline_keyword", label: "离线关键词报表", desc: "关键词维度的消耗与效果表现。" },
    { key: "offline_search_word", label: "离线搜索词报表", desc: "搜索词触发与效果统计。" },
    { key: "offline_note", label: "离线笔记报表", desc: "笔记维度投放效果数据。" },
    { key: "offline_spu", label: "离线 SPU 报表", desc: "SPU 商品维度投放数据。" },
    { key: "offline_easy_promotion_group", label: "轻投放-广告组报表", desc: "轻投放广告组层级数据。" },
    { key: "offline_easy_promotion_note", label: "轻投放-笔记报表", desc: "轻投放笔记层级数据。" },
    { key: "offline_easy_promotion_base", label: "轻投放-基础报表", desc: "轻投放基础效果统计数据。" },
    { key: "campaign_group_base_list", label: "广告组基础列表", desc: "广告组基础信息清单，用于主数据同步。" },
    { key: "ube_extra_query", label: "UBE 扩展查询", desc: "用于补充查询简单投标相关信息。" },
  ],
  red_chengfeng: [
    { key: "offline_campaign", label: "乘风离线计划报表", desc: "乘风平台计划层级核心投放数据。" },
    { key: "offline_creative", label: "乘风离线创意报表", desc: "乘风平台创意层级效果数据。" },
    { key: "offline_account", label: "乘风离线账户报表", desc: "乘风平台账户维度汇总数据。" },
    { key: "offline_unit", label: "乘风离线单元报表", desc: "乘风平台单元层级效果数据。" },
    { key: "offline_keyword", label: "乘风离线关键词报表", desc: "乘风平台关键词维度效果数据。" },
    { key: "offline_search_word", label: "乘风离线搜索词报表", desc: "乘风平台搜索词触发与效果统计。" },
    { key: "offline_note", label: "乘风离线笔记报表", desc: "乘风平台笔记维度效果数据。" },
    { key: "offline_spu", label: "乘风离线 SPU 报表", desc: "乘风平台商品维度投放数据。" },
    { key: "offline_easy_promotion_group", label: "乘风轻投放-广告组报表", desc: "乘风轻投放广告组层级数据。" },
    { key: "offline_easy_promotion_note", label: "乘风轻投放-笔记报表", desc: "乘风轻投放笔记层级数据。" },
    { key: "offline_easy_promotion_base", label: "乘风轻投放-基础报表", desc: "乘风轻投放基础效果统计数据。" },
    { key: "campaign_group_base_list", label: "乘风广告组基础列表", desc: "乘风广告组主数据同步。" },
    { key: "ube_extra_query", label: "乘风 UBE 扩展查询", desc: "乘风简单投标扩展查询信息。" },
  ],
  oceanengine: [
    { key: "ad_report", label: "计划报表", desc: "巨量引擎基础广告计划数据。" },
  ],
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

function statusTagColor(status) {
  const key = String(status || "").trim().toLowerCase()
  if (key === "ready" || key === "active" || key === "success") return "success"
  if (key === "running" || key === "pending") return "processing"
  if (key === "partial") return "warning"
  if (key === "failed" || key === "error" || key === "exception" || key === "refresh_failed" || key === "abnormal") {
    return "error"
  }
  return "default"
}

function connectionToneTagColor(tone) {
  const key = String(tone || "").trim().toLowerCase()
  if (key === "active") return "success"
  if (key === "warning") return "warning"
  return "default"
}

function platformMappingLabel(platform, platformLabelMap = {}) {
  const key = String(platform || "").trim().toLowerCase()
  return platformLabelMap[key] || platform || "-"
}

function syncModeLabel(mode) {
  const key = String(mode || "").trim().toLowerCase()
  if (key === "incremental") return "增量同步"
  if (key === "full_refresh") return "全量覆盖"
  return String(mode || "-")
}

function isCursorType(type) {
  const key = String(type || "").trim().toUpperCase()
  return ["TIMESTAMP", "DATE", "DATETIME", "INTEGER", "BIGINT", "INT", "NUMBER"].includes(key)
}

function FieldRoleBadge({ type }) {
  if (type === "primary") {
    return (
      <span className="inline-flex items-center gap-1 rounded-full bg-amber-50 px-2 py-0.5 text-xs font-medium text-amber-700">
        <svg viewBox="0 0 20 20" fill="currentColor" className="h-3.5 w-3.5">
          <path d="M11.75 2.5a5.25 5.25 0 0 0-4.22 8.37l-4.8 4.8a1.25 1.25 0 0 0 .88 2.13h2.15a1.25 1.25 0 0 0 1.25-1.25v-.9h.9a1.25 1.25 0 0 0 1.25-1.25v-.9h.9a1.25 1.25 0 0 0 .88-.37l.85-.85a5.25 5.25 0 1 0-2.04-9.78Zm2.5 5.25a1.25 1.25 0 1 1 0-2.5 1.25 1.25 0 0 1 0 2.5Z" />
        </svg>
        Primary
      </span>
    )
  }
  if (type === "cursor") {
    return (
      <span className="inline-flex items-center gap-1 rounded-full bg-sky-50 px-2 py-0.5 text-xs font-medium text-sky-700">
        <svg viewBox="0 0 20 20" fill="currentColor" className="h-3.5 w-3.5">
          <path
            fillRule="evenodd"
            d="M10 2.5a7.5 7.5 0 1 0 7.5 7.5A7.5 7.5 0 0 0 10 2.5Zm.75 3.75a.75.75 0 0 0-1.5 0V10c0 .2.08.39.22.53l2.25 2.25a.75.75 0 0 0 1.06-1.06l-2.03-2.03V6.25Z"
            clipRule="evenodd"
          />
        </svg>
        Cursor
      </span>
    )
  }
  return <span className="text-slate-400">-</span>
}

function slugifyName(name) {
  const raw = String(name || "").trim().toLowerCase()
  if (!raw) return "default_destination"
  return raw
    .replace(/\s+/g, "_")
    .replace(/[^\w\u4e00-\u9fff-]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "") || "default_destination"
}

function destinationTypeLabel(item) {
  const key = String(item?.destination_type || "").toLowerCase()
  if (key === "managed_local_file") {
    const format = String(item?.config?.local_format || "JSONL").toUpperCase()
    return `📁 系统托管文件 (${format})`
  }
  if (key.includes("postgre")) return "🗄 PostgreSQL"
  if (key.includes("clickhouse")) return "🗄 ClickHouse"
  if (key.includes("mysql")) return "🗄 MySQL"
  return String(item?.destination_type || "-")
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
  const [platformMenuOpen, setPlatformMenuOpen] = useState(false)
  const [applicationMenuOpen, setApplicationMenuOpen] = useState(false)
  const [healthText, setHealthText] = useState("健康检查中...")
  const [destinationTab, setDestinationTab] = useState("target_setup")
  const [destinationProfiles, setDestinationProfiles] = useState([])

  const [connections, setConnections] = useState([])
  const [connectionsLoading, setConnectionsLoading] = useState(false)
  const [connectionSearch, setConnectionSearch] = useState("")
  const [selectedConnectionIds, setSelectedConnectionIds] = useState([])
  const [executionSubmittingByProject, setExecutionSubmittingByProject] = useState({})
  const [connectionDeletingByProject, setConnectionDeletingByProject] = useState({})
  const [executionDetailDialog, setExecutionDetailDialog] = useState(null)
  const [connectionDetailLoading, setConnectionDetailLoading] = useState(false)
  const [connectionDetailError, setConnectionDetailError] = useState("")
  const [connectionDetailProject, setConnectionDetailProject] = useState(null)
  const [connectionDetailStreams, setConnectionDetailStreams] = useState([])
  const [streamPreviewDialog, setStreamPreviewDialog] = useState(null)
  const [streamPreviewViewMode, setStreamPreviewViewMode] = useState("table")
  const [connectionDrawerMode, setConnectionDrawerMode] = useState("setup")
  const [wizardConnectionSaving, setWizardConnectionSaving] = useState(false)
  const [wizardTestLoading, setWizardTestLoading] = useState(false)
  const [wizardTestResult, setWizardTestResult] = useState(null)
  const [wizardTaskName, setWizardTaskName] = useState("")
  const [wizardProjectId, setWizardProjectId] = useState(null)
  const [wizardPlatform, setWizardPlatform] = useState("")
  const [wizardCredentialOptions, setWizardCredentialOptions] = useState([])
  const [wizardSelectedAppIds, setWizardSelectedAppIds] = useState([])
  const [wizardDestination, setWizardDestination] = useState("ClickHouse_DW")
  const [wizardScheduleCron, setWizardScheduleCron] = useState("0 * * * *")
  const [wizardCredentialSearch, setWizardCredentialSearch] = useState("")
  const [wizardSearch, setWizardSearch] = useState("")
  const [wizardQuickFilter, setWizardQuickFilter] = useState("all")
  const [wizardExpandedGroups, setWizardExpandedGroups] = useState([])
  const [wizardCheckedLeafIds, setWizardCheckedLeafIds] = useState([])
  const [wizardActiveLeafId, setWizardActiveLeafId] = useState("")
  const [wizardLeafSyncMode, setWizardLeafSyncMode] = useState({})
  const [wizardLeafCursorKey, setWizardLeafCursorKey] = useState({})
  const [wizardSchemaStreams, setWizardSchemaStreams] = useState([])
  const [wizardSchemaLoading, setWizardSchemaLoading] = useState(false)
  const [wizardContracts, setWizardContracts] = useState({})
  const [wizardScheduleMode, setWizardScheduleMode] = useState("basic")
  const [wizardFrequencyType, setWizardFrequencyType] = useState("daily")
  const [wizardMinuteInterval, setWizardMinuteInterval] = useState("15")
  const [wizardHourlyMode, setWizardHourlyMode] = useState("interval")
  const [wizardHourlyInterval, setWizardHourlyInterval] = useState("2")
  const [wizardHourlySpecificTimes, setWizardHourlySpecificTimes] = useState(["08:00", "12:00"])
  const [wizardDailyTime, setWizardDailyTime] = useState("02:00")
  const [wizardWeeklyDays, setWizardWeeklyDays] = useState(["MON"])
  const [wizardWeeklyTime, setWizardWeeklyTime] = useState("03:00")
  const [wizardMonthlyDay, setWizardMonthlyDay] = useState("1")
  const [wizardMonthlyTime, setWizardMonthlyTime] = useState("04:00")
  const [destinationForm, setDestinationForm] = useState({
    profile_name: "",
    engine_category: "database",
    database_type: "PostgreSQL",
    host: "",
    port: "5432",
    user: "",
    password: "",
    database: "",
    storage_provider: "aliyun_oss",
    bucket_name: "",
    endpoint: "",
    region: "",
    access_key_id: "",
    access_key_secret: "",
    path_prefix: "project_name/stream_name",
    file_format: "CSV",
    compression: "NONE",
    include_header: true,
    delimiter: ",",
    chunk_rows: "100000",
    chunk_size_mb: "50",
    local_format: "JSONL",
  })
  const [destinationTestLoading, setDestinationTestLoading] = useState(false)
  const [destinationTestResult, setDestinationTestResult] = useState(null)
  const [destinationTestPassed, setDestinationTestPassed] = useState(false)
  const [destinationFileDialog, setDestinationFileDialog] = useState(null)
  const [destinationFileLoading, setDestinationFileLoading] = useState(false)
  const [destinationDeleteDialog, setDestinationDeleteDialog] = useState(null)
  const [destinationDeleting, setDestinationDeleting] = useState(false)
  const [retentionSettings, setRetentionSettings] = useState({ enabled: false, retention_days: 30 })
  const [retentionSaving, setRetentionSaving] = useState(false)
  const [retentionRunning, setRetentionRunning] = useState(false)
  const [catalogActiveTableId, setCatalogActiveTableId] = useState("")
  const [appSettings, setAppSettings] = useState({
    db_enabled_runtime: false,
    db_enabled_next: false,
    database_url_runtime: "",
    database_url_next: "",
    db_enabled_source: "default",
    database_url_source: "default",
    restart_required: true,
  })
  const [settingsSaving, setSettingsSaving] = useState(false)
  const [settingsLoading, setSettingsLoading] = useState(false)

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
  const [selectedSourceAppIds, setSelectedSourceAppIds] = useState([])
  const [batchDeleting, setBatchDeleting] = useState(false)
  const [batchRefreshing, setBatchRefreshing] = useState(false)
  const [batchBarMounted, setBatchBarMounted] = useState(false)
  const [batchBarVisible, setBatchBarVisible] = useState(false)
  const [connectionBatchUpdating, setConnectionBatchUpdating] = useState(false)
  const [connectionBatchDeleting, setConnectionBatchDeleting] = useState(false)
  const [connectionBatchBarMounted, setConnectionBatchBarMounted] = useState(false)
  const [connectionBatchBarVisible, setConnectionBatchBarVisible] = useState(false)
  const [tokenRefreshingByAppId, setTokenRefreshingByAppId] = useState({})
  const [streamDrawerOpen, setStreamDrawerOpen] = useState(false)
  const [currentConfigAccount, setCurrentConfigAccount] = useState(null)
  const [activeStreams, setActiveStreams] = useState({})
  const [streamSaving, setStreamSaving] = useState(false)
  const [streamDrawerLoading, setStreamDrawerLoading] = useState(false)
  const [editDialog, setEditDialog] = useState(null)
  const [editForm, setEditForm] = useState(initialEditForm)
  const [editLoading, setEditLoading] = useState(false)
  const [editSaving, setEditSaving] = useState(false)
  const [appDrawerOpen, setAppDrawerOpen] = useState(false)
  const [actionModal, setActionModal] = useState(null)
  const [actionModalLoading, setActionModalLoading] = useState(false)
  const [whitelistDialog, setWhitelistDialog] = useState(null)
  const [whitelistValue, setWhitelistValue] = useState("")
  const [whitelistSaving, setWhitelistSaving] = useState(false)
  const [taskSearch, setTaskSearch] = useState("")
  const [taskStatus, setTaskStatus] = useState("all")
  const [platformConfigs, setPlatformConfigs] = useState([])
  const [platformSearch, setPlatformSearch] = useState("")
  const [platformForm, setPlatformForm] = useState(initialPlatformForm)
  const [platformLoading, setPlatformLoading] = useState(false)
  const [platformSubmitting, setPlatformSubmitting] = useState(false)
  const [platformDrawerOpen, setPlatformDrawerOpen] = useState(false)
  const [platformDrawerVisible, setPlatformDrawerVisible] = useState(false)
  const [platformDrawerMode, setPlatformDrawerMode] = useState("add")
  const [platformFormTouched, setPlatformFormTouched] = useState(false)
  const [platformCodeError, setPlatformCodeError] = useState("")

  const [autoRefresh, setAutoRefresh] = useState(false)
  const [toast, setToast] = useState("")

  const [accountForm, setAccountForm] = useState(initialAccountForm)
  const selectedCount = selectedSourceAppIds.length
  const selectedConnectionCount = selectedConnectionIds.length
  const destinationProfileNames = useMemo(
    () => destinationProfiles.map((item) => String(item?.name || "").trim()).filter(Boolean),
    [destinationProfiles]
  )

  const flatNavItems = useMemo(() => {
    return navItems.flatMap((item) => (item.children ? item.children : item))
  }, [])

  const activeModule = useMemo(() => {
    const normalizePath = (value) => {
      const raw = String(value || "").trim()
      if (!raw) return "/"
      if (raw === "/") return raw
      return raw.endsWith("/") ? raw.slice(0, -1) : raw
    }
    const currentPath = normalizePath(location.pathname)
    if (currentPath === "/appauth") return "application_credentials"
    if (currentPath === "/application") return "application_connection"
    if (currentPath.startsWith("/application/connection")) return "application_connection"
    if (currentPath === "/apihub" || currentPath.startsWith("/apihub/")) return "apihub"
    const hit = flatNavItems.find((item) => normalizePath(item.path) === currentPath)
    return hit ? hit.key : "dashboard"
  }, [flatNavItems, location.pathname])
  const isConnectionCreateView = String(location.pathname || "").startsWith("/application/connection/create")
  const connectionDetailId = useMemo(() => {
    const match = String(location.pathname || "").match(/^\/application\/connection\/(\d+)$/)
    if (!match) return null
    const value = Number(match[1])
    return Number.isFinite(value) && value > 0 ? value : null
  }, [location.pathname])
  const isConnectionDetailView = connectionDetailId !== null

  const isPlatformFolderActive = activeModule === "platform_management" || activeModule === "application_credentials"
  const isApplicationFolderActive =
    activeModule === "application_connection" ||
    activeModule === "application_transformation" ||
    activeModule === "application_destination"

  useEffect(() => {
    if (isPlatformFolderActive) {
      setPlatformMenuOpen(true)
    }
  }, [isPlatformFolderActive])

  useEffect(() => {
    if (isApplicationFolderActive) {
      setApplicationMenuOpen(true)
    }
  }, [isApplicationFolderActive])

  useEffect(() => {
    if (!isConnectionCreateView) return
    if (!wizardPlatform) return
    if (wizardExpandedGroups.length > 0) return
    setWizardExpandedGroups((CONNECTION_SCHEMA_PRESETS[wizardPlatform] || []).map((group) => group.id))
  }, [isConnectionCreateView, wizardExpandedGroups.length, wizardPlatform])

  useEffect(() => {
    if (!isConnectionCreateView) return
    if (!wizardPlatform) {
      setWizardSchemaStreams([])
      return
    }

    let cancelled = false
    const loadSchema = async () => {
      setWizardSchemaLoading(true)
      try {
        const schema = await apiFetch(
          `/api/v1/connections/schema?platform_code=${encodeURIComponent(wizardPlatform)}`
        )
        if (!cancelled) {
          setWizardSchemaStreams(Array.isArray(schema.streams) ? schema.streams : [])
        }
      } catch (err) {
        if (!cancelled) {
          setWizardSchemaStreams([])
          showToast(`接口能力加载失败: ${err.message}`)
        }
      } finally {
        if (!cancelled) {
          setWizardSchemaLoading(false)
        }
      }
    }

    loadSchema()
    return () => {
      cancelled = true
    }
  }, [isConnectionCreateView, wizardPlatform])

  useEffect(() => {
    if (!isConnectionCreateView) return
    if (connectionDrawerMode !== "setup") return

    if (destinationProfileNames.length === 0) {
      if (!String(wizardDestination || "").trim()) {
        setWizardDestination("ClickHouse_DW")
      }
      return
    }
    if (!destinationProfileNames.includes(wizardDestination)) {
      setWizardDestination(destinationProfileNames[0])
    }
  }, [
    connectionDrawerMode,
    destinationProfileNames,
    isConnectionCreateView,
    wizardDestination,
  ])

  function gotoModule(moduleKey) {
    const hit = flatNavItems.find((item) => item.key === moduleKey)
    if (!hit) {
      navigate("/dashboard")
      return
    }
    navigate(hit.path)
  }

  function gotoPath(path) {
    const target = String(path || "").trim()
    if (!target) {
      navigate("/dashboard")
      return
    }
    navigate(target)
  }

  function resetConnectionWizard() {
    setConnectionDrawerMode("setup")
    setWizardConnectionSaving(false)
    setWizardTestLoading(false)
    setWizardTestResult(null)
    setWizardTaskName("")
    setWizardProjectId(null)
    setWizardPlatform("")
    setWizardCredentialOptions([])
    setWizardSelectedAppIds([])
    setWizardDestination("")
    setWizardScheduleCron("0 * * * *")
    setWizardCredentialSearch("")
    setWizardSearch("")
    setWizardQuickFilter("all")
    setWizardExpandedGroups([])
    setWizardCheckedLeafIds([])
    setWizardActiveLeafId("")
    setWizardLeafSyncMode({})
    setWizardLeafCursorKey({})
    setWizardSchemaStreams([])
    setWizardContracts({})
    setWizardScheduleMode("basic")
    setWizardFrequencyType("daily")
    setWizardMinuteInterval("15")
    setWizardHourlyMode("interval")
    setWizardHourlyInterval("2")
    setWizardHourlySpecificTimes(["08:00", "12:00"])
    setWizardDailyTime("02:00")
    setWizardWeeklyDays(["MON"])
    setWizardWeeklyTime("03:00")
    setWizardMonthlyDay("1")
    setWizardMonthlyTime("04:00")
  }

  function openConnectionWizard() {
    resetConnectionWizard()
    setConnectionDrawerMode("setup")
    navigate("/application/connection/create")
  }

  function closeConnectionWizard() {
    navigate("/application/connection")
  }

  function openConnectionDetailPage(conn) {
    const projectId = Number(conn?.id)
    if (!Number.isFinite(projectId) || projectId <= 0) {
      showToast("项目 ID 无效，无法进入详情页")
      return
    }
    navigate(`/application/connection/${projectId}`)
  }

  function openConnectionManageWorkspace(conn) {
    resetConnectionWizard()
    setConnectionDrawerMode("workspace")
    setWizardTaskName(String(conn?.name || ""))
    setWizardProjectId(Number(conn?.id) || null)
    setWizardPlatform(String(conn?.platform_code || ""))
    setWizardDestination(String(conn?.destination || "ClickHouse_DW"))
    setWizardScheduleCron(String(conn?.schedule_cron || "0 * * * *"))
    const appIds =
      Array.isArray(conn?.app_ids)
        ? conn.app_ids.map((x) => String(x || "").trim()).filter(Boolean)
        : []
    const appId = String(conn?.app_id || "").trim()
    setWizardSelectedAppIds(appIds.length > 0 ? appIds : (appId ? [appId] : []))

    const streams = Array.isArray(conn?.streams) ? conn.streams : []
    const leafIds = streams.map((x) => String(x.stream_name || "").trim()).filter(Boolean)
    const leafSyncMode = {}
    const leafCursor = {}
    streams.forEach((x) => {
      const streamName = String(x.stream_name || "").trim()
      if (!streamName) return
      const mode = String(x.sync_mode || "").trim().toLowerCase()
      leafSyncMode[streamName] = mode === "full_refresh" ? "full_refresh" : "incremental"
      leafCursor[streamName] = String(x.cursor_field || "")
    })
    setWizardCheckedLeafIds(leafIds)
    setWizardLeafSyncMode(leafSyncMode)
    setWizardLeafCursorKey(leafCursor)
    setWizardActiveLeafId(leafIds[0] || "")
    setWizardQuickFilter("all")
    navigate("/application/connection/create")
  }

  function toggleWizardLeaf(leafId, checked) {
    setWizardCheckedLeafIds((prev) => {
      const has = prev.includes(leafId)
      if (checked && !has) return [...prev, leafId]
      if (!checked && has) return prev.filter((id) => id !== leafId)
      return prev
    })
  }

  function buildPresetContractForLeaf(leaf) {
    if (!leaf) return null
    const now = new Date().toISOString()
    const fields = (leaf.columns || []).map((field) => ({
      name: String(field.name || ""),
      path: [String(field.name || "")],
      type: String(field.type || "STRING"),
      source_type: String(field.type || "Unknown"),
      is_primary_key: !!field.primary_key,
      is_cursor_field: !!field.cursor_candidate || isCursorType(field.type),
      selected: true,
      is_new: false,
    }))
    return {
      stream_name: leaf.id,
      description: leaf.description || "",
      discovered_at: now,
      fields,
    }
  }

  async function handleSaveWorkspaceConfiguration() {
    const appIds = wizardSelectedAppIds.map((x) => String(x || "").trim()).filter(Boolean)
    if (appIds.length === 0) {
      showToast("至少选择 1 个授权账号后才能保存")
      return
    }

    const configuredStreams = wizardCheckedLeafIds.map((id) => ({
      stream_name: id,
      sync_mode: (wizardLeafSyncMode[id] || "incremental").toLowerCase(),
    }))

    const snapshot = {
      project_id: Number(wizardProjectId) || null,
      app_ids: appIds,
      configured_streams: configuredStreams,
    }
    setWizardConnectionSaving(true)
    try {
      const projectId = Number(wizardProjectId)
      if (!Number.isFinite(projectId) || projectId <= 0) {
        throw new Error("缺少 project_id，请先保存项目基础信息")
      }

      await apiFetch(`/api/v1/connections/projects/${projectId}/app-ids`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          app_ids: appIds,
        }),
      })

      if (configuredStreams.length > 0) {
        await apiFetch(`/api/v1/connections/projects/${projectId}/streams`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            streams: configuredStreams,
          }),
        })
      }

      setDetailText(JSON.stringify(snapshot, null, 2))
      await loadConnections()
      if (configuredStreams.length > 0) {
        showToast(`项目已保存：${appIds.length} 个授权账号，${configuredStreams.length} 个接口流`)
      } else {
        showToast(`项目授权账号已保存：${appIds.length} 个`)
      }
    } catch (err) {
      showToast(`保存失败: ${err.message}`)
    } finally {
      setWizardConnectionSaving(false)
    }
  }

  async function saveConnectionProjectSetup() {
    const appIds = wizardSelectedAppIds.map((x) => String(x || "").trim()).filter(Boolean)
    if (!wizardPlatform || appIds.length === 0) {
      showToast("请先选择平台和凭证")
      return
    }
    setWizardConnectionSaving(true)
    try {
      const baseName = wizardTaskName.trim() || `${wizardPlatform}_project`
      const resolvedDestination = destinationProfileNames.includes(wizardDestination)
        ? wizardDestination
        : (destinationProfileNames[0] || wizardDestination || "ClickHouse_DW")
      await apiFetch("/api/v1/connections", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: baseName,
          platform_code: wizardPlatform,
          credential_id: null,
          app_ids: appIds,
          destination: resolvedDestination,
          schedule_cron: wizardScheduleCron || "0 * * * *",
          status: 1,
          streams: [],
        }),
      })
      await loadConnections()
      closeConnectionWizard()
      showToast(`项目创建成功（绑定 ${appIds.length} 个账号），请在列表展开行后管理接口任务`)
    } catch (err) {
      showToast(`项目创建失败: ${err.message}`)
    } finally {
      setWizardConnectionSaving(false)
    }
  }

  async function handleWizardTestConnection() {
    if (!wizardPlatform || wizardSelectedAppIds.length === 0) {
      showToast("请选择平台与授权凭证")
      return
    }
    setWizardTestLoading(true)
    setWizardTestResult(null)
    try {
      const results = []
      for (const appId of wizardSelectedAppIds) {
        const data = await apiFetch("/api/v1/connections/test", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            platform_code: wizardPlatform,
            credential_id: null,
            app_id: appId,
          }),
        })
        results.push({ appId, ...data })
      }
      const failed = results.filter((x) => !x.success)
      const success = failed.length === 0
      const avgLatency =
        results.length > 0
          ? Math.round(results.reduce((sum, x) => sum + Number(x.latency_ms || 0), 0) / results.length)
          : 0
      setWizardTestResult({
        success,
        message: success
          ? `连接测试成功（${results.length}/${results.length}）`
          : `连接测试失败（${results.length - failed.length}/${results.length}）: ${failed
              .map((x) => x.appId)
              .join(", ")}`,
        latency_ms: avgLatency,
      })
      if (success) {
        const schema = await apiFetch(
          `/api/v1/connections/schema?platform_code=${encodeURIComponent(wizardPlatform)}`
        )
        setWizardSchemaStreams(schema.streams || [])
      }
    } catch (err) {
      setWizardTestResult({ success: false, message: err.message, latency_ms: 0 })
    } finally {
      setWizardTestLoading(false)
    }
  }

  const currentMeta = moduleMeta[activeModule] || moduleMeta.dashboard
  const connectionLabelMap = useMemo(() => {
    const map = {}
    platformConfigs.forEach((item) => {
      const key = String(item.platform || "").trim().toLowerCase()
      if (!key) return
      map[key] = String(item.label || item.platform || "").trim() || key
    })
    return map
  }, [platformConfigs])
  const connectionPlatformOptions = useMemo(() => {
    const fromConfig = platformConfigs.map((item) => String(item.platform || "").trim()).filter(Boolean)
    if (fromConfig.length > 0) {
      return fromConfig.map((platform) => ({
        value: platform,
        label: platformMappingLabel(platform, connectionLabelMap),
      }))
    }
    return Object.keys(CONNECTION_SCHEMA_PRESETS).map((platform) => ({
      value: platform,
      label: platformMappingLabel(platform, connectionLabelMap),
    }))
  }, [connectionLabelMap, platformConfigs])
  const wizardSchemaGroups = useMemo(() => {
    const streams = wizardSchemaStreams.map((item) => {
      const fields = (item.schema?.fields || []).map((field) => ({
        name: String(field.name || ""),
        type: String(field.type || ""),
        primary_key: !!field.primary_key,
        cursor_candidate: !!field.cursor_candidate,
      }))
      return {
        id: item.stream_name,
        label: item.stream_name,
        description: item.description || "",
        supported_sync_modes: item.supported_sync_modes || [],
        source_defined_cursor: item.source_defined_cursor || "",
        columns: fields,
      }
    })
    return [{ id: "streams", label: "Streams", items: streams }]
  }, [wizardSchemaStreams])
  const wizardDisplayGroups = useMemo(() => {
    const kw = wizardSearch.trim().toLowerCase()
    if (!kw) return wizardSchemaGroups
    return wizardSchemaGroups
      .map((group) => ({
        ...group,
        items: group.items.filter((item) => {
          const pool = `${group.label} ${item.label} ${item.description}`.toLowerCase()
          return pool.includes(kw)
        }),
      }))
      .filter((group) => group.items.length > 0)
  }, [wizardSchemaGroups, wizardSearch])
  const wizardVisibleLeafIds = useMemo(
    () => wizardDisplayGroups.flatMap((group) => group.items.map((item) => item.id)),
    [wizardDisplayGroups]
  )
  const filteredWizardCredentialOptions = useMemo(() => {
    const kw = wizardCredentialSearch.trim().toLowerCase()
    if (!kw) return wizardCredentialOptions
    return wizardCredentialOptions.filter((item) => {
      const appId = String(item.appId || "").toLowerCase()
      const name = String(item.name || "").toLowerCase()
      const label = String(item.label || "").toLowerCase()
      return appId.includes(kw) || name.includes(kw) || label.includes(kw)
    })
  }, [wizardCredentialOptions, wizardCredentialSearch])
  const selectedFilteredCredentialCount = useMemo(
    () => filteredWizardCredentialOptions.filter((x) => wizardSelectedAppIds.includes(x.appId)).length,
    [filteredWizardCredentialOptions, wizardSelectedAppIds]
  )
  const wizardActiveLeaf = useMemo(() => {
    for (const group of wizardSchemaGroups) {
      const hit = group.items.find((item) => item.id === wizardActiveLeafId)
      if (hit) return hit
    }
    return null
  }, [wizardActiveLeafId, wizardSchemaGroups])
  const wizardStreamCards = useMemo(
    () => wizardDisplayGroups.flatMap((group) => group.items),
    [wizardDisplayGroups]
  )
  const wizardCardsByQuickFilter = useMemo(() => {
    if (wizardQuickFilter === "checked") {
      return wizardStreamCards.filter((item) => wizardCheckedLeafIds.includes(item.id))
    }
    return wizardStreamCards
  }, [wizardCheckedLeafIds, wizardQuickFilter, wizardStreamCards])
  const wizardCheckedStreamCount = wizardCheckedLeafIds.length
  const wizardWorkspaceSaveDisabled = wizardSelectedAppIds.length === 0
  const destinationCatalogGroups = useMemo(() => {
    return connections.map((conn) => {
      const schemaGroups = CONNECTION_SCHEMA_PRESETS[conn.platform_code] || []
      const allPresetItems = schemaGroups.flatMap((g) => g.items)
      const selectedStreams = Array.isArray(conn.streams) ? conn.streams : []
      return {
        id: conn.id,
        label: `${conn.name} (${platformMappingLabel(conn.platform_code, connectionLabelMap)})`,
        items: selectedStreams.map((stream) => {
          const hit = allPresetItems.find((x) => x.id === stream.stream_name)
          return (
            hit || {
              id: stream.stream_name,
              label: stream.stream_name,
              description: "",
              columns: [],
            }
          )
        }).map((item) => ({ ...item, sourceConnection: conn.name })),
      }
    })
  }, [connections, connectionLabelMap])
  const filteredConnections = useMemo(() => {
    const kw = String(connectionSearch || "").trim().toLowerCase()
    if (!kw) return connections
    return connections.filter((conn) => {
      const pool = `${conn.name || ""} ${conn.platform_code || ""} ${conn.destination || ""}`.toLowerCase()
      return pool.includes(kw)
    })
  }, [connectionSearch, connections])
  const connectionSummary = useMemo(() => {
    let active = 0
    let paused = 0
    let warning = 0
    connections.forEach((conn) => {
      const sync = String(conn.last_sync_status || "").toUpperCase()
      if (Number(conn.status) === 0) {
        paused += 1
      } else if (sync === "FAILED" || sync === "ERROR" || Number(conn.status) !== 1) {
        warning += 1
      } else {
        active += 1
      }
    })
    return { active, paused, warning, total: connections.length }
  }, [connections])

  const connectionHeaderMenu = {
    items: [
      { key: "refresh", label: "刷新列表" },
      { key: "toggle_auto_refresh", label: `自动刷新: ${autoRefresh ? "开" : "关"}` },
      { key: "attributes", label: "Attributes" },
    ],
    onClick: ({ key }) => {
      if (key === "refresh") {
        loadConnections().catch((e) => showToast(e.message))
        return
      }
      if (key === "toggle_auto_refresh") {
        setAutoRefresh((v) => !v)
        return
      }
      showToast("属性面板建设中")
    },
  }

  const connectionRowSelection = useMemo(
    () => ({
      selectedRowKeys: selectedConnectionIds,
      onChange: (nextKeys) => {
        const next = nextKeys.map((id) => Number(id)).filter((id) => Number.isFinite(id))
        setSelectedConnectionIds(next)
      },
    }),
    [selectedConnectionIds]
  )

  const connectionListColumns = [
      {
        title: "Connection name",
        key: "name",
        render: (_, conn) => (
          <Button type="link" onClick={() => openConnectionDetailPage(conn)}>
            {conn.name || "-"}
          </Button>
        ),
      },
      {
        title: "Source type",
        key: "platform_code",
        render: (_, conn) => (
          <Space direction="vertical" size={2}>
            <Space size={6}>
              <Tag color="blue">{connectionSourceIcon(conn.platform_code)}</Tag>
              <span>{platformMappingLabel(conn.platform_code, platformLabelMap)}</span>
            </Space>
            {Array.isArray(conn.app_ids) && conn.app_ids.length > 1 ? (
              <span className="text-xs text-slate-500">包含 {conn.app_ids.length} 个授权账号</span>
            ) : null}
          </Space>
        ),
      },
      {
        title: "Destination",
        key: "destination",
        render: (_, conn) => (
          <Space size={6}>
            <Tag color="geekblue">{connectionDestinationIcon(conn.destination)}</Tag>
            <span>{conn.destination || "ClickHouse_DW"}</span>
          </Space>
        ),
      },
      {
        title: "Status",
        key: "status",
        render: (_, conn) => {
          const statusMeta = connectionStatusMeta(conn.status)
          return <Tag color={connectionToneTagColor(statusMeta.tone)}>{statusMeta.text}</Tag>
        },
      },
      {
        title: "Last synced",
        key: "last_sync_time",
        render: (_, conn) => formatTimeText(conn.last_sync_time),
      },
      {
        title: "Actions",
        key: "actions",
        render: (_, conn) => {
          const projectId = Number(conn.id)
          const executionSubmitting = !!executionSubmittingByProject[projectId]
          const deleting = !!connectionDeletingByProject[projectId]
          return (
            <Space size={0}>
              <Button type="link" onClick={() => openConnectionManageWorkspace(conn)}>
                Edit
              </Button>
              <Button
                type="link"
                onClick={() => handleRunNowConnection(conn)}
                disabled={executionSubmitting || deleting}
              >
                {executionSubmitting ? "Running..." : "Run Now"}
              </Button>
              <Button
                type="link"
                danger
                onClick={() => handleDeleteSingleConnection(conn)}
                disabled={deleting}
              >
                {deleting ? "Deleting..." : "Delete"}
              </Button>
            </Space>
          )
        },
      },
    ]

  const connectionDetailColumns = [
      {
        title: "序号",
        key: "index",
        width: 80,
        render: (_, __, index) => index + 1,
      },
      {
        title: "Stream",
        dataIndex: "stream_name",
        key: "stream_name",
      },
      {
        title: "Sync Mode",
        key: "sync_mode",
        render: (_, row) => syncModeLabel(row.sync_mode),
      },
      {
        title: "Cursor Field",
        dataIndex: "cursor_field",
        key: "cursor_field",
        render: (value) => value || "-",
      },
      {
        title: "Last Synced",
        key: "last_routine_finished_at",
        render: (_, row) => formatTimeText(row.last_routine_finished_at),
      },
      {
        title: "Actions",
        key: "actions",
        render: (_, row) => (
          <Button
            type="link"
            onClick={() => handlePreviewStream(connectionDetailProject.id, row.stream_name)}
          >
            Preview
          </Button>
        ),
      },
    ]

  const credentialRowSelection = useMemo(
    () => ({
      selectedRowKeys: selectedSourceAppIds,
      onChange: (nextKeys) => {
        setSelectedSourceAppIds(nextKeys.map((value) => String(value)))
      },
      getCheckboxProps: (record) => ({
        disabled: !record.app_id || !!tokenRefreshingByAppId[String(record.app_id || "")],
      }),
    }),
    [selectedSourceAppIds, tokenRefreshingByAppId]
  )

  const credentialColumns = [
      {
        title: "序号",
        dataIndex: "row_no",
        key: "row_no",
        width: 80,
      },
      {
        title: "名称",
        dataIndex: "name",
        key: "name",
      },
      {
        title: "App_ID",
        dataIndex: "app_id",
        key: "app_id",
        render: (value) => <span className="mono-ui">{value || "-"}</span>,
      },
      {
        title: "平台",
        key: "platform",
        render: (_, item) => platformMappingLabel(item.platform, platformLabelMap),
      },
      {
        title: "状态",
        key: "status",
        render: (_, item) => <Tag color={statusTagColor(item.status)}>{item.status || "-"}</Tag>,
      },
      {
        title: "Token 状态",
        key: "token_status",
        render: (_, item) => {
          const tokenStatus = item.token_status || (item.has_access_token ? "ready" : "missing")
          return <Tag color={statusTagColor(tokenStatus)}>{tokenStatus}</Tag>
        },
      },
      {
        title: "更新时间",
        key: "token_updated_at",
        render: (_, item) => <span className="mono-ui">{formatTimeText(item.token_updated_at)}</span>,
      },
      {
        title: "access_token",
        key: "access_token",
        render: (_, item) => <span className="mono-ui text-xs">{item.access_token || "-"}</span>,
      },
      {
        title: "操作",
        key: "actions",
        width: 280,
        render: (_, item) => {
          const refreshing = isRowTokenRefreshing(item.app_id)
          const actionDisabled = !item.app_id || refreshing
          return (
            <Space size={0} wrap>
              <Button
                type="link"
                disabled={refreshing}
                onClick={() => openEditCredentialDialog(item)}
              >
                编辑
              </Button>
              <Button
                type="link"
                loading={refreshing}
                disabled={actionDisabled}
                onClick={() => openTokenConfirm(item)}
              >
                {refreshing ? "更新中..." : "更新 Token"}
              </Button>
              <Button
                type="link"
                disabled={actionDisabled}
                onClick={() => openStreamDrawerForAccount(item)}
              >
                配置接口
              </Button>
              <Dropdown
                trigger={["click"]}
                menu={{
                  items: [
                    { key: "view", label: "查看" },
                    { key: "delete", label: "删除", danger: true },
                  ],
                  onClick: ({ key }) => {
                    if (key === "view") {
                      setDetailText(JSON.stringify(item, null, 2))
                      return
                    }
                    handleDeleteSingleCredential(item)
                  },
                }}
              >
                <Button type="link" disabled={refreshing}>更多</Button>
              </Dropdown>
            </Space>
          )
        },
      },
    ]

  const credentialPagination = {
    current: sourcePage,
    pageSize: sourcePageSize,
    total: sourceTotal,
    showSizeChanger: false,
    showTotal: (total) => `共 ${total} 条，每页 ${sourcePageSize} 条`,
    onChange: (page) => {
      loadCredentialSource(page).catch((err) => showToast(err.message))
    },
  }

  const destinationCatalogActive = useMemo(() => {
    for (const group of destinationCatalogGroups) {
      const hit = group.items.find((item) => `${group.id}:${item.id}` === catalogActiveTableId)
      if (hit) return { ...hit, groupLabel: group.label }
    }
    return null
  }, [catalogActiveTableId, destinationCatalogGroups])

  useEffect(() => {
    if (!isConnectionCreateView) return
    if (wizardVisibleLeafIds.length === 0) {
      setWizardActiveLeafId("")
      return
    }
    if (!wizardVisibleLeafIds.includes(wizardActiveLeafId)) {
      setWizardActiveLeafId(wizardVisibleLeafIds[0])
    }
  }, [isConnectionCreateView, wizardActiveLeafId, wizardVisibleLeafIds])

  useEffect(() => {
    if (!wizardActiveLeaf) return
    const streamId = wizardActiveLeaf.id
    const current = String(wizardLeafCursorKey[streamId] || "").trim()
    if (current) return
    const suggested = String(wizardActiveLeaf.source_defined_cursor || "").trim()
    if (!suggested) return
    setWizardLeafCursorKey((prev) => ({ ...prev, [streamId]: suggested }))
  }, [wizardActiveLeaf, wizardLeafCursorKey])

  useEffect(() => {
    if (!wizardActiveLeaf) return
    const streamId = String(wizardActiveLeaf.id || "").trim()
    if (!streamId) return
    const existing = wizardContracts[streamId]
    if (existing && Array.isArray(existing.fields) && existing.fields.length > 0) return
    const preset = buildPresetContractForLeaf(wizardActiveLeaf)
    if (!preset || !Array.isArray(preset.fields) || preset.fields.length === 0) return
    setWizardContracts((prev) => ({ ...prev, [streamId]: preset }))
  }, [wizardActiveLeaf, wizardContracts])

  useEffect(() => {
    if (destinationCatalogGroups.length === 0) {
      setCatalogActiveTableId("")
      return
    }
    const valid = destinationCatalogGroups.some((group) =>
      group.items.some((item) => `${group.id}:${item.id}` === catalogActiveTableId)
    )
    if (!valid) {
      const firstGroup = destinationCatalogGroups[0]
      const firstItem = firstGroup?.items?.[0]
      setCatalogActiveTableId(firstItem ? `${firstGroup.id}:${firstItem.id}` : "")
    }
  }, [catalogActiveTableId, destinationCatalogGroups])

  const portalRoot = typeof document !== "undefined" ? document.body : null

  function showToast(message) {
    setToast(message)
    window.setTimeout(() => setToast(""), 2400)
  }

  async function handleSaveAppSettings() {
    setSettingsSaving(true)
    try {
      const data = await apiFetch("/api/v1/settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          db_enabled: !!appSettings.db_enabled_next,
          database_url: String(appSettings.database_url_next || "").trim(),
        }),
      })
      setAppSettings({
        db_enabled_runtime: !!data.db_enabled_runtime,
        db_enabled_next: !!data.db_enabled_next,
        database_url_runtime: String(data.database_url_runtime || ""),
        database_url_next: String(data.database_url_next || ""),
        db_enabled_source: String(data.db_enabled_source || "default"),
        database_url_source: String(data.database_url_source || "default"),
        restart_required: !!data.restart_required,
      })
      showToast("设置已保存，DB 配置将在服务重启后生效")
    } catch (err) {
      showToast(`设置保存失败: ${err.message}`)
    } finally {
      setSettingsSaving(false)
    }
  }

  function closeStreamDrawer() {
    setStreamDrawerOpen(false)
    setCurrentConfigAccount(null)
    setActiveStreams({})
    setStreamSaving(false)
    setStreamDrawerLoading(false)
  }

  async function openStreamDrawerForAccount(item) {
    const appId = String(item?.app_id || "").trim()
    const accountId = appId ? Number(accountIdByAppId.get(appId) || 0) : 0
    if (!accountId) {
      showToast("该凭证尚未同步到数据库账号，请先点击“同步凭证 JSON”")
      return
    }

    setCurrentConfigAccount({
      id: accountId,
      app_id: appId,
      name: String(item?.name || ""),
      platform: normalizePlatformCode(item?.platform),
    })
    setActiveStreams({})
    setStreamDrawerLoading(true)
    setStreamDrawerOpen(true)
    try {
      const data = await apiFetch(`/api/v1/accounts/${accountId}/streams`)
      const enabled = {}
      const streamKeys = Array.isArray(data?.streams) ? data.streams : []
      streamKeys.forEach((key) => {
        const streamKey = String(key || "").trim()
        if (!streamKey) return
        enabled[streamKey] = true
      })
      setActiveStreams(enabled)
    } catch (err) {
      showToast(`加载接口配置失败: ${err.message}`)
    } finally {
      setStreamDrawerLoading(false)
    }
  }

  async function handleSaveAccountStreams() {
    const accountId = Number(currentConfigAccount?.id || 0)
    if (!accountId) {
      showToast("缺少账号信息，无法保存")
      return
    }
    setStreamSaving(true)
    try {
      const selectedStreamKeys = Object.entries(activeStreams)
        .filter(([, isActive]) => !!isActive)
        .map(([key]) => key)
      await apiFetch(`/api/v1/accounts/${accountId}/streams`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ streams: selectedStreamKeys }),
      })
      showToast("接口配置已保存")
      closeStreamDrawer()
    } catch (err) {
      showToast(`保存失败: ${err.message}`)
    } finally {
      setStreamSaving(false)
    }
  }

  function openActionModal(config) {
    setActionModal(config)
  }

  function closeActionModal() {
    if (actionModalLoading) return
    setActionModal(null)
  }

  function formatTimeText(value) {
    const raw = String(value || "").trim()
    if (!raw) return "-"
    const normalized = raw.replace("T", " ").replace("Z", "")
    const noMillis = normalized.split(".")[0]
    const noOffset = noMillis.split("+")[0]
    return noOffset
  }

  function applySchedulePreset(preset) {
    setWizardScheduleMode("basic")
    if (preset === "daily_2am") {
      setWizardFrequencyType("daily")
      setWizardDailyTime("02:00")
      return
    }
    if (preset === "hourly_1") {
      setWizardFrequencyType("hourly")
      setWizardHourlyMode("interval")
      setWizardHourlyInterval("1")
      return
    }
    if (preset === "hourly_6") {
      setWizardFrequencyType("hourly")
      setWizardHourlyMode("interval")
      setWizardHourlyInterval("6")
    }
  }

  function pad2(num) {
    return String(num).padStart(2, "0")
  }

  function parseHourMinute(text) {
    const raw = String(text || "").trim()
    const [h, m] = raw.split(":")
    const hour = Number.parseInt(h || "0", 10)
    const minute = Number.parseInt(m || "0", 10)
    return {
      hour: Number.isFinite(hour) ? Math.max(0, Math.min(23, hour)) : 0,
      minute: Number.isFinite(minute) ? Math.max(0, Math.min(59, minute)) : 0,
    }
  }

  const scheduleTimeOptions = useMemo(() => {
    const rows = []
    for (let h = 0; h < 24; h += 1) {
      for (let m = 0; m < 60; m += 30) {
        rows.push(`${pad2(h)}:${pad2(m)}`)
      }
    }
    return rows
  }, [])

  const computedBasicCron = useMemo(() => {
    if (wizardFrequencyType === "minutes") {
      return `*/${wizardMinuteInterval} * * * *`
    }
    if (wizardFrequencyType === "hourly") {
      if (wizardHourlyMode === "interval") {
        return `0 */${wizardHourlyInterval} * * *`
      }
      const hours = wizardHourlySpecificTimes
        .map((item) => parseHourMinute(item).hour)
        .filter((x, idx, arr) => Number.isFinite(x) && arr.indexOf(x) === idx)
        .sort((a, b) => a - b)
      if (hours.length === 0) return "0 8 * * *"
      return `0 ${hours.join(",")} * * *`
    }
    if (wizardFrequencyType === "daily") {
      const { hour, minute } = parseHourMinute(wizardDailyTime)
      return `${minute} ${hour} * * *`
    }
    if (wizardFrequencyType === "weekly") {
      const { hour, minute } = parseHourMinute(wizardWeeklyTime)
      const byDay = wizardWeeklyDays.length > 0 ? wizardWeeklyDays.join(",") : "MON"
      return `${minute} ${hour} * * ${byDay}`
    }
    if (wizardFrequencyType === "monthly") {
      const { hour, minute } = parseHourMinute(wizardMonthlyTime)
      const day = wizardMonthlyDay === "last" ? "L" : wizardMonthlyDay
      return `${minute} ${hour} ${day} * *`
    }
    return "0 * * * *"
  }, [
    wizardDailyTime,
    wizardFrequencyType,
    wizardHourlyInterval,
    wizardHourlyMode,
    wizardHourlySpecificTimes,
    wizardMinuteInterval,
    wizardMonthlyDay,
    wizardMonthlyTime,
    wizardWeeklyDays,
    wizardWeeklyTime,
  ])

  const estimatedNextRunText = useMemo(() => {
    if (wizardScheduleMode === "advanced") return "-"
    const now = new Date()

    const toText = (dt) =>
      `${dt.getFullYear()}-${pad2(dt.getMonth() + 1)}-${pad2(dt.getDate())} ${pad2(dt.getHours())}:${pad2(
        dt.getMinutes()
      )}`

    if (wizardFrequencyType === "minutes") {
      const interval = Number.parseInt(wizardMinuteInterval, 10) || 15
      const next = new Date(now)
      next.setSeconds(0, 0)
      next.setMinutes(next.getMinutes() + 1)
      while (next.getMinutes() % interval !== 0) {
        next.setMinutes(next.getMinutes() + 1)
      }
      return toText(next)
    }

    if (wizardFrequencyType === "hourly") {
      if (wizardHourlyMode === "interval") {
        const interval = Number.parseInt(wizardHourlyInterval, 10) || 2
        const next = new Date(now)
        next.setSeconds(0, 0)
        next.setMinutes(0)
        next.setHours(next.getHours() + 1)
        while (next.getHours() % interval !== 0) {
          next.setHours(next.getHours() + 1)
        }
        return toText(next)
      }
      const hours = wizardHourlySpecificTimes
        .map((item) => parseHourMinute(item).hour)
        .filter((x, idx, arr) => arr.indexOf(x) === idx)
        .sort((a, b) => a - b)
      for (const h of hours) {
        const candidate = new Date(now)
        candidate.setHours(h, 0, 0, 0)
        if (candidate > now) return toText(candidate)
      }
      if (hours.length > 0) {
        const nextDay = new Date(now)
        nextDay.setDate(nextDay.getDate() + 1)
        nextDay.setHours(hours[0], 0, 0, 0)
        return toText(nextDay)
      }
    }

    if (wizardFrequencyType === "daily") {
      const { hour, minute } = parseHourMinute(wizardDailyTime)
      const next = new Date(now)
      next.setHours(hour, minute, 0, 0)
      if (next <= now) next.setDate(next.getDate() + 1)
      return toText(next)
    }

    if (wizardFrequencyType === "weekly") {
      const { hour, minute } = parseHourMinute(wizardWeeklyTime)
      const dayMap = { SUN: 0, MON: 1, TUE: 2, WED: 3, THU: 4, FRI: 5, SAT: 6 }
      const selected = wizardWeeklyDays.length > 0 ? wizardWeeklyDays : ["MON"]
      for (let offset = 0; offset <= 14; offset += 1) {
        const d = new Date(now)
        d.setDate(now.getDate() + offset)
        const dow = d.getDay()
        const hit = selected.some((x) => dayMap[x] === dow)
        if (!hit) continue
        d.setHours(hour, minute, 0, 0)
        if (d > now) return toText(d)
      }
    }

    if (wizardFrequencyType === "monthly") {
      const { hour, minute } = parseHourMinute(wizardMonthlyTime)
      const tryMonth = (base, monthOffset) => {
        const d = new Date(base.getFullYear(), base.getMonth() + monthOffset, 1, hour, minute, 0, 0)
        if (wizardMonthlyDay === "last") {
          d.setMonth(d.getMonth() + 1, 0)
        } else {
          const day = Math.max(1, Math.min(28, Number.parseInt(wizardMonthlyDay, 10) || 1))
          d.setDate(day)
        }
        return d
      }
      const thisMonth = tryMonth(now, 0)
      if (thisMonth > now) return toText(thisMonth)
      return toText(tryMonth(now, 1))
    }

    return "-"
  }, [
    wizardDailyTime,
    wizardFrequencyType,
    wizardHourlyInterval,
    wizardHourlyMode,
    wizardHourlySpecificTimes,
    wizardMinuteInterval,
    wizardMonthlyDay,
    wizardMonthlyTime,
    wizardScheduleMode,
    wizardWeeklyDays,
    wizardWeeklyTime,
  ])

  useEffect(() => {
    if (wizardScheduleMode !== "basic") return
    setWizardScheduleCron(computedBasicCron)
  }, [computedBasicCron, wizardScheduleMode])

  function connectionStatusMeta(status) {
    if (Number(status) === 1) return { text: "Active", tone: "active", dotCls: "is-running" }
    if (Number(status) === 0) return { text: "Paused", tone: "paused", dotCls: "is-paused" }
    return { text: "Warning", tone: "warning", dotCls: "is-error" }
  }

  function connectionSourceIcon(platformCode) {
    const key = String(platformCode || "").trim().toLowerCase()
    if (key.includes("wechat")) return "微"
    if (key.includes("red") || key.includes("xiaohongshu")) return "红"
    if (key.includes("ocean") || key.includes("jl")) return "巨"
    if (key.includes("meta") || key.includes("facebook")) return "M"
    return "S"
  }

  function connectionDestinationIcon(destination) {
    const key = String(destination || "").trim().toLowerCase()
    if (key.includes("clickhouse")) return "CH"
    if (key.includes("postgres")) return "PG"
    if (key.includes("lakehouse")) return "LK"
    return "DW"
  }

  function clearConnectionSelection() {
    setSelectedConnectionIds([])
  }

  async function updateConnectionBatchStatus(status) {
    if (!selectedConnectionCount) {
      showToast("请先选择要操作的 Connection")
      return
    }
    setConnectionBatchUpdating(true)
    try {
      const data = await apiFetch("/api/v1/connections/batch/status", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          connection_ids: selectedConnectionIds,
          status,
        }),
      })
      showToast(`批量状态更新完成：请求 ${data.total}，更新 ${data.updated}`)
      await loadConnections()
      clearConnectionSelection()
    } catch (err) {
      showToast(`批量状态更新失败: ${err.message}`)
    } finally {
      setConnectionBatchUpdating(false)
    }
  }

  async function handleBatchActivateConnections() {
    await updateConnectionBatchStatus(1)
  }

  async function handleBatchPauseConnections() {
    await updateConnectionBatchStatus(0)
  }

  async function handleBatchDeleteConnections() {
    if (!selectedConnectionCount) {
      showToast("请先选择要删除的 Connection")
      return
    }
    openActionModal({
      title: "批量删除 Connection",
      content: `确认删除选中的 ${selectedConnectionCount} 个 Connection 项目吗？`,
      confirmText: "确认删除",
      isDanger: true,
      onConfirm: async () => {
        setConnectionBatchDeleting(true)
        try {
          const data = await apiFetch("/api/v1/connections/batch/delete", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              connection_ids: selectedConnectionIds,
            }),
          })
          showToast(`批量删除完成：请求 ${data.total}，删除 ${data.deleted}`)
          await loadConnections()
          clearConnectionSelection()
        } catch (err) {
          showToast(`批量删除失败: ${err.message}`)
        } finally {
          setConnectionBatchDeleting(false)
        }
      },
    })
  }

  async function handleDeleteSingleConnection(conn) {
    const projectId = Number(conn?.id)
    if (!Number.isFinite(projectId) || projectId <= 0) {
      showToast("项目 ID 无效，无法删除")
      return
    }
    if (connectionDeletingByProject[projectId]) return

    const projectName = String(conn?.name || `Connection#${projectId}`)
    const hasConfiguredStreams = Array.isArray(conn?.streams) && conn.streams.length > 0

    const performDelete = async () => {
      setConnectionDeletingByProject((prev) => ({ ...prev, [projectId]: true }))
      try {
        const data = await apiFetch("/api/v1/connections/batch/delete", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            connection_ids: [projectId],
          }),
        })
        showToast(`删除完成：请求 ${data.total}，删除 ${data.deleted}`)
        await loadConnections()
        setSelectedConnectionIds((prev) => prev.filter((id) => id !== projectId))
      } catch (err) {
        showToast(`删除失败: ${err.message}`)
      } finally {
        setConnectionDeletingByProject((prev) => ({ ...prev, [projectId]: false }))
      }
    }

    if (!hasConfiguredStreams) {
      await performDelete()
      return
    }

    openActionModal({
      title: "删除 Connection",
      content: `「${projectName}」已配置 ${conn.streams.length} 个 stream，确认仍要删除吗？`,
      confirmText: "确认删除",
      isDanger: true,
      onConfirm: performDelete,
    })
  }

  function openAppDrawer() {
    setAppDrawerOpen(true)
  }

  function closeAppDrawer() {
    setAppDrawerOpen(false)
    setAccountForm(initialAccountForm)
  }

  function normalizePlatformCode(value) {
    return String(value || "").trim().toLowerCase()
  }

  function openPlatformDrawerForAdd() {
    setPlatformDrawerMode("add")
    setPlatformCodeError("")
    setPlatformForm(initialPlatformForm)
    setPlatformFormTouched(false)
    setPlatformDrawerOpen(true)
    window.requestAnimationFrame(() => setPlatformDrawerVisible(true))
  }

  function openPlatformDrawerForEdit(item) {
    if (!item || !item.mutable) return
    setPlatformDrawerMode("edit")
    setPlatformCodeError("")
    setPlatformForm({
      platform: String(item.platform || ""),
      label: String(item.label || ""),
      helper: String(item.helper || ""),
      docs_url: String(item.docs_url || ""),
      status: String(item.status || "active"),
    })
    setPlatformFormTouched(false)
    setPlatformDrawerOpen(true)
    window.requestAnimationFrame(() => setPlatformDrawerVisible(true))
  }

  function closePlatformDrawerInternal() {
    setPlatformDrawerVisible(false)
    window.setTimeout(() => setPlatformDrawerOpen(false), 220)
    setPlatformCodeError("")
    setPlatformFormTouched(false)
    setPlatformForm(initialPlatformForm)
  }

  function closePlatformDrawer(force = false) {
    if (!force && platformFormTouched) {
      openActionModal({
        title: "关闭前确认",
        content: "数据未保存，确认关闭吗？",
        confirmText: "确认关闭",
        onConfirm: async () => {
          closePlatformDrawerInternal()
        },
      })
      return
    }
    closePlatformDrawerInternal()
  }

  function validatePlatformForm() {
    const platform = normalizePlatformCode(platformForm.platform)
    const label = String(platformForm.label || "").trim()
    const helper = String(platformForm.helper || "").trim()
    const docsUrl = String(platformForm.docs_url || "").trim()

    if (!platform) return "请填写平台编码"
    if (!/^[a-z0-9_]+$/.test(platform)) return "平台编码仅支持小写字母、数字和下划线"
    if (!label) return "请填写平台名称"
    if (label.length < 2 || label.length > 50) return "平台名称长度需在 2-50 个字符之间"
    if (helper.length > 200) return "平台说明最多 200 个字符"
    if (docsUrl) {
      try {
        const url = new URL(docsUrl)
        if (url.protocol !== "http:" && url.protocol !== "https:") {
          return "文档链接必须以 http:// 或 https:// 开头"
        }
      } catch {
        return "请输入合法的文档链接"
      }
    }

    if (platformCodeError) return platformCodeError
    if (platformDrawerMode === "add") {
      const exists = availablePlatformConfigs.some((item) => normalizePlatformCode(item.platform) === platform)
      if (exists) return "该编码已被注册"
    }
    return ""
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

  async function loadProjectExecutions(projectId, { limit = 50 } = {}) {
    const pid = Number(projectId)
    if (!Number.isFinite(pid) || pid <= 0) return []
    const rows = await apiFetch(`/api/v1/connections/projects/${pid}/executions?limit=${limit}`)
    return Array.isArray(rows) ? rows : []
  }

  async function handleRunNowConnection(conn) {
    const projectId = Number(conn?.id)
    if (!Number.isFinite(projectId) || projectId <= 0) {
      showToast("项目 ID 无效，无法触发执行")
      return
    }
    const streamTaskIds = (Array.isArray(conn?.streams) ? conn.streams : [])
      .map((item) => Number(item?.id))
      .filter((id) => Number.isFinite(id) && id > 0)
    if (streamTaskIds.length === 0) {
      showToast("当前项目没有可执行的 stream task，请先配置接口流")
      return
    }

    setExecutionSubmittingByProject((prev) => ({ ...prev, [projectId]: true }))
    try {
      const created = await apiFetch(`/api/v1/connections/projects/${projectId}/executions/routine`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          stream_task_ids: streamTaskIds,
          triggered_by: "user",
        }),
      })
      const count = Array.isArray(created) ? created.length : 0
      showToast(`已触发 ${count} 个 stream 执行任务`)
      await loadProjectExecutions(projectId)
      window.setTimeout(() => {
        loadProjectExecutions(projectId).catch(() => {})
      }, 1800)
      await loadConnections()
    } catch (err) {
      showToast(`触发失败: ${err.message}`)
    } finally {
      setExecutionSubmittingByProject((prev) => ({ ...prev, [projectId]: false }))
    }
  }

  const loadConnections = useCallback(async () => {
    setConnectionsLoading(true)
    try {
      const data = await apiFetch("/api/v1/connections")
      setConnections(Array.isArray(data) ? data : [])
    } finally {
      setConnectionsLoading(false)
    }
  }, [])

  const loadConnectionDetail = useCallback(async (projectId) => {
    const pid = Number(projectId)
    if (!Number.isFinite(pid) || pid <= 0) return
    setConnectionDetailLoading(true)
    setConnectionDetailError("")
    try {
      const [project, streams] = await Promise.all([
        apiFetch(`/api/v1/connections/projects/${pid}`),
        apiFetch(`/api/v1/connections/projects/${pid}/streams`),
      ])
      setConnectionDetailProject(project || null)
      setConnectionDetailStreams(Array.isArray(streams) ? streams : [])
    } catch (err) {
      setConnectionDetailError(err.message)
      setConnectionDetailProject(null)
      setConnectionDetailStreams([])
    } finally {
      setConnectionDetailLoading(false)
    }
  }, [])

  async function handlePreviewStream(projectId, streamName) {
    const pid = Number(projectId)
    const targetStream = String(streamName || "").trim()
    if (!Number.isFinite(pid) || pid <= 0 || !targetStream) {
      showToast("预览参数无效")
      return
    }
    setStreamPreviewDialog({
      loading: true,
      error: "",
      payload: null,
    })
    setStreamPreviewViewMode("table")
    try {
      const data = await apiFetch(
        `/api/v1/connections/projects/${pid}/streams/${encodeURIComponent(targetStream)}/preview?limit=100`
      )
      const hasTableRows = Array.isArray(data?.rows) && data.rows.length > 0 && Array.isArray(data?.columns) && data.columns.length > 0
      setStreamPreviewViewMode(hasTableRows ? "table" : "json")
      setStreamPreviewDialog({
        loading: false,
        error: "",
        payload: data || null,
      })
    } catch (err) {
      setStreamPreviewDialog({
        loading: false,
        error: err.message,
        payload: null,
      })
    }
  }

  const loadWizardCredentialOptions = useCallback(async (platformCode) => {
    const target = String(platformCode || "").trim().toLowerCase()
    if (!target) {
      setWizardCredentialOptions([])
      return
    }
    const allRows = []
    let page = 1
    let totalPages = 1
    while (page <= totalPages) {
      const params = new URLSearchParams()
      params.set("page", String(page))
      params.set("page_size", "100")
      params.set("platform", target)
      params.set("status", "ready")
      const data = await apiFetch(`/api/v1/accounts/credentials/source?${params.toString()}`)
      const entries = Array.isArray(data.entries) ? data.entries : []
      allRows.push(...entries)
      totalPages = Number(data.total_pages || 1)
      page += 1
    }
    const dedup = new Map()
    allRows.forEach((row) => {
      const appId = String(row.app_id || "").trim()
      if (!appId) return
      if (dedup.has(appId)) return
      const name = String(row.name || "").trim() || "-"
      dedup.set(appId, {
        appId,
        name,
        label: `${appId}_${name}`,
      })
    })
    setWizardCredentialOptions(Array.from(dedup.values()))
  }, [])

  useEffect(() => {
    if (!isConnectionCreateView) return
    if (!wizardPlatform) {
      setWizardCredentialOptions([])
      setWizardSelectedAppIds([])
      return
    }
    loadWizardCredentialOptions(wizardPlatform).catch((err) => showToast(err.message))
  }, [isConnectionCreateView, loadWizardCredentialOptions, wizardPlatform])

  const loadPlatformConfigs = useCallback(async () => {
    setPlatformLoading(true)
    try {
      const data = await apiFetch("/api/v1/platform-configs")
      setPlatformConfigs(Array.isArray(data) ? data : [])
    } finally {
      setPlatformLoading(false)
    }
  }, [])

  const loadDestinationProfiles = useCallback(async () => {
    const data = await apiFetch("/api/v1/destinations")
    const rows = Array.isArray(data) ? data : []
    setDestinationProfiles(rows)
  }, [])

  const loadStorageRetentionSettings = useCallback(async () => {
    const data = await apiFetch("/api/v1/destinations/retention/settings")
    setRetentionSettings({
      enabled: !!data.enabled,
      retention_days: Number(data.retention_days || 30),
    })
  }, [])

  const loadAppSettings = useCallback(async () => {
    setSettingsLoading(true)
    try {
      const data = await apiFetch("/api/v1/settings")
      setAppSettings({
        db_enabled_runtime: !!data.db_enabled_runtime,
        db_enabled_next: !!data.db_enabled_next,
        database_url_runtime: String(data.database_url_runtime || ""),
        database_url_next: String(data.database_url_next || ""),
        db_enabled_source: String(data.db_enabled_source || "default"),
        database_url_source: String(data.database_url_source || "default"),
        restart_required: !!data.restart_required,
      })
    } finally {
      setSettingsLoading(false)
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
    setSourcePage(data.page || 1)
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
      Promise.all([
        loadAccounts(),
        loadTasks(),
        loadPlatformConfigs(),
        loadConnections(),
        loadDestinationProfiles(),
        loadStorageRetentionSettings(),
        loadAppSettings(),
      ]).catch((err) =>
        showToast(err.message)
      )
    }, 0)
    return () => window.clearTimeout(timer)
  }, [loadAppSettings, loadConnections, loadDestinationProfiles, loadPlatformConfigs, loadStorageRetentionSettings])

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
    if (selectedConnectionCount > 0) {
      if (!connectionBatchBarMounted) {
        setConnectionBatchBarMounted(true)
        const rafId = window.requestAnimationFrame(() => setConnectionBatchBarVisible(true))
        return () => window.cancelAnimationFrame(rafId)
      }
      setConnectionBatchBarVisible(true)
      return undefined
    }

    if (!connectionBatchBarMounted) return undefined
    setConnectionBatchBarVisible(false)
    const timer = window.setTimeout(() => setConnectionBatchBarMounted(false), 200)
    return () => window.clearTimeout(timer)
  }, [selectedConnectionCount, connectionBatchBarMounted])

  useEffect(() => {
    if (connections.length === 0) {
      setSelectedConnectionIds([])
      return
    }
    const validIds = new Set(connections.map((item) => Number(item.id)).filter((id) => Number.isFinite(id)))
    setSelectedConnectionIds((prev) => prev.filter((id) => validIds.has(id)))
  }, [connections])

  useEffect(() => {
    if (!isConnectionDetailView || !connectionDetailId) {
      setConnectionDetailError("")
      setConnectionDetailProject(null)
      setConnectionDetailStreams([])
      setStreamPreviewDialog(null)
      return
    }
    loadConnectionDetail(connectionDetailId).catch((err) => {
      setConnectionDetailError(err.message)
    })
  }, [connectionDetailId, isConnectionDetailView, loadConnectionDetail])

  useEffect(() => {
    if (destinationForm.engine_category !== "local") {
      setDestinationTestPassed(false)
      setDestinationTestResult(null)
      return
    }
    setDestinationTestPassed(false)
    setDestinationTestResult(null)
  }, [destinationForm.engine_category, destinationForm.local_format, destinationForm.profile_name])

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
      closeStreamDrawer()
      closePlatformDrawerInternal()
      setActionModal(null)
      setWhitelistDialog(null)
      setEditDialog(null)
    }

    window.addEventListener("keydown", handleEsc)
    return () => {
      window.removeEventListener("keydown", handleEsc)
    }
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

  const availablePlatformConfigs = useMemo(() => {
    return platformConfigs
  }, [platformConfigs])

  const filteredPlatformConfigs = useMemo(() => {
    const keyword = platformSearch.trim().toLowerCase()
    if (!keyword) return availablePlatformConfigs
    return availablePlatformConfigs.filter((item) => {
      const platform = String(item.platform || "").toLowerCase()
      const label = String(item.label || "").toLowerCase()
      return platform.includes(keyword) || label.includes(keyword)
    })
  }, [availablePlatformConfigs, platformSearch])

  const platformLabelMap = useMemo(() => {
    const map = {}
    availablePlatformConfigs.forEach((item) => {
      const key = String(item.platform || "").trim().toLowerCase()
      if (!key) return
      map[key] = String(item.label || item.platform || "").trim() || key
    })
    return map
  }, [availablePlatformConfigs])

  const platformsInUse = useMemo(() => {
    const used = new Set()
    accounts.forEach((item) => {
      const platform = normalizePlatformCode(item?.platform)
      if (platform) used.add(platform)
    })
    return used
  }, [accounts])

  const accountIdByAppId = useMemo(() => {
    const map = new Map()
    accounts.forEach((item) => {
      const appId = String(item?.app_id || "").trim()
      const accountId = Number(item?.id)
      if (!appId || !Number.isFinite(accountId) || accountId <= 0) return
      map.set(appId, accountId)
    })
    return map
  }, [accounts])

  const currentSupportedStreams = useMemo(() => {
    const platform = normalizePlatformCode(currentConfigAccount?.platform)
    return SUPPORTED_STREAMS[platform] || []
  }, [currentConfigAccount])

  const currentPlatformSchema = useMemo(() => {
    const current = availablePlatformConfigs.find((item) => item.platform === accountForm.platform)
    if (current) {
      return {
        label: current.label || current.platform,
        helper: current.helper || "",
        docsUrl: current.docs_url || "",
      }
    }
    return { label: "-", helper: "请先在平台管理中注册平台，再创建应用凭证。", docsUrl: "" }
  }, [accountForm.platform, availablePlatformConfigs])

  useEffect(() => {
    if (!availablePlatformConfigs.length) {
      setAccountForm((prev) => ({ ...prev, platform: "" }))
      return
    }
    const available = new Set(availablePlatformConfigs.map((item) => item.platform))
    setAccountForm((prev) => {
      if (available.has(prev.platform)) return prev
      return { ...prev, platform: "" }
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
      const appName = accountForm.name.trim()
      const status = accountForm.status.trim()
      if (!appName) {
        showToast("请填写应用名称")
        return
      }
      if (!accountForm.platform) {
        showToast("请先选择平台")
        return
      }
      if (!status) {
        showToast("请先选择状态")
        return
      }
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
        name: appName,
        platform: accountForm.platform,
        status,
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

  async function handleActionModalConfirm() {
    if (!actionModal?.onConfirm) return
    setActionModalLoading(true)
    try {
      await actionModal.onConfirm()
      setActionModal(null)
    } finally {
      setActionModalLoading(false)
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

  async function handleSubmitPlatformDrawer(e) {
    e.preventDefault()
    setPlatformFormTouched(true)
    const formError = validatePlatformForm()
    if (formError) {
      showToast(formError)
      return
    }
    const platform = normalizePlatformCode(platformForm.platform)
    setPlatformSubmitting(true)
    try {
      const method = platformDrawerMode === "add" ? "POST" : "PUT"
      const endpoint =
        platformDrawerMode === "add"
          ? "/api/v1/platform-configs"
          : `/api/v1/platform-configs/${encodeURIComponent(platform)}`
      await apiFetch(endpoint, {
        method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...(platformDrawerMode === "add" ? { platform } : {}),
          label: platformForm.label.trim(),
          helper: platformForm.helper.trim(),
          docs_url: platformForm.docs_url.trim(),
          status: platformForm.status,
        }),
      })
      showToast(platformDrawerMode === "add" ? "平台注册成功" : "平台信息已更新")
      await loadPlatformConfigs()
      closePlatformDrawer(true)
    } catch (err) {
      showToast(`${platformDrawerMode === "add" ? "注册" : "更新"}失败: ${err.message}`)
    } finally {
      setPlatformSubmitting(false)
    }
  }

  async function handleDeletePlatform(item) {
    const platform = String(item?.platform || "").trim()
    if (!platform) return
    openActionModal({
      title: "删除平台",
      content: `确认删除平台 ${platform} 吗？`,
      confirmText: "确认删除",
      isDanger: true,
      onConfirm: async () => {
        try {
          await apiFetch(`/api/v1/platform-configs/${encodeURIComponent(platform)}`, {
            method: "DELETE",
          })
          showToast("平台已删除")
          await loadPlatformConfigs()
        } catch (err) {
          showToast(`删除失败: ${err.message}`)
        }
      },
    })
  }

  function handlePlatformFieldChange(field, value) {
    setPlatformFormTouched(true)
    setPlatformForm((prev) => ({ ...prev, [field]: value }))
    if (field === "platform") {
      setPlatformCodeError("")
    }
  }

  function handlePlatformCodeBlur() {
    if (platformDrawerMode !== "add") return
    const code = normalizePlatformCode(platformForm.platform)
    if (!code) {
      setPlatformCodeError("")
      return
    }
    if (!/^[a-z0-9_]+$/.test(code)) {
      setPlatformCodeError("平台编码仅支持小写字母、数字和下划线")
      return
    }
    const exists = availablePlatformConfigs.some((item) => normalizePlatformCode(item.platform) === code)
    setPlatformCodeError(exists ? "该编码已被注册" : "")
    if (!exists) {
      setPlatformForm((prev) => ({ ...prev, platform: code }))
    }
  }

  function clearSourceSelection() {
    setSelectedSourceAppIds([])
  }

  async function openEditCredentialDialog(item) {
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
        platform: data.platform || item.platform || "",
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
    if (!selectedCount) {
      showToast("请先选择要删除的应用")
      return
    }
    openActionModal({
      title: "批量删除",
      content: `确认删除选中的 ${selectedCount} 个应用凭证吗？`,
      confirmText: "确认删除",
      isDanger: true,
      onConfirm: async () => {
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
      },
    })
  }

  async function handleBatchRefreshTokens() {
    if (!selectedCount) {
      showToast("请先选择要更新 Token 的应用")
      return
    }
    openActionModal({
      title: "批量更新 Token",
      content: `确认批量更新选中的 ${selectedCount} 个应用 Token 吗？`,
      confirmText: "确认更新",
      isDanger: true,
      onConfirm: async () => {
        setBatchRefreshing(true)
        const appIds = [...selectedSourceAppIds]
        setTokenRefreshingByAppId((prev) => {
          const next = { ...prev }
          appIds.forEach((id) => {
            next[String(id)] = true
          })
          return next
        })
        try {
          const data = await apiFetch("/api/v1/accounts/credentials/source/token/refresh/batch", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ app_ids: appIds }),
          })

          const okItems = Array.isArray(data.items) ? data.items.filter((x) => x && x.ok && x.result) : []
          const failedItems = Array.isArray(data.items) ? data.items.filter((x) => x && !x.ok) : []
          const byAppId = {}
          okItems.forEach((item) => {
            byAppId[String(item.app_id)] = item.result
          })
          const failedAppIds = new Set(failedItems.map((item) => String(item.app_id || "").trim()).filter(Boolean))
          const failedAt = new Date().toISOString()
          if (okItems.length > 0 || failedAppIds.size > 0) {
            setSourceAccounts((prev) =>
              prev.map((row) => {
                const appId = String(row.app_id || "")
                const hit = byAppId[appId]
                if (hit) {
                  return {
                    ...row,
                    has_access_token: !!hit.has_access_token,
                    access_token: hit.access_token || row.access_token || null,
                    refresh_token: hit.refresh_token || row.refresh_token || null,
                    token_status: hit.token_status || (hit.has_access_token ? "ready" : "missing"),
                    token_updated_at: hit.token_updated_at || row.token_updated_at,
                  }
                }
                if (failedAppIds.has(appId)) {
                  return {
                    ...row,
                    token_status: "refresh_failed",
                    token_updated_at: failedAt,
                  }
                }
                return row
              })
            )
          }

          showToast(`批量更新完成：成功 ${data.refreshed}，失败 ${data.failed}`)
        } catch (err) {
          showToast(`批量更新失败: ${err.message}`)
        } finally {
          setBatchRefreshing(false)
          setTokenRefreshingByAppId((prev) => {
            const next = { ...prev }
            appIds.forEach((id) => {
              next[String(id)] = false
            })
            return next
          })
        }
      },
    })
  }

  function isRowTokenRefreshing(appId) {
    return !!tokenRefreshingByAppId[String(appId || "")]
  }

  function openTokenConfirm(item) {
    const appId = String(item?.app_id || "").trim()
    if (!appId) {
      showToast("当前记录缺少 app_id，无法更新 Token")
      return
    }
    openActionModal({
      title: "强制更新 Token",
      content: "确定要强制更新此凭证的 Token 吗？旧的 Token 将立即失效，可能导致正在运行的同步任务中断。",
      confirmText: "确认更新",
      isDanger: true,
      onConfirm: async () => {
        await handleRefreshToken(item)
      },
    })
  }

  async function handleRefreshToken(item) {
    const appId = String(item?.app_id || "").trim()
    if (!appId) {
      showToast("当前记录缺少 app_id，无法更新 Token")
      return
    }
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
      const failedAt = new Date().toISOString()
      setSourceAccounts((prev) =>
        prev.map((row) =>
          String(row.app_id || "") === appId
            ? {
                ...row,
                token_status: "refresh_failed",
                token_updated_at: failedAt,
              }
            : row
        )
      )
      showToast(err.message || "更新失败：网络超时，请重试")
    } finally {
      setTokenRefreshingByAppId((prev) => ({ ...prev, [appId]: false }))
    }
  }

  async function handleDeleteSingleCredential(item) {
    const appId = String(item?.app_id || "").trim()
    if (!appId) {
      showToast("当前记录缺少 app_id，无法删除")
      return
    }
    openActionModal({
      title: "删除应用凭证",
      content: `确认删除应用 ${appId} 吗？`,
      confirmText: "确认删除",
      isDanger: true,
      onConfirm: async () => {
        try {
          const data = await apiFetch("/api/v1/accounts/credentials/source/delete", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ app_ids: [appId] }),
          })
          showToast(`删除完成：删除 ${data.deleted} 条`)
          setSelectedSourceAppIds((prev) => prev.filter((x) => x !== appId))
          await loadCredentialSource(sourcePage)
        } catch (err) {
          showToast(`删除失败: ${err.message}`)
        }
      },
    })
  }

  async function handleTaskDetail(id) {
    try {
      const data = await apiFetch(`/api/v1/tasks/${id}`)
      setDetailText(JSON.stringify(data, null, 2))
    } catch (err) {
      showToast(err.message)
    }
  }

  async function handleSaveDestinationProfile() {
    const profileName = String(destinationForm.profile_name || "").trim()
    if (!profileName) {
      showToast("请先填写目标名称")
      return
    }

    if (destinationForm.engine_category === "local" && !destinationTestPassed) {
      showToast("请先通过本地目录写入测试")
      return
    }

    const managedRelativePath = `destinations/${slugifyName(profileName)}`
    const destinationType =
      destinationForm.engine_category === "database"
        ? String(destinationForm.database_type || "PostgreSQL")
        : destinationForm.engine_category === "local"
          ? "managed_local_file"
          : String(destinationForm.storage_provider || "aliyun_oss")

    const normalizedConfig =
      destinationForm.engine_category === "local"
        ? {
            profile_name: profileName,
            engine_category: "local",
            local_format: destinationForm.local_format || "JSONL",
            managed_relative_path: managedRelativePath,
          }
        : { ...destinationForm }

    const payload = {
      name: profileName,
      engine_category: destinationForm.engine_category,
      destination_type: destinationType,
      status: "active",
      config: normalizedConfig,
    }

    try {
      await apiFetch("/api/v1/destinations", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })
      await loadDestinationProfiles()
      setWizardDestination(profileName)
      showToast("目标配置已保存")
    } catch (err) {
      showToast(`保存失败: ${err.message}`)
    }
  }

  async function handleTestDestinationProfile() {
    const destinationType =
      destinationForm.engine_category === "database"
        ? String(destinationForm.database_type || "PostgreSQL")
        : destinationForm.engine_category === "local"
          ? "managed_local_file"
          : String(destinationForm.storage_provider || "aliyun_oss")

    setDestinationTestLoading(true)
    setDestinationTestResult(null)
    try {
      const data = await apiFetch("/api/v1/destinations/test", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: String(destinationForm.profile_name || "").trim() || "default_destination",
          destination_type: destinationType,
          config: { ...destinationForm },
        }),
      })
      setDestinationTestPassed(true)
      setDestinationTestResult({
        success: true,
        message: String(data.message || "连接测试通过"),
        normalized_path: String(data.normalized_path || ""),
      })
      showToast("目标连接测试通过")
    } catch (err) {
      setDestinationTestPassed(false)
      setDestinationTestResult({
        success: false,
        message: String(err.message || "连接测试失败"),
        normalized_path: "",
      })
      showToast(`目标测试失败: ${err.message}`)
    } finally {
      setDestinationTestLoading(false)
    }
  }

  function formatBytes(size) {
    const value = Number(size || 0)
    if (!Number.isFinite(value) || value < 1024) return `${Math.max(0, Math.round(value))} B`
    if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`
    if (value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MB`
    return `${(value / (1024 * 1024 * 1024)).toFixed(1)} GB`
  }

  async function openManagedDestinationFiles(profile) {
    if (!profile || String(profile.destination_type || "").toLowerCase() !== "managed_local_file") {
      showToast("仅系统托管本地目标支持文件浏览")
      return
    }
    setDestinationFileDialog({
      profile,
      files: [],
      relative_path: "",
      absolute_path: "",
    })
    setDestinationFileLoading(true)
    try {
      const data = await apiFetch(`/api/v1/destinations/${profile.id}/files`)
      setDestinationFileDialog({
        profile,
        files: Array.isArray(data.files) ? data.files : [],
        relative_path: String(data.relative_path || ""),
        absolute_path: String(data.absolute_path || ""),
      })
    } catch (err) {
      showToast(`文件列表加载失败: ${err.message}`)
      setDestinationFileDialog(null)
    } finally {
      setDestinationFileLoading(false)
    }
  }

  async function handleDeleteDestinationProfile() {
    if (!destinationDeleteDialog?.profile?.id) return
    const profile = destinationDeleteDialog.profile
    setDestinationDeleting(true)
    try {
      await apiFetch(`/api/v1/destinations/${profile.id}?purge_files=${destinationDeleteDialog.purgeFiles ? "true" : "false"}`, {
        method: "DELETE",
      })
      showToast("目标已删除")
      setDestinationDeleteDialog(null)
      await loadDestinationProfiles()
    } catch (err) {
      showToast(`删除失败: ${err.message}`)
    } finally {
      setDestinationDeleting(false)
    }
  }

  async function handleSaveRetentionSettings() {
    setRetentionSaving(true)
    try {
      const payload = {
        enabled: !!retentionSettings.enabled,
        retention_days: Math.max(1, Math.min(3650, Number(retentionSettings.retention_days || 30))),
      }
      const data = await apiFetch("/api/v1/destinations/retention/settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })
      setRetentionSettings({
        enabled: !!data.enabled,
        retention_days: Number(data.retention_days || 30),
      })
      showToast("保留策略已更新")
    } catch (err) {
      showToast(`保留策略更新失败: ${err.message}`)
    } finally {
      setRetentionSaving(false)
    }
  }

  async function handleRunRetentionNow() {
    setRetentionRunning(true)
    try {
      const data = await apiFetch("/api/v1/destinations/retention/run", {
        method: "POST",
      })
      const deletedFiles = Number(data.deleted_files || 0)
      const deletedDirs = Number(data.deleted_dirs || 0)
      showToast(`清理完成：删除文件 ${deletedFiles} 个，空目录 ${deletedDirs} 个`)
    } catch (err) {
      showToast(`执行清理失败: ${err.message}`)
    } finally {
      setRetentionRunning(false)
    }
  }

  return (
    <div className="min-h-screen bg-[#F0EFEC] lg:grid lg:grid-cols-[260px_1fr]">
      <aside className="border-r border-slate-200 bg-[#F0EFEC]/80 px-4 py-6 text-slate-700 backdrop-blur-sm">
        <div className="mb-8 flex items-center gap-3 rounded-sm bg-[#F0EFEC] p-3">
          <div className="grid h-10 w-10 place-items-center overflow-hidden rounded-sm border border-slate-200 bg-[#F0EFEC]">
            <img src="/static/brand-logo.png" alt="Simon Logo" className="h-full w-full object-cover" />
          </div>
          <div>
            <p className="m-0 text-sm font-extrabold mono-ui">SimonOpenPlatfrom</p>
            <p className="m-0 text-xs text-slate-500 mono-ui">Management Console</p>
          </div>
        </div>

        <nav className="-mx-4 grid gap-0">
          {navItems.map((item) => {
            if (!item.children) {
              return (
                <button
                  key={item.key}
                  type="button"
                  onClick={() => gotoModule(item.key)}
                  className={`nav-item w-full rounded-none ${activeModule === item.key ? "active" : ""}`}
                >
                  {item.label}
                </button>
              )
            }

            return (
              <div key={item.key}>
                <button
                  type="button"
                  className={`nav-item nav-folder-trigger w-full rounded-none ${
                    item.key === "platform_and_app"
                      ? isPlatformFolderActive
                        ? "active"
                        : ""
                      : isApplicationFolderActive
                        ? "active"
                        : ""
                  }`}
                  onClick={() => {
                    if (item.key === "platform_and_app") {
                      setPlatformMenuOpen((v) => !v)
                    } else {
                      setApplicationMenuOpen((v) => !v)
                    }
                  }}
                >
                  <span>{item.label}</span>
                  <span
                    className={`nav-chevron ${
                      item.key === "platform_and_app"
                        ? platformMenuOpen
                          ? "open"
                          : ""
                        : applicationMenuOpen
                          ? "open"
                          : ""
                    }`}
                  >
                    ⌄
                  </span>
                </button>
                {(item.key === "platform_and_app" ? platformMenuOpen : applicationMenuOpen) && (
                  <div className="nav-submenu">
                    {item.children.map((child) => (
                      <button
                        key={child.key}
                        type="button"
                        onClick={() => gotoPath(child.path)}
                        className={`nav-subitem w-full ${activeModule === child.key ? "active" : ""}`}
                      >
                        {child.label}
                      </button>
                    ))}
                  </div>
                )}
              </div>
            )
          })}
        </nav>

        <div className="mt-8 rounded-sm bg-[#F0EFEC] p-3 text-xs">
          <p className="mb-2 text-slate-500 mono-ui">服务状态</p>
          <p className="inline-flex rounded-sm border border-slate-200 bg-[#F0EFEC] px-2 py-1 text-slate-700 mono-ui">{healthText}</p>
          <div className="mt-3">
            <a href="/docs" target="_blank" rel="noreferrer" className="text-[#0000E1] mono-ui hover:underline">
              打开 OpenAPI 文档
            </a>
          </div>
        </div>
      </aside>

      <main className={`p-6 md:p-8 ${activeModule === "application_credentials" ? "flex min-h-screen flex-col" : ""}`}>
        <header className="card mb-6 flex flex-col gap-2 p-4 md:flex-row md:items-center md:justify-between">
          <div className="flex flex-wrap items-center gap-2 text-xs text-slate-500">
            <span>SimonOpenPlatfrom</span>
            <span>/</span>
            <span className="font-bold text-slate-700">
              {currentMeta.title}
              {isConnectionCreateView ? " / 新建项目" : ""}
            </span>
          </div>
          <div className="flex flex-wrap gap-2 text-xs">
            <span className="rounded-sm border border-slate-300 bg-[#F0EFEC] px-2 py-1 text-slate-700 mono-ui">ENV: DEV</span>
            <span className="rounded-sm border border-slate-200 bg-[#F0EFEC] px-2 py-1 text-slate-700 mono-ui">登录账号: SimonOpenPlatfrom</span>
          </div>
        </header>

        {!isConnectionCreateView && activeModule !== "application_connection" && (
          <header className="mb-6 flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
            <div>
              <h1 className="m-0 text-2xl font-extrabold text-slate-900 mono-ui">{currentMeta.title}</h1>
              <p className="mt-2 text-sm text-slate-500">{currentMeta.desc}</p>
            </div>
            {activeModule !== "platform_management" &&
              activeModule !== "application_credentials" &&
              activeModule !== "application_connection" &&
              activeModule !== "application_transformation" &&
              activeModule !== "application_destination" &&
              activeModule !== "apihub" && (
              <div className="flex flex-wrap gap-2">
                <button className="btn-subtle" onClick={() => loadAccounts().catch((e) => showToast(e.message))}>刷新账号</button>
                <button className="btn-subtle" onClick={() => loadTasks().catch((e) => showToast(e.message))}>刷新任务</button>
                <button className="btn-brand" onClick={() => setAutoRefresh((v) => !v)}>{`自动刷新: ${autoRefresh ? "开" : "关"}`}</button>
              </div>
            )}
          </header>
        )}

        {activeModule === "dashboard" && (
          <section className="flex flex-1 min-h-0 flex-col space-y-3">
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              {statsWithTrend.map((card) => {
                const up = card.key === "success"
                const down = card.key === "failed"
                return (
                  <article key={card.key} className="card p-5">
                    <div className="flex items-start justify-between">
                      <p className="text-xs text-slate-500">{card.label}</p>
                      <span className="inline-flex h-6 min-w-6 items-center justify-center rounded-full bg-[#F0EFEC] px-2 text-xs text-slate-600">
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
                              : "bg-[#F0EFEC] text-slate-500"
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
                  <span className="rounded-full bg-[#F0EFEC] px-2 py-1 text-xs text-slate-600">24h</span>
                </div>
                <div className="h-24 rounded-sm border border-slate-300 bg-[#F0EFEC] p-2">
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
                  <button className="btn-brand" onClick={() => gotoModule("application_credentials")}>创建应用账号</button>
                  <button className="btn-ghost-brand" onClick={() => gotoModule("apihub")}>打开 Connector Builder</button>
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

        {activeModule === "apihub" && (
          <section className="space-y-3">
            <ConnectorBuilderPage embedded />
          </section>
        )}

        {activeModule === "application_connection" && (
          <section className="space-y-4">
            {isConnectionDetailView ? (
              <article className="space-y-4">
                <div className="space-y-4">
                  <div className="flex items-start justify-between">
                    <div>
                      <h3 className="m-0 text-[34px] font-bold leading-none text-slate-900">Connection Detail</h3>
                      {connectionDetailProject ? (
                        <div className="mt-3 flex flex-wrap items-center gap-2 text-sm">
                          {(() => {
                            const statusMeta = connectionStatusMeta(connectionDetailProject.status)
                            return (
                              <span
                                className={`inline-flex items-center gap-1 rounded-full px-3 py-1 font-medium ${
                                  statusMeta.tone === "active"
                                    ? "bg-emerald-50 text-emerald-700"
                                    : statusMeta.tone === "paused"
                                      ? "bg-[#F0EFEC] text-slate-700"
                                      : "bg-amber-50 text-amber-700"
                                }`}
                              >
                                {statusMeta.tone === "active" ? "🟢" : statusMeta.tone === "paused" ? "⏸️" : "⚠"}
                                {statusMeta.text}
                              </span>
                            )
                          })()}
                          <span className="inline-flex items-center gap-1 rounded-full bg-[#F0EFEC] px-3 py-1 font-medium text-slate-700 mono-ui">
                            Project ID: {connectionDetailId}
                          </span>
                          <span className="inline-flex items-center gap-1 rounded-full bg-[#F0EFEC] px-3 py-1 font-medium text-slate-700">
                            关联账号
                            {Array.isArray(connectionDetailProject.app_ids)
                              ? connectionDetailProject.app_ids.length
                              : (connectionDetailProject.app_id ? 1 : 0)}
                            个
                          </span>
                        </div>
                      ) : (
                        <p className="mt-2 text-sm text-slate-500 mono-ui">Project ID: {connectionDetailId}</p>
                      )}
                    </div>
                    <button className="btn-subtle" onClick={() => navigate("/application/connection")}>返回列表</button>
                  </div>
                </div>

                {connectionDetailLoading ? (
                  <div className="rounded-sm border border-slate-200 bg-[#F0EFEC] px-3 py-2 text-sm text-slate-500">加载详情中...</div>
                ) : connectionDetailError ? (
                  <div className="rounded-sm border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
                    加载失败：{connectionDetailError}
                  </div>
                ) : !connectionDetailProject ? (
                  <div className="rounded-sm border border-slate-200 bg-[#F0EFEC] px-3 py-2 text-sm text-slate-500">未找到该 Connection 项目。</div>
                ) : (
                  <>
                    <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                      <div className="rounded-sm border border-slate-200 bg-[#F0EFEC] p-3">
                        <p className="text-xs text-slate-500">Connection Name</p>
                        <p className="mt-1 text-sm font-semibold text-slate-900">{connectionDetailProject.name || "-"}</p>
                      </div>
                      <div className="rounded-sm border border-slate-200 bg-[#F0EFEC] p-3">
                        <p className="text-xs text-slate-500">Source Type</p>
                        <p className="mt-1 inline-flex items-center gap-2 text-sm font-semibold text-slate-900">
                          <span className="inline-flex h-5 w-5 items-center justify-center rounded bg-sky-100 text-[10px] font-semibold text-sky-700">
                            {connectionSourceIcon(connectionDetailProject.platform_code)}
                          </span>
                          {platformMappingLabel(connectionDetailProject.platform_code, platformLabelMap)}
                        </p>
                      </div>
                      <div className="rounded-sm border border-slate-200 bg-[#F0EFEC] p-3">
                        <p className="text-xs text-slate-500">Destination</p>
                        <p className="mt-1 inline-flex items-center gap-2 text-sm font-semibold text-slate-900">
                          <span className="inline-flex h-5 w-5 items-center justify-center rounded bg-indigo-100 text-[10px] font-bold text-indigo-700">
                            {connectionDestinationIcon(connectionDetailProject.destination)}
                          </span>
                          {connectionDetailProject.destination || "-"}
                        </p>
                      </div>
                      <div className="rounded-sm border border-slate-200 bg-[#F0EFEC] p-3">
                        <p className="text-xs text-slate-500">Schedule</p>
                        <p className="mt-1 text-sm font-semibold text-slate-900 mono-ui">{connectionDetailProject.schedule_cron || "-"}</p>
                      </div>
                    </div>

                    <div className="overflow-auto rounded-lg border border-slate-200 bg-[#F0EFEC] p-2">
                      <Table
                        rowKey={(row) => Number(row.id) || String(row.stream_name || "")}
                        dataSource={connectionDetailStreams}
                        columns={connectionDetailColumns}
                        pagination={false}
                        size="middle"
                        locale={{ emptyText: "当前 Connection 暂未配置 stream" }}
                      />
                    </div>
                  </>
                )}
              </article>
            ) : (
              <>
            <div className="space-y-4">
              <div className="flex items-start justify-between">
                <div>
                  <h3 className="m-0 text-[34px] font-bold leading-none text-slate-900">Connection</h3>
                  <div className="mt-3 flex flex-wrap items-center gap-2 text-sm">
                    <span className="inline-flex items-center gap-1 rounded-full bg-emerald-50 px-3 py-1 font-medium text-emerald-700">
                      🟢 {connectionSummary.active} Active
                    </span>
                    <span className="inline-flex items-center gap-1 rounded-full bg-[#F0EFEC] px-3 py-1 font-medium text-slate-700">
                      ⏸️ {connectionSummary.paused} Paused
                    </span>
                    {connectionSummary.warning > 0 && (
                      <span className="inline-flex items-center gap-1 rounded-full bg-amber-50 px-3 py-1 font-medium text-amber-700">
                        ⚠ {connectionSummary.warning} Warning
                      </span>
                    )}
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Button type="primary" onClick={openConnectionWizard}>+ 新建项目</Button>
                  <Dropdown menu={connectionHeaderMenu} trigger={["click"]}>
                    <Button>更多</Button>
                  </Dropdown>
                </div>
              </div>

              <div className="flex flex-wrap items-center gap-3">
                <Input
                  allowClear
                  value={connectionSearch}
                  onChange={(e) => setConnectionSearch(e.target.value)}
                  placeholder="Search by project name..."
                  className="max-w-xl"
                />
              </div>
            </div>

            <div className="overflow-auto rounded-lg border border-slate-200 bg-[#F0EFEC] p-2">
              <Table
                rowKey={(conn) => Number(conn.id) || String(conn.id)}
                dataSource={filteredConnections}
                loading={connectionsLoading}
                columns={connectionListColumns}
                rowSelection={connectionRowSelection}
                pagination={false}
                size="middle"
                locale={{ emptyText: "暂无连接，点击右上角创建新项目" }}
              />
            </div>
            <ConnectionWizardModal
              isOpen={isConnectionCreateView}
              mode={connectionDrawerMode}
              onClose={closeConnectionWizard}
            >
              {connectionDrawerMode === "setup" ? (
                    <div className="flex-1 overflow-y-auto px-6 py-6">
                      <div className="grid gap-3">
                      <label className="field-label">
                        项目名称
                        <Input className="mt-1" value={wizardTaskName} onChange={(e) => setWizardTaskName(e.target.value)} placeholder="例如：红书聚光_离线报表项目" />
                      </label>
                      <label className="field-label">
                        外部平台
                        <Select
                          className="mt-1"
                          value={wizardPlatform || undefined}
                          onChange={(value) => {
                            setWizardPlatform(value || "")
                            setWizardSelectedAppIds([])
                            setWizardTestResult(null)
                          }}
                          options={[
                            { value: "", label: "请选择平台" },
                            ...connectionPlatformOptions.map((item) => ({ value: item.value, label: item.label })),
                          ]}
                        />
                      </label>
                      <div className="field-label">
                        凭证 ID (app_id)
                        <div className="mt-1 rounded-xl bg-[#F0EFEC] p-3">
                          <Input
                            placeholder="搜索 app_id / name"
                            value={wizardCredentialSearch}
                            onChange={(e) => setWizardCredentialSearch(e.target.value)}
                            disabled={!wizardPlatform}
                          />
                          <div className="mt-3 flex items-center justify-between text-sm text-slate-600">
                            <Checkbox
                              checked={filteredWizardCredentialOptions.length > 0 && selectedFilteredCredentialCount === filteredWizardCredentialOptions.length}
                              indeterminate={selectedFilteredCredentialCount > 0 && selectedFilteredCredentialCount < filteredWizardCredentialOptions.length}
                              onChange={(e) => {
                                const checked = e.target.checked
                                const visibleIds = filteredWizardCredentialOptions.map((x) => x.appId)
                                setWizardSelectedAppIds((prev) => {
                                  if (checked) return Array.from(new Set([...prev, ...visibleIds]))
                                  const visible = new Set(visibleIds)
                                  return prev.filter((x) => !visible.has(x))
                                })
                                setWizardTestResult(null)
                              }}
                              disabled={!wizardPlatform || filteredWizardCredentialOptions.length === 0}
                            >
                              {wizardCredentialSearch.trim()
                                ? `全选搜索结果 (${filteredWizardCredentialOptions.length} 项)`
                                : `全选 (共 ${filteredWizardCredentialOptions.length} 项)`}
                            </Checkbox>
                            {selectedFilteredCredentialCount > 0 && (
                              <div className="inline-flex items-center gap-3">
                                <span className="text-xs text-[#0000E1]">已选 {selectedFilteredCredentialCount} 项</span>
                                <Button
                                  type="text"
                                  size="small"
                                  danger
                                  onClick={() => {
                                    const visible = new Set(filteredWizardCredentialOptions.map((x) => x.appId))
                                    setWizardSelectedAppIds((prev) => prev.filter((x) => !visible.has(x)))
                                    setWizardTestResult(null)
                                  }}
                                >
                                  清空已选
                                </Button>
                              </div>
                            )}
                          </div>
                          <div className="mt-2 max-h-[180px] space-y-1 overflow-auto rounded-lg border border-slate-100 bg-[#F0EFEC] p-2">
                            {filteredWizardCredentialOptions.map((item) => (
                              <div key={item.appId} className="rounded-md px-1 py-1 hover:bg-[#E7E6E2]">
                                <Checkbox
                                  checked={wizardSelectedAppIds.includes(item.appId)}
                                  onChange={(e) => {
                                    const checked = e.target.checked
                                    setWizardSelectedAppIds((prev) => {
                                      if (checked) return prev.includes(item.appId) ? prev : [...prev, item.appId]
                                      return prev.filter((x) => x !== item.appId)
                                    })
                                    setWizardTestResult(null)
                                  }}
                                >
                                  <span className="mono-ui text-sm">{item.label}</span>
                                </Checkbox>
                              </div>
                            ))}
                            {filteredWizardCredentialOptions.length === 0 && (
                              <p className="m-0 text-xs text-slate-400">{wizardPlatform ? "当前搜索无匹配凭证" : "请先选择平台"}</p>
                            )}
                          </div>
                        </div>
                      </div>
                      <label className="field-label">
                        写入目标
                        <Select
                          className="mt-1"
                          value={wizardDestination}
                          onChange={setWizardDestination}
                          options={
                            destinationProfiles.length > 0
                              ? destinationProfiles.map((item) => ({ value: item.name, label: item.name }))
                              : [
                                { value: "ClickHouse_DW", label: "ClickHouse_DW" },
                                { value: "PostgreSQL_DW", label: "PostgreSQL_DW" },
                              ]
                          }
                        />
                      </label>
                      <div className="mt-3">
                        <div className="mb-2 flex items-center justify-between">
                          <label className="block text-sm font-medium text-slate-800">同步频率 (Schedule)</label>
                          <Segmented
                            size="small"
                            value={wizardScheduleMode}
                            onChange={setWizardScheduleMode}
                            options={[
                              { label: "基础配置", value: "basic" },
                              { label: "高级(Cron)", value: "advanced" },
                            ]}
                          />
                        </div>

                        <div className="mb-2 flex flex-wrap items-center gap-1 rounded-lg bg-[#F0EFEC] p-1">
                          <Button type="default" size="small" onClick={() => applySchedulePreset("daily_2am")}>
                            每天凌晨 2 点
                          </Button>
                          <Button type="default" size="small" onClick={() => applySchedulePreset("hourly_1")}>
                            每 1 小时
                          </Button>
                          <Button type="default" size="small" onClick={() => applySchedulePreset("hourly_6")}>
                            每 6 小时
                          </Button>
                          <Button type="default" size="small" onClick={() => setWizardScheduleMode("advanced")}>
                            自定义...
                          </Button>
                        </div>

                        {wizardScheduleMode === "basic" ? (
                          <div className="rounded-xl border border-slate-200/80 bg-[#F0EFEC] p-4 shadow-sm">
                            <p className="mb-2 text-xs font-medium tracking-wide text-slate-500">运行间隔</p>
                            <div className="overflow-x-auto pb-1">
                              <div className="inline-flex min-w-max items-center gap-2.5 whitespace-nowrap text-[14px] text-slate-600">
                              <span className="whitespace-nowrap text-slate-500">每</span>

                              <Select
                                size="small"
                                value={wizardFrequencyType}
                                onChange={setWizardFrequencyType}
                                options={[
                                  { value: "daily", label: "天 (Daily)" },
                                  { value: "hourly", label: "小时 (Hourly)" },
                                  { value: "weekly", label: "周 (Weekly)" },
                                ]}
                                style={{ width: 128 }}
                              />

                              {wizardFrequencyType === "hourly" && (
                                <Select
                                  size="small"
                                  value={wizardHourlyInterval}
                                  onChange={(value) => {
                                    setWizardHourlyMode("interval")
                                    setWizardHourlyInterval(value)
                                  }}
                                  options={[
                                    { value: "1", label: "1 小时" },
                                    { value: "2", label: "2 小时" },
                                    { value: "4", label: "4 小时" },
                                    { value: "6", label: "6 小时" },
                                    { value: "12", label: "12 小时" },
                                  ]}
                                  style={{ width: 110 }}
                                />
                              )}

                              {wizardFrequencyType === "daily" && (
                                <>
                                  <span className="whitespace-nowrap text-slate-500">的</span>
                                  <Select
                                    size="small"
                                    value={wizardDailyTime}
                                    onChange={setWizardDailyTime}
                                    options={scheduleTimeOptions.map((item) => ({ value: item, label: item }))}
                                    style={{ width: 110 }}
                                  />
                                </>
                              )}

                              {wizardFrequencyType === "weekly" && (
                                <>
                                  <span className="whitespace-nowrap text-slate-500">的</span>
                                  <Select
                                    size="small"
                                    value={wizardWeeklyDays[0] || "MON"}
                                    onChange={(value) => setWizardWeeklyDays([value])}
                                    options={[
                                      { value: "MON", label: "周一" },
                                      { value: "TUE", label: "周二" },
                                      { value: "WED", label: "周三" },
                                      { value: "THU", label: "周四" },
                                      { value: "FRI", label: "周五" },
                                      { value: "SAT", label: "周六" },
                                      { value: "SUN", label: "周日" },
                                    ]}
                                    style={{ width: 100 }}
                                  />
                                  <Select
                                    size="small"
                                    value={wizardWeeklyTime}
                                    onChange={setWizardWeeklyTime}
                                    options={scheduleTimeOptions.map((item) => ({ value: item, label: item }))}
                                    style={{ width: 110 }}
                                  />
                                </>
                              )}

                              <span className="whitespace-nowrap text-slate-500">执行</span>
                              </div>
                            </div>

                            <div className="mt-4 flex flex-wrap items-center gap-2 border-t border-slate-100 pt-3 text-sm text-slate-500">
                              <span>Cron:</span>
                              <code className="rounded-md border border-slate-200/70 bg-[#F0EFEC] px-1.5 py-0.5 font-mono text-xs text-slate-700">{wizardScheduleCron}</code>
                              <span className="text-slate-300">·</span>
                              <span>下次运行: <span className="font-medium text-slate-600">{estimatedNextRunText}</span></span>
                            </div>
                          </div>
                        ) : (
                          <div className="rounded-lg border border-slate-200 bg-[#F0EFEC]/50 p-4">
                            <label className="field-label">
                              Cron 表达式
                              <Input
                                className="mt-1 mono-ui"
                                value={wizardScheduleCron}
                                onChange={(e) => setWizardScheduleCron(e.target.value)}
                                placeholder="例如：0 2 * * *"
                              />
                            </label>
                          </div>
                        )}
                      </div>

                      <div className="sticky bottom-0 z-10 -mx-6 mt-3 border-t border-slate-100 bg-[#F0EFEC]/95 px-6 py-4 backdrop-blur">
                        <div className="flex items-center justify-between gap-3">
                          <div className="min-h-4 text-xs">
                            {wizardTestResult ? (
                              <span className={wizardTestResult.success ? "text-emerald-600" : "text-rose-600"}>{wizardTestResult.message}</span>
                            ) : null}
                          </div>
                          <div className="flex items-center gap-2">
                            <Button onClick={handleWizardTestConnection} loading={wizardTestLoading} disabled={!wizardPlatform || wizardSelectedAppIds.length === 0}>
                              {wizardTestLoading ? "测试中..." : "测试连接"}
                            </Button>
                            <Button
                              type="primary"
                              onClick={saveConnectionProjectSetup}
                              loading={wizardConnectionSaving}
                            >
                              {wizardConnectionSaving ? "保存中..." : "保存项目"}
                            </Button>
                          </div>
                        </div>
                      </div>
                      </div>
                    </div>
                  ) : (
                    <div className="flex-1 overflow-y-auto px-6 py-6">
                      <div className="mb-4 flex items-start gap-3 rounded-lg border border-indigo-100 bg-indigo-50 px-4 py-3">
                        <svg className="mt-0.5 h-5 w-5 shrink-0 text-[#4F46E5]" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                        </svg>
                        <div className="text-sm leading-relaxed text-indigo-900">
                          <strong className="mb-0.5 block font-semibold">项目级全局配置</strong>
                          当前配置的接口规则将统一应用于该项目下的
                          <span className="mx-1 rounded border border-indigo-200 bg-[#F0EFEC] px-1.5 py-0.5 font-semibold shadow-sm">{wizardSelectedAppIds.length}</span>
                          个授权账号，底层引擎会自动扇出并发执行。
                        </div>
                      </div>

                      <div className="mb-4 rounded-xl border border-slate-200 bg-[#F0EFEC] p-3">
                        <div className="mb-2 flex items-center justify-between">
                          <p className="m-0 text-sm font-semibold text-slate-800">授权账号 (app_id)</p>
                          <span className="text-xs text-slate-500">已选 {wizardSelectedAppIds.length} 项</span>
                        </div>
                        <Input
                          placeholder="搜索 app_id / name"
                          value={wizardCredentialSearch}
                          onChange={(e) => setWizardCredentialSearch(e.target.value)}
                          disabled={!wizardPlatform}
                        />
                        <div className="mt-3 flex items-center justify-between text-sm text-slate-600">
                          <Checkbox
                            checked={filteredWizardCredentialOptions.length > 0 && selectedFilteredCredentialCount === filteredWizardCredentialOptions.length}
                            indeterminate={selectedFilteredCredentialCount > 0 && selectedFilteredCredentialCount < filteredWizardCredentialOptions.length}
                            onChange={(e) => {
                              const checked = e.target.checked
                              const visibleIds = filteredWizardCredentialOptions.map((x) => x.appId)
                              setWizardSelectedAppIds((prev) => {
                                if (checked) return Array.from(new Set([...prev, ...visibleIds]))
                                const visible = new Set(visibleIds)
                                return prev.filter((x) => !visible.has(x))
                              })
                            }}
                            disabled={!wizardPlatform || filteredWizardCredentialOptions.length === 0}
                          >
                            {wizardCredentialSearch.trim()
                              ? `全选搜索结果 (${filteredWizardCredentialOptions.length} 项)`
                              : `全选 (共 ${filteredWizardCredentialOptions.length} 项)`}
                          </Checkbox>
                        </div>
                        <div className="mt-2 max-h-[180px] space-y-1 overflow-auto rounded-lg border border-slate-100 bg-[#F0EFEC] p-2">
                          {filteredWizardCredentialOptions.map((item) => (
                            <div key={item.appId} className="rounded-md px-1 py-1 hover:bg-[#E7E6E2]">
                              <Checkbox
                                checked={wizardSelectedAppIds.includes(item.appId)}
                                onChange={(e) => {
                                  const checked = e.target.checked
                                  setWizardSelectedAppIds((prev) => {
                                    if (checked) return prev.includes(item.appId) ? prev : [...prev, item.appId]
                                    return prev.filter((x) => x !== item.appId)
                                  })
                                }}
                              >
                                <span className="mono-ui text-sm">{item.label}</span>
                              </Checkbox>
                            </div>
                          ))}
                          {filteredWizardCredentialOptions.length === 0 && (
                            <p className="m-0 text-xs text-slate-400">{wizardPlatform ? "当前平台暂无可选凭证" : "请先选择平台"}</p>
                          )}
                        </div>
                      </div>

                      <div className="mb-3 space-y-2">
                        <Input
                          value={wizardSearch}
                          onChange={(e) => setWizardSearch(e.target.value)}
                          placeholder="搜索接口名称或描述..."
                        />
                        <div className="flex flex-wrap items-center gap-2">
                          <Segmented
                            size="small"
                            value={wizardQuickFilter}
                            onChange={setWizardQuickFilter}
                            options={[
                              { value: "all", label: `全部 (${wizardStreamCards.length})` },
                              { value: "checked", label: `已开启 (${wizardCheckedStreamCount})` },
                            ]}
                          />
                        </div>
                      </div>

                      <div className="pb-24">
                        <div className="overflow-hidden rounded-xl border border-slate-200 bg-[#F0EFEC] shadow-sm">
                          <div className="flex items-center justify-between border-b border-slate-100 bg-[#F0EFEC]/70 px-5 py-4">
                            <div>
                              <h3 className="text-sm font-semibold text-slate-800">选择要同步的接口 (Streams)</h3>
                              <p className="mt-1 text-xs text-slate-500">开启后，系统会全量抓取接口返回的所有字段，原始 JSON 写入 `_raw_data`。</p>
                            </div>
                            <div className="rounded border border-slate-200 bg-[#F0EFEC] px-2.5 py-1 text-xs font-medium text-slate-600">
                              已开启 {wizardCheckedStreamCount} / {wizardStreamCards.length}
                            </div>
                          </div>

                          <div className="divide-y divide-slate-100">
                            {wizardSchemaLoading && (
                              <div className="px-5 py-4 text-xs text-slate-500">正在拉取接口列表...</div>
                            )}

                            {!wizardSchemaLoading && wizardCardsByQuickFilter.map((stream) => {
                              const checked = wizardCheckedLeafIds.includes(stream.id)
                              const mode = wizardLeafSyncMode[stream.id] || "incremental"
                              return (
                                <div
                                  key={stream.id}
                                  className={`flex items-center justify-between gap-4 p-5 transition-colors ${
                                    checked ? "bg-indigo-50/20" : "hover:bg-[#E7E6E2]/60"
                                  }`}
                                >
                                  <div className="flex min-w-0 items-start gap-4">
                                    <Switch checked={checked} onChange={(next) => toggleWizardLeaf(stream.id, next)} />

                                    <div className="min-w-0">
                                      <p className="text-sm font-semibold text-slate-800">
                                        {stream.label}
                                        <span className="ml-1 font-mono text-xs font-normal text-slate-400">{stream.id}</span>
                                      </p>
                                      <p className="mt-0.5 text-xs text-slate-500">{stream.description || "同步该接口返回的业务数据。"}</p>
                                    </div>
                                  </div>

                                  {checked ? (
                                    <div className="flex items-center gap-3">
                                      <span className="text-xs font-medium text-slate-400">同步模式:</span>
                                      <Select
                                        size="small"
                                        value={mode}
                                        onChange={(value) => setWizardLeafSyncMode((prev) => ({ ...prev, [stream.id]: value }))}
                                        options={[
                                          { value: "incremental", label: "增量同步 (Incremental)" },
                                          { value: "full_refresh", label: "全量覆盖 (Full Refresh)" },
                                        ]}
                                        style={{ width: 190 }}
                                      />
                                    </div>
                                  ) : (
                                    <div className="text-xs text-slate-400">未开启</div>
                                  )}
                                </div>
                              )
                            })}

                            {!wizardSchemaLoading && wizardCardsByQuickFilter.length === 0 && (
                              <div className="px-5 py-6 text-center text-sm text-slate-500">当前筛选条件下没有接口流</div>
                            )}
                          </div>
                        </div>
                      </div>

                      <div className="sticky bottom-0 z-10 -mx-6 border-t border-slate-100 bg-[#F0EFEC]/95 px-6 py-3 backdrop-blur">
                        <div className="flex items-center justify-between gap-3">
                          <div className="text-xs text-slate-500">
                            已选 <span className="font-semibold text-slate-700">{wizardSelectedAppIds.length}</span> 个授权账号，
                            已开启 <span className="font-semibold text-slate-700">{wizardCheckedStreamCount}</span> 个接口流。
                          </div>
                          <Button
                            type="primary"
                            onClick={handleSaveWorkspaceConfiguration}
                            disabled={wizardWorkspaceSaveDisabled}
                            loading={wizardConnectionSaving}
                          >
                            {wizardConnectionSaving ? "保存中..." : "保存项目"}
                          </Button>
                        </div>
                      </div>
                    </div>
                  )}
            </ConnectionWizardModal>
            </>
            )}
          </section>
        )}


        {activeModule === "application_transformation" && (
          <section className="section-block">
            <h3 className="panel-title mb-3">Transformation（dbt 预留）</h3>
            <p className="mb-4 text-sm text-slate-500">
              未来将支持绑定 dbt 项目仓库，在控制台触发 <code>dbt run</code> / <code>dbt test</code> 并查看运行结果。
            </p>
            <div className="mb-4 rounded-sm border border-[#0000E1]/20 bg-[#0000E1]/5 px-3 py-2 text-xs text-slate-700">
              数据加工边界：字段重命名、脱敏、类型强转、业务过滤等仅在 Transformation 执行，Connection 不承担数据清洗职责。
            </div>
            <div className="grid gap-3 md:grid-cols-3">
              <button className="btn-subtle" disabled>
                绑定 Git 仓库（建设中）
              </button>
              <button className="btn-subtle" disabled>
                触发 dbt run（建设中）
              </button>
              <button className="btn-subtle" disabled>
                触发 dbt test（建设中）
              </button>
            </div>
          </section>
        )}

        {activeModule === "application_destination" && (
          <section className="section-block space-y-3">
            <div className="flex items-center gap-2 border-b border-slate-200 pb-2">
              <button
                className={`btn-subtle ${destinationTab === "target_setup" ? "border-[#0000E1] text-[#0000E1]" : ""}`}
                onClick={() => setDestinationTab("target_setup")}
              >
                目标配置
              </button>
              <button
                className={`btn-subtle ${destinationTab === "synced_tables" ? "border-[#0000E1] text-[#0000E1]" : ""}`}
                onClick={() => setDestinationTab("synced_tables")}
              >
                已同步数据表
              </button>
            </div>

            {destinationTab === "target_setup" && (
              <div className="space-y-4">
                <div className="grid gap-3 md:grid-cols-3">
                  <button
                    className={`rounded-sm border px-4 py-3 text-left transition ${
                      destinationForm.engine_category === "database"
                        ? "border-[#0000E1] bg-[#0000E1]/5 text-[#0000E1]"
                        : "border-slate-200 bg-[#F0EFEC] text-slate-700 hover:border-slate-300"
                    }`}
                    onClick={() => setDestinationForm((p) => ({ ...p, engine_category: "database" }))}
                  >
                    <div className="text-sm font-semibold">🗄 Database</div>
                    <div className="mt-1 text-xs text-slate-500">MySQL / PostgreSQL / ClickHouse</div>
                  </button>
                  <button
                    className={`rounded-sm border px-4 py-3 text-left transition ${
                      destinationForm.engine_category === "file"
                        ? "border-[#0000E1] bg-[#0000E1]/5 text-[#0000E1]"
                        : "border-slate-200 bg-[#F0EFEC] text-slate-700 hover:border-slate-300"
                    }`}
                    onClick={() => setDestinationForm((p) => ({ ...p, engine_category: "file" }))}
                  >
                    <div className="text-sm font-semibold">📁 Cloud / File</div>
                    <div className="mt-1 text-xs text-slate-500">OSS / S3 / COS / SFTP + CSV/JSON</div>
                  </button>
                  <button
                    className={`rounded-sm border px-4 py-3 text-left transition ${
                      destinationForm.engine_category === "local"
                        ? "border-[#0000E1] bg-[#0000E1]/5 text-[#0000E1]"
                        : "border-slate-200 bg-[#F0EFEC] text-slate-700 hover:border-slate-300"
                    }`}
                    onClick={() => setDestinationForm((p) => ({ ...p, engine_category: "local" }))}
                  >
                    <div className="text-sm font-semibold">🖴 Local File</div>
                    <div className="mt-1 text-xs text-slate-500">Host Path / JSON / JSONL</div>
                  </button>
                </div>

                {destinationForm.engine_category === "database" ? (
                  <div className="grid gap-3 md:grid-cols-2">
                    <label className="field-label md:col-span-2">
                      目标名称
                      <input
                        className="input-base mt-1"
                        value={destinationForm.profile_name}
                        onChange={(e) => setDestinationForm((p) => ({ ...p, profile_name: e.target.value }))}
                        placeholder="例如：ClickHouse_DW / OSS_Raw_Lake"
                      />
                    </label>
                    <label className="field-label">
                      数据库类型
                      <select
                        className="input-base mt-1"
                        value={destinationForm.database_type}
                        onChange={(e) => setDestinationForm((p) => ({ ...p, database_type: e.target.value }))}
                      >
                        <option>PostgreSQL</option>
                        <option>ClickHouse</option>
                        <option>MySQL</option>
                      </select>
                    </label>
                    <label className="field-label">
                      Host
                      <input className="input-base mt-1" value={destinationForm.host} onChange={(e) => setDestinationForm((p) => ({ ...p, host: e.target.value }))} />
                    </label>
                    <label className="field-label">
                      Port
                      <input className="input-base mt-1" value={destinationForm.port} onChange={(e) => setDestinationForm((p) => ({ ...p, port: e.target.value }))} />
                    </label>
                    <label className="field-label">
                      User
                      <input className="input-base mt-1" value={destinationForm.user} onChange={(e) => setDestinationForm((p) => ({ ...p, user: e.target.value }))} />
                    </label>
                    <label className="field-label">
                      Password
                      <input type="password" className="input-base mt-1" value={destinationForm.password} onChange={(e) => setDestinationForm((p) => ({ ...p, password: e.target.value }))} />
                    </label>
                    <label className="field-label">
                      Database Name
                      <input className="input-base mt-1" value={destinationForm.database} onChange={(e) => setDestinationForm((p) => ({ ...p, database: e.target.value }))} />
                    </label>
                  </div>
                ) : destinationForm.engine_category === "file" ? (
                  <div className="space-y-4">
                    <div className="rounded-sm border border-slate-200 bg-[#F0EFEC] p-3">
                      <h4 className="m-0 text-sm font-semibold text-slate-800">存储鉴权 (Storage Credentials)</h4>
                      <div className="mt-3 grid gap-3 md:grid-cols-2">
                        <label className="field-label md:col-span-2">
                          目标名称
                          <input
                            className="input-base mt-1"
                            value={destinationForm.profile_name}
                            onChange={(e) => setDestinationForm((p) => ({ ...p, profile_name: e.target.value }))}
                            placeholder="例如：OSS_Raw_Lake"
                          />
                        </label>
                        <label className="field-label">
                          存储类型
                          <select
                            className="input-base mt-1"
                            value={destinationForm.storage_provider}
                            onChange={(e) => setDestinationForm((p) => ({ ...p, storage_provider: e.target.value }))}
                          >
                            <option value="aliyun_oss">阿里云 OSS</option>
                            <option value="aws_s3">AWS S3</option>
                            <option value="tencent_cos">腾讯云 COS</option>
                            <option value="sftp">SFTP</option>
                          </select>
                        </label>
                        <label className="field-label">
                          Bucket / Path
                          <input className="input-base mt-1" value={destinationForm.bucket_name} onChange={(e) => setDestinationForm((p) => ({ ...p, bucket_name: e.target.value }))} />
                        </label>
                        <label className="field-label">
                          Endpoint
                          <input className="input-base mt-1" value={destinationForm.endpoint} onChange={(e) => setDestinationForm((p) => ({ ...p, endpoint: e.target.value }))} />
                        </label>
                        <label className="field-label">
                          Region
                          <input className="input-base mt-1" value={destinationForm.region} onChange={(e) => setDestinationForm((p) => ({ ...p, region: e.target.value }))} />
                        </label>
                        <label className="field-label">
                          AccessKey ID
                          <input className="input-base mt-1" value={destinationForm.access_key_id} onChange={(e) => setDestinationForm((p) => ({ ...p, access_key_id: e.target.value }))} />
                        </label>
                        <label className="field-label">
                          AccessKey Secret
                          <input type="password" className="input-base mt-1" value={destinationForm.access_key_secret} onChange={(e) => setDestinationForm((p) => ({ ...p, access_key_secret: e.target.value }))} />
                        </label>
                      </div>
                    </div>

                    <div className="rounded-sm border border-slate-200 bg-[#F0EFEC] p-3">
                      <h4 className="m-0 text-sm font-semibold text-slate-800">文件格式设置 (File Format)</h4>
                      <div className="mt-3 grid gap-3 md:grid-cols-2">
                        <label className="field-label">
                          Format
                          <select className="input-base mt-1" value={destinationForm.file_format} onChange={(e) => setDestinationForm((p) => ({ ...p, file_format: e.target.value }))}>
                            <option value="CSV">CSV</option>
                            <option value="JSON">JSON</option>
                            <option value="PARQUET">Parquet (Preview)</option>
                          </select>
                        </label>
                        <label className="field-label">
                          Compression
                          <select className="input-base mt-1" value={destinationForm.compression} onChange={(e) => setDestinationForm((p) => ({ ...p, compression: e.target.value }))}>
                            <option value="NONE">None</option>
                            <option value="GZIP">GZIP</option>
                          </select>
                        </label>
                        <label className="field-label">
                          路径前缀
                          <input className="input-base mt-1 mono-ui" value={destinationForm.path_prefix} onChange={(e) => setDestinationForm((p) => ({ ...p, path_prefix: e.target.value }))} />
                        </label>
                        <label className="field-label">
                          分块行数阈值
                          <input className="input-base mt-1" value={destinationForm.chunk_rows} onChange={(e) => setDestinationForm((p) => ({ ...p, chunk_rows: e.target.value }))} />
                        </label>
                        <label className="field-label">
                          分块大小阈值 (MB)
                          <input className="input-base mt-1" value={destinationForm.chunk_size_mb} onChange={(e) => setDestinationForm((p) => ({ ...p, chunk_size_mb: e.target.value }))} />
                        </label>
                      </div>

                      {destinationForm.file_format === "CSV" && (
                        <div className="mt-3 grid gap-3 md:grid-cols-2">
                          <label className="field-label flex items-center justify-between gap-3 rounded-sm border border-slate-200 bg-[#F0EFEC] px-3 py-2">
                            <span>Include Header</span>
                            <input
                              type="checkbox"
                              checked={!!destinationForm.include_header}
                              onChange={(e) => setDestinationForm((p) => ({ ...p, include_header: e.target.checked }))}
                            />
                          </label>
                          <label className="field-label">
                            Delimiter
                            <select className="input-base mt-1" value={destinationForm.delimiter} onChange={(e) => setDestinationForm((p) => ({ ...p, delimiter: e.target.value }))}>
                              <option value=",">, (Comma)</option>
                              <option value="\t">\\t (Tab)</option>
                              <option value="|">| (Pipe)</option>
                            </select>
                          </label>
                        </div>
                      )}
                    </div>
                  </div>
                ) : (
                  <div className="space-y-4">
                    <div className="rounded-sm border border-slate-200 bg-[#F0EFEC] p-3">
                      <h4 className="m-0 text-sm font-semibold text-slate-800">系统托管文件配置 (Managed Local File)</h4>
                      <div className="mt-3 grid gap-3 md:grid-cols-2">
                        <label className="field-label md:col-span-2">
                          目标名称
                          <input
                            className="input-base mt-1"
                            value={destinationForm.profile_name}
                            onChange={(e) => setDestinationForm((p) => ({ ...p, profile_name: e.target.value }))}
                            placeholder="例如：本地测试 JSON 库"
                          />
                        </label>
                        <div className="field-label md:col-span-2">
                          存储位置 (只读)
                          <div className="mt-1 flex items-center justify-between rounded-lg bg-[#F0EFEC] px-3 py-2">
                            <span className="mono-ui text-xs text-slate-700">
                              {`storage/destinations/${slugifyName(destinationForm.profile_name || "default_destination")}/`}
                            </span>
                            <button
                              type="button"
                              className="rounded-md px-2 py-1 text-xs text-slate-500 transition hover:bg-[#E1DFDB] hover:text-[#0000E1]"
                              onClick={async () => {
                                const text = `storage/destinations/${slugifyName(destinationForm.profile_name || "default_destination")}/`
                                await navigator.clipboard.writeText(text)
                                showToast("已复制存储路径")
                              }}
                            >
                              📄 复制
                            </button>
                          </div>
                        </div>
                        <div className="md:col-span-2 rounded-r-xl border-l-4 border-[#0000E1] bg-[#0000E1]/5 px-3 py-2 text-xs text-slate-700">
                          💡 系统会统一将文件写入内部 storage 根目录（由 `SIMON_DATA_STORAGE_PATH` 控制），无需手动填写绝对路径。
                        </div>
                        <label className="field-label">
                          文件格式 (Format)
                          <select
                            className="input-base mt-1"
                            value={destinationForm.local_format}
                            onChange={(e) => setDestinationForm((p) => ({ ...p, local_format: e.target.value }))}
                          >
                            <option value="JSONL">JSONL (Recommended)</option>
                            <option value="CSV">CSV</option>
                          </select>
                        </label>
                        <div className="rounded-sm border border-slate-200 bg-[#F0EFEC] px-3 py-2 text-xs text-slate-600">
                          推荐选择 JSONL：支持 append 写入，适合增量同步与大文件场景。
                        </div>
                      </div>
                    </div>
                  </div>
                )}

                <div className="rounded-sm border border-slate-200 bg-[#F0EFEC] p-3">
                  <p className="m-0 text-xs text-slate-600">推荐写入路径规范：</p>
                  <p className="mt-1 text-xs text-slate-700 mono-ui break-all">
                    /{`{root_path}`}/{`{项目名称}`}/{`{接口表名}`}/YYYY/MM/DD/{`YYYYMMDD_HHMMSS`}_batch_{`N`}.{destinationForm.engine_category === "local" ? String(destinationForm.local_format || "jsonl").toLowerCase() : destinationForm.engine_category === "file" ? String(destinationForm.file_format || "csv").toLowerCase() : "bin"}
                  </p>
                </div>

                <div className="flex items-center gap-2">
                  <button className="btn-subtle" onClick={handleTestDestinationProfile} disabled={destinationTestLoading}>
                    {destinationTestLoading ? "测试中..." : "测试连接"}
                  </button>
                  <button className="btn-brand" onClick={handleSaveDestinationProfile}>
                    保存配置
                  </button>
                </div>
                {destinationTestResult && (
                  <div className={`rounded-sm px-3 py-2 text-xs ${destinationTestResult.success ? "bg-emerald-50 text-emerald-700" : "bg-rose-50 text-rose-700"}`}>
                    {destinationTestResult.message}
                    {destinationTestResult.normalized_path ? ` (${destinationTestResult.normalized_path})` : ""}
                  </div>
                )}

                <div className="rounded-sm border border-slate-200 bg-[#F0EFEC] p-3">
                  <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <h4 className="m-0 text-sm font-semibold text-slate-800">Retention Policy</h4>
                      <p className="mt-1 text-xs text-slate-500">自动清理 Destination 中超过指定天数的历史文件（全局生效）。</p>
                    </div>
                    <button
                      className="btn-subtle px-2 py-1 text-xs"
                      onClick={() => loadStorageRetentionSettings().catch((err) => showToast(err.message))}
                      disabled={retentionSaving || retentionRunning}
                    >
                      刷新
                    </button>
                  </div>
                  <div className="grid gap-3 md:grid-cols-[minmax(0,1fr)_220px]">
                    <label className="field-label">
                      开启自动清理
                      <div className="mt-2 flex items-center gap-2">
                        <input
                          type="checkbox"
                          checked={!!retentionSettings.enabled}
                          onChange={(e) =>
                            setRetentionSettings((prev) => ({
                              ...prev,
                              enabled: e.target.checked,
                            }))
                          }
                        />
                        <span className="text-sm text-slate-700">{retentionSettings.enabled ? "已启用" : "未启用"}</span>
                      </div>
                    </label>
                    <label className="field-label">
                      保留天数
                      <input
                        type="number"
                        min="1"
                        max="3650"
                        className="input-base mt-1"
                        value={retentionSettings.retention_days}
                        onChange={(e) =>
                          setRetentionSettings((prev) => ({
                            ...prev,
                            retention_days: e.target.value,
                          }))
                        }
                      />
                    </label>
                  </div>
                  <div className="mt-3 flex flex-wrap items-center gap-2">
                    <button className="btn-brand" onClick={handleSaveRetentionSettings} disabled={retentionSaving}>
                      {retentionSaving ? "保存中..." : "保存策略"}
                    </button>
                    <button className="btn-subtle" onClick={handleRunRetentionNow} disabled={retentionRunning}>
                      {retentionRunning ? "清理中..." : "立即执行一次清理"}
                    </button>
                  </div>
                </div>

                <div className="rounded-sm border border-slate-200 bg-[#F0EFEC] p-3">
                  <div className="mb-2 flex items-center justify-between">
                    <h4 className="m-0 text-sm font-semibold text-slate-800">已创建目标</h4>
                    <button className="btn-subtle px-2 py-1 text-xs" onClick={() => loadDestinationProfiles().catch((err) => showToast(err.message))}>刷新</button>
                  </div>
                  <div className="overflow-auto">
                    <table className="w-full min-w-[720px] border-collapse text-left">
                      <thead>
                        <tr>
                          <th className="border-b border-slate-200 px-3 py-3 text-xs font-semibold uppercase tracking-wider text-slate-400">名称</th>
                          <th className="border-b border-slate-200 px-3 py-3 text-xs font-semibold uppercase tracking-wider text-slate-400">类型</th>
                          <th className="border-b border-slate-200 px-3 py-3 text-xs font-semibold uppercase tracking-wider text-slate-400">状态</th>
                          <th className="border-b border-slate-200 px-3 py-3 text-xs font-semibold uppercase tracking-wider text-slate-400">操作</th>
                        </tr>
                      </thead>
                      <tbody>
                        {destinationProfiles.map((item) => (
                          <tr key={item.id} className="border-b border-slate-100 transition-colors hover:bg-[#E7E6E2]/70">
                            <td className="px-3 py-3 text-sm font-medium text-slate-700">{item.name}</td>
                            <td className="px-3 py-3 text-sm text-slate-600">{destinationTypeLabel(item)}</td>
                            <td className="px-3 py-3">
                              <span className={`inline-flex rounded-full px-2.5 py-1 text-xs font-semibold ${String(item.status || "").toLowerCase() === "active" ? "bg-emerald-50 text-emerald-700" : "bg-[#F0EFEC] text-slate-600"}`}>
                                {String(item.status || "-").toUpperCase()}
                              </span>
                            </td>
                            <td className="px-3 py-3">
                              <div className="inline-flex items-center gap-3">
                                {String(item.destination_type || "").toLowerCase() === "managed_local_file" ? (
                                  <button className="text-xs font-medium text-[#0000E1] hover:opacity-70" onClick={() => openManagedDestinationFiles(item)}>
                                    📂 查看已同步文件
                                  </button>
                                ) : (
                                  <span className="text-xs text-slate-400">-</span>
                                )}
                                <button
                                  className="text-xs font-medium text-rose-600 hover:opacity-70"
                                  onClick={() => setDestinationDeleteDialog({ profile: item, purgeFiles: false })}
                                >
                                  删除
                                </button>
                              </div>
                            </td>
                          </tr>
                        ))}
                        {destinationProfiles.length === 0 && (
                          <tr>
                            <td className="px-3 py-6 text-sm text-slate-400" colSpan={4}>暂无目标配置</td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            )}

            {destinationTab === "synced_tables" && (
              <div className="grid gap-3 lg:grid-cols-[300px_minmax(0,1fr)]">
                <aside className="rounded-sm border border-slate-200 bg-[#F0EFEC] p-3">
                  <h4 className="mb-2 text-sm font-semibold text-slate-700">Schema List</h4>
                  <div className="max-h-[420px] overflow-auto">
                    {destinationCatalogGroups.map((group) => (
                      <div key={group.id} className="mb-2">
                        <p className="mb-1 text-xs font-semibold text-slate-500">{group.label}</p>
                        {group.items.map((item) => {
                          const key = `${group.id}:${item.id}`
                          const active = catalogActiveTableId === key
                          return (
                            <button
                              key={key}
                              className={`mb-1 w-full rounded-sm px-2 py-1 text-left text-sm ${
                                active ? "bg-[#0000E1]/10 text-[#0000E1]" : "hover:bg-[#E7E6E2]"
                              }`}
                              onClick={() => setCatalogActiveTableId(key)}
                            >
                              {item.label}
                            </button>
                          )
                        })}
                      </div>
                    ))}
                  </div>
                </aside>
                <div className="rounded-sm border border-slate-200 bg-[#F0EFEC] p-3">
                  {destinationCatalogActive ? (
                    <>
                      <h4 className="text-base font-semibold text-slate-900">{destinationCatalogActive.label}</h4>
                      <p className="mt-1 text-sm text-slate-500">{destinationCatalogActive.description}</p>
                      <div className="mt-2 grid gap-2 text-xs text-slate-500 md:grid-cols-2">
                        <span>来源 Connection：{destinationCatalogActive.sourceConnection}</span>
                        <span>所属分组：{destinationCatalogActive.groupLabel}</span>
                      </div>
                      <div className="mt-3 overflow-auto">
                        <table className="table-shell min-w-[520px]">
                          <thead>
                            <tr className="table-head-row">
                              <th className="table-head-cell">Column</th>
                              <th className="table-head-cell">Type</th>
                              <th className="table-head-cell">Description</th>
                            </tr>
                          </thead>
                          <tbody>
                            {destinationCatalogActive.columns.map((col) => (
                              <tr key={col.name}>
                                <td className="table-cell mono-ui">{col.name}</td>
                                <td className="table-cell">{col.type}</td>
                                <td className="table-cell text-slate-500">{col.role ? `${col.role} key` : "-"}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </>
                  ) : (
                    <p className="text-sm text-slate-500">请选择左侧表查看结构信息。</p>
                  )}
                </div>
              </div>
            )}
          </section>
        )}

        {(activeModule === "application_credentials" || activeModule === "platform_management") && (
          <section className={`${activeModule === "application_credentials" ? "flex flex-1 min-h-0 flex-col" : ""} space-y-3`}>
            {activeModule === "application_credentials" && (
              <>
            <article className="section-block flex-1 min-h-0 overflow-auto">
              <div className="mb-4 flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                <div className="flex items-center gap-3">
                  <h3 className="panel-title">应用管理列表</h3>
                  <a href="/docs" target="_blank" rel="noreferrer" className="text-xs font-medium text-slate-500 hover:text-[#0000E1]">
                    查看安全最佳实践
                  </a>
                </div>
                <div className="flex items-center gap-2">
                  <Button onClick={handleSyncCredentialSource}>
                    同步凭证 JSON
                  </Button>
                  <Button type="primary" onClick={openAppDrawer}>
                    + 新建应用凭证
                  </Button>
                </div>
              </div>

              <div className="mb-4 grid gap-2 sm:grid-cols-3">
                <Input
                  allowClear
                  placeholder="搜索应用名称"
                  value={accountSearch}
                  onChange={(e) => setAccountSearch(e.target.value)}
                />
                <Select
                  value={accountPlatform}
                  onChange={setAccountPlatform}
                  options={[
                    { value: "all", label: "全部平台" },
                    ...sourcePlatforms.map((platform) => ({
                      value: platform,
                      label: platformMappingLabel(platform, platformLabelMap),
                    })),
                  ]}
                />
                <Select
                  value={accountStatus}
                  onChange={setAccountStatus}
                  options={[
                    { value: "all", label: "全部状态" },
                    { value: "ready", label: "凭证完整" },
                    { value: "partial", label: "凭证缺失" },
                  ]}
                />
              </div>

              <Table
                rowKey={(item) => String(item.app_id || item.source_path || item.row_no)}
                dataSource={sourceAccounts}
                columns={credentialColumns}
                rowSelection={credentialRowSelection}
                pagination={credentialPagination}
                size="middle"
              />
            </article>

            <CreateCredentialDrawerModal
              open={appDrawerOpen}
              form={accountForm}
              availablePlatformConfigs={availablePlatformConfigs}
              currentPlatformSchema={currentPlatformSchema}
              accountConfigPreview={accountConfigPreview}
              onClose={closeAppDrawer}
              onSubmit={handleCreateAccount}
              onFieldChange={(field, value) => setAccountForm((x) => ({ ...x, [field]: value }))}
            />

            <CredentialEditModal
              dialog={editDialog}
              loading={editLoading}
              saving={editSaving}
              form={editForm}
              availablePlatformConfigs={availablePlatformConfigs}
              onChangeField={(field, value) => setEditForm((x) => ({ ...x, [field]: value }))}
              onClose={() => setEditDialog(null)}
              onSave={handleSaveCredentialEdit}
            />

            <WhitelistModal
              dialog={whitelistDialog}
              value={whitelistValue}
              saving={whitelistSaving}
              onValueChange={(e) => setWhitelistValue(e.target.value)}
              onClose={() => setWhitelistDialog(null)}
              onSave={handleSaveWhitelist}
            />

            <Modal
              open={streamDrawerOpen}
              onCancel={closeStreamDrawer}
              title={`配置同步接口 / ${currentConfigAccount?.name || "-"}`}
              width={920}
              destroyOnHidden
              footer={
                <div className="flex items-center justify-between">
                  <span className="text-xs text-slate-500">
                    已选择 <strong className="text-indigo-600">{Object.values(activeStreams).filter(Boolean).length}</strong> 个接口
                  </span>
                  <Space>
                    <Button onClick={closeStreamDrawer}>取消</Button>
                    <Button
                      type="primary"
                      loading={streamSaving}
                      disabled={streamDrawerLoading}
                      onClick={handleSaveAccountStreams}
                    >
                      保存接口配置
                    </Button>
                  </Space>
                </div>
              }
            >
              <p className="mt-0 text-xs text-slate-500">开启接口后，系统将按调度策略自动拉取并入库。</p>
              <div className="max-h-[62vh] overflow-y-auto rounded-lg border border-slate-100 p-2">
                {streamDrawerLoading ? (
                  <div className="py-12 text-center text-sm text-slate-500">加载中...</div>
                ) : (
                  <div className="divide-y divide-slate-50">
                    {currentSupportedStreams.map((stream) => {
                      const isEnabled = !!activeStreams[stream.key]
                      return (
                        <div
                          key={stream.key}
                          className={`m-2 flex items-center justify-between rounded-lg border p-4 ${
                            isEnabled ? "border-indigo-100/50 bg-indigo-50/30" : "border-transparent hover:bg-slate-50"
                          }`}
                        >
                          <div className="flex items-start gap-4">
                            <Switch checked={isEnabled} onChange={() => setActiveStreams((prev) => ({ ...prev, [stream.key]: !isEnabled }))} />
                            <div className="flex flex-col">
                              <span className={`text-sm font-semibold ${isEnabled ? "text-indigo-900" : "text-slate-700"}`}>
                                {stream.label}
                                <Tag className="ml-2">{stream.key}</Tag>
                              </span>
                              <span className="mt-1 max-w-md text-xs leading-relaxed text-slate-500">{stream.desc}</span>
                            </div>
                          </div>
                          {isEnabled ? <Tag color="processing">已启用</Tag> : null}
                        </div>
                      )
                    })}
                    {currentSupportedStreams.length === 0 ? (
                      <div className="py-12 text-center text-sm text-slate-400">该平台暂无预置接口。</div>
                    ) : null}
                  </div>
                )}
              </div>
            </Modal>

              </>
            )}

            {activeModule === "platform_management" && (
              <>
              <PlatformConfigsTablePanel
                platformSearch={platformSearch}
                onPlatformSearchChange={(e) => setPlatformSearch(e.target.value)}
                onRefresh={() => loadPlatformConfigs().catch((err) => showToast(err.message))}
                onCreate={openPlatformDrawerForAdd}
                filteredPlatformConfigs={filteredPlatformConfigs}
                platformLoading={platformLoading}
                platformsInUse={platformsInUse}
                normalizePlatformCode={normalizePlatformCode}
                onEdit={openPlatformDrawerForEdit}
                onDelete={handleDeletePlatform}
              />
              <PlatformDrawerModal
                portalRoot={portalRoot}
                open={platformDrawerOpen}
                visible={platformDrawerVisible}
                mode={platformDrawerMode}
                form={platformForm}
                codeError={platformCodeError}
                submitting={platformSubmitting}
                onClose={closePlatformDrawer}
                onSubmit={handleSubmitPlatformDrawer}
                onFieldChange={handlePlatformFieldChange}
                onCodeBlur={handlePlatformCodeBlur}
              />
              </>
            )}
          </section>
        )}

        {activeModule === "settings" && (
          <section className="space-y-3">
            <article className="section-block">
              <div className="mb-4 flex items-center justify-between">
                <h3 className="panel-title">系统设置</h3>
                <button className="btn-subtle" onClick={() => loadAppSettings().catch((err) => showToast(err.message))}>
                  刷新
                </button>
              </div>
              <div className="grid gap-4">
                <label className="field-label flex items-center justify-between gap-3 rounded-sm border border-slate-200 bg-[#F0EFEC] px-3 py-2">
                  <span>启用数据库（DB）</span>
                  <input
                    type="checkbox"
                    checked={!!appSettings.db_enabled_next}
                    onChange={(e) => setAppSettings((prev) => ({ ...prev, db_enabled_next: e.target.checked }))}
                    disabled={settingsLoading || settingsSaving}
                  />
                </label>
                <label className="field-label block">
                  数据库 URL
                  <input
                    className="input-base mt-1 mono-ui"
                    value={appSettings.database_url_next}
                    onChange={(e) => setAppSettings((prev) => ({ ...prev, database_url_next: e.target.value }))}
                    placeholder="sqlite:///... 或 postgresql://..."
                    disabled={settingsLoading || settingsSaving}
                  />
                </label>
                <div className="rounded-sm border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700">
                  DB 配置修改后需要重启后端服务才会生效。
                </div>
                <div className="grid gap-2 text-xs text-slate-500 md:grid-cols-2">
                  <div>运行时 DB 开关：<span className="mono-ui">{String(appSettings.db_enabled_runtime)}</span></div>
                  <div>下次启动 DB 开关：<span className="mono-ui">{String(appSettings.db_enabled_next)}</span></div>
                  <div>运行时 DB URL：<span className="mono-ui break-all">{appSettings.database_url_runtime || "-"}</span></div>
                  <div>下次启动 DB URL：<span className="mono-ui break-all">{appSettings.database_url_next || "-"}</span></div>
                  <div>DB 开关来源：<span className="mono-ui">{appSettings.db_enabled_source}</span></div>
                  <div>DB URL 来源：<span className="mono-ui">{appSettings.database_url_source}</span></div>
                </div>
                <div className="flex justify-end gap-2">
                  <button
                    className="btn-subtle"
                    onClick={() => loadAppSettings().catch((err) => showToast(err.message))}
                    disabled={settingsSaving}
                  >
                    重置
                  </button>
                  <button className="btn-brand" onClick={handleSaveAppSettings} disabled={settingsSaving || settingsLoading}>
                    {settingsSaving ? "保存中..." : "保存设置"}
                  </button>
                </div>
              </div>
            </article>
          </section>
        )}

        {activeModule === "monitor" && (
          <MonitorTasksPanel
            taskSearch={taskSearch}
            onTaskSearchChange={(e) => setTaskSearch(e.target.value)}
            taskStatus={taskStatus}
            onTaskStatusChange={(e) => setTaskStatus(e.target.value)}
            filteredTasks={filteredTasks}
            statusClass={statusClass}
            onTaskDetail={handleTaskDetail}
            detailText={detailText}
          />
        )}

        {activeModule === "application_connection" && connectionBatchBarMounted && (
          <BatchActionBar
            visible={connectionBatchBarVisible}
            selectedCount={selectedConnectionCount}
            actions={[
              {
                key: "activate",
                tone: "update",
                label: connectionBatchUpdating ? "执行中..." : "批量启用",
                onClick: handleBatchActivateConnections,
                disabled: connectionBatchUpdating || connectionBatchDeleting,
              },
              {
                key: "pause",
                tone: "update",
                label: connectionBatchUpdating ? "执行中..." : "批量暂停",
                onClick: handleBatchPauseConnections,
                disabled: connectionBatchUpdating || connectionBatchDeleting,
              },
              {
                key: "delete",
                tone: "delete",
                label: connectionBatchDeleting ? "批量删除中..." : "批量删除",
                onClick: handleBatchDeleteConnections,
                disabled: connectionBatchDeleting || connectionBatchUpdating,
              },
            ]}
            onClose={clearConnectionSelection}
          />
        )}

        {activeModule === "application_credentials" && batchBarMounted && (
          <BatchActionBar
            visible={batchBarVisible}
            selectedCount={selectedCount}
            actions={[
              {
                key: "refresh",
                tone: "update",
                label: batchRefreshing ? "批量更新中..." : "批量更新",
                onClick: handleBatchRefreshTokens,
                disabled: batchRefreshing || batchDeleting,
              },
              {
                key: "delete",
                tone: "delete",
                label: batchDeleting ? "批量删除中..." : "批量删除",
                onClick: handleBatchDeleteCredentials,
                disabled: batchDeleting,
              },
            ]}
            onClose={clearSourceSelection}
          />
        )}

        <DestinationFilesModal
          portalRoot={portalRoot}
          dialog={destinationFileDialog}
          loading={destinationFileLoading}
          onClose={() => setDestinationFileDialog(null)}
          formatBytes={formatBytes}
          formatTimeText={formatTimeText}
        />

        <DestinationDeleteModal
          portalRoot={portalRoot}
          dialog={destinationDeleteDialog}
          deleting={destinationDeleting}
          onClose={() => setDestinationDeleteDialog(null)}
          onTogglePurgeFiles={(e) => setDestinationDeleteDialog((prev) => ({ ...prev, purgeFiles: e.target.checked }))}
          onConfirmDelete={handleDeleteDestinationProfile}
        />

        {streamPreviewDialog && portalRoot && createPortal(
          <div className="fixed inset-0 z-[88] grid place-items-center bg-black/35 p-4" onClick={() => setStreamPreviewDialog(null)}>
            <article className="w-full max-w-6xl rounded-xl bg-[#F0EFEC] p-5 shadow-[0_18px_40px_rgba(15,23,42,0.24)]" onClick={(e) => e.stopPropagation()}>
              <div className="mb-3 flex items-center justify-between">
                <h4 className="m-0 text-base font-semibold text-slate-900">Stream 数据预览</h4>
                <button className="btn-subtle px-2 py-1 text-xs" onClick={() => setStreamPreviewDialog(null)}>关闭</button>
              </div>
              {streamPreviewDialog.loading ? (
                <p className="text-sm text-slate-500">预览加载中...</p>
              ) : streamPreviewDialog.error ? (
                <p className="text-sm text-rose-600">加载失败：{streamPreviewDialog.error}</p>
              ) : !streamPreviewDialog.payload ? (
                <p className="text-sm text-slate-500">暂无可预览数据</p>
              ) : (
                <>
                  <div className="mb-3 inline-flex rounded-md border border-slate-200 bg-[#F0EFEC] p-1">
                    <button
                      className={`rounded px-3 py-1 text-xs font-medium ${
                        streamPreviewViewMode === "table"
                          ? "bg-[#F0EFEC] text-slate-900 shadow-sm"
                          : "text-slate-500 hover:text-slate-700"
                      }`}
                      onClick={() => setStreamPreviewViewMode("table")}
                      disabled={!Array.isArray(streamPreviewDialog.payload.columns) || streamPreviewDialog.payload.columns.length === 0}
                    >
                      Table
                    </button>
                    <button
                      className={`rounded px-3 py-1 text-xs font-medium ${
                        streamPreviewViewMode === "json"
                          ? "bg-[#F0EFEC] text-slate-900 shadow-sm"
                          : "text-slate-500 hover:text-slate-700"
                      }`}
                      onClick={() => setStreamPreviewViewMode("json")}
                    >
                      Raw JSON
                    </button>
                  </div>
                  <p className="mb-3 text-xs text-slate-500">
                    Stream: <span className="mono-ui">{streamPreviewDialog.payload.stream_name}</span> ·
                    执行ID <span className="mono-ui">{streamPreviewDialog.payload.execution_id || "-"}</span> ·
                    状态 <span className="mono-ui">{streamPreviewDialog.payload.execution_status || "-"}</span> ·
                    最后运行完成于 <span className="mono-ui">{formatTimeText(streamPreviewDialog.payload.execution_finished_at)}</span> ·
                    返回 {streamPreviewDialog.payload.returned_records} / 总计 {streamPreviewDialog.payload.total_records}
                  </p>
                  {streamPreviewDialog.payload.execution_error_message ? (
                    <p className="mb-3 text-xs text-rose-600">
                      该次执行报错：{streamPreviewDialog.payload.execution_error_message}
                    </p>
                  ) : null}
                  {streamPreviewViewMode === "table" ? (
                    <div className="max-h-[520px] overflow-auto rounded-lg border border-slate-200">
                      <table className="table-shell min-w-[900px]">
                        <thead>
                          <tr className="table-head-row">
                            {streamPreviewDialog.payload.columns.map((column) => (
                              <th key={column} className="table-head-cell text-center mono-ui">{column}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {streamPreviewDialog.payload.rows.map((row, idx) => (
                            <tr key={idx}>
                              {streamPreviewDialog.payload.columns.map((column) => (
                                <td key={`${idx}-${column}`} className="table-cell text-center mono-ui">
                                  {row[column] === null || row[column] === undefined
                                    ? "-"
                                    : typeof row[column] === "object"
                                      ? JSON.stringify(row[column])
                                      : String(row[column])}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="max-h-[520px] overflow-auto rounded-lg border border-slate-200 bg-[#F0EFEC] p-3">
                      <p className="mb-2 text-xs text-slate-500">最后一次运行 response（原始 JSON）</p>
                      <pre className="mono-ui m-0 whitespace-pre-wrap break-words text-xs text-slate-700">
                        {JSON.stringify(streamPreviewDialog.payload.raw_response || {}, null, 2)}
                      </pre>
                    </div>
                  )}
                </>
              )}
            </article>
          </div>,
          portalRoot
        )}

        {executionDetailDialog && portalRoot && createPortal(
          <div className="fixed inset-0 z-[90] grid place-items-center bg-black/35 p-4" onClick={() => setExecutionDetailDialog(null)}>
            <article className="w-full max-w-3xl rounded-xl bg-[#F0EFEC] p-5 shadow-[0_18px_40px_rgba(15,23,42,0.24)]" onClick={(e) => e.stopPropagation()}>
              <div className="mb-3 flex items-center justify-between">
                <h4 className="m-0 text-base font-semibold text-slate-900">{executionDetailDialog.title}</h4>
                <button className="btn-subtle px-2 py-1 text-xs" onClick={() => setExecutionDetailDialog(null)}>关闭</button>
              </div>
              <pre className="max-h-[520px] overflow-auto rounded-sm border border-slate-300 bg-[#F0EFEC] p-3 text-xs text-slate-700 mono-ui">
                {JSON.stringify(executionDetailDialog.payload || {}, null, 2)}
              </pre>
            </article>
          </div>,
          portalRoot
        )}
      </main>

      <ConfirmModal
        isOpen={!!actionModal}
        onClose={closeActionModal}
        onConfirm={handleActionModalConfirm}
        title={actionModal?.title || "确认操作"}
        content={actionModal?.content || "确定继续吗？"}
        confirmText={actionModal?.confirmText || "确定"}
        cancelText={actionModal?.cancelText || "取消"}
        isDanger={!!actionModal?.isDanger}
        isLoading={actionModalLoading}
      />

      {toast && <div className="fixed bottom-4 right-4 rounded-sm border border-slate-900 bg-[#F0EFEC] px-4 py-2 text-sm text-slate-700 mono-ui">{toast}</div>}
    </div>
  )
}

export default App
