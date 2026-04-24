import confetti from 'canvas-confetti'
import axios from 'axios'
import {
  Activity,
  Ban,
  Briefcase,
  CheckCircle2,
  ChevronDown,
  Clipboard,
  Copy,
  Database,
  DollarSign,
  Download,
  Eye,
  EyeOff,
  LayoutDashboard,
  Lock,
  Mail,
  MessageCircle,
  PhoneCall,
  PlusCircle,
  RefreshCw,
  Save,
  Search,
  Send,
  Settings,
  Sparkles,
  Rocket,
  Target,
  TerminalSquare,
  Trash2,
  GripVertical,
  TrendingUp,
  Users,
  Zap,
} from 'lucide-react'
import { DndContext, KeyboardSensor, PointerSensor, closestCenter, useSensor, useSensors } from '@dnd-kit/core'
import { SortableContext, arrayMove, sortableKeyboardCoordinates, useSortable, verticalListSortingStrategy } from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'
import { AnimatePresence, motion as Motion } from 'framer-motion'
import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import toast, { Toaster } from 'react-hot-toast'
import Footer from './Footer'
import { getStoredValue } from './authStorage'
// ── Performance modules ────────────────────────────────────────────────────
import { useDebounce } from './hooks/useDebounce'
import { invalidateLeadsCache } from './hooks/useLeadsCache'
import { LeadCardSkeletonList, StatCardSkeletonList } from './components/SkeletonLoaders'
import { ScrapeProgressBar, ScrapeProgressBadge } from './components/ScrapeProgressBar'

const MRR_GOAL_EUR = 16000
const SETUP_MILESTONE_EUR = 6500
const DEFAULT_AVERAGE_DEAL_VALUE = 1000
const LEADS_PAGE_SIZE = 50
const BYPASS_LEAD_FILTERS = true

const QUALIFIER_LOSS_MULTIPLIER_RULES = [
  { terms: ['dentist', 'dental', 'orthodont'], multiplier: 1.55 },
  { terms: ['lawyer', 'legal', 'attorney'], multiplier: 1.65 },
  { terms: ['clinic', 'medical', 'medspa', 'dermatology', 'surgery'], multiplier: 1.75 },
  { terms: ['plumber', 'hvac', 'roof', 'electrician'], multiplier: 1.35 },
  { terms: ['restaurant', 'hotel', 'salon', 'spa', 'gym', 'fitness'], multiplier: 1.2 },
]

const QUALIFIER_FINDING_MODELS = [
  {
    key: 'ghost',
    countKeys: ['ghost', 'no_website'],
    listKeys: ['ghost', 'no_website'],
    finding: 'missing website foundation',
    perLeadLoss: 900,
  },
  {
    key: 'invisible',
    countKeys: ['invisible_giant', 'invisible_local'],
    listKeys: ['invisible_giant', 'invisible_local'],
    finding: 'weak local discoverability',
    perLeadLoss: 700,
  },
  {
    key: 'tech_debt',
    countKeys: ['tech_debt', 'low_authority'],
    listKeys: ['tech_debt', 'low_authority'],
    finding: 'conversion-killing tech debt',
    perLeadLoss: 600,
  },
]

const COUNTRIES = [
  { code: 'AE', name: 'UAE' },
  { code: 'AR', name: 'Argentina' },
  { code: 'AT', name: 'Austria' },
  { code: 'AU', name: 'Australia' },
  { code: 'BE', name: 'Belgium' },
  { code: 'BG', name: 'Bulgaria' },
  { code: 'BR', name: 'Brazil' },
  { code: 'CA', name: 'Canada' },
  { code: 'CH', name: 'Switzerland' },
  { code: 'CL', name: 'Chile' },
  { code: 'CN', name: 'China' },
  { code: 'CO', name: 'Colombia' },
  { code: 'CZ', name: 'Czech Republic' },
  { code: 'DE', name: 'Germany' },
  { code: 'DK', name: 'Denmark' },
  { code: 'EE', name: 'Estonia' },
  { code: 'ES', name: 'Spain' },
  { code: 'FI', name: 'Finland' },
  { code: 'FR', name: 'France' },
  { code: 'GB', name: 'United Kingdom' },
  { code: 'GR', name: 'Greece' },
  { code: 'HR', name: 'Croatia' },
  { code: 'HU', name: 'Hungary' },
  { code: 'IE', name: 'Ireland' },
  { code: 'IL', name: 'Israel' },
  { code: 'IN', name: 'India' },
  { code: 'IT', name: 'Italy' },
  { code: 'JP', name: 'Japan' },
  { code: 'KR', name: 'South Korea' },
  { code: 'LT', name: 'Lithuania' },
  { code: 'LV', name: 'Latvia' },
  { code: 'MX', name: 'Mexico' },
  { code: 'NG', name: 'Nigeria' },
  { code: 'NL', name: 'Netherlands' },
  { code: 'NO', name: 'Norway' },
  { code: 'NZ', name: 'New Zealand' },
  { code: 'PL', name: 'Poland' },
  { code: 'PT', name: 'Portugal' },
  { code: 'RO', name: 'Romania' },
  { code: 'RS', name: 'Serbia' },
  { code: 'RU', name: 'Russia' },
  { code: 'SA', name: 'Saudi Arabia' },
  { code: 'SE', name: 'Sweden' },
  { code: 'SG', name: 'Singapore' },
  { code: 'SI', name: 'Slovenia' },
  { code: 'SK', name: 'Slovakia' },
  { code: 'TR', name: 'Turkey' },
  { code: 'UA', name: 'Ukraine' },
  { code: 'US', name: 'United States' },
  { code: 'ZA', name: 'South Africa' },
]

const COUNTRY_TIMEZONES = {
  AE: { tz: 'Asia/Dubai',                     city: 'Dubai' },
  AR: { tz: 'America/Argentina/Buenos_Aires', city: 'Buenos Aires' },
  AT: { tz: 'Europe/Vienna',                  city: 'Vienna' },
  AU: { tz: 'Australia/Sydney',               city: 'Sydney' },
  BE: { tz: 'Europe/Brussels',                city: 'Brussels' },
  BG: { tz: 'Europe/Sofia',                   city: 'Sofia' },
  BR: { tz: 'America/Sao_Paulo',              city: 'São Paulo' },
  CA: { tz: 'America/Toronto',                city: 'Toronto' },
  CH: { tz: 'Europe/Zurich',                  city: 'Zurich' },
  CL: { tz: 'America/Santiago',               city: 'Santiago' },
  CN: { tz: 'Asia/Shanghai',                  city: 'Shanghai' },
  CO: { tz: 'America/Bogota',                 city: 'Bogotá' },
  CZ: { tz: 'Europe/Prague',                  city: 'Prague' },
  DE: { tz: 'Europe/Berlin',                  city: 'Berlin' },
  DK: { tz: 'Europe/Copenhagen',              city: 'Copenhagen' },
  EE: { tz: 'Europe/Tallinn',                 city: 'Tallinn' },
  ES: { tz: 'Europe/Madrid',                  city: 'Madrid' },
  FI: { tz: 'Europe/Helsinki',                city: 'Helsinki' },
  FR: { tz: 'Europe/Paris',                   city: 'Paris' },
  GB: { tz: 'Europe/London',                  city: 'London' },
  GR: { tz: 'Europe/Athens',                  city: 'Athens' },
  HR: { tz: 'Europe/Zagreb',                  city: 'Zagreb' },
  HU: { tz: 'Europe/Budapest',               city: 'Budapest' },
  IE: { tz: 'Europe/Dublin',                  city: 'Dublin' },
  IL: { tz: 'Asia/Jerusalem',                 city: 'Tel Aviv' },
  IN: { tz: 'Asia/Kolkata',                   city: 'Mumbai' },
  IT: { tz: 'Europe/Rome',                    city: 'Rome' },
  JP: { tz: 'Asia/Tokyo',                     city: 'Tokyo' },
  KR: { tz: 'Asia/Seoul',                     city: 'Seoul' },
  LT: { tz: 'Europe/Vilnius',                 city: 'Vilnius' },
  LV: { tz: 'Europe/Riga',                    city: 'Riga' },
  MX: { tz: 'America/Mexico_City',            city: 'Mexico City' },
  NG: { tz: 'Africa/Lagos',                   city: 'Lagos' },
  NL: { tz: 'Europe/Amsterdam',               city: 'Amsterdam' },
  NO: { tz: 'Europe/Oslo',                    city: 'Oslo' },
  NZ: { tz: 'Pacific/Auckland',               city: 'Auckland' },
  PL: { tz: 'Europe/Warsaw',                  city: 'Warsaw' },
  PT: { tz: 'Europe/Lisbon',                  city: 'Lisbon' },
  RO: { tz: 'Europe/Bucharest',               city: 'Bucharest' },
  RS: { tz: 'Europe/Belgrade',                city: 'Belgrade' },
  RU: { tz: 'Europe/Moscow',                  city: 'Moscow' },
  SA: { tz: 'Asia/Riyadh',                    city: 'Riyadh' },
  SE: { tz: 'Europe/Stockholm',               city: 'Stockholm' },
  SG: { tz: 'Asia/Singapore',                 city: 'Singapore' },
  SI: { tz: 'Europe/Ljubljana',               city: 'Ljubljana' },
  SK: { tz: 'Europe/Bratislava',              city: 'Bratislava' },
  TR: { tz: 'Europe/Istanbul',                city: 'Istanbul' },
  UA: { tz: 'Europe/Kiev',                    city: 'Kyiv' },
  US: { tz: 'America/New_York',               city: 'New York' },
  ZA: { tz: 'Africa/Johannesburg',            city: 'Johannesburg' },
}

function getCountryTZInfo(code) {
  return COUNTRY_TIMEZONES[String(code).toUpperCase()] || COUNTRY_TIMEZONES['US']
}

function getTZAbbr(tz) {
  try {
    return new Intl.DateTimeFormat('en-US', { timeZone: tz, timeZoneName: 'short' })
      .formatToParts(new Date())
      .find((p) => p.type === 'timeZoneName')?.value || tz
  } catch {
    return tz
  }
}

function getLocalTimeStr(tz) {
  return new Date().toLocaleTimeString('en-GB', { timeZone: tz, hour: '2-digit', minute: '2-digit', hour12: false })
}

function localHourToET(localHour, tz) {
  const now = new Date()
  const localNow = new Date(now.toLocaleString('en-US', { timeZone: tz }))
  const etNow = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }))
  const offsetHours = Math.round((localNow - etNow) / (1000 * 60 * 60))
  return (((localHour - offsetHours) % 24) + 24) % 24
}

function CountrySelect({ value, onChange }) {
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const ref = useRef(null)

  useEffect(() => {
    function onMouseDown(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false)
    }
    document.addEventListener('mousedown', onMouseDown)
    return () => document.removeEventListener('mousedown', onMouseDown)
  }, [])

  const filtered = COUNTRIES.filter(
    (c) =>
      c.name.toLowerCase().includes(query.toLowerCase()) ||
      c.code.toLowerCase().includes(query.toLowerCase()),
  )
  const selected = COUNTRIES.find((c) => c.code === value) || COUNTRIES[0]

  return (
    <div ref={ref} className="relative">
      <button
        type="button"
        className="glass-input w-full flex items-center justify-between gap-2 text-left cursor-pointer"
        onClick={() => setOpen((v) => !v)}
      >
        <span className="flex items-center gap-2 min-w-0">
          <img
            src={`https://flagcdn.com/w20/${selected.code.toLowerCase()}.png`}
            width="20"
            height="14"
            alt={selected.code}
            className="rounded-sm shrink-0"
          />
          <span className="font-medium text-white">{selected.code}</span>
          <span className="text-slate-400 truncate text-sm">{selected.name}</span>
        </span>
        <ChevronDown className="h-4 w-4 text-slate-400 shrink-0" />
      </button>

      {open && (
        <div className="absolute z-50 mt-1 w-full rounded-xl border border-white/10 bg-slate-900 shadow-2xl overflow-hidden" style={{minWidth: '220px'}}>
          <div className="p-2 border-b border-white/8">
            <input
              autoFocus
              type="text"
              placeholder="Search country…"
              className="w-full bg-transparent text-sm text-white placeholder-slate-500 outline-none px-2 py-1"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
          </div>
          <div className="max-h-56 overflow-y-auto">
            {filtered.map((c) => (
              <button
                key={c.code}
                type="button"
                className={`w-full flex items-center gap-2.5 px-3 py-2 text-sm transition-colors text-left ${
                  c.code === value ? 'bg-yellow-500/10 text-yellow-300' : 'text-slate-200 hover:bg-white/5'
                }`}
                onClick={() => { onChange(c.code); setOpen(false); setQuery('') }}
              >
                <img
                  src={`https://flagcdn.com/w20/${c.code.toLowerCase()}.png`}
                  width="20"
                  height="14"
                  alt={c.code}
                  className="rounded-sm shrink-0"
                />
                <span className="font-semibold w-8 shrink-0">{c.code}</span>
                <span className="text-slate-400">{c.name}</span>
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
const SETUP_FEE_WEBSITE = 1300
const MRR_WEBSITE_ONLY = 200
const MRR_ADS_ONLY = 1100
const MRR_ADS_AND_WEBSITE = 1400
const SETUP_FEE_BY_TIER = { standard: SETUP_FEE_WEBSITE, premium_ads: SETUP_FEE_WEBSITE }
const MRR_BY_TIER = { standard: MRR_WEBSITE_ONLY, premium_ads: MRR_ADS_ONLY }
const leadStatusOptions = ['Pending', 'Emailed', 'Interested', 'Replied', 'Meeting Set', 'Zoom Scheduled', 'Closed', 'Paid', 'Failed', 'Generation Failed', 'retry_later', 'Blacklisted', 'Skipped (Unsubscribed)', 'Skipped (Test Lead)', 'low_priority', 'QUALIFIED_NOT_INTERESTED']
const leadPipelineOptions = ['Scraped', 'Contacted', 'Replied', 'Won (Paid)']
const tierOptions = ['standard', 'premium_ads']
const taskLabels = {
  scrape: 'Scrape',
  enrich: 'Enrichment',
  mailer: 'Mailer',
}
const TASK_MANAGER_STORAGE_KEY = 'lf_custom_tasks'
const TASK_MANAGER_ORDER_STORAGE_KEY = 'lf_task_order'
const TASK_MANAGER_DISMISSED_STORAGE_KEY = 'lf_dismissed_auto_tasks'
const TASK_MANAGER_AUTO_PRIORITY_KEY = 'lf_auto_task_priority'
const PERSONAL_GOAL_NAME_STORAGE_KEY = 'lf_personal_goal_name'
const PERSONAL_GOAL_AMOUNT_STORAGE_KEY = 'lf_personal_goal_amount'
const PERSONAL_GOAL_CURRENCY_STORAGE_KEY = 'lf_personal_goal_currency'
const DEFAULT_GOAL_CURRENCY = 'EUR'
const GOAL_CURRENCY_OPTIONS = ['EUR', 'USD', 'GBP']
const TASK_MANAGER_PRIORITIES = ['High', 'Medium', 'Low']
const TASK_MANAGER_STATUSES = ['To Outreach', 'Waiting', 'Follow-up', 'Done']
const TAB_QUERY_KEYS = new Set(['leads', 'blacklist', 'workers', 'tasks', 'history', 'mail', 'qualify', 'export', 'clients', 'config'])

function normalizeTabParam(raw, fallback = 'leads') {
  const tab = String(raw || '').toLowerCase().trim()
  if (tab === 'active') return 'tasks'
  if (tab === 'delivery') return 'tasks'
  if (tab === 'task') return 'tasks'
  if (tab === 'history') return 'tasks'
  if (TAB_QUERY_KEYS.has(tab)) return tab
  return fallback
}

function mapDeliveryStatusToTaskStatus(status) {
  const raw = String(status || '').toLowerCase().trim()
  if (raw === 'done') return 'Done'
  if (raw === 'blocked') return 'Waiting'
  if (raw === 'in_progress') return 'Follow-up'
  return 'To Outreach'
}

function mapTaskStatusToDeliveryStatus(status) {
  const raw = String(status || '').toLowerCase().trim()
  if (raw === 'done') return 'done'
  if (raw === 'waiting') return 'blocked'
  if (raw === 'follow-up' || raw === 'follow_up') return 'in_progress'
  return 'todo'
}

function priorityWeight(priority) {
  const raw = String(priority || '').toLowerCase()
  if (raw === 'high') return 3
  if (raw === 'medium') return 2
  return 1
}

function priorityDotClass(priority) {
  const raw = String(priority || '').toLowerCase()
  if (raw === 'high') return 'bg-rose-400 shadow-[0_0_10px_rgba(251,113,133,0.8)]'
  if (raw === 'medium') return 'bg-amber-300 shadow-[0_0_10px_rgba(252,211,77,0.75)]'
  return 'bg-cyan-300 shadow-[0_0_10px_rgba(34,211,238,0.7)]'
}
const mainNavItems = [
  { tab: 'leads', label: 'Search', icon: Search },
  { tab: 'blacklist', label: 'Blacklist', icon: Ban },
  { tab: 'workers', label: 'Workers', icon: Users },
  { tab: 'tasks', label: 'Tasks', icon: Clipboard },
  { tab: 'mail', label: 'Mail', icon: Mail },
  { tab: 'clients', label: 'Clients', icon: LayoutDashboard },
]
const templateCardIcons = {
  ghost: Search,
  golden: TrendingUp,
  competitor: Users,
  speed: Activity,
}
const templatePlaceholderTokens = ['{BusinessName}', '{City}', '{Niche}', '{YourName}']
const TOP_UP_PACKAGES = [
  { id: 'credits_1000', credits: 1000, priceUsd: 29.99, badge: '' },
  { id: 'credits_2500', credits: 2500, priceUsd: 59.00, badge: '' },
  { id: 'credits_5000', credits: 5000, priceUsd: 99.00, badge: 'MOST POPULAR' },
  { id: 'credits_10000', credits: 10000, priceUsd: 169.00, badge: 'BEST VALUE' },
  { id: 'credits_25000', credits: 25000, priceUsd: 349.00, badge: '' },
  { id: 'credits_50000', credits: 50000, priceUsd: 699.00, badge: '' },
  { id: 'credits_100000', credits: 100000, priceUsd: 1119.00, badge: '' },
  { id: 'credits_250000', credits: 250000, priceUsd: 2199.00, badge: '' },
  { id: 'credits_500000', credits: 500000, priceUsd: 3499.00, badge: '' },
]
const TOP_UP_PACKAGE_OPTIONS = TOP_UP_PACKAGES.map((pkg) => ({
  ...pkg,
  label: `${pkg.credits.toLocaleString('en-US')} Credits - $${Number(pkg.priceUsd || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
}))
const formatUsd = (value) => Number(value || 0).toLocaleString('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
})
const DEFAULT_FREE_CREDIT_LIMIT = 50
const SUBSCRIPTION_PLAN_DETAILS = {
  free: { displayName: 'The Starter', credits: 50 },
  hustler: { displayName: 'The Hustler', credits: 2000 },
  growth: { displayName: 'The Growth', credits: 7000 },
  scale: { displayName: 'The Scale', credits: 20000 },
  empire: { displayName: 'The Empire', credits: 100000 },
  pro: { displayName: 'Pro Plan', credits: 7000 },
}

function normalizePlanAccessKey(rawPlanKey) {
  const value = String(rawPlanKey || '').trim().toLowerCase()
  if (value === 'starter') return 'free'
  if (value === 'basic') return 'hustler'
  if (value === 'business') return 'scale'
  if (value === 'elite') return 'empire'
  return value || 'free'
}

function getDefaultFeatureAccess(rawPlanKey = 'free') {
  const planKey = normalizePlanAccessKey(rawPlanKey)
  const access = {
    plan_key: planKey,
    plan_type: SUBSCRIPTION_PLAN_DETAILS[planKey]?.displayName || 'The Starter',
    basic_search: true,
    mailer_send: true,
    deep_analysis: true,
    bulk_export: false,
    drip_campaigns: false,
    ai_lead_scoring: false,
    webhooks: false,
    advanced_reporting: false,
    client_success_dashboard: false,
    queue_priority: false,
    ai_model: 'gpt-4o-mini',
  }

  if (planKey === 'hustler') {
    access.ai_lead_scoring = true
  }
  if (['growth', 'pro', 'scale', 'empire'].includes(planKey)) {
    access.deep_analysis = true
    access.bulk_export = true
    access.drip_campaigns = true
    access.ai_lead_scoring = true
    access.ai_model = 'gpt-4o'
  }
  if (['scale', 'empire'].includes(planKey)) {
    access.webhooks = true
    access.advanced_reporting = true
    access.client_success_dashboard = true
  }
  if (planKey === 'empire') {
    access.queue_priority = true
  }

  return access
}

function resolveFeatureAccess(rawPlanKey, featureAccess) {
  const base = getDefaultFeatureAccess(rawPlanKey)
  if (!featureAccess || typeof featureAccess !== 'object') return base

  return {
    ...base,
    ...featureAccess,
    plan_key: String(featureAccess.plan_key || base.plan_key).toLowerCase().trim() || base.plan_key,
    plan_type: String(featureAccess.plan_type || base.plan_type).trim() || base.plan_type,
    ai_model: String(featureAccess.ai_model || base.ai_model).trim() || base.ai_model,
  }
}

const sleep = (ms) => new Promise((resolve) => window.setTimeout(resolve, ms))

const SidebarBillingCard = memo(function SidebarBillingCard({
  isPaid,
  planName,
  cancelPending = false,
  cancelUntilLabel = '',
  onUpgrade,
  onChangeSubscription,
}) {
  const resolvedPlanName = String(planName || 'Free Plan').trim() || 'Free Plan'
  const statusText = cancelPending ? 'Canceled subscription' : (isPaid ? 'Subscription active' : 'You are currently on the free tier')
  const actionLabel = cancelPending ? 'Upgrade Plan' : (isPaid ? 'Change Plans' : 'Upgrade Plan')

  return (
    <div className="rounded-2xl border border-slate-700/70 bg-[linear-gradient(180deg,#0D1117_0%,#0B1220_100%)] p-3.5 shadow-[0_14px_30px_rgba(2,6,23,0.45)]">
      <div className="space-y-3">
        <div className="space-y-1.5">
          <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-400">Current Plan</p>
          <div className="rounded-xl border border-[#FFC107]/15 bg-[#111827]/80 px-3 py-2.5">
            <p className="text-[1rem] font-semibold leading-tight text-[#FFC107]">{resolvedPlanName}</p>
            <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px] leading-relaxed">
              <span className={cancelPending ? 'font-semibold text-rose-300' : 'text-slate-400'}>{statusText}</span>
              {cancelPending && cancelUntilLabel && (
                <span className="text-slate-400">until {cancelUntilLabel}</span>
              )}
            </div>
          </div>
        </div>

        <button
          type="button"
          className="inline-flex w-full items-center justify-center gap-1.5 rounded-xl border border-[#FFC107]/80 bg-gradient-to-r from-[#d9a406] to-[#FFC107] px-3 py-2 text-xs font-semibold text-[#0a1422] shadow-[0_8px_20px_rgba(255,193,7,0.28)] transition-all duration-200 hover:brightness-105"
          onClick={cancelPending ? onUpgrade : (isPaid ? onChangeSubscription : onUpgrade)}
        >
          <Zap className="h-3.5 w-3.5" /> {actionLabel}
        </button>
      </div>
    </div>
  )
})

const SidebarLeadFlowPanel = memo(function SidebarLeadFlowPanel({
  isPaid,
  planName,
  cancelPending,
  cancelUntilLabel,
  creditsBalance,
  monthlyLimit,
  creditsPercent,
  creditsLabelClass,
  resetLabel,
  topupLabel,
  onUpgrade,
  onChangeSubscription,
  onTopUp,
}) {
  return (
    <div className="mt-4 space-y-2.5">
      <SidebarBillingCard
        isPaid={isPaid}
        planName={planName}
        cancelPending={cancelPending}
        cancelUntilLabel={cancelUntilLabel}
        onUpgrade={onUpgrade}
        onChangeSubscription={onChangeSubscription}
      />

      <div className="rounded-xl border border-slate-700/70 bg-[#0D1117] p-3 shadow-[0_14px_30px_rgba(2,6,23,0.45)]">
        <div className="mb-2 flex items-center justify-between text-sm font-semibold">
          <p className="text-white">Credits</p>
          <p className={creditsLabelClass}>
            {creditsBalance.toLocaleString('en-US')} / {monthlyLimit.toLocaleString('en-US')}
          </p>
        </div>
        <div className="h-2 w-full overflow-hidden rounded-xl bg-slate-700/70">
          <div
            className="h-full rounded-xl bg-gradient-to-r from-[#d9a406] to-[#FFC107] transition-[width] duration-200 ease-out"
            style={{ width: `${creditsPercent}%` }}
          />
        </div>
        <div className="mt-2 flex items-center gap-2 text-[11px] text-slate-400">
          <span className="h-1.5 w-1.5 rounded-full bg-slate-500" />
          {resetLabel}
        </div>
        {topupLabel ? (
          <div className="mt-1 flex items-center gap-2 text-[11px] text-[#FFE082]">
            <span className="h-1.5 w-1.5 rounded-full bg-[#FFC107]" />
            {topupLabel}
          </div>
        ) : null}
        <button
          className="topbar-nav mt-3 w-full justify-center rounded-xl border border-[#FFC107]/55 bg-[linear-gradient(135deg,rgba(255,193,7,0.18),rgba(13,17,23,0.95))] text-[#FFC107] transition-all duration-200 hover:border-[#FFC107]/85 hover:text-[#FFE082]"
          type="button"
          onClick={onTopUp}
        >
          <Zap className="h-4 w-4" /> + Top Up
        </button>
      </div>
    </div>
  )
})

const TopUpCreditsModal = memo(function TopUpCreditsModal({
  isOpen,
  selectedPackageId,
  selectedPackage,
  packages,
  loadingPackageId,
  preparingPackageId,
  onClose,
  onPackageChange,
  onProceed,
}) {
  const isPreparingSelected = Boolean(preparingPackageId) && preparingPackageId === selectedPackageId
  const isBusy = Boolean(loadingPackageId) || isPreparingSelected
  return (
    <AnimatePresence>
      {isOpen && (
        <Motion.div
          className="fixed inset-0 z-[70] flex items-center justify-center bg-[rgba(2,6,23,0.82)] p-3 backdrop-blur-sm sm:p-5"
          onClick={(e) => { if (e.target === e.currentTarget) onClose() }}
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.2 }}
        >
          <Motion.div
            className="w-full max-w-xl rounded-xl border border-[#FFC107]/40 bg-[#0D1117] p-5 shadow-[0_30px_100px_rgba(255,193,7,0.18)] sm:p-6"
            initial={{ opacity: 0, y: 8, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 8, scale: 0.98 }}
            transition={{ duration: 0.2 }}
          >
            <div className="mb-4 flex items-start justify-between gap-4 sm:mb-6">
              <div>
                <div className="mb-2 inline-flex h-8 w-8 items-center justify-center rounded-xl" style={{ background: 'linear-gradient(135deg, #d9a406, #FFC107)' }}>
                  <Zap className="h-4 w-4" style={{ color: '#0a1422' }} />
                </div>
                <p className="label-overline text-[#FFC107]">Billing</p>
                <h2 className="mt-1.5 text-xl font-semibold text-white sm:text-2xl">Top Up Credits</h2>
              </div>
              <button
                type="button"
                className="rounded-xl p-2 text-slate-400 transition-all duration-200 hover:bg-white/10 hover:text-white"
                onClick={onClose}
                aria-label="Close"
              >
                ✕
              </button>
            </div>

            <div className="rounded-xl border border-slate-700/70 bg-[#0D1117] p-4">
              <label htmlFor="topup-package" className="mb-2 block text-xs font-semibold uppercase tracking-[0.12em] text-slate-400">
                Select Package
              </label>
              <div className="relative">
                <select
                  id="topup-package"
                  value={selectedPackageId}
                  onChange={(e) => onPackageChange(String(e.target.value || ''))}
                  className="saas-select w-full appearance-none rounded-xl border border-slate-600/80 bg-[#0D1117] px-3 py-3 pr-10 text-sm font-medium text-white outline-none transition-all duration-200 focus:border-[#FFC107] focus:ring-2 focus:ring-[#FFC107]/35"
                >
                  {packages.map((pkg) => (
                    <option key={pkg.id} value={pkg.id}>
                      {pkg.label || `${pkg.credits.toLocaleString('en-US')} Credits - $${formatUsd(pkg.priceUsd)}`}
                    </option>
                  ))}
                </select>
                <ChevronDown className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[#FFC107]" />
              </div>

              <div className="mt-4 rounded-xl border border-[#FFC107]/25 bg-[#111827] p-3">
                <div className="flex items-center justify-between text-sm">
                  <span className="text-slate-300">Selected package</span>
                  <span className="font-semibold text-[#FFE082]">
                    {Number(selectedPackage?.credits || 0).toLocaleString('en-US')} credits
                  </span>
                </div>
                <div className="mt-1.5 flex items-center justify-between text-base">
                  <span className="font-semibold text-white">Total Price</span>
                  <span className="font-bold text-[#FFC107]">
                    ${formatUsd(selectedPackage?.priceUsd)}
                  </span>
                </div>
                <p className="mt-2 text-xs text-slate-400">
                  One-time Stripe payment. Credits are added instantly and your current subscription tier stays unchanged.
                </p>
              </div>

              <button
                type="button"
                className="mt-4 inline-flex w-full items-center justify-center rounded-xl border border-[#FFC107]/85 bg-gradient-to-r from-[#d9a406] to-[#FFC107] px-4 py-3 text-sm font-bold text-[#0a1422] shadow-[0_10px_24px_rgba(255,193,7,0.3)] transition-all duration-200 hover:brightness-105 disabled:cursor-not-allowed disabled:opacity-60"
                disabled={isBusy || !selectedPackageId}
                onClick={() => onProceed(selectedPackageId)}
              >
                {loadingPackageId ? 'Redirecting…' : isPreparingSelected ? 'Preparing checkout…' : 'Purchase'}
              </button>
            </div>
          </Motion.div>
        </Motion.div>
      )}
    </AnimatePresence>
  )
})

const defaultScrape = { keyword: '', results: 25, country: 'US', headless: true, exportTargets: true }
const defaultEnrich = { limit: 50, headless: true, skipExport: false }
const defaultMailer = { limit: 10, delayMin: 7, delayMax: 15 }
const defaultCampaignSequenceForm = {
  name: 'Main 3-Step Sequence',
  activeStep: 1,
  step1_subject: 'Quick idea for {BusinessName}',
  step1_body: 'Hi {BusinessName},\n\nSpotted a quick win in {City} that could help you pull in more local leads.',
  step2_delay_days: 3,
  step2_subject: 'Following up on {BusinessName}',
  step2_body: 'Just floating this back to the top in case improving local conversion is still on your radar.',
  step3_delay_days: 7,
  step3_subject: 'Last note for {BusinessName}',
  step3_body: 'Happy to send a short 2-minute breakdown if it would be useful.',
  ab_subject_a: 'Quick idea for {BusinessName}',
  ab_subject_b: '{BusinessName} — local traffic idea',
  active: true,
}
const defaultManualLead = { contactName: '', email: '', businessName: '' }
const defaultWorkerForm = { workerName: '', role: 'DEV', monthlyCost: '', status: 'Active', commsLink: '' }
const createEmptySmtpAccount = () => ({
  host: 'smtp.gmail.com',
  port: 587,
  email: '',
  password: '',
  from_name: '',
  password_set: false,
})
const liveMailTemplateCards = [
  {
    key: 'ghost',
    title: 'No Website',
    description: 'Used when the business has no website. This is your website-first opener.',
    subjectKey: 'ghost_subject_template',
    bodyKey: 'ghost_body_template',
  },
  {
    key: 'golden',
    title: 'Traffic Opportunity',
    description: 'Used for highest-score leads with a strong site but visible paid traffic upside.',
    subjectKey: 'golden_subject_template',
    bodyKey: 'golden_body_template',
  },
  {
    key: 'competitor',
    title: 'Competitor Gap',
    description: 'Used when competitors visibly outrank them and the site is missing conversion/SEO basics.',
    subjectKey: 'competitor_subject_template',
    bodyKey: 'competitor_body_template',
  },
  {
    key: 'speed',
    title: 'Site Speed',
    description: 'Used for lower high-fit leads where page speed and site quality are the main hook.',
    subjectKey: 'speed_subject_template',
    bodyKey: 'speed_body_template',
  },
]

const liveMailTemplateCardMetaByNiche = {
  'Web Design & Dev': {
    ghost: { title: 'No Website', description: 'Used when the business has no website. This is your website-first opener.' },
    golden: { title: 'Traffic Opportunity', description: 'Used for highest-score leads with a strong site but visible paid traffic upside.' },
    competitor: { title: 'Competitor Gap', description: 'Used when competitors visibly outrank them and the site is missing conversion/SEO basics.' },
    speed: { title: 'Site Speed', description: 'Used for lower high-fit leads where page speed and site quality are the main hook.' },
  },
  'Paid Ads Agency': {
    ghost: { title: 'Pixel Missing', description: 'Use when Meta or Google pixel is missing and retargeting is blind.' },
    golden: { title: 'Ad Visibility Gap', description: 'Use when competitors are aggressively visible while this lead is not.' },
    competitor: { title: 'Auction Pressure', description: 'Use when competitor share-of-voice dominates paid placements.' },
    speed: { title: 'Tracking Quality', description: 'Use when conversion tracking quality is too weak for optimization.' },
  },
  'SEO & Content': {
    ghost: { title: 'No Content Base', description: 'Use when the site lacks a proper blog or supporting content layer.' },
    golden: { title: 'Keyword Opportunity', description: 'Use when high-intent keyword clusters are under-captured.' },
    competitor: { title: 'Ranking Gap', description: 'Use when competitors dominate page one for target terms.' },
    speed: { title: 'Technical SEO Drag', description: 'Use when speed and technical debt suppress crawl and ranking performance.' },
  },
  'Lead Gen Agency': {
    ghost: { title: 'No Capture Path', description: 'Use when contact flow and lead capture are missing or broken.' },
    golden: { title: 'CTA Opportunity', description: 'Use when intent exists but there is no strong CTA sequence.' },
    competitor: { title: 'Offer Positioning Gap', description: 'Use when competitors communicate offer/value more clearly.' },
    speed: { title: 'Funnel Friction', description: 'Use when page friction and speed reduce inquiry conversion.' },
  },
  'B2B Service Provider': {
    ghost: { title: 'Outbound Missing', description: 'Use when there is no consistent outbound or partner-sourcing workflow.' },
    golden: { title: 'LinkedIn Opportunity', description: 'Use when authority exists but LinkedIn demand capture is weak.' },
    competitor: { title: 'Partner Gap', description: 'Use when competitors build visibility via stronger direct outreach.' },
    speed: { title: 'Automation Friction', description: 'Use when pipeline and outreach handoffs are too manual.' },
  },
}

function resolveLiveMailTemplateCardsForNiche(rawNiche) {
  const niche = String(rawNiche || '').trim()
  const nicheMeta = liveMailTemplateCardMetaByNiche[niche]
  if (!nicheMeta) return liveMailTemplateCards
  return liveMailTemplateCards.map((card) => ({
    ...card,
    ...(nicheMeta[card.key] || {}),
  }))
}
const mailTemplatePacks = [
  {
    key: 'clean',
    label: 'Clean',
    description: 'Short, calm, low-friction outreach.',
    templates: {
      ghost_subject_template: 'quick question for {BusinessName}',
      ghost_body_template: 'Hi,\n\nI was looking for {BusinessName} in {City} and noticed you do not have a website live right now.\n\nThat usually means Google Maps traffic and direct searches are leaking to competitors who look easier to trust online.\n\nI build simple, high-converting service pages that get local businesses live fast and bring in more booked jobs.\n\nOpen to a quick 10-minute call this week?\n\nBest, {YourName}',
      golden_subject_template: '{BusinessName} and local traffic',
      golden_body_template: 'Hi,\n\nYour site for {BusinessName} already looks solid, but you are missing visibility in high-intent searches for {Niche} in {City}.\n\nThat means competitors are likely taking the easiest leads before people ever reach you.\n\nI can send over a short 2-minute video with a simple plan to capture more of that traffic with a better landing page and tighter Google Ads setup. Would you be against me sending it?\n\nBest, {YourName}',
      competitor_subject_template: '{BusinessName} - quick idea',
      competitor_body_template: 'Hi,\n\nI noticed competitors are showing up above {BusinessName} for {Niche} searches in {City}.\n\nUsually that happens when the site structure, SEO basics, or tracking setup are weaker than they should be.\n\nMy team fixes that end-to-end so local businesses turn more search traffic into booked work. If helpful, I can send over a short 2-minute breakdown.\n\nBest, {YourName}',
      speed_subject_template: '{BusinessName} site speed',
      speed_body_template: 'Hi,\n\nI checked {BusinessName} and the site appears slow enough on mobile that it may be hurting both rankings and conversion rate.\n\nFor {Niche} businesses in {City}, that usually means lost calls and form fills.\n\nI can show you how we fix speed, tighten the page, and make the traffic convert better.\n\nWorth a quick 10-minute call?\n\nBest, {YourName}',
    },
  },
  {
    key: 'local-first',
    label: 'Local First',
    description: 'Heavier local context and map visibility angle.',
    templates: {
      ghost_subject_template: 'noticed this about {BusinessName} in {City}',
      ghost_body_template: 'Hi,\n\nI was searching for businesses like {BusinessName} in {City} today and noticed you still do not have a website live.\n\nFor local service companies, that usually means people see the Google listing, but then choose someone else because there is nothing online that builds trust fast.\n\nI build fast local landing pages designed to turn map traffic into calls and booked jobs. If helpful, I can send over a 2-minute video showing what I would build first.\n\nBest, {YourName}',
      golden_subject_template: '{BusinessName} and missed local demand',
      golden_body_template: 'Hi,\n\nI was reviewing {BusinessName} and noticed you are not taking enough visible share of high-intent local traffic for {Niche} in {City}.\n\nThat often means the business is strong, but the landing page and ad presence are not doing enough to capture demand already in the market.\n\nI can send over a tight website-plus-ads plan built specifically for local lead flow. Would you be against me sending a short 2-minute breakdown?\n\nBest, {YourName}',
      competitor_subject_template: '{BusinessName} vs competitors in {City}',
      competitor_body_template: 'Hi,\n\nI noticed competitors around {City} are occupying more of the visible search space than {BusinessName} right now.\n\nThat usually comes down to a better landing page structure, cleaner SEO signals, and stronger tracking.\n\nWe fix those gaps so local businesses stop leaking easy demand to nearby competitors. If useful, I can send over a short 2-minute walkthrough.\n\nBest, {YourName}',
      speed_subject_template: '{BusinessName} mobile experience',
      speed_body_template: 'Hi,\n\nI checked {BusinessName} and the mobile experience looks slow enough that it may be pushing both users and rankings in the wrong direction.\n\nIn local {Niche} searches around {City}, faster pages usually win more clicks and more calls.\n\nI can show you a fast cleanup plan and what it would take to improve it quickly.\n\nOpen to a short call this week?\n\nBest, {YourName}',
    },
  },
  {
    key: 'aggressive',
    label: 'Aggressive',
    description: 'Sharper pain framing and stronger commercial angle.',
    templates: {
      ghost_subject_template: '{BusinessName} is likely losing easy leads',
      ghost_body_template: 'Hi,\n\nI looked up {BusinessName} in {City} and noticed there is still no website live.\n\nThat usually means potential customers are finding you, hesitating, and then booking the competitor that looks more established online.\n\nWe build fast service pages that fix that immediately and give you a proper base for SEO and Google Ads.\n\nDo you have 10 minutes this week to see what that would look like?\n\nBest, {YourName}',
      golden_subject_template: '{BusinessName} is missing high-intent traffic',
      golden_body_template: 'Hi,\n\n{BusinessName} looks strong, but you are still leaving valuable search traffic on the table for {Niche} in {City}.\n\nRight now competitors are likely buying or capturing leads that should be coming to you first.\n\nI can send over a short plan showing how to tighten the page, improve conversion, and layer in ads that bring in higher-quality demand.\n\nBest, {YourName}',
      competitor_subject_template: 'competitors are outranking {BusinessName}',
      competitor_body_template: 'Hi,\n\nI noticed competitors are beating {BusinessName} in Google for {Niche} around {City}.\n\nThat usually means your current site and tracking setup are not strong enough to convert or signal quality properly.\n\nWe rebuild that stack so the business is easier to find, easier to trust, and easier to contact.\n\nDo you have 10 minutes this week for a quick walkthrough?\n\nBest, {YourName}',
      speed_subject_template: '{BusinessName} may be getting penalized',
      speed_body_template: 'Hi,\n\nI ran a quick check on {BusinessName} and the site looks slow enough on mobile to hurt both rankings and lead volume.\n\nFor {Niche} in {City}, that usually means people bounce fast and Google pushes competitors above you.\n\nI can send over a short 2-minute walkthrough showing the fastest way to clean that up and turn the page into something that actually brings in business.\n\nBest, {YourName}',
    },
  },
]

const nicheTemplateBaseByCategory = {
  'Paid Ads Agency': {
    ghost_subject_template: '{BusinessName} tracking gap',
    ghost_body_template: 'Hi,\n\nI reviewed {BusinessName} and noticed your paid tracking setup for {Niche} in {City} appears incomplete.\n\nWhen pixel events are missing, retargeting and optimization get weaker, so budget burns with limited learning.\n\nI can share a short fix plan to restore clean signal quality quickly.\n\nBest, {YourName}',
    golden_subject_template: '{BusinessName} paid demand opportunity',
    golden_body_template: 'Hi,\n\nThere is clear paid demand for {Niche} in {City}, but {BusinessName} is not capturing enough of that intent.\n\nThis is usually fixable with better campaign structure, offer match, and event hygiene.\n\nI can send a concise 2-minute action plan tailored to your account.\n\nBest, {YourName}',
    competitor_subject_template: '{BusinessName} paid competitor gap',
    competitor_body_template: 'Hi,\n\nCompetitors are taking stronger paid visibility than {BusinessName} for {Niche} around {City}.\n\nThat normally points to better segmentation and cleaner conversion signaling on their side.\n\nIf useful, I can send a short pressure-gap breakdown with exact fixes.\n\nBest, {YourName}',
    speed_subject_template: '{BusinessName} attribution quality',
    speed_body_template: 'Hi,\n\nI ran a quick quality check and {BusinessName} appears to have attribution friction that can hurt campaign efficiency.\n\nFor {Niche} in {City}, cleaner event mapping usually lowers CPA and improves lead quality.\n\nI can send a practical cleanup checklist in priority order.\n\nBest, {YourName}',
  },
  'SEO & Content': {
    ghost_subject_template: '{BusinessName} content base gap',
    ghost_body_template: 'Hi,\n\nI checked {BusinessName} and the current content footprint for {Niche} in {City} looks too thin for stable page-one visibility.\n\nWithout stronger topical coverage, competitors keep winning high-intent clicks.\n\nI can send a short 3-keyword content roadmap that is fast to implement.\n\nBest, {YourName}',
    golden_subject_template: '{BusinessName} keyword upside',
    golden_body_template: 'Hi,\n\nThere is keyword opportunity in {City} for {Niche}, but {BusinessName} is not taking enough page-one share yet.\n\nUsually this comes down to missing content depth and weak internal topic structure.\n\nI can send a concise ranking plan for the highest-impact terms first.\n\nBest, {YourName}',
    competitor_subject_template: '{BusinessName} ranking gap',
    competitor_body_template: 'Hi,\n\nCompetitors are outranking {BusinessName} for valuable {Niche} searches around {City}.\n\nThis is often a combination of stronger relevance signals and better content architecture.\n\nIf helpful, I can send a brief gap analysis with immediate wins.\n\nBest, {YourName}',
    speed_subject_template: '{BusinessName} technical SEO drag',
    speed_body_template: 'Hi,\n\nI ran a quick technical pass and {BusinessName} has performance issues that likely suppress both rankings and conversion quality.\n\nFor {Niche} in {City}, these issues usually create avoidable traffic loss.\n\nI can share a short technical cleanup sequence to fix this fast.\n\nBest, {YourName}',
  },
  'Lead Gen Agency': {
    ghost_subject_template: '{BusinessName} lead capture leak',
    ghost_body_template: 'Hi,\n\nI reviewed {BusinessName} and your lead capture path for {Niche} in {City} appears weaker than it should be.\n\nWhen CTA flow is unclear, qualified visitors leave without turning into booked conversations.\n\nI can send a short funnel upgrade plan that lifts capture without increasing traffic.\n\nBest, {YourName}',
    golden_subject_template: '{BusinessName} CTA opportunity',
    golden_body_template: 'Hi,\n\n{BusinessName} seems to have intent traffic, but conversion handoff for {Niche} in {City} is not tight enough yet.\n\nThis is typically a message and page-flow issue, not a demand issue.\n\nI can share a concise conversion blueprint you can apply immediately.\n\nBest, {YourName}',
    competitor_subject_template: '{BusinessName} offer positioning gap',
    competitor_body_template: 'Hi,\n\nCompetitors are likely winning more inbound demand because their offer framing is clearer than {BusinessName}.\n\nIn {City} for {Niche}, this creates a silent but expensive inquiry leak.\n\nIf useful, I can send a short positioning and CTA fix plan.\n\nBest, {YourName}',
    speed_subject_template: '{BusinessName} funnel friction',
    speed_body_template: 'Hi,\n\nI ran a quick check and {BusinessName} has avoidable conversion friction that is probably reducing lead volume.\n\nSmall UX and speed adjustments often create outsized gains for {Niche} in {City}.\n\nI can send a practical fix list with priority order.\n\nBest, {YourName}',
  },
  'B2B Service Provider': {
    ghost_subject_template: '{BusinessName} outbound pipeline gap',
    ghost_body_template: 'Hi,\n\nI looked at {BusinessName} and your outbound pipeline for {Niche} appears under-structured right now.\n\nThat usually leads to inconsistent deal flow even when service quality is strong.\n\nI can send a lightweight system for predictable partner sourcing and first-touch outreach.\n\nBest, {YourName}',
    golden_subject_template: '{BusinessName} LinkedIn demand opportunity',
    golden_body_template: 'Hi,\n\n{BusinessName} has strong potential, but LinkedIn demand capture for {Niche} in {City} looks underused.\n\nThis often means high-fit conversations are going to teams with more consistent outbound cadence.\n\nI can send a concise LinkedIn plus email workflow to close that gap.\n\nBest, {YourName}',
    competitor_subject_template: '{BusinessName} partner outreach gap',
    competitor_body_template: 'Hi,\n\nCompetitors are frequently growing faster by running more systematic direct outreach than {BusinessName}.\n\nFor B2B services, this compounds into a measurable pipeline gap over time.\n\nIf helpful, I can send a short partner-sourcing playbook tailored to your ICP.\n\nBest, {YourName}',
    speed_subject_template: '{BusinessName} outreach handoff friction',
    speed_body_template: 'Hi,\n\nI noticed likely friction between sourcing, first touch, and follow-up in your current workflow.\n\nThose handoff gaps usually slow meeting velocity and reduce conversion quality.\n\nI can share a short automation-first sequence to tighten the full pipeline.\n\nBest, {YourName}',
  },
}

function applyPackToneToBody(body, packKey) {
  if (!body) return body
  if (packKey === 'local-first') {
    return body.replace('Hi,\n\n', 'Hi,\n\nI focused on your local market in {City} and found this quick win.\n\n')
  }
  if (packKey === 'aggressive') {
    return body.replace('If useful,', 'Directly:').replace('I can', 'I can immediately')
  }
  return body
}

function resolveMailTemplatePacksForNiche(rawNiche) {
  const niche = String(rawNiche || '').trim()
  const baseTemplates = nicheTemplateBaseByCategory[niche]
  if (!baseTemplates) return mailTemplatePacks
  return mailTemplatePacks.map((pack) => {
    const tonedTemplates = Object.fromEntries(
      Object.entries(baseTemplates).map(([key, value]) => {
        if (!key.endsWith('_body_template')) return [key, value]
        return [key, applyPackToneToBody(value, pack.key)]
      }),
    )
    return {
      ...pack,
      templates: {
        ...pack.templates,
        ...tonedTemplates,
      },
    }
  })
}

function getIdleTask(taskType) {
  return { id: null, task_type: taskType, status: 'idle', running: false, created_at: null, started_at: null, finished_at: null, last_request: null, result: null, error: null }
}

function isBlacklistedLeadStatus(status) {
  const s = String(status || '').trim().toLowerCase()
  return s === 'blacklisted' || s === 'skipped (unsubscribed)'
}

function normalizeLeadStatus(status) {
  const s = String(status || '').trim().toLowerCase()
  if (s === 'emailed') return 'Emailed'
  if (s === 'interested') return 'Interested'
  if (s === 'replied') return 'Replied'
  if (s === 'meeting set') return 'Meeting Set'
  if (s === 'zoom scheduled') return 'Zoom Scheduled'
  if (s === 'closed') return 'Closed'
  if (s === 'paid') return 'Paid'
  if (s === 'failed') return 'Failed'
  if (s === 'generation_failed' || s === 'generation failed') return 'Generation Failed'
  if (s === 'retry_later') return 'retry_later'
  if (s === 'low_priority') return 'low_priority'
  if (s === 'qualified_not_interested' || s === 'qualified not interested') return 'QUALIFIED_NOT_INTERESTED'
  if (s === 'skipped (unsubscribed)') return 'Skipped (Unsubscribed)'
  if (s === 'skipped (test lead)') return 'Skipped (Test Lead)'
  if (s === 'blacklisted') return 'Blacklisted'
  return 'Pending'
}

async function fetchJson(path, options) {
  const apiBaseRaw = String(import.meta.env.VITE_API_BASE_URL || '').trim()
  const apiBase = apiBaseRaw ? apiBaseRaw.replace(/\/$/, '') : ''
  const normalizedPath = String(path || '')
  const forceSameOriginApi =
    normalizedPath.startsWith('/api')
    && typeof window !== 'undefined'
    && String(window.location?.hostname || '').toLowerCase().endsWith('.vercel.app')
  const requestUrl = /^https?:\/\//i.test(String(path || ''))
    ? String(path)
    : forceSameOriginApi
      ? normalizedPath
      : apiBase && normalizedPath.startsWith('/api')
      ? `${apiBase}${path}`
      : path
  const token = getStoredValue('lf_token')
  const headers = {
    ...(options?.headers || {}),
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  }
  const method = String(options?.method || 'GET').toUpperCase()
  const isDynamicPollingEndpoint =
    method === 'GET' && (
      normalizedPath === '/api/tasks'
      || normalizedPath === '/api/task'
      || normalizedPath === '/api/stats'
    )

  const response = await fetch(requestUrl, {
    ...(options || {}),
    headers,
    ...(isDynamicPollingEndpoint ? { cache: 'no-store' } : {}),
  })
  const data = await response.json().catch(() => ({}))
  if (!response.ok) {
    const detail = typeof data.detail === 'string' ? data.detail : `Request failed (${response.status})`
    const error = new Error(detail)
    error.status = response.status
    error.path = requestUrl
    throw error
  }
  return data
}

function buildApiUrl(path) {
  const apiBaseRaw = String(import.meta.env.VITE_API_BASE_URL || '').trim()
  const apiBase = apiBaseRaw ? apiBaseRaw.replace(/\/$/, '') : ''
  const normalizedPath = String(path || '')
  const forceSameOriginApi =
    normalizedPath.startsWith('/api')
    && typeof window !== 'undefined'
    && String(window.location?.hostname || '').toLowerCase().endsWith('.vercel.app')
  if (/^https?:\/\//i.test(String(path || ''))) return String(path)
  if (forceSameOriginApi) return normalizedPath
  return apiBase && normalizedPath.startsWith('/api') ? `${apiBase}${path}` : normalizedPath
}

function isAiEndpoint(path) {
  const endpoint = String(path || '').toLowerCase()
  return endpoint.includes('/api/enrich')
    || endpoint.includes('/api/cold-email-opener')
    || endpoint.includes('/api/mailer/cold-outreach')
    || endpoint.includes('/api/mailer/preview')
    || endpoint.includes('/api/recommend-niche')
}

function getFriendlyAiError(path, status, detail) {
  const normalized = String(detail || '').toLowerCase()
  if (!isAiEndpoint(path)) return detail || 'Unknown API error'
  if (normalized.includes('insufficient_quota') || normalized.includes('quota') || normalized.includes('credit') || normalized.includes('billing')) {
    return 'Please check your API credits.'
  }
  if (status === 429 || normalized.includes('rate limit') || normalized.includes('too many requests')) {
    return 'Our AI is a bit busy, retrying in 5 seconds...'
  }
  if (status >= 500) {
    return 'Our AI is a bit busy, retrying in 5 seconds...'
  }
  return detail || 'Unknown API error'
}

function fmtCountdown(isoString) {
  if (!isoString) return null
  const ms = new Date(isoString).getTime() - Date.now()
  if (ms <= 0) return 'now'
  const totalSec = Math.floor(ms / 1000)
  const min = Math.floor(totalSec / 60)
  const sec = totalSec % 60
  return `${min}m ${String(sec).padStart(2, '0')}s`
}

function fmtDigestCountdown() {
  const now = new Date()
  const next = new Date()
  next.setUTCHours(8, 0, 0, 0)
  if (next <= now) next.setUTCDate(next.getUTCDate() + 1)
  const ms = next.getTime() - now.getTime()
  const h = Math.floor(ms / 3600000)
  const m = Math.floor((ms % 3600000) / 60000)
  const s = Math.floor((ms % 60000) / 1000)
  return `${h}h ${String(m).padStart(2, '0')}m ${String(s).padStart(2, '0')}s`
}

function formatCurrencyEur(value) {
  return `${Number(value || 0).toLocaleString('de-DE')} €`
}

function formatGoalCurrency(value, currency = DEFAULT_GOAL_CURRENCY) {
  const amount = Number(value || 0)
  const safeCurrency = GOAL_CURRENCY_OPTIONS.includes(String(currency || '').toUpperCase())
    ? String(currency || '').toUpperCase()
    : DEFAULT_GOAL_CURRENCY
  try {
    return new Intl.NumberFormat('de-DE', {
      style: 'currency',
      currency: safeCurrency,
      maximumFractionDigits: 0,
    }).format(amount)
  } catch {
    return `${amount.toLocaleString('de-DE')} ${safeCurrency}`
  }
}

function dayKey(raw) {
  if (!raw) return null
  const date = new Date(raw)
  if (Number.isNaN(date.getTime())) return null
  return date.toISOString().slice(0, 10)
}

function formatFeedTime(raw) {
  if (!raw) return '--:--'
  const date = new Date(raw)
  if (Number.isNaN(date.getTime())) return '--:--'
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

function deriveToneProfile(subject, body) {
  const text = `${subject || ''} ${body || ''}`.toLowerCase()
  const urgentHits = ['urgent', 'immediately', 'losing', 'penalized', 'quick question'].reduce((sum, token) => sum + (text.includes(token) ? 1 : 0), 0)
  const helpfulHits = ['help', 'show', 'walkthrough', 'plan', 'open to', 'no pressure'].reduce((sum, token) => sum + (text.includes(token) ? 1 : 0), 0)
  const professionalHits = ['strategy', 'business', 'local', 'conversion', 'google'].reduce((sum, token) => sum + (text.includes(token) ? 1 : 0), 0)

  const scores = {
    Professional: Math.min(100, 30 + professionalHits * 14),
    Urgent: Math.min(100, 20 + urgentHits * 18),
    Helpful: Math.min(100, 25 + helpfulHits * 15),
  }

  const dominant = Object.entries(scores).sort((a, b) => b[1] - a[1])[0] || ['Professional', 0]
  return {
    dominantLabel: dominant[0],
    dominantScore: dominant[1],
    scores,
  }
}

function buildSparkPoints(values, width = 160, height = 44) {
  const safe = Array.isArray(values) && values.length ? values : [0]
  const max = Math.max(...safe, 1)
  const stepX = safe.length > 1 ? width / (safe.length - 1) : width
  return safe.map((value, index) => {
    const x = index * stepX
    const y = height - ((value || 0) / max) * height
    return `${x},${y}`
  }).join(' ')
}

function scoreHeatTone(score) {
  const numeric = Number(score || 0)
  if (numeric >= 9) return 'score-orb-hot'
  if (numeric >= 7) return 'score-orb-warm'
  if (numeric >= 4) return 'score-orb-mid'
  return 'score-orb-low'
}

function normalizeTierValue(tier) {
  const t = String(tier || '').trim().toLowerCase()
  return t === 'premium_ads' ? 'premium_ads' : 'standard'
}

const qualifiedLeadStatuses = new Set([
  'queued_mail',
  'emailed',
  'interested',
  'replied',
  'meeting set',
  'zoom scheduled',
  'closed',
  'paid',
  'qualified_not_interested',
  'qualified not interested',
])

const mailedLeadStatuses = new Set([
  'emailed',
  'replied',
  'interested',
  'meeting set',
  'zoom scheduled',
  'closed',
  'paid',
])

const repliedLeadStatuses = new Set([
  'replied',
  'interested',
  'meeting set',
  'zoom scheduled',
  'closed',
  'paid',
  'qualified_not_interested',
  'qualified not interested',
])

function isQualifiedLead(lead) {
  const status = String(lead?.status || '').toLowerCase().trim()
  const score = Number(lead?.ai_score || 0)
  return qualifiedLeadStatuses.has(status) || score >= 7
}

function hasSentMail(lead) {
  const status = String(lead?.status || '').toLowerCase().trim()
  return Boolean(lead?.sent_at || lead?.last_contacted_at || lead?.last_sender_email || mailedLeadStatuses.has(status))
}

function hasOpenedMail(lead) {
  return Boolean(Number(lead?.open_count || 0) > 0 || lead?.first_opened_at || lead?.last_opened_at)
}

function hasReply(lead) {
  const status = String(lead?.status || '').toLowerCase().trim()
  return repliedLeadStatuses.has(status)
}

function normalizeLeadInsightList(value, limit = 3) {
  const raw = Array.isArray(value)
    ? value
    : typeof value === 'string'
      ? value.split(/\n|\||;|•/)
      : []

  return raw
    .map((item) => String(item || '').trim())
    .filter(Boolean)
    .slice(0, limit)
}

function resolvePipelineStage(lead) {
  const explicit = String(lead?.pipeline_stage || '').trim().toLowerCase()
  if (explicit === 'contacted') return 'Contacted'
  if (explicit === 'replied') return 'Replied'
  if (explicit === 'won (paid)' || explicit === 'won paid') return 'Won (Paid)'
  if (explicit === 'scraped') return 'Scraped'

  const status = String(lead?.status || '').trim().toLowerCase()
  if (lead?.paid_at || ['paid', 'closed', 'won (paid)'].includes(status)) return 'Won (Paid)'
  if (lead?.reply_detected_at || ['replied', 'interested', 'meeting set', 'zoom scheduled'].includes(status)) return 'Replied'
  if (lead?.sent_at || lead?.last_contacted_at || ['emailed', 'contacted', 'failed', 'bounced'].includes(status)) return 'Contacted'
  return 'Scraped'
}

function pipelineStageBadgeClass(stage) {
  if (stage === 'Won (Paid)') return 'border-amber-500/30 bg-amber-500/10 text-amber-200'
  if (stage === 'Replied') return 'border-emerald-500/30 bg-emerald-500/10 text-emerald-200'
  if (stage === 'Contacted') return 'border-cyan-500/30 bg-cyan-500/10 text-cyan-200'
  return 'border-slate-600/40 bg-slate-800/70 text-slate-300'
}

function normalizeLeadScoreTen(rawScore) {
  const numeric = Number(rawScore || 0)
  if (!Number.isFinite(numeric) || numeric <= 0) return 0
  const normalized = numeric > 10 ? numeric / 10 : numeric
  return Math.max(0, Math.min(10, Math.round(normalized * 10) / 10))
}

function formatLeadScoreValue(rawScore) {
  const normalized = normalizeLeadScoreTen(rawScore)
  if (normalized <= 0) return '0'
  return Number.isInteger(normalized) ? String(normalized) : normalized.toFixed(1)
}

function resolveBestLeadScore(lead) {
  const aiScore = Number(lead?.ai_score ?? 0)
  if (Number.isFinite(aiScore) && aiScore > 0) {
    return normalizeLeadScoreTen(aiScore)
  }

  const directScore = Number(lead?.best_lead_score ?? lead?.lead_score_100 ?? lead?.score_100 ?? 0)
  if (Number.isFinite(directScore) && directScore > 0) {
    return normalizeLeadScoreTen(directScore)
  }

  const aiSentiment = Number(lead?.ai_sentiment_score ?? (aiScore <= 10 ? aiScore * 10 : aiScore) ?? 0)
  const employeeCount = Number(lead?.employee_count ?? 0)
  const emailComponent = lead?.email ? 40 : 8
  const sizeComponent = employeeCount >= 100 ? 30 : employeeCount >= 40 ? 26 : employeeCount >= 15 ? 22 : employeeCount >= 5 ? 16 : 10
  const fallbackScore = Math.min(100, emailComponent + sizeComponent + Math.max(0, Math.min(100, aiSentiment)) * 0.3)
  return normalizeLeadScoreTen(fallbackScore)
}

function resolveLeadSignalScore(lead) {
  const rawSignal = Number(lead?.ai_sentiment_score ?? lead?.lead_score_100 ?? lead?.score_100 ?? 0)
  if (Number.isFinite(rawSignal) && rawSignal > 0) {
    return normalizeLeadScoreTen(rawSignal)
  }
  return resolveBestLeadScore(lead)
}

function titleCaseLeadLabel(value) {
  return String(value || '')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase())
}

function deriveLeadIndustry(lead) {
  const raw = String(lead?.company_audit?.industry || lead?.industry || lead?.search_keyword || '').trim()
  if (!raw) return 'Other'
  const primary = raw.split(/[|,]/)[0]?.trim() || raw
  const compact = primary.split(/\s+/).filter(Boolean).slice(0, 2).join(' ')
  return titleCaseLeadLabel(compact || primary)
}

function deriveLeadRevenueBand(lead) {
  const employees = Number(lead?.employee_count || 0)
  if (employees >= 200) return '$10M+'
  if (employees >= 50) return '$2M-$10M'
  if (employees >= 10) return '$500k-$2M'
  return '<$500k'
}

function normalizeSavedSegmentFilters(filters) {
  const advanced = filters?.advancedLeadFilters || {}
  return {
    leadSearch: String(filters?.leadSearch || ''),
    leadStatusFilter: String(filters?.leadStatusFilter || 'all'),
    leadQuickFilter: String(filters?.leadQuickFilter || 'all'),
    leadSortMode: String(filters?.leadSortMode || 'best'),
    showBlacklisted: Boolean(filters?.showBlacklisted),
    advancedLeadFilters: {
      industries: Array.isArray(advanced?.industries) ? advanced.industries : [],
      revenueBands: Array.isArray(advanced?.revenueBands) ? advanced.revenueBands : [],
      techStacks: Array.isArray(advanced?.techStacks) ? advanced.techStacks : [],
      highScoreOnly: Boolean(advanced?.highScoreOnly),
    },
  }
}

function describeSavedSegment(segment) {
  const filters = normalizeSavedSegmentFilters(segment?.filters || {})
  const parts = []
  if (filters.advancedLeadFilters.techStacks.length) {
    parts.push(filters.advancedLeadFilters.techStacks.slice(0, 2).join(' + '))
  }
  if (filters.advancedLeadFilters.highScoreOnly) {
    parts.push('Score > 8/10')
  }
  if (filters.leadStatusFilter !== 'all') {
    parts.push(titleCaseLeadLabel(filters.leadStatusFilter))
  }
  if (filters.advancedLeadFilters.industries.length) {
    parts.push(filters.advancedLeadFilters.industries.slice(0, 1).join(''))
  }
  return parts.join(' • ') || 'One-click lead view'
}

function resolveQualifierBucketCount(data, bucket) {
  const counts = data?.counts || {}
  const explicit = bucket.countKeys
    .map((key) => Number(counts?.[key] || 0))
    .find((value) => Number.isFinite(value) && value > 0)
  if (Number.isFinite(explicit) && explicit > 0) return explicit

  return bucket.listKeys
    .map((key) => (Array.isArray(data?.[key]) ? data[key].length : 0))
    .find((value) => Number.isFinite(value) && value > 0) || 0
}

function resolveNicheLossMultiplier(rawNiche) {
  const niche = String(rawNiche || '').toLowerCase()
  if (!niche) return 1
  for (const rule of QUALIFIER_LOSS_MULTIPLIER_RULES) {
    if (rule.terms.some((term) => niche.includes(term))) return rule.multiplier
  }
  return 1
}

function buildQualifierLossInsight(data) {
  if (!data) return null
  const ranked = QUALIFIER_FINDING_MODELS
    .map((bucket) => ({
      ...bucket,
      count: resolveQualifierBucketCount(data, bucket),
    }))
    .filter((bucket) => bucket.count > 0)
    .sort((a, b) => b.count - a.count)

  if (!ranked.length) return null

  const dominant = ranked[0]
  const nicheMultiplier = resolveNicheLossMultiplier(data?.selected_niche)
  const estimatedMonthlyLoss = Math.round((dominant.count * dominant.perLeadLoss * nicheMultiplier) / 50) * 50

  return {
    leadCount: dominant.count,
    finding: dominant.finding,
    estimatedMonthlyLoss,
    niche: String(data?.selected_niche || '').trim() || 'your niche',
  }
}

function shootConfetti() {
  confetti({ particleCount: 120, spread: 80, origin: { y: 0.6 }, colors: ['#14b8a6', '#f59e0b', '#22d3ee', '#fff'] })
  setTimeout(() => confetti({ particleCount: 60, spread: 120, origin: { y: 0.4 } }), 350)
}

function formatTaskPayload(payload, taskType) {
  if (!payload || typeof payload !== 'object') return '—'
  const type = String(taskType || '').toLowerCase()

  if (type === 'scrape') {
    const keyword = payload.keyword || '—'
    const results = payload.results || '—'
    const country = payload.country || 'US'
    const headless = payload.headless ? 'headless' : 'visible'
    return `Keyword: ${keyword}\nResults: ${results}\nCountry: ${country}\nBrowser: ${headless}`
  }

  if (type === 'enrich') {
    const limit = payload.limit || '—'
    const headless = payload.headless ? 'headless' : 'visible'
    return `Limit: ${limit}\nBrowser: ${headless}`
  }

  if (type === 'mailer') {
    const limit = payload.limit || '—'
    const delayMin = payload.delay_min || 0
    const delayMax = payload.delay_max || 0
    const dripFeed = payload.drip_feed ? 'Yes' : 'No'
    return `Limit: ${limit}\nDelay: ${delayMin}-${delayMax}ms\nDrip Feed: ${dripFeed}`
  }

  return JSON.stringify(payload)
}

function formatTaskResult(result, taskType, error) {
  if (error) return `Error: ${error}`
  if (!result || typeof result !== 'object') return '—'

  const type = String(taskType || '').toLowerCase()

  if (type === 'scrape') {
    const scraped = result.scraped || 0
    const inserted = result.inserted || 0
    const duplicates = result.duplicates || 0
    return `Scraped: ${scraped}\nNew: ${inserted}\nDuplicates: ${duplicates}`
  }

  if (type === 'enrich') {
    const processed = result.processed || 0
    const queued = result.queued_for_mail || 0
    return `Processed: ${processed}\nQueued for Mail: ${queued}`
  }

  if (type === 'mailer') {
    const sent = result.sent || 0
    const skipped = result.skipped || 0
    const failed = result.failed || 0
    const dripFeed = result.drip_feed ? `\nNext drip: ${new Date(result.next_drip_at).toLocaleString()}` : ''
    return `Sent: ${sent}\nSkipped: ${skipped}\nFailed: ${failed}${dripFeed}`
  }

  return JSON.stringify(result)
}

function toIsoDate(value) {
  if (!value) return null
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return null
  return d
}

function formatDeliveryCountdown(dueAt) {
  const due = toIsoDate(dueAt)
  if (!due) return 'No ETA'
  const now = new Date()
  const diffMs = due.getTime() - now.getTime()
  if (diffMs <= 0) return 'Due now'
  const mins = Math.ceil(diffMs / 60000)
  if (mins < 60) return `Est. ${mins} min`
  const hrs = Math.floor(mins / 60)
  const rem = mins % 60
  return rem ? `Est. ${hrs}h ${rem}m` : `Est. ${hrs}h`
}

function mapDeliveryTaskType(taskType) {
  const raw = String(taskType || '').toLowerCase().trim()
  if (raw.includes('export') || raw.includes('csv')) return { label: 'Bulk Export', action: 'download' }
  if (raw.includes('enrich')) return { label: 'AI Enrichment', action: 'view' }
  if (raw.includes('scrap') || raw.includes('mine')) return { label: 'Lead Mining', action: 'view' }
  if (raw.includes('lead')) return { label: 'Lead Mining', action: 'view' }
  return { label: 'AI Enrichment', action: 'view' }
}

function QualifierLeadCard({ lead, accentClass, badgeClass }) {
  const stars = typeof lead.rating === 'number' ? `${lead.rating.toFixed(1)}★` : '—'
  const [hookCopied, setHookCopied] = useState(false)

  async function copySuggestedHook() {
    const hook = String(lead?.suggested_hook || '').trim()
    if (!hook) return
    try {
      if (!navigator?.clipboard?.writeText) throw new Error('Clipboard unavailable')
      await navigator.clipboard.writeText(hook)
      setHookCopied(true)
      window.setTimeout(() => setHookCopied(false), 1600)
      toast.success('Suggested hook copied')
    } catch {
      toast.error('Could not copy suggested hook')
    }
  }

  return (
    <div className={`rounded-2xl border p-4 ${accentClass}`}>
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="flex-1 min-w-0">
          <p className="font-semibold text-white text-sm truncate">{lead.business_name}</p>
          <p className="text-xs text-slate-400 mt-0.5 truncate">{lead.city || lead.address || '—'}</p>
        </div>
        <div className="flex flex-shrink-0 flex-wrap gap-1.5">
          <span className={`rounded-full px-2 py-0.5 text-[11px] font-semibold ${badgeClass}`}>{stars}</span>
          <span className="rounded-full bg-white/5 px-2 py-0.5 text-[11px] font-semibold text-slate-400">
            {lead.review_count ?? 0} reviews
          </span>
          {lead.search_keyword && (
            <span className="rounded-full bg-white/5 px-2 py-0.5 text-[11px] text-slate-500">{lead.search_keyword}</span>
          )}
          {lead.ai_score != null && (
            <span className="rounded-full bg-white/5 px-2 py-0.5 text-[11px] text-slate-500">score {lead.ai_score}</span>
          )}
        </div>
      </div>
      {lead.pain_point && (
        <div className="mt-3 rounded-xl bg-black/20 px-3 py-2.5">
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500 mb-1">Pain Point</p>
          <p className="text-sm text-slate-300 leading-relaxed">{lead.pain_point}</p>
        </div>
      )}
      {lead.suggested_hook && (
        <div className="mt-3 rounded-xl border border-cyan-400/20 bg-cyan-900/10 px-3 py-2.5">
          <div className="mb-1 flex items-center justify-between gap-2">
            <p className="text-xs font-semibold uppercase tracking-wide text-cyan-300/80">Suggested Hook</p>
            <button
              type="button"
              onClick={copySuggestedHook}
              disabled={hookCopied}
              className="inline-flex items-center gap-1 rounded-md border border-cyan-300/30 bg-cyan-400/10 px-2 py-1 text-[11px] font-semibold text-cyan-200 transition hover:bg-cyan-400/20 disabled:cursor-not-allowed disabled:opacity-70"
              title="Copy suggested hook"
            >
              {hookCopied ? <CheckCircle2 className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
              {hookCopied ? 'Copied!' : 'Copy Hook'}
            </button>
          </div>
          <p className="text-sm text-slate-200 leading-relaxed">{lead.suggested_hook}</p>
        </div>
      )}
    </div>
  )
}

function TaskManagerCard({
  item,
  isFading,
  isUpdating,
  keyboardMode,
  onToggleDone,
  onStatusChange,
  onNoteChange,
  onDelete,
  onPreviewAiMessage,
  onViewLeads,
  onDownload,
}) {
  const {
    attributes,
    listeners,
    setNodeRef,
    setActivatorNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: String(item.id) })
  const isCustom = item.source === 'custom'
  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
  }

  return (
    <Motion.article
      ref={setNodeRef}
      style={style}
      layout
      transition={{ type: 'spring', stiffness: 420, damping: 34, mass: 0.55 }}
      className={`group relative rounded-2xl border border-slate-700/50 bg-slate-900/70 p-4 shadow-[0_10px_35px_rgba(2,6,23,0.28)] transition-all duration-200 focus-within:ring-2 focus-within:ring-blue-500/75 focus-within:ring-offset-2 focus-within:ring-offset-slate-900 ${
        isDragging ? 'scale-[1.01] border-blue-400/50 shadow-[0_22px_52px_rgba(59,130,246,0.35)]' : ''
      } ${
        keyboardMode && isDragging ? 'ring-2 ring-blue-500 ring-offset-2 ring-offset-slate-900' : ''
      } ${
        isFading ? 'opacity-0 scale-[0.98]' : 'opacity-100 scale-100'
      }`}
    >
      <button
        type="button"
        className="absolute right-3 top-3 rounded-md p-1 text-slate-500 opacity-0 transition hover:bg-rose-500/15 hover:text-rose-300 group-hover:opacity-100"
        onClick={() => onDelete(item)}
        aria-label="Delete task"
        title="Delete task"
      >
        <Trash2 className="h-4 w-4" />
      </button>

      <div className="flex flex-wrap items-start gap-3">
        <div className="flex items-start gap-2 pt-0.5">
          <button
            type="button"
            ref={setActivatorNodeRef}
            className="cursor-grab rounded-md border border-slate-700 bg-slate-800/70 p-1 text-slate-400 outline-none transition hover:text-cyan-200 focus-visible:ring-2 focus-visible:ring-blue-500 active:cursor-grabbing"
            {...attributes}
            {...listeners}
            title="Drag to reorder"
            aria-label="Drag to reorder. Press Space then Arrow keys for keyboard sorting."
          >
            <GripVertical className="h-4 w-4" />
          </button>
          <label className="inline-flex items-center">
            <input
              type="checkbox"
              className="h-4 w-4 rounded border-slate-600 bg-slate-800 accent-cyan-400"
              checked={Boolean(item.checked)}
              onChange={(e) => void onToggleDone(item, e.target.checked)}
            />
          </label>
        </div>

        <div className="min-w-0 flex-1 pr-10">
          <div className="flex flex-wrap items-center gap-2">
            <p className="truncate text-sm font-semibold text-white">{item.title}</p>
            <span className={`inline-flex h-2.5 w-2.5 rounded-full ${priorityDotClass(item.priority)}`} />
            <span className="text-xs text-slate-300">{item.priority}</span>
            {item.source === 'auto' && (
              <span className="rounded-full border border-cyan-400/30 bg-cyan-500/10 px-2 py-0.5 text-[10px] font-semibold text-cyan-200">AI Automation</span>
            )}
          </div>

          <div className="mt-2 grid gap-2 md:grid-cols-[180px_200px_1fr_auto] md:items-center">
            <div>
              <p className="text-[10px] uppercase tracking-[0.14em] text-slate-500">Status</p>
              <div className="relative mt-1">
                <select
                  className="glass-input appearance-none pr-8 text-xs"
                  disabled={!isCustom && isUpdating}
                  value={item.status}
                  onChange={(e) => void onStatusChange(item, e.target.value)}
                >
                  {TASK_MANAGER_STATUSES.map((value) => <option key={value} value={value}>{value}</option>)}
                </select>
                <ChevronDown className="select-chevron" />
              </div>
            </div>

            <div>
              <p className="text-[10px] uppercase tracking-[0.14em] text-slate-500">Worker</p>
              <p className="mt-2 inline-flex items-center gap-1.5 rounded-full border border-slate-700 bg-slate-800/60 px-2.5 py-1 text-xs text-slate-200">
                <TerminalSquare className="h-3.5 w-3.5 text-cyan-300" />
                {item.workerLabel}
              </p>
            </div>

            <div>
              <p className="text-[10px] uppercase tracking-[0.14em] text-slate-500">Note</p>
              <input
                className="glass-input mt-1 text-xs"
                value={item.note || ''}
                placeholder="He said to call after 3 PM"
                onChange={(e) => {
                  if (isCustom) onNoteChange(item, e.target.value)
                }}
                onBlur={(e) => {
                  if (!isCustom) {
                    void onNoteChange(item, e.target.value)
                  }
                }}
              />
            </div>

            <div className="flex items-center justify-end gap-2">
              <span className="text-[11px] text-slate-400">{formatDeliveryCountdown(item.dueAt)}</span>
              {item.source === 'auto' && (
                <button
                  type="button"
                  className="quick-action-btn"
                  disabled={!item.canPreviewAiMessage}
                  onClick={() => onPreviewAiMessage(item)}
                  title={item.canPreviewAiMessage ? 'Open AI message preview' : 'No AI message generated yet'}
                >
                  <Eye className="h-3.5 w-3.5" /> Preview AI Message
                </button>
              )}
              {item.source === 'auto' && item.action === 'download' ? (
                <button type="button" className="quick-action-btn" onClick={onDownload}>
                  <Download className="h-3.5 w-3.5" /> Download CSV
                </button>
              ) : (
                <button type="button" className="quick-action-btn" onClick={onViewLeads}>
                  <Eye className="h-3.5 w-3.5" /> View Leads
                </button>
              )}
            </div>
          </div>
        </div>
      </div>
    </Motion.article>
  )
}

function App({ initialTab = 'leads' }) {
  const sessionToken = getStoredValue('lf_token')
  const hasSessionToken = Boolean(sessionToken)
  const displayName = getStoredValue('lf_display_name') || getStoredValue('lf_email') || 'there'
  const currentUserEmail = getStoredValue('lf_email') || ''
  const currentUserName = getStoredValue('lf_display_name') || getStoredValue('lf_contact_name') || ''
  const [user, setUser] = useState(() => ({
    credits: Number(getStoredValue('lf_credits') || DEFAULT_FREE_CREDIT_LIMIT),
    credits_balance: Number(getStoredValue('lf_credits_balance') || getStoredValue('lf_credits') || DEFAULT_FREE_CREDIT_LIMIT),
    topup_credits_balance: Number(getStoredValue('lf_topup_credits_balance') || 0),
    credits_limit: Number(getStoredValue('lf_credits_limit') || DEFAULT_FREE_CREDIT_LIMIT),
    isSubscribed: String(getStoredValue('lf_is_subscribed') || '').trim().toLowerCase() === 'true',
    subscription_active: String(getStoredValue('lf_is_subscribed') || '').trim().toLowerCase() === 'true',
    subscription_status: '',
    subscription_cancel_at: null,
    subscription_cancel_at_period_end: false,
    currentPlanName: String(getStoredValue('lf_plan_name') || 'Free Plan').trim() || 'Free Plan',
    plan_key: String(getStoredValue('lf_plan_key') || 'free').trim().toLowerCase() || 'free',
    plan_type: String(getStoredValue('lf_plan_key') || 'free').trim().toLowerCase() || 'free',
    feature_access: getDefaultFeatureAccess(getStoredValue('lf_plan_key') || 'free'),
    average_deal_value: Number(getStoredValue('lf_average_deal_value') || DEFAULT_AVERAGE_DEAL_VALUE),
    niche: String(getStoredValue('lf_niche') || '').trim(),
  }))
  const [searchParams, setSearchParams] = useSearchParams()
  const initialTabResolved = normalizeTabParam(searchParams.get('tab'), normalizeTabParam(initialTab, 'leads'))
  const [health, setHealth] = useState('checking')
  const [configHealth, setConfigHealth] = useState({ ok: false, openai_ok: false, smtp_ok: false, error: null })
  const [leads, setLeads] = useState([])
  const [stats, setStats] = useState({
    total_leads: 0,
    emails_sent: 0,
    opened_count: 0,
    opens_total: 0,
    open_rate: 0,
    paid_count: 0,
    total_revenue: 0,
    setup_revenue: 0,
    setup_milestone: SETUP_MILESTONE_EUR,
    milestone_progress_pct: 0,
    monthly_recurring_revenue: 0,
    website_clients: 0,
    ads_clients: 0,
    ads_and_website_clients: 0,
    mrr_goal: MRR_GOAL_EUR,
    queued_mail_count: 0,
    next_drip_at: null,
    reply_rate: 0,
    replies_count: 0,
    found_this_month: 0,
    contacted_this_month: 0,
    replied_this_month: 0,
    won_this_month: 0,
    found_this_week: 0,
    contacted_this_week: 0,
    replied_this_week: 0,
    won_this_week: 0,
    client_folder_count: 0,
    pipeline: { scraped: 0, contacted: 0, replied: 0, won_paid: 0 },
  })
  const [tasks, setTasks] = useState({})
  const [taskHistory, setTaskHistory] = useState([])
  const [scrapeForm, setScrapeForm] = useState(defaultScrape)
  const [enrichForm, setEnrichForm] = useState(defaultEnrich)
  const [mailerForm, setMailerForm] = useState(defaultMailer)
  const [campaignStats, setCampaignStats] = useState({
    sent: 0,
    opened: 0,
    replied: 0,
    bounced: 0,
    opens_total: 0,
    open_rate: 0,
    reply_rate: 0,
    bounce_rate: 0,
    ab_breakdown: { A: 0, B: 0 },
    sequences: [],
    saved_templates: [],
    recent_events: [],
  })
  const [sequenceForm, setSequenceForm] = useState(defaultCampaignSequenceForm)
  const [campaignLoading, setCampaignLoading] = useState(false)
  const [savingSequence, setSavingSequence] = useState(false)
  const [manualLeadForm, setManualLeadForm] = useState(defaultManualLead)
  const [pendingRequest, setPendingRequest] = useState('')
  const [pendingStatusLeadId, setPendingStatusLeadId] = useState(null)
  const [pendingTierLeadId, setPendingTierLeadId] = useState(null)
  const [retryingTaskId, setRetryingTaskId] = useState(null)
  const [, setLastResult] = useState('')
  const [lastError, setLastError] = useState('')
  const [enrichRetrySeconds, setEnrichRetrySeconds] = useState(0)
  const [activeTab, setActiveTab] = useState(initialTabResolved)
  const [countdown, setCountdown] = useState(null)
  const [digestCountdown, setDigestCountdown] = useState(() => fmtDigestCountdown())
  // (job queue removed — direct execution)
  const [leadSearch, setLeadSearch] = useState('')
  // useDebounce replaces the manual setTimeout useEffect — avoids CPU spikes on every keystroke
  const debouncedLeadSearch = useDebounce(leadSearch, 300)
  const [leadPage, setLeadPage] = useState(0)
  const [leadStatusFilter, setLeadStatusFilter] = useState('all')
  const [leadQuickFilter, setLeadQuickFilter] = useState('all')
  const [leadSortMode, setLeadSortMode] = useState('best')
  const [showBlacklisted, setShowBlacklisted] = useState(false)
  const [loadingLeads, setLoadingLeads] = useState(false)
  const [leadServerTotal, setLeadServerTotal] = useState(0)
  const [lastLeadsApiPayload, setLastLeadsApiPayload] = useState(null)
  const [leadFilterPanelOpen, setLeadFilterPanelOpen] = useState(false)
  const [advancedLeadFilters, setAdvancedLeadFilters] = useState({
    industries: [],
    revenueBands: [],
    techStacks: [],
    highScoreOnly: false,
  })
  const [savedSegments, setSavedSegments] = useState([])
  const [loadingSavedSegments, setLoadingSavedSegments] = useState(false)

  const refreshLeads = useCallback(async (options = {}) => {
    const silent = options?.silent !== undefined ? options.silent : true
    if (!silent) {
      setLoadingLeads(true)
    }
    try {
      const params = new URLSearchParams({
        limit: String(LEADS_PAGE_SIZE),
        page: String(leadPage + 1),
        sort: String(leadSortMode || 'recent'),
        include_blacklisted: showBlacklisted ? '1' : '0',
        _ts: String(Date.now()),
      })
      if (leadStatusFilter !== 'all') {
        params.set('status', leadStatusFilter)
      }
      if (leadQuickFilter !== 'all') {
        params.set('quick_filter', leadQuickFilter)
      }
      if (debouncedLeadSearch.trim()) {
        params.set('search', debouncedLeadSearch.trim())
      }
      const data = await fetchJson(`/api/leads?${params.toString()}`)
      const items = Array.isArray(data?.items)
        ? data.items
        : Array.isArray(data?.leads)
          ? data.leads
          : Array.isArray(data?.data)
            ? data.data
            : []
      console.log('[LeadManagement] /api/leads response', {
        url: `/api/leads?${params.toString()}`,
        total: Number(data?.total || data?.count || items.length || 0),
        itemsLength: items.length,
        sample: items.slice(0, 3),
      })
      setLastLeadsApiPayload(data)
      setLeads(items)
      setLeadServerTotal(Number(data?.total || data?.count || data?.total_count || items.length || 0))
    } catch (error) {
      setLastError(error instanceof Error ? error.message : 'Unknown error while loading leads')
    } finally {
      if (!silent) {
        setLoadingLeads(false)
      }
    }
  }, [debouncedLeadSearch, leadPage, leadQuickFilter, leadSortMode, leadStatusFilter, showBlacklisted])
  const [savingSegment, setSavingSegment] = useState(false)
  const [segmentNameDraft, setSegmentNameDraft] = useState('')
  const [deletingSegmentId, setDeletingSegmentId] = useState(null)
  const [exportingTargets, setExportingTargets] = useState(false)
  const [exportingAI, setExportingAI] = useState(false)
  const [webhookExporting, setWebhookExporting] = useState('')
  const [weeklyReport, setWeeklyReport] = useState(null)
  const [loadingWeeklyReport, setLoadingWeeklyReport] = useState(false)
  const [sendingWeeklyReport, setSendingWeeklyReport] = useState(false)
  const [monthlyReport, setMonthlyReport] = useState(null)
  const [loadingMonthlyReport, setLoadingMonthlyReport] = useState(false)
  const [sendingMonthlyReport, setSendingMonthlyReport] = useState(false)
  const [clientFolders, setClientFolders] = useState([])
  const [loadingClientFolders, setLoadingClientFolders] = useState(false)
  const [assigningClientFolderLeadId, setAssigningClientFolderLeadId] = useState(null)
  const [clientFolderForm, setClientFolderForm] = useState({ name: '', description: '' })
  const [creatingClientFolder, setCreatingClientFolder] = useState(false)
  const [clientDashboard, setClientDashboard] = useState({
    total_clients: 0,
    active_clients: 0,
    folder_count: 0,
    unassigned_count: 0,
    pipeline: { scraped: 0, contacted: 0, replied: 0, won_paid: 0 },
    folders: [],
  })
  const [loadingClientDashboard, setLoadingClientDashboard] = useState(false)
  const [configForm, setConfigForm] = useState({
    smtp_accounts: [createEmptySmtpAccount()],
    sending_strategy: 'round_robin',
    open_tracking_base_url: '',
    proxy_urls: '',
    hubspot_webhook_url: '',
    google_sheets_webhook_url: '',
    auto_weekly_report_email: true,
    auto_monthly_report_email: true,
    mail_signature: '',
    ghost_subject_template: '',
    ghost_body_template: '',
    golden_subject_template: '',
    golden_body_template: '',
    competitor_subject_template: '',
    competitor_body_template: '',
    speed_subject_template: '',
    speed_body_template: '',
  })
  const [configFormLoaded, setConfigFormLoaded] = useState(false)
  const [savingConfig, setSavingConfig] = useState(false)
  const [showSmtpPasswords, setShowSmtpPasswords] = useState({})
  const [smtpTestResults, setSmtpTestResults] = useState({})
  const [testingSmtpIndex, setTestingSmtpIndex] = useState(null)
  const [templatePreview, setTemplatePreview] = useState({ mode: '', subject: '', body: '' })
  const [pendingBlacklistLeadId, setPendingBlacklistLeadId] = useState(null)
  const [pendingBlacklistEntryKey, setPendingBlacklistEntryKey] = useState('')
  const [blacklistEntries, setBlacklistEntries] = useState([])
  const [blacklistForm, setBlacklistForm] = useState({ kind: 'email', value: '', reason: 'Manual dashboard block' })
  const [submittingBlacklistEntry, setSubmittingBlacklistEntry] = useState(false)
  const [showTopUpModal, setShowTopUpModal] = useState(false)
  const [topUpLoadingPackageId, setTopUpLoadingPackageId] = useState('')
  const [topUpPreparingPackageId, setTopUpPreparingPackageId] = useState('')
  const [selectedTopUpPackageId, setSelectedTopUpPackageId] = useState(TOP_UP_PACKAGES[3]?.id || TOP_UP_PACKAGES[0]?.id || '')
  const topUpCheckoutInFlightRef = useRef({})
  const [animatedCreditsPercent, setAnimatedCreditsPercent] = useState(0)
  const [showSaleModal, setShowSaleModal] = useState(false)
  const [saleForm, setSaleForm] = useState({ amount: '', serviceType: 'Google Ads Setup', leadName: '', leadId: '', isRecurring: false })
  const [submittingSale, setSubmittingSale] = useState(false)
  const [revenueLog, setRevenueLog] = useState([])
  const [workers, setWorkers] = useState([])
  const [workerMetrics, setWorkerMetrics] = useState({ total_team_cost: 0, delivery_efficiency_days: 0, net_agency_margin: 0 })
  const [workerAudit, setWorkerAudit] = useState([])
  const [showHireWorkerForm, setShowHireWorkerForm] = useState(false)
  const [workerForm, setWorkerForm] = useState(defaultWorkerForm)
  const [creatingWorker, setCreatingWorker] = useState(false)
  const [editingWorkerId, setEditingWorkerId] = useState(null)
  const [workerEditForm, setWorkerEditForm] = useState(defaultWorkerForm)
  const [deletingWorkerId, setDeletingWorkerId] = useState(null)
  const [assigningWorkerLeadId, setAssigningWorkerLeadId] = useState(null)
  const [deliveryTasks, setDeliveryTasks] = useState([])
  const [deliverySummary, setDeliverySummary] = useState({ total: 0, todo: 0, in_progress: 0, blocked: 0, done: 0 })
  const [updatingDeliveryTaskId, setUpdatingDeliveryTaskId] = useState(null)
  const [showCustomTaskForm, setShowCustomTaskForm] = useState(false)
  const [customTaskDraft, setCustomTaskDraft] = useState({
    title: '',
    priority: 'High',
    status: 'To Outreach',
    note: '',
  })
  const [customTasks, setCustomTasks] = useState(() => {
    try {
      const raw = localStorage.getItem(TASK_MANAGER_STORAGE_KEY)
      const parsed = raw ? JSON.parse(raw) : []
      return Array.isArray(parsed) ? parsed : []
    } catch {
      return []
    }
  })
  const [taskOrder, setTaskOrder] = useState(() => {
    try {
      const raw = localStorage.getItem(TASK_MANAGER_ORDER_STORAGE_KEY)
      const parsed = raw ? JSON.parse(raw) : []
      return Array.isArray(parsed) ? parsed.map((x) => String(x)) : []
    } catch {
      return []
    }
  })
  const [dismissedAutoTaskIds, setDismissedAutoTaskIds] = useState(() => {
    try {
      const raw = localStorage.getItem(TASK_MANAGER_DISMISSED_STORAGE_KEY)
      const parsed = raw ? JSON.parse(raw) : []
      return Array.isArray(parsed) ? parsed.map((x) => Number(x)).filter((x) => Number.isFinite(x)) : []
    } catch {
      return []
    }
  })
  const [autoPriorityOverrides, setAutoPriorityOverrides] = useState(() => {
    try {
      const raw = localStorage.getItem(TASK_MANAGER_AUTO_PRIORITY_KEY)
      const parsed = raw ? JSON.parse(raw) : {}
      return parsed && typeof parsed === 'object' ? parsed : {}
    } catch {
      return {}
    }
  })
  const [fadingTaskIds, setFadingTaskIds] = useState({})
  const [keyboardSorting, setKeyboardSorting] = useState(false)
  const [savingTaskOrder, setSavingTaskOrder] = useState(false)
  const [goalSettings, setGoalSettings] = useState(() => {
    let name = 'My Goal'
    let amount = MRR_GOAL_EUR
    let currency = DEFAULT_GOAL_CURRENCY

    try {
      const storedName = String(localStorage.getItem(PERSONAL_GOAL_NAME_STORAGE_KEY) || '').trim()
      if (storedName) name = storedName
      const storedAmount = Number(localStorage.getItem(PERSONAL_GOAL_AMOUNT_STORAGE_KEY) || '')
      if (Number.isFinite(storedAmount) && storedAmount > 0) {
        amount = storedAmount
      }
      const storedCurrency = String(localStorage.getItem(PERSONAL_GOAL_CURRENCY_STORAGE_KEY) || '').toUpperCase().trim()
      if (GOAL_CURRENCY_OPTIONS.includes(storedCurrency)) {
        currency = storedCurrency
      }
    } catch {
      // Ignore storage errors and use defaults.
    }

    return { name, amount, currency }
  })
  const [goalDraft, setGoalDraft] = useState(() => {
    let name = 'My Goal'
    let amount = MRR_GOAL_EUR
    let currency = DEFAULT_GOAL_CURRENCY

    try {
      const storedName = String(localStorage.getItem(PERSONAL_GOAL_NAME_STORAGE_KEY) || '').trim()
      if (storedName) name = storedName
      const storedAmount = Number(localStorage.getItem(PERSONAL_GOAL_AMOUNT_STORAGE_KEY) || '')
      if (Number.isFinite(storedAmount) && storedAmount > 0) {
        amount = storedAmount
      }
      const storedCurrency = String(localStorage.getItem(PERSONAL_GOAL_CURRENCY_STORAGE_KEY) || '').toUpperCase().trim()
      if (GOAL_CURRENCY_OPTIONS.includes(storedCurrency)) {
        currency = storedCurrency
      }
    } catch {
      // Ignore storage errors and use defaults.
    }

    return {
      name,
      amount: String(amount),
      currency,
    }
  })
  const [nicheAdvice, setNicheAdvice] = useState({ loading: false, data: null, error: '' })
  const [marketPickIndex, setMarketPickIndex] = useState(0)
  const [qualifierData, setQualifierData] = useState({ loading: false, data: null, error: '' })
  const [refreshingDashboard, setRefreshingDashboard] = useState(false)
  const [lastManualRefreshAt, setLastManualRefreshAt] = useState(null)
  const [mailPreview, setMailPreview] = useState({ subject: '', body: '', generatedAt: null })
  const defaultColdOutreachForm = { businessName: '', city: '', niche: '', painPoint: '', competitors: '', monthlyLoss: '', contactName: '', contactEmail: '' }
  const [coldOutreachForm, setColdOutreachForm] = useState(defaultColdOutreachForm)
  const [coldOutreachResult, setColdOutreachResult] = useState({ subject: '', body: '', generatedAt: null })
  const [coldOutreachLoading, setColdOutreachLoading] = useState(false)
  const [coldOutreachError, setColdOutreachError] = useState('')
  const [emailPreviewLead, setEmailPreviewLead] = useState(null)
  const [aiSummaryPreviewLead, setAiSummaryPreviewLead] = useState(null)
  const [taskAiPreviewLead, setTaskAiPreviewLead] = useState(null)
  const [previewLoading, setPreviewLoading] = useState(false)
  const [activeMailPack, setActiveMailPack] = useState('')
  const [activeLiveMailTemplateKey, setActiveLiveMailTemplateKey] = useState(liveMailTemplateCards[0]?.key || 'ghost')
  const [activeMailEditorTab, setActiveMailEditorTab] = useState('live')
  const [showMailerConfirm, setShowMailerConfirm] = useState(false)
  const [mailerScheduledHour, setMailerScheduledHour] = useState('now')
  const [mailerHourOpen, setMailerHourOpen] = useState(false)
  const [mailerStopRequested, setMailerStopRequested] = useState(false)
  const previousTasksRef = useRef({})
  const leadSearchRef = useRef(null)
  const workflowRef = useRef(null)
  const mainPanelRef = useRef(null)
  const pendingDeletesRef = useRef({})

  useEffect(() => {
    if (hasSessionToken) return
    const rawSearch = new URLSearchParams(window.location.search || '')
    const activeTab = String(rawSearch.get('tab') || '').trim().toLowerCase()
    const cleanAppTarget = activeTab ? `/app?tab=${encodeURIComponent(activeTab)}` : '/app'
    const nextUrl = `/login?redirect=${encodeURIComponent(cleanAppTarget)}`
    window.location.replace(nextUrl)
  }, [hasSessionToken])
  const taskFetchFailCountRef = useRef(0)
  const taskFetchBackoffUntilRef = useRef(0)

  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: { distance: 8 },
    }),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    }),
  )

  const scrapeTask = tasks.scrape || getIdleTask('scrape')
  const enrichTask = tasks.enrich || getIdleTask('enrich')
  const mailerTask = tasks.mailer || getIdleTask('mailer')
  const scrapeTaskStateRef = useRef({ id: null, status: 'idle' })

  useEffect(() => {
    if (!mailerTask.running) {
      setMailerStopRequested(false)
    }
  }, [mailerTask.running])

  useEffect(() => {
    localStorage.setItem(TASK_MANAGER_STORAGE_KEY, JSON.stringify(customTasks))
  }, [customTasks])

  useEffect(() => {
    localStorage.setItem(TASK_MANAGER_ORDER_STORAGE_KEY, JSON.stringify(taskOrder))
  }, [taskOrder])

  useEffect(() => {
    localStorage.setItem(TASK_MANAGER_DISMISSED_STORAGE_KEY, JSON.stringify(dismissedAutoTaskIds))
  }, [dismissedAutoTaskIds])

  useEffect(() => {
    localStorage.setItem(TASK_MANAGER_AUTO_PRIORITY_KEY, JSON.stringify(autoPriorityOverrides))
  }, [autoPriorityOverrides])

  useEffect(() => {
    try {
      localStorage.setItem(PERSONAL_GOAL_NAME_STORAGE_KEY, String(goalSettings.name || 'My Goal'))
      localStorage.setItem(PERSONAL_GOAL_AMOUNT_STORAGE_KEY, String(goalSettings.amount || MRR_GOAL_EUR))
      localStorage.setItem(PERSONAL_GOAL_CURRENCY_STORAGE_KEY, String(goalSettings.currency || DEFAULT_GOAL_CURRENCY))
    } catch {
      // Ignore storage errors.
    }
  }, [goalSettings])

  useEffect(() => {
    return () => {
      Object.values(pendingDeletesRef.current).forEach((entry) => {
        window.clearTimeout(entry.removeTimer)
        window.clearTimeout(entry.finalizeTimer)
      })
      pendingDeletesRef.current = {}
    }
  }, [])

  useEffect(() => {
    const fromUrl = normalizeTabParam(searchParams.get('tab'), '')
    if (!fromUrl) return
    setActiveTab((prev) => (prev === fromUrl ? prev : fromUrl))
  }, [searchParams])

  useEffect(() => {
    const current = normalizeTabParam(searchParams.get('tab'), '')
    if (current === activeTab) return
    const next = new URLSearchParams(searchParams)
    next.set('tab', activeTab)
    setSearchParams(next, { replace: true })
  }, [activeTab, searchParams, setSearchParams])

  const leadsById = useMemo(() => {
    const indexed = new Map()
    for (const lead of leads) {
      if (!Number.isFinite(Number(lead?.id))) continue
      indexed.set(Number(lead.id), lead)
    }
    return indexed
  }, [leads])

  const automatedTaskItems = useMemo(
    () => deliveryTasks
      .filter((task) => !dismissedAutoTaskIds.includes(Number(task.id)))
      .map((task) => {
      const linkedLead = Number.isFinite(Number(task.lead_id)) ? leadsById.get(Number(task.lead_id)) : null
      const mapped = mapDeliveryTaskType(task.task_type)
      const status = mapDeliveryStatusToTaskStatus(task.status)
      const templateNote = task.notes || 'Live Mail template ready for this lead.'
      const fallbackPriority = String(task.client_tier || '').toLowerCase().includes('ads') ? 'High' : 'Medium'
      const priority = autoPriorityOverrides[String(task.id)] || fallbackPriority
      return {
        id: `auto-${task.id}`,
        source: 'auto',
        rawTask: task,
        title: task.business_name || 'Unnamed client',
        priority,
        status,
        note: templateNote,
        workerLabel: task.worker_name || (task.worker_id ? `AI-Worker-${String(task.worker_id).padStart(2, '0')}` : 'AI-Queue'),
        taskTypeLabel: mapped.label,
        action: mapped.action,
        dueAt: task.due_at || null,
        checked: status === 'Done',
        clientTier: task.client_tier || null,
        linkedLead,
        canPreviewAiMessage: Boolean(linkedLead?.ai_description || linkedLead?.generated_email_body),
      }
    }),
    [deliveryTasks, dismissedAutoTaskIds, autoPriorityOverrides, leadsById],
  )

  const customTaskItems = useMemo(
    () => (customTasks || []).map((task) => ({
      id: String(task.id),
      source: 'custom',
      title: task.title || 'Untitled task',
      priority: task.priority || 'Medium',
      status: task.status || 'To Outreach',
      note: task.note || '',
      workerLabel: 'Personal',
      taskTypeLabel: 'Custom Follow-up',
      action: 'view',
      dueAt: task.dueAt || null,
      checked: Boolean(task.checked || String(task.status || '').toLowerCase() === 'done'),
      clientTier: null,
    })),
    [customTasks],
  )

  const taskManagerItems = useMemo(() => {
    const merged = [...customTaskItems, ...automatedTaskItems]
    const orderIndex = new Map(taskOrder.map((id, idx) => [String(id), idx]))
    return merged.sort((a, b) => {
      const ai = orderIndex.get(String(a.id))
      const bi = orderIndex.get(String(b.id))
      if (ai != null && bi != null && ai !== bi) return ai - bi
      if (ai != null && bi == null) return -1
      if (ai == null && bi != null) return 1
      if (a.checked !== b.checked) return a.checked ? 1 : -1
      const pa = priorityWeight(a.priority)
      const pb = priorityWeight(b.priority)
      if (pa !== pb) return pb - pa
      return String(a.title || '').localeCompare(String(b.title || ''))
    })
  }, [customTaskItems, automatedTaskItems, taskOrder])

  const highPriorityOpenCount = useMemo(
    () => taskManagerItems.filter((task) => String(task.priority).toLowerCase() === 'high' && !task.checked).length,
    [taskManagerItems],
  )

  function addCustomTask(e) {
    e.preventDefault()
    const title = String(customTaskDraft.title || '').trim()
    if (!title) {
      toast.error('Task title is required')
      return
    }
    const newTask = {
      id: `custom-${Date.now()}`,
      title,
      priority: customTaskDraft.priority || 'Medium',
      status: customTaskDraft.status || 'To Outreach',
      note: String(customTaskDraft.note || '').trim(),
      checked: String(customTaskDraft.status || '').toLowerCase() === 'done',
      dueAt: null,
    }
    setCustomTasks((prev) => [newTask, ...prev])
    setCustomTaskDraft({ title: '', priority: 'High', status: 'To Outreach', note: '' })
    setShowCustomTaskForm(false)
    toast.success('Custom task added')
  }

  function updateCustomTask(taskId, patch) {
    setCustomTasks((prev) => prev.map((task) => (String(task.id) === String(taskId) ? { ...task, ...patch } : task)))
  }

  async function updateAutomatedTask(rawTaskId, patch) {
    await _updateDeliveryTask(rawTaskId, patch)
  }

  async function toggleTaskDone(item, checked) {
    if (item.source === 'custom') {
      updateCustomTask(item.id, {
        checked,
        status: checked ? 'Done' : 'To Outreach',
      })
      if (checked) shootConfetti()
      return
    }

    const nextStatus = checked ? 'done' : 'todo'
    await updateAutomatedTask(item.rawTask.id, { status: nextStatus })
    if (checked) shootConfetti()
  }

  async function updateTaskStatus(item, status) {
    if (item.source === 'custom') {
      updateCustomTask(item.id, { status, checked: String(status).toLowerCase() === 'done' })
      return
    }
    await updateAutomatedTask(item.rawTask.id, { status: mapTaskStatusToDeliveryStatus(status) })
  }

  async function updateTaskNote(item, note) {
    if (item.source === 'custom') {
      updateCustomTask(item.id, { note })
      return
    }
    await updateAutomatedTask(item.rawTask.id, { notes: note })
  }

  function applyTaskReorder(nextItems) {
    setTaskOrder(nextItems.map((item) => String(item.id)))

    const total = Math.max(1, nextItems.length)
    const highCutoff = Math.ceil(total / 3)
    const mediumCutoff = Math.ceil((total * 2) / 3)
    const priorityById = {}

    nextItems.forEach((item, index) => {
      let priority = 'Low'
      if (index < highCutoff) priority = 'High'
      else if (index < mediumCutoff) priority = 'Medium'
      priorityById[String(item.id)] = priority
    })

    setCustomTasks((prev) => prev.map((task) => {
      const key = String(task.id)
      const nextPriority = priorityById[key]
      return nextPriority ? { ...task, priority: nextPriority } : task
    }))

    setAutoPriorityOverrides((prev) => {
      const updated = { ...prev }
      nextItems.forEach((item) => {
        if (item.source !== 'auto' || !item.rawTask?.id) return
        const nextPriority = priorityById[String(item.id)]
        if (nextPriority) {
          updated[String(item.rawTask.id)] = nextPriority
        }
      })
      return updated
    })
  }

  async function persistTaskReorder(nextItems, previousItems) {
    const taskIds = nextItems
      .filter((item) => item.source === 'auto' && Number.isFinite(Number(item.rawTask?.id)))
      .map((item) => Number(item.rawTask.id))

    if (taskIds.length === 0) return

    const token = getStoredValue('lf_token')
    setSavingTaskOrder(true)
    try {
      await axios.post(
        '/api/tasks/reorder',
        { task_ids: taskIds },
        {
          headers: token ? { Authorization: `Bearer ${token}` } : {},
        },
      )
    } catch (error) {
      applyTaskReorder(previousItems)
      const message = error instanceof Error ? error.message : 'Failed to persist task order'
      setLastError(message)
      toast.error(message)
    } finally {
      setSavingTaskOrder(false)
    }
  }

  function undoDelete(taskKey) {
    const pending = pendingDeletesRef.current[taskKey]
    if (!pending) return

    window.clearTimeout(pending.removeTimer)
    window.clearTimeout(pending.finalizeTimer)

    setFadingTaskIds((prev) => {
      const clone = { ...prev }
      delete clone[taskKey]
      return clone
    })

    if (pending.removed) {
      if (pending.source === 'custom' && pending.customTask) {
        setCustomTasks((prev) => {
          if (prev.some((task) => String(task.id) === taskKey)) return prev
          return [pending.customTask, ...prev]
        })
      }
      if (pending.source === 'auto' && Number.isFinite(pending.rawId)) {
        setDismissedAutoTaskIds((prev) => prev.filter((id) => Number(id) !== Number(pending.rawId)))
      }
    }

    if (Array.isArray(pending.orderSnapshot) && pending.orderSnapshot.length > 0) {
      setTaskOrder(pending.orderSnapshot)
    }

    delete pendingDeletesRef.current[taskKey]
    toast.dismiss(pending.toastId)
    toast.success('Deletion undone')
  }

  function deleteTask(item) {
    const taskKey = String(item.id)
    if (pendingDeletesRef.current[taskKey]) return

    const orderSnapshot = taskManagerItems.map((task) => String(task.id))
    const customTask = item.source === 'custom'
      ? customTasks.find((task) => String(task.id) === taskKey) || null
      : null
    const rawId = Number(item.rawTask?.id)

    setFadingTaskIds((prev) => ({ ...prev, [taskKey]: true }))

    const pending = {
      taskKey,
      source: item.source,
      rawId,
      customTask,
      orderSnapshot,
      removed: false,
      removeTimer: null,
      finalizeTimer: null,
      toastId: '',
    }
    pendingDeletesRef.current[taskKey] = pending

    pending.removeTimer = window.setTimeout(() => {
      pending.removed = true
      if (pending.source === 'custom') {
        setCustomTasks((prev) => prev.filter((task) => String(task.id) !== taskKey))
      } else if (Number.isFinite(pending.rawId)) {
        setDismissedAutoTaskIds((prev) => Array.from(new Set([...prev, pending.rawId])))
      }

      setTaskOrder((prev) => {
        const base = prev.length ? prev : pending.orderSnapshot
        return base.filter((id) => String(id) !== taskKey)
      })

      setFadingTaskIds((prev) => {
        const clone = { ...prev }
        delete clone[taskKey]
        return clone
      })
    }, 220)

    pending.finalizeTimer = window.setTimeout(() => {
      delete pendingDeletesRef.current[taskKey]
      toast.dismiss(pending.toastId)
      toast.success('Task permanently removed')
    }, 5000)

    pending.toastId = toast(
      (t) => (
        <div className="flex items-center gap-3">
          <span className="text-sm text-slate-100">Task deleted</span>
          <button
            type="button"
            className="rounded-md border border-cyan-300/50 bg-cyan-400/15 px-2.5 py-1 text-xs font-semibold text-cyan-200 transition hover:bg-cyan-400/25"
            onClick={() => {
              toast.dismiss(t.id)
              undoDelete(taskKey)
            }}
          >
            Undo
          </button>
        </div>
      ),
      {
        duration: 5000,
        position: 'bottom-center',
      },
    )
  }

  function onDragStart(event) {
    const keyboard = event?.activatorEvent instanceof KeyboardEvent
    setKeyboardSorting(Boolean(keyboard))
  }

  function onDragCancel() {
    setKeyboardSorting(false)
  }

  function onTaskReorderEnd(event) {
    const { active, over } = event
    setKeyboardSorting(false)
    if (!over || active.id === over.id) return
    if (savingTaskOrder) return

    const oldIndex = taskManagerItems.findIndex((task) => String(task.id) === String(active.id))
    const newIndex = taskManagerItems.findIndex((task) => String(task.id) === String(over.id))
    if (oldIndex < 0 || newIndex < 0) return

    const previousItems = [...taskManagerItems]
    const nextItems = arrayMove(taskManagerItems, oldIndex, newIndex)
    applyTaskReorder(nextItems)
    void persistTaskReorder(nextItems, previousItems)
  }

  const activeTasks = useMemo(
    () => [scrapeTask, enrichTask, mailerTask].filter((t) => t.running),
    [scrapeTask, enrichTask, mailerTask],
  )

  const scrapeProgress = useMemo(() => {
    const status = String(scrapeTask.status || 'idle').toLowerCase()
    const result = scrapeTask.result && typeof scrapeTask.result === 'object' ? scrapeTask.result : {}
    const requestedFromTask = Number(scrapeTask.last_request?.results || 0)
    const requestedFromForm = Number(scrapeForm.results || 0)
    const totalToFind = Number(result.total_to_find || requestedFromTask || requestedFromForm || 0)
    const currentFound = Number(result.current_found || (status === 'completed' ? result.scraped || 0 : 0))
    const scannedCount = Number(result.scanned_count || 0)
    const inserted = Number(result.inserted || (status === 'completed' ? result.scraped || 0 : 0))
    const phase = String(result.phase || '')
    const statusMessage = String(result.status_message || '').trim()
    // isLoading = scraper launched but Maps hasn't returned any card yet
    const isLoading = (status === 'running' || status === 'queued') && currentFound === 0 && scannedCount === 0

    let percent = 0
    if (totalToFind > 0) {
      percent = Math.min(100, Math.round((currentFound / totalToFind) * 100))
    }
    if (status === 'completed') percent = 100

    return {
      status,
      totalToFind,
      currentFound,
      scannedCount,
      inserted,
      percent,
      phase,
      statusMessage,
      isLoading,
      isVisible: ['queued', 'running', 'completed', 'failed'].includes(status),
    }
  }, [scrapeTask, scrapeForm.results])

  const enrichProgress = useMemo(() => {
    const status = String(enrichTask.status || 'idle').toLowerCase()
    const result = enrichTask.result && typeof enrichTask.result === 'object' ? enrichTask.result : {}
    const requestedLimit = Number(enrichTask.last_request?.limit || enrichForm.limit || 50)
    const totalFromTask = Number(result.total || requestedLimit || 0)
    const processed = Number(result.processed || 0)
    const queued = Number(result.queued_for_mail || 0)
    const withEmail = Number(result.with_email || 0)
    const currentLead = String(result.current_lead || '').trim()

    const total = totalFromTask > 0 ? totalFromTask : requestedLimit

    let percent = 0
    if (total > 0) {
      percent = Math.min(100, Math.round((processed / total) * 100))
    }
    if (status === 'completed') percent = 100

    return {
      status,
      requestedLimit,
      total,
      processed,
      queued,
      withEmail,
      currentLead,
      percent,
      isVisible: ['running', 'completed', 'failed'].includes(status),
    }
  }, [enrichTask, enrichForm.limit])

  const mailerProgress = useMemo(() => {
    const baseStatus = String(mailerTask.status || 'idle').toLowerCase()
    const result = mailerTask.result && typeof mailerTask.result === 'object' ? mailerTask.result : {}
    const requestedLimit = Number(mailerTask.last_request?.limit || mailerForm.limit || 10)
    const effectiveLimit = Number(result.effective_limit || requestedLimit)
    const sent = Number(result.sent || 0)
    const skipped = Number(result.skipped || 0)
    const failed = Number(result.failed || 0)
    const errorText = String(mailerTask.error || '').toLowerCase()
    const stoppedByUser = Boolean(result.stopped_by_user) || errorText.includes('stopped by user')
    const stopLikeFailure = stoppedByUser || errorText.includes('worker not active in current process')
    const status = mailerStopRequested && baseStatus === 'running'
      ? 'stopping'
      : baseStatus === 'stopped' || (baseStatus === 'failed' && stopLikeFailure)
        ? 'stopped'
        : baseStatus

    let percent = 0
    if (effectiveLimit > 0) {
      percent = Math.min(100, Math.round((sent / effectiveLimit) * 100))
    }
    if (baseStatus === 'completed') percent = 100

    return {
      status,
      baseStatus,
      requestedLimit,
      effectiveLimit,
      sent,
      skipped,
      failed,
      stoppedByUser,
      percent,
      isVisible: ['running', 'stopping', 'completed', 'failed', 'stopped'].includes(status),
    }
  }, [mailerTask, mailerForm.limit, mailerStopRequested])

  const livePendingMailCount = useMemo(() => {
    const baseQueuedMailCount = Number(stats.queued_mail_count || 0)
    if (!['running', 'stopping', 'completed', 'failed', 'stopped'].includes(mailerProgress.status)) {
      return baseQueuedMailCount
    }
    return Math.max(0, baseQueuedMailCount - mailerProgress.sent)
  }, [stats.queued_mail_count, mailerProgress.status, mailerProgress.sent])

  const scrapeSummary = useMemo(() => {
    const status = String(scrapeTask.status || '').toLowerCase()
    if (status !== 'completed') return null

    const result = scrapeTask.result && typeof scrapeTask.result === 'object' ? scrapeTask.result : null
    if (!result) return null

    const totalToFind = Number(result.total_to_find || scrapeTask.last_request?.results || 0)
    const scraped = Number(result.scraped || result.current_found || 0)
    const inserted = Number(result.inserted || 0)
    const duplicates = Number(result.duplicates || 0)
    return {
      totalToFind,
      scraped,
      inserted,
      duplicates,
    }
  }, [scrapeTask])

  const agencyMrrForGoal = useMemo(() => {
    const fromServer = Number(stats.monthly_recurring_revenue || 0)
    if (fromServer > 0) return fromServer

    let total = 0
    for (const lead of leads) {
      if (String(lead.status || '').toLowerCase() !== 'paid') continue

      const isAds = Number(lead.is_ads_client || 0) === 1
      const isWeb = Number(lead.is_website_client || 0) === 1
      if (isAds && isWeb) {
        total += MRR_ADS_AND_WEBSITE
        continue
      }
      if (isAds) {
        total += MRR_ADS_ONLY
        continue
      }
      if (isWeb) {
        total += MRR_WEBSITE_ONLY
        continue
      }

      const tierKey = String(lead.client_tier || 'standard').toLowerCase()
      if (tierKey === 'premium_ads') total += MRR_ADS_ONLY
      else total += MRR_WEBSITE_ONLY
    }
    return total
  }, [leads, stats.monthly_recurring_revenue])

  const revenueProgress = useMemo(() => {
    const goal = Number(goalSettings.amount || stats.mrr_goal || MRR_GOAL_EUR)
    if (!goal) return 0
    return Math.min(100, Math.round((Number(agencyMrrForGoal || 0) / goal) * 100))
  }, [agencyMrrForGoal, goalSettings.amount, stats.mrr_goal])

  const mrrRemaining = useMemo(() => {
    const goal = Number(goalSettings.amount || stats.mrr_goal || MRR_GOAL_EUR)
    return Math.max(0, goal - Number(agencyMrrForGoal || 0))
  }, [agencyMrrForGoal, goalSettings.amount, stats.mrr_goal])

  const tierSummary = useMemo(() => {
    return {
      standard: Number(stats.website_clients || 0),
      premium_ads: Number(stats.ads_clients || 0),
      both: Number(stats.ads_and_website_clients || 0),
    }
  }, [stats.website_clients, stats.ads_clients, stats.ads_and_website_clients])

  const averageDealValue = useMemo(() => {
    const raw = Number(user?.average_deal_value ?? DEFAULT_AVERAGE_DEAL_VALUE)
    if (!Number.isFinite(raw) || raw <= 0) return DEFAULT_AVERAGE_DEAL_VALUE
    return raw
  }, [user?.average_deal_value])

  const hotOpportunityLeads = useMemo(
    () => leads.filter((lead) => resolveBestLeadScore(lead) >= 7),
    [leads],
  )
  const hotOpportunityCount = hotOpportunityLeads.length
  const contactedHotLeadCount = useMemo(
    () => hotOpportunityLeads.filter((lead) => hasSentMail(lead)).length,
    [hotOpportunityLeads],
  )
  const totalOpportunityValue = useMemo(
    () => hotOpportunityCount * averageDealValue,
    [averageDealValue, hotOpportunityCount],
  )
  const hotLeadContactPct = useMemo(() => {
    if (!hotOpportunityCount) return 0
    return Math.max(0, Math.min(100, Math.round((contactedHotLeadCount / hotOpportunityCount) * 100)))
  }, [contactedHotLeadCount, hotOpportunityCount])
  const remainingHotLeadCount = Math.max(0, hotOpportunityCount - contactedHotLeadCount)

  // Reset to page 0 whenever the filter set changes
  useEffect(() => { setLeadPage(0) }, [debouncedLeadSearch, leadStatusFilter, leadQuickFilter, leadSortMode, showBlacklisted, advancedLeadFilters])

  const industryFilterOptions = useMemo(
    () => Array.from(new Set(leads.map((lead) => deriveLeadIndustry(lead)).filter(Boolean))).sort((a, b) => a.localeCompare(b)),
    [leads],
  )

  const revenueFilterOptions = useMemo(() => {
    const ordered = ['<$500k', '$500k-$2M', '$2M-$10M', '$10M+']
    const available = new Set(leads.map((lead) => deriveLeadRevenueBand(lead)).filter(Boolean))
    return ordered.filter((label) => available.has(label))
  }, [leads])

  const techStackFilterOptions = useMemo(
    () => Array.from(
      new Set(
        leads.flatMap((lead) => normalizeLeadInsightList(lead.tech_stack, 5)).map((item) => String(item || '').trim()).filter(Boolean),
      ),
    ).sort((a, b) => a.localeCompare(b)).slice(0, 14),
    [leads],
  )

  const filteredLeads = useMemo(() => {
    let result = [...leads]
    if (!BYPASS_LEAD_FILTERS) {
      if (!showBlacklisted) {
        result = result.filter((l) => !isBlacklistedLeadStatus(l.status))
      }
      if (leadStatusFilter !== 'all') {
        result = result.filter((l) => String(l.status || 'pending').toLowerCase() === leadStatusFilter.toLowerCase())
      }
      if (leadQuickFilter === 'qualified') {
        result = result.filter((l) => isQualifiedLead(l))
      }
      if (leadQuickFilter === 'not_qualified') {
        result = result.filter((l) => !isQualifiedLead(l))
      }
      if (leadQuickFilter === 'mailed') {
        result = result.filter((l) => hasSentMail(l))
      }
      if (leadQuickFilter === 'opened') {
        result = result.filter((l) => hasOpenedMail(l))
      }
      if (leadQuickFilter === 'replied') {
        result = result.filter((l) => hasReply(l))
      }
      if (debouncedLeadSearch.trim()) {
        const q = debouncedLeadSearch.trim().toLowerCase()
        result = result.filter(
          (l) => (l.business_name || '').toLowerCase().includes(q) ||
                 (l.contact_name || '').toLowerCase().includes(q) ||
                 (l.email || '').toLowerCase().includes(q),
        )
      }
      if (advancedLeadFilters.industries.length > 0) {
        result = result.filter((lead) => advancedLeadFilters.industries.includes(deriveLeadIndustry(lead)))
      }
      if (advancedLeadFilters.revenueBands.length > 0) {
        result = result.filter((lead) => advancedLeadFilters.revenueBands.includes(deriveLeadRevenueBand(lead)))
      }
      if (advancedLeadFilters.techStacks.length > 0) {
        result = result.filter((lead) => {
          const stackSet = new Set(normalizeLeadInsightList(lead.tech_stack, 5))
          return advancedLeadFilters.techStacks.some((stack) => stackSet.has(stack))
        })
      }
      if (advancedLeadFilters.highScoreOnly) {
        result = result.filter((lead) => resolveBestLeadScore(lead) >= 8)
      }
    }

    const sorted = [...result]
    sorted.sort((a, b) => {
      if (leadSortMode === 'name') {
        return String(a.business_name || '').localeCompare(String(b.business_name || ''), undefined, { sensitivity: 'base' })
      }
      if (leadSortMode === 'score') {
        return Number(b.ai_score || 0) - Number(a.ai_score || 0)
      }
      if (leadSortMode === 'recent') {
        return Number(b.id || 0) - Number(a.id || 0)
      }

      const bestDiff = resolveBestLeadScore(b) - resolveBestLeadScore(a)
      if (Math.abs(bestDiff) > 0.01) return bestDiff

      const emailDiff = Number(Boolean(b.email)) - Number(Boolean(a.email))
      if (emailDiff !== 0) return emailDiff

      const employeeDiff = Number(b.employee_count || 0) - Number(a.employee_count || 0)
      if (employeeDiff !== 0) return employeeDiff

      return Number(b.ai_sentiment_score || b.ai_score || 0) - Number(a.ai_sentiment_score || a.ai_score || 0)
    })
    return sorted
  }, [leads, debouncedLeadSearch, leadStatusFilter, leadQuickFilter, leadSortMode, showBlacklisted, advancedLeadFilters])

  const leadsPageCount = Math.max(1, Math.ceil(Math.max(leadServerTotal, filteredLeads.length) / LEADS_PAGE_SIZE))
  const pagedLeads = useMemo(() => {
    const start = leadPage * LEADS_PAGE_SIZE
    return filteredLeads.slice(start, start + LEADS_PAGE_SIZE)
  }, [filteredLeads, leadPage])

  useEffect(() => {
    if (leadPage <= 0) return
    const maxPageIndex = Math.max(0, Math.ceil(filteredLeads.length / LEADS_PAGE_SIZE) - 1)
    if (leadPage > maxPageIndex) {
      setLeadPage(maxPageIndex)
    }
  }, [filteredLeads.length, leadPage])

  const leadQuickCounts = useMemo(() => {
    const visible = showBlacklisted
      ? leads
      : leads.filter((l) => !isBlacklistedLeadStatus(l.status))
    return {
      total: visible.length,
      qualified: visible.filter((l) => isQualifiedLead(l)).length,
      notQualified: visible.filter((l) => !isQualifiedLead(l)).length,
      mailed: visible.filter((l) => hasSentMail(l)).length,
      opened: visible.filter((l) => hasOpenedMail(l)).length,
      replied: visible.filter((l) => hasReply(l)).length,
    }
  }, [leads, showBlacklisted])

  useEffect(() => {
    if (activeTab !== 'leads') return
    setLeadStatusFilter('all')
    setLeadQuickFilter('all')
    setLeadSearch('')
    setLeadPage(0)
    void refreshLeads({ silent: false })
  }, [activeTab, refreshLeads])

  useEffect(() => {
    if (!lastLeadsApiPayload) return
    console.log('[LeadManagement useEffect] last /api/leads payload', {
      total: Number(lastLeadsApiPayload?.total || lastLeadsApiPayload?.count || 0),
      itemsLength: Array.isArray(lastLeadsApiPayload?.items) ? lastLeadsApiPayload.items.length : 0,
      sample: Array.isArray(lastLeadsApiPayload?.items) ? lastLeadsApiPayload.items.slice(0, 3) : [],
    })
  }, [lastLeadsApiPayload])

  useEffect(() => {
    const previous = scrapeTaskStateRef.current
    const currentStatus = String(scrapeTask.status || 'idle').toLowerCase()
    const sameTask = previous.id === scrapeTask.id
    const transitionedToCompleted = sameTask
      ? (previous.status === 'running' || previous.status === 'queued') && currentStatus === 'completed'
      : currentStatus === 'completed'

    if (transitionedToCompleted) {
      void refreshLeads({ silent: false })
    }

    scrapeTaskStateRef.current = { id: scrapeTask.id, status: currentStatus }
  }, [scrapeTask.id, scrapeTask.status, refreshLeads])

  useEffect(() => {
    if (leadPage > 0 && leadPage >= leadsPageCount) {
      setLeadPage(Math.max(0, leadsPageCount - 1))
    }
  }, [leadPage, leadsPageCount])

  const qualifierLossInsight = useMemo(
    () => buildQualifierLossInsight(qualifierData.data),
    [qualifierData.data],
  )

  const blacklistedLeads = useMemo(
    () => leads.filter((lead) => isBlacklistedLeadStatus(lead.status)),
    [leads],
  )

  const workflowStats = useMemo(() => {
    const enrichmentStatuses = new Set([
      'enriched',
      'invalid_email',
      'queued_mail',
      'emailed',
      'interested',
      'replied',
      'meeting set',
      'zoom scheduled',
      'closed',
      'paid',
      'failed',
      'generation failed',
      'generation_failed',
      'retry_later',
      'low_priority',
      'qualified_not_interested',
      'qualified not interested',
    ])

    const total = leads.length
    const scraped = leads.filter((lead) => String(lead.status || '').toLowerCase() === 'scraped').length
    const enriched = leads.filter((lead) => String(lead.status || '').toLowerCase() === 'enriched').length
    const queued = leads.filter((lead) => String(lead.status || '').toLowerCase() === 'queued_mail').length

    const enrichmentDone = leads.filter((lead) => {
      const status = String(lead.status || '').toLowerCase().trim()
      return Boolean(lead.enriched_at) || enrichmentStatuses.has(status)
    }).length
    const notEnriched = Math.max(0, total - enrichmentDone)

    return { total, scraped, enriched, queued, enrichmentDone, notEnriched }
  }, [leads])

  const performanceSeries = useMemo(() => {
    const days = Array.from({ length: 7 }, (_, idx) => {
      const day = new Date()
      day.setHours(0, 0, 0, 0)
      day.setDate(day.getDate() - (6 - idx))
      return day.toISOString().slice(0, 10)
    })

    const revenueDaily = Object.fromEntries(days.map((day) => [day, 0]))
    const mrrDaily = Object.fromEntries(days.map((day) => [day, 0]))
    const replyDaily = Object.fromEntries(days.map((day) => [day, 0]))

    for (const lead of leads) {
      const tierKey = String(lead.client_tier || 'standard').toLowerCase()
      const paidDay = String(lead.status || '').toLowerCase() === 'paid' ? dayKey(lead.status_updated_at) : null
      if (paidDay && revenueDaily[paidDay] != null) {
        revenueDaily[paidDay] += Number(SETUP_FEE_BY_TIER[tierKey] || 0)
        mrrDaily[paidDay] += Number(MRR_BY_TIER[tierKey] || 0)
      }

      const replyStatus = String(lead.status || '').toLowerCase()
      const replyDay = ['interested', 'meeting set'].includes(replyStatus) ? dayKey(lead.status_updated_at) : null
      if (replyDay && replyDaily[replyDay] != null) {
        replyDaily[replyDay] += 1
      }
    }

    let revenueRunning = 0
    let mrrRunning = 0
    let replyRunning = 0

    return {
      revenue: days.map((day) => { revenueRunning += revenueDaily[day]; return revenueRunning }),
      mrr: days.map((day) => { mrrRunning += mrrDaily[day]; return mrrRunning }),
      replies: days.map((day) => { replyRunning += replyDaily[day]; return replyRunning }),
      days,
    }
  }, [leads])

  const activityFeed = useMemo(() => {
    const feed = []

    for (const task of taskHistory.slice(0, 12)) {
      const result = task.result || {}
      const timestamp = task.finished_at || task.created_at
      if (task.task_type === 'scrape' && task.status === 'completed') {
        feed.push({
          at: timestamp,
          message: `🚀 Scraper found ${Number(result.scraped || 0)} leads and imported ${Number(result.inserted || 0)}.`,
        })
      }
      if (task.task_type === 'enrich' && task.status === 'completed') {
        feed.push({
          at: timestamp,
          message: `✨ AI enriched ${Number(result.processed || 0)} leads and queued ${Number(result.queued_for_mail || 0)} for outreach.`,
        })
      }
      if (task.task_type === 'mailer' && task.status === 'completed') {
        feed.push({
          at: timestamp,
          message: `✉️ Mailer sent ${Number(result.sent || 0)} emails, skipped ${Number(result.skipped || 0)}, failed ${Number(result.failed || 0)}.`,
        })
      }
      if (task.status === 'failed' && task.error) {
        feed.push({ at: timestamp, message: `⚠️ ${taskLabels[task.task_type] || task.task_type} failed: ${task.error}` })
      }
    }

    for (const lead of leads.slice(0, 40)) {
      if (Number(lead.ai_score || 0) >= 9 && lead.enriched_at) {
        feed.push({ at: lead.enriched_at, message: `🌟 AI enriched '${lead.business_name}' - Score: ${Number(lead.ai_score).toFixed(1)}` })
      }

      const status = String(lead.status || '').toLowerCase()
      if ((status === 'interested' || status === 'meeting set') && lead.status_updated_at) {
        feed.push({ at: lead.status_updated_at, message: `📞 ${lead.business_name} moved to ${normalizeLeadStatus(status)}.` })
      }
    }

    return feed
      .filter((item) => item.at)
      .sort((a, b) => new Date(b.at).getTime() - new Date(a.at).getTime())
      .slice(0, 14)
  }, [taskHistory, leads])

  // Live countdown ticker
  useEffect(() => {
    const id = setInterval(() => setCountdown(fmtCountdown(stats.next_drip_at)), 1000)
    setCountdown(fmtCountdown(stats.next_drip_at))
    return () => clearInterval(id)
  }, [stats.next_drip_at])

  // Daily digest countdown ticker
  useEffect(() => {
    const id = setInterval(() => setDigestCountdown(fmtDigestCountdown()), 1000)
    return () => clearInterval(id)
  }, [])

  useEffect(() => {
    if (enrichRetrySeconds <= 0) return undefined
    const id = window.setInterval(() => {
      setEnrichRetrySeconds((prev) => (prev <= 1 ? 0 : prev - 1))
    }, 1000)
    return () => window.clearInterval(id)
  }, [enrichRetrySeconds])

  useEffect(() => {
    void refreshUserProfile()
    const profileId = window.setInterval(() => {
      void refreshUserProfile()
    }, 30000)
    return () => window.clearInterval(profileId)
  }, [])

  const fetchNicheAdvice = useCallback(async ({ silent = false, forceRefresh = false, countryCode = null } = {}) => {
    try {
      setNicheAdvice((prev) => ({ ...prev, loading: true, error: '' }))
      const selectedCountry = String(countryCode || scrapeForm.country || 'US').toUpperCase()
      const params = new URLSearchParams({ country: selectedCountry })
      if (forceRefresh) params.set('refresh', '1')
      const data = await fetchJson(`/api/recommend-niche?${params.toString()}`)
      setNicheAdvice({ loading: false, data, error: '' })
      const recommendationCount = Array.isArray(data?.recommendations) ? data.recommendations.length : 0
      setMarketPickIndex(forceRefresh && recommendationCount > 1 ? 1 : 0)
      if (forceRefresh) {
        setLastManualRefreshAt(data?.generated_at || new Date().toISOString())
      }
      if (!silent) {
        toast.success('AI strategy refreshed')
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not load niche recommendation'
      setNicheAdvice({
        loading: false,
        data: null,
        error: message,
      })
      if (!silent) {
        toast.error(message)
      }
    }
  }, [scrapeForm.country])

  useEffect(() => {
    if (activeTab !== 'leads') return undefined
    void fetchNicheAdvice({ silent: true, countryCode: scrapeForm.country })
    const paidSignalUser = Boolean(user?.isSubscribed || user?.subscription_active) && String(user?.plan_key || '').toLowerCase() !== 'free'
    const intervalMs = paidSignalUser ? 60 * 60 * 1000 : 6 * 60 * 60 * 1000
    const nicheRefreshId = window.setInterval(() => {
      void fetchNicheAdvice({ silent: true, countryCode: scrapeForm.country })
    }, intervalMs)
    return () => window.clearInterval(nicheRefreshId)
  }, [activeTab, fetchNicheAdvice, scrapeForm.country, user?.isSubscribed, user?.subscription_active, user?.plan_key])

  useEffect(() => {
    const previousTasks = previousTasksRef.current
    for (const taskType of Object.keys(taskLabels)) {
      const cur = tasks[taskType]
      const prev = previousTasks[taskType]
      if (!cur || !prev) continue
      const wasRunning = ['queued', 'running'].includes(String(prev.status || '').toLowerCase())
      const isCompleted = String(cur.status || '').toLowerCase() === 'completed'
      const isFailed = String(cur.status || '').toLowerCase() === 'failed'
      const sameTask = cur.id === prev.id
      if (wasRunning && sameTask && isCompleted) {
        toast.success(`${taskLabels[taskType]} completed`)
        if (taskType === 'scrape') {
          const inserted = Number(cur.result?.inserted || 0)
          if (inserted > 0) shootConfetti()
          if (inserted > 0) {
            // Make newly scraped rows visible immediately.
            setLeadStatusFilter('all')
            setLeadQuickFilter('all')
            setLeadSearch('')
            setLeadPage(0)
          }
          setLastResult('')
        } else if (cur.result) {
          if (taskType === 'enrich') {
            const nextBalance = Number(cur.result?.credits_balance)
            const nextLimit = Number(cur.result?.credits_limit)
            if (Number.isFinite(nextBalance)) {
              setUser((prevUser) => ({
                ...prevUser,
                credits: Math.max(0, nextBalance),
                credits_balance: Math.max(0, nextBalance),
                creditLimit: Number.isFinite(nextLimit) ? Math.max(1, nextLimit) : prevUser.creditLimit,
                credits_limit: Number.isFinite(nextLimit) ? Math.max(1, nextLimit) : prevUser.credits_limit,
              }))
              toast.success(`Credits remaining: ${Math.max(0, nextBalance).toLocaleString('en-US')}`)
            }
            const billingWarning = String(cur.result?.billing_warning || '').trim()
            if (billingWarning) {
              toast.error(billingWarning)
            }
          }
          setLastResult(JSON.stringify(cur.result, null, 2))
        }
        void Promise.allSettled([refreshLeads(), refreshStats(), refreshConfigHealth()])
        invalidateLeadsCache() // clear SWR cache so next open shows fresh leads
      }
      if (wasRunning && sameTask && isFailed) {
        toast.error(`${taskLabels[taskType]} failed`)
        if (cur.error) setLastError(String(cur.error))
        void Promise.allSettled([refreshLeads(), refreshStats(), refreshConfigHealth()])
      }
    }
    previousTasksRef.current = tasks
  }, [refreshLeads, tasks])

  async function refreshDashboard() {
    setRefreshingDashboard(true)
    try {
      await Promise.allSettled([
        checkHealth(),
        refreshConfigHealth(),
        refreshLeads(),
        refreshStats(),
        fetchTaskState(),
        fetchRevenueLog(),
        fetchNicheAdvice({ silent: true }),
        refreshWorkers(),
        refreshDeliveryTasks(),
        refreshUserProfile(),
        fetchMailerCampaignStats({ silent: true }),
        refreshWeeklyReport({ silent: true }),
        refreshMonthlyReport({ silent: true }),
        refreshClientFolders({ silent: true }),
        refreshClientDashboard({ silent: true }),
      ])
      setLastManualRefreshAt(new Date().toISOString())
      toast.success('Dashboard refreshed')
    } finally {
      setRefreshingDashboard(false)
    }
  }

  async function checkHealth() {
    try { await fetchJson('/api/health'); setHealth('online') }
    catch { setHealth('offline') }
  }

  async function refreshConfigHealth() {
    try { setConfigHealth(await fetchJson('/api/health')) }
    catch (error) { setConfigHealth({ ok: false, openai_ok: false, smtp_ok: false, error: error instanceof Error ? error.message : 'Unknown error' }) }
  }

  async function loadConfigForm() {
    if (configFormLoaded) return
    try {
      const data = await fetchJson('/api/config')
      const smtpAccounts = Array.isArray(data.smtp_accounts) && data.smtp_accounts.length
        ? data.smtp_accounts.map((account) => ({
          host: account.host || 'smtp.gmail.com',
          port: account.port || 587,
          email: account.email || '',
          password: '',
          from_name: account.from_name || '',
          password_set: Boolean(account.password_set),
        }))
        : [{
          host: data.smtp_host || 'smtp.gmail.com',
          port: data.smtp_port || 587,
          email: data.smtp_email || '',
          password: '',
          from_name: '',
          password_set: Boolean(data.smtp_password_set),
        }]

      const nextConfig = {
        smtp_accounts: smtpAccounts,
        sending_strategy: data.sending_strategy || 'round_robin',
        open_tracking_base_url: data.open_tracking_base_url || '',
        proxy_urls: data.proxy_urls || '',
        hubspot_webhook_url: data.hubspot_webhook_url || '',
        google_sheets_webhook_url: data.google_sheets_webhook_url || '',
        auto_weekly_report_email: data.auto_weekly_report_email !== false,
        auto_monthly_report_email: data.auto_monthly_report_email !== false,
        mail_signature: data.mail_signature || '',
        ghost_subject_template: data.ghost_subject_template || '',
        ghost_body_template: data.ghost_body_template || '',
        golden_subject_template: data.golden_subject_template || '',
        golden_body_template: data.golden_body_template || '',
        competitor_subject_template: data.competitor_subject_template || '',
        competitor_body_template: data.competitor_body_template || '',
        speed_subject_template: data.speed_subject_template || '',
        speed_body_template: data.speed_body_template || '',
      }
      setConfigForm(nextConfig)
      setSmtpTestResults({})
      await previewMailTemplate({ silent: true, configOverride: nextConfig })
      setConfigFormLoaded(true)
    } catch { /* silent */ }
  }

  async function fetchPersonalGoal() {
    try {
      const data = await fetchJson('/api/auth/personal-goal')
      const nextName = String(data.name || '').trim() || 'My Goal'
      const nextAmount = Number(data.amount || MRR_GOAL_EUR)
      const nextCurrencyRaw = String(data.currency || DEFAULT_GOAL_CURRENCY).toUpperCase().trim()
      const nextCurrency = GOAL_CURRENCY_OPTIONS.includes(nextCurrencyRaw) ? nextCurrencyRaw : DEFAULT_GOAL_CURRENCY
      const safeAmount = Number.isFinite(nextAmount) && nextAmount > 0 ? Math.round(nextAmount) : MRR_GOAL_EUR

      setGoalSettings({ name: nextName, amount: safeAmount, currency: nextCurrency })
      setGoalDraft({ name: nextName, amount: String(safeAmount), currency: nextCurrency })
    } catch {
      // Keep local fallback values.
    }
  }

  async function refreshUserProfile(options = {}) {
    const token = getStoredValue('lf_token')
    if (!token) return null

    const preservePlanKey = String(options?.preservePlanKey || '').trim().toLowerCase()
    const preservePlanName = String(options?.preservePlanName || '').trim()
    const preserveCreditsLimit = Math.max(0, Number(options?.preserveCreditsLimit || 0))
    const preserveCreditsBalance = Math.max(0, Number(options?.preserveCreditsBalance || 0))
    const preserveTopupBalance = Math.max(0, Number(options?.preserveTopupBalance || 0))

    try {
      const data = await fetchJson('/api/auth/profile', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token }),
      })

      const resolvedPlanKey = String(data?.plan_key ?? '').toLowerCase().trim()
      const resolvedCreditsLimit = Number(data?.monthly_quota ?? data?.monthly_limit ?? data?.credits_limit ?? data?.creditLimit ?? 0)
      const resolvedCreditsBalance = Number(data?.credits_balance ?? data?.credits ?? 0)
      const resolvedTopupBalance = Number(data?.topup_credits_balance ?? 0)
      const resolvedIsSubscribed = Boolean(data?.isSubscribed ?? data?.subscription_active ?? false)
      const shouldPreservePaidState = Boolean(
        preservePlanKey
        && preservePlanKey !== 'free'
        && (!resolvedIsSubscribed || resolvedPlanKey === 'free')
        && resolvedCreditsLimit < preserveCreditsLimit,
      )
      const shouldPreserveTopUpState = Boolean(
        preserveCreditsBalance > 0
        && (resolvedCreditsBalance < preserveCreditsBalance || resolvedTopupBalance < preserveTopupBalance),
      )

      setUser((prev) => {
        const resolvedFeatureAccess = resolveFeatureAccess(
          data?.plan_type ?? data?.plan_key ?? prev?.plan_type ?? prev?.plan_key ?? 'free',
          data?.feature_access ?? prev?.feature_access,
        )
        const nextState = {
          ...prev,
          ...data,
          credits: Number(data?.credits ?? data?.credits_balance ?? prev.credits ?? prev.credits_balance ?? 0),
          creditLimit: Number(data?.creditLimit ?? data?.monthly_quota ?? data?.monthly_limit ?? data?.credits_limit ?? prev.creditLimit ?? prev.credits_limit ?? DEFAULT_FREE_CREDIT_LIMIT),
          credits_balance: Number(data?.credits_balance ?? prev.credits_balance ?? 0),
          credits_limit: Number(data?.monthly_quota ?? data?.monthly_limit ?? data?.credits_limit ?? prev.credits_limit ?? DEFAULT_FREE_CREDIT_LIMIT),
          monthly_limit: Number(data?.monthly_quota ?? data?.monthly_limit ?? prev.monthly_limit ?? prev.credits_limit ?? DEFAULT_FREE_CREDIT_LIMIT),
          monthly_quota: Number(data?.monthly_quota ?? prev.monthly_quota ?? prev.monthly_limit ?? prev.credits_limit ?? DEFAULT_FREE_CREDIT_LIMIT),
          topup_credits_balance: Number(data?.topup_credits_balance ?? prev.topup_credits_balance ?? 0),
          next_reset_at: data?.next_reset_at ?? prev.next_reset_at,
          next_reset_in_days: data?.next_reset_in_days ?? prev.next_reset_in_days,
          isSubscribed: Boolean(data?.isSubscribed ?? prev.isSubscribed ?? false),
          subscription_active: Boolean(data?.subscription_active ?? prev.subscription_active ?? false),
          subscriptionStatus: String(data?.subscription_status ?? data?.subscriptionStatus ?? prev.subscriptionStatus ?? '').toLowerCase().trim(),
          subscription_status: String(data?.subscription_status ?? prev.subscription_status ?? '').toLowerCase().trim(),
          subscription_cancel_at: data?.subscription_cancel_at ?? prev.subscription_cancel_at ?? null,
          subscription_cancel_at_period_end: Boolean(data?.subscription_cancel_at_period_end ?? prev.subscription_cancel_at_period_end ?? false),
          currentPlanName: String(data?.currentPlanName ?? prev.currentPlanName ?? 'Free Plan').trim() || 'Free Plan',
          plan_key: String(data?.plan_key ?? prev.plan_key ?? 'free').toLowerCase().trim(),
          plan_type: String(data?.plan_type ?? data?.plan_key ?? prev.plan_type ?? prev.plan_key ?? 'free').toLowerCase().trim(),
          feature_access: resolvedFeatureAccess,
          average_deal_value: Number(data?.average_deal_value ?? prev.average_deal_value ?? DEFAULT_AVERAGE_DEAL_VALUE),
          niche: String(data?.niche ?? prev.niche ?? '').trim(),
        }

        if (!shouldPreservePaidState && !shouldPreserveTopUpState) {
          return nextState
        }

        const protectedBalance = Math.max(
          Number(nextState.credits_balance ?? nextState.credits ?? 0),
          Number(prev?.credits_balance ?? prev?.credits ?? 0),
          preserveCreditsLimit,
          preserveCreditsBalance,
        )
        const protectedTopupBalance = Math.max(
          Number(nextState.topup_credits_balance ?? 0),
          Number(prev?.topup_credits_balance ?? 0),
          preserveTopupBalance,
        )
        const protectedState = {
          ...nextState,
          credits: protectedBalance,
          credits_balance: protectedBalance,
          topup_credits_balance: protectedTopupBalance,
        }

        if (!shouldPreservePaidState) {
          return protectedState
        }

        return {
          ...protectedState,
          isSubscribed: true,
          subscription_active: true,
          subscription_status: String(nextState.subscription_status || 'active').toLowerCase().trim() || 'active',
          currentPlanName: preservePlanName || String(prev?.currentPlanName || 'Pro Plan').trim() || 'Pro Plan',
          plan_key: preservePlanKey,
          creditLimit: Math.max(Number(nextState.creditLimit || 0), preserveCreditsLimit),
          credits_limit: Math.max(Number(nextState.credits_limit || 0), preserveCreditsLimit),
          monthly_limit: Math.max(Number(nextState.monthly_limit || 0), preserveCreditsLimit),
          monthly_quota: Math.max(Number(nextState.monthly_quota || 0), preserveCreditsLimit),
        }
      })

      if (shouldPreservePaidState || shouldPreserveTopUpState) {
        localStorage.setItem('lf_credits', String(Math.max(resolvedCreditsBalance, preserveCreditsBalance, preserveCreditsLimit)))
        localStorage.setItem('lf_credits_balance', String(Math.max(resolvedCreditsBalance, preserveCreditsBalance, preserveCreditsLimit)))
        localStorage.setItem('lf_topup_credits_balance', String(Math.max(resolvedTopupBalance, preserveTopupBalance)))
        if (shouldPreservePaidState) {
          localStorage.setItem('lf_credits_limit', String(preserveCreditsLimit))
          localStorage.setItem('lf_plan_key', preservePlanKey)
          localStorage.setItem('lf_plan_name', preservePlanName || 'Pro Plan')
          localStorage.setItem('lf_is_subscribed', 'true')
        }
        localStorage.setItem('lf_average_deal_value', String(Number(data?.average_deal_value ?? DEFAULT_AVERAGE_DEAL_VALUE)))
        localStorage.setItem('lf_niche', String(data?.niche ?? '').trim())
      } else {
        localStorage.setItem('lf_credits', String(Number(data?.credits ?? data?.credits_balance ?? 0)))
        localStorage.setItem('lf_credits_balance', String(Number(data?.credits_balance ?? data?.credits ?? 0)))
        localStorage.setItem('lf_topup_credits_balance', String(Number(data?.topup_credits_balance ?? 0)))
        localStorage.setItem('lf_credits_limit', String(Number(data?.monthly_quota ?? data?.monthly_limit ?? data?.credits_limit ?? data?.creditLimit ?? DEFAULT_FREE_CREDIT_LIMIT)))
        localStorage.setItem('lf_plan_key', String(data?.plan_key ?? 'free').toLowerCase().trim() || 'free')
        localStorage.setItem('lf_plan_name', String(data?.currentPlanName ?? 'Free Plan').trim() || 'Free Plan')
        localStorage.setItem('lf_is_subscribed', String(Boolean(data?.isSubscribed ?? data?.subscription_active ?? false)))
        localStorage.setItem('lf_average_deal_value', String(Number(data?.average_deal_value ?? DEFAULT_AVERAGE_DEAL_VALUE)))
        localStorage.setItem('lf_niche', String(data?.niche ?? '').trim())
      }
      return data
    } catch {
      // Keep existing credits values if profile call fails.
      return null
    }
  }

  const applyOptimisticSubscriptionState = useCallback((rawPlanKey) => {
    const normalizedPlanKey = String(rawPlanKey || '').trim().toLowerCase()
    const plan = SUBSCRIPTION_PLAN_DETAILS[normalizedPlanKey]
    if (!plan || normalizedPlanKey === 'free') return

    const nextLimit = Math.max(1, Number(plan.credits || DEFAULT_FREE_CREDIT_LIMIT))
    setUser((prev) => {
      const currentBalance = Number(prev?.credits_balance ?? prev?.credits ?? 0)
      const nextBalance = Math.max(currentBalance, nextLimit)
      return {
        ...prev,
        isSubscribed: true,
        subscription_active: true,
        subscription_status: String(prev?.subscription_status || 'active').toLowerCase().trim() || 'active',
        currentPlanName: plan.displayName,
        plan_key: normalizedPlanKey,
        plan_type: normalizedPlanKey,
        feature_access: getDefaultFeatureAccess(normalizedPlanKey),
        credits: nextBalance,
        credits_balance: nextBalance,
        creditLimit: nextLimit,
        credits_limit: nextLimit,
        monthly_limit: nextLimit,
        monthly_quota: nextLimit,
      }
    })
    localStorage.setItem('lf_plan_key', normalizedPlanKey)
    localStorage.setItem('lf_plan_name', plan.displayName)
    localStorage.setItem('lf_is_subscribed', 'true')
    localStorage.setItem('lf_credits_limit', String(nextLimit))
    localStorage.setItem('lf_credits', String(nextLimit))
    localStorage.setItem('lf_credits_balance', String(nextLimit))
  }, [])

  const syncBillingStateAfterCheckout = useCallback(async (rawPlanKey = '', options = {}) => {
    const normalizedPlanKey = String(rawPlanKey || '').trim().toLowerCase()
    const expectedPlan = SUBSCRIPTION_PLAN_DETAILS[normalizedPlanKey]
    const preserveCreditsBalance = Math.max(0, Number(options?.preserveCreditsBalance || 0))
    const preserveTopupBalance = Math.max(0, Number(options?.preserveTopupBalance || 0))
    if (expectedPlan && normalizedPlanKey !== 'free') {
      applyOptimisticSubscriptionState(normalizedPlanKey)
    }

    const retryDelays = [0, 1500, 3500, 6000, 9000]
    for (const delayMs of retryDelays) {
      if (delayMs > 0) {
        await sleep(delayMs)
      }
      const data = await refreshUserProfile({
        preservePlanKey: normalizedPlanKey,
        preservePlanName: expectedPlan?.displayName || '',
        preserveCreditsLimit: Number(expectedPlan?.credits || 0),
        preserveCreditsBalance,
        preserveTopupBalance,
      })
      if (!expectedPlan) {
        const resolvedBalance = Number(data?.credits_balance ?? data?.credits ?? 0)
        const resolvedTopup = Number(data?.topup_credits_balance ?? 0)
        if (resolvedBalance >= preserveCreditsBalance && resolvedTopup >= preserveTopupBalance) {
          return
        }
        continue
      }
      const resolvedPlanKey = String(data?.plan_key || '').trim().toLowerCase()
      const resolvedLimit = Number(data?.monthly_quota ?? data?.monthly_limit ?? data?.credits_limit ?? 0)
      if (resolvedPlanKey === normalizedPlanKey && resolvedLimit >= Number(expectedPlan.credits || 0)) {
        return
      }
    }
  }, [applyOptimisticSubscriptionState])

  useEffect(() => {
    const checkoutStatus = String(searchParams.get('checkout') || '').trim().toLowerCase()
    const topupStatus = String(searchParams.get('topup') || '').trim().toLowerCase()
    const topupCreditsParam = Number(searchParams.get('topup_credits') || 0)
    const storedCheckoutPlanKey = (() => {
      try {
        return String(window.localStorage.getItem('lf_pending_checkout_plan') || '').trim().toLowerCase()
      } catch {
        return ''
      }
    })()
    const checkoutPlanKey = String(searchParams.get('plan') || storedCheckoutPlanKey || '').trim().toLowerCase()
    if (!checkoutStatus && !topupStatus) return

    let cancelled = false
    const finalizeCheckoutRedirect = () => {
      if (cancelled) return
      const nextParams = new URLSearchParams(searchParams)
      nextParams.delete('checkout')
      nextParams.delete('topup')
      nextParams.delete('topup_package')
      nextParams.delete('topup_credits')
      nextParams.delete('plan')
      nextParams.delete('session_id')
      setSearchParams(nextParams, { replace: true })
      try {
        window.localStorage.removeItem('lf_pending_checkout_plan')
      } catch {
        // Ignore storage failures.
      }
    }

    const runCheckoutRedirectSync = async () => {
      if (checkoutStatus === 'success') {
        if (checkoutPlanKey) {
          applyOptimisticSubscriptionState(checkoutPlanKey)
        }
        const nextPlanName = SUBSCRIPTION_PLAN_DETAILS[checkoutPlanKey]?.displayName || 'your subscription'
        toast.success(`Payment successful — ${nextPlanName} is now active`)
        await syncBillingStateAfterCheckout(checkoutPlanKey)
      } else if (checkoutStatus === 'cancel') {
        toast('Subscription checkout cancelled', { icon: 'ℹ️' })
      }

      if (topupStatus === 'success') {
        if (Number.isFinite(topupCreditsParam) && topupCreditsParam > 0) {
          setUser((prev) => {
            const currentBalance = Number(prev?.credits_balance ?? prev?.credits ?? 0)
            const currentTopup = Number(prev?.topup_credits_balance ?? 0)
            const nextBalance = currentBalance + topupCreditsParam
            const nextTopup = currentTopup + topupCreditsParam
            localStorage.setItem('lf_credits', String(nextBalance))
            localStorage.setItem('lf_credits_balance', String(nextBalance))
            return {
              ...prev,
              credits: nextBalance,
              credits_balance: nextBalance,
              topup_credits_balance: nextTopup,
            }
          })
        }
        toast.success('Top-up payment received')
        const optimisticTopupDelta = Number.isFinite(topupCreditsParam) ? Math.max(0, topupCreditsParam) : 0
        const currentBalance = Number(user?.credits_balance ?? user?.credits ?? 0)
        const currentTopup = Number(user?.topup_credits_balance ?? 0)
        await syncBillingStateAfterCheckout('', {
          preserveCreditsBalance: currentBalance + optimisticTopupDelta,
          preserveTopupBalance: currentTopup + optimisticTopupDelta,
        })
      } else if (topupStatus === 'cancel') {
        toast('Top-up checkout cancelled', { icon: 'ℹ️' })
      }

      finalizeCheckoutRedirect()
    }

    void runCheckoutRedirectSync()
    return () => {
      cancelled = true
    }
  }, [
    applyOptimisticSubscriptionState,
    searchParams,
    setSearchParams,
    syncBillingStateAfterCheckout,
    user?.credits,
    user?.credits_balance,
    user?.topup_credits_balance,
  ])

  const requestTopUpCheckoutUrl = useCallback(async (rawPackageId, { markPreparing = false } = {}) => {
    const packageId = String(rawPackageId || '').trim()
    if (!packageId) return ''

    const existingPromise = topUpCheckoutInFlightRef.current[packageId]
    if (existingPromise) {
      return existingPromise
    }

    if (markPreparing) {
      setTopUpPreparingPackageId(packageId)
    }

    const sessionPromise = (async () => {
      const data = await fetchJson('/api/stripe/create-topup-session', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ package_id: packageId }),
      })
      return String(data?.url || '').trim()
    })()

    topUpCheckoutInFlightRef.current[packageId] = sessionPromise

    try {
      return await sessionPromise
    } finally {
      delete topUpCheckoutInFlightRef.current[packageId]
      if (markPreparing) {
        setTopUpPreparingPackageId((prev) => (prev === packageId ? '' : prev))
      }
    }
  }, [])

  const handleTopUpClick = useCallback(async () => {
    setShowTopUpModal(true)
    void requestTopUpCheckoutUrl(selectedTopUpPackageId, { markPreparing: false })
  }, [requestTopUpCheckoutUrl, selectedTopUpPackageId])

  const closeTopUpModal = useCallback(() => {
    setShowTopUpModal(false)
  }, [])

  const handleTopUpPackageChange = useCallback((packageId) => {
    setSelectedTopUpPackageId(packageId)
  }, [])

  useEffect(() => {
    if (!showTopUpModal || !selectedTopUpPackageId) return
    const timer = window.setTimeout(() => {
      void requestTopUpCheckoutUrl(selectedTopUpPackageId, { markPreparing: false })
    }, 120)
    return () => window.clearTimeout(timer)
  }, [showTopUpModal, selectedTopUpPackageId, requestTopUpCheckoutUrl])

  const navigateToCheckoutWithFallback = useCallback((checkoutUrl) => {
    const targetUrl = String(checkoutUrl || '').trim()
    if (!targetUrl) return

    const beforeHref = window.location.href
    window.location.assign(targetUrl)

    // Fallback: if browser/extensions block same-tab redirect, open checkout in a new tab.
    window.setTimeout(() => {
      const stillVisible = document.visibilityState === 'visible'
      const hrefUnchanged = window.location.href === beforeHref
      if (!stillVisible || !hrefUnchanged) return
      const fallbackTab = window.open(targetUrl, '_blank', 'noopener,noreferrer')
      if (fallbackTab) {
        toast.success('Opened checkout in a new tab')
      } else {
        toast.error('Checkout pop-up blocked. Please allow pop-ups and try again.')
      }
    }, 700)
  }, [])

  const openPricingSection = useCallback(() => {
    window.location.assign('/pricing')
  }, [])

  const startTopUpCheckout = useCallback(async (packageId) => {
    const normalizedPackageId = String(packageId || '').trim()
    setTopUpLoadingPackageId(normalizedPackageId)
    try {
      const checkoutUrl = await requestTopUpCheckoutUrl(normalizedPackageId, { markPreparing: true })
      if (checkoutUrl) {
        navigateToCheckoutWithFallback(checkoutUrl)
        return
      }
      toast.error('Could not open Stripe checkout.')
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Top-up checkout failed.'
      setLastError(message)
      toast.error(message)
    } finally {
      setTopUpLoadingPackageId('')
    }
  }, [navigateToCheckoutWithFallback, requestTopUpCheckoutUrl])

  const handleTopUpProceed = useCallback((packageId) => {
    void startTopUpCheckout(packageId)
  }, [startTopUpCheckout])

  async function saveConfig(e) {
    e.preventDefault()
    setSavingConfig(true)
    try {
      const result = await fetchJson('/api/config', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          smtp_accounts: (configForm.smtp_accounts || [])
            .map((account) => ({
              host: String(account.host || '').trim(),
              port: account.port ? Number(account.port) : 587,
              email: String(account.email || '').trim(),
              password: String(account.password || '').trim() || null,
              from_name: String(account.from_name || '').trim() || null,
            }))
            .filter((account) => account.host && account.email),
          sending_strategy: configForm.sending_strategy || 'round_robin',
          open_tracking_base_url: String(configForm.open_tracking_base_url || '').trim() || null,
          proxy_urls: String(configForm.proxy_urls || '').trim() || null,
          hubspot_webhook_url: String(configForm.hubspot_webhook_url || '').trim() || null,
          google_sheets_webhook_url: String(configForm.google_sheets_webhook_url || '').trim() || null,
          auto_weekly_report_email: Boolean(configForm.auto_weekly_report_email),
          auto_monthly_report_email: Boolean(configForm.auto_monthly_report_email),
          mail_signature: configForm.mail_signature,
          ghost_subject_template: configForm.ghost_subject_template,
          ghost_body_template: configForm.ghost_body_template,
          golden_subject_template: configForm.golden_subject_template,
          golden_body_template: configForm.golden_body_template,
          competitor_subject_template: configForm.competitor_subject_template,
          competitor_body_template: configForm.competitor_body_template,
          speed_subject_template: configForm.speed_subject_template,
          speed_body_template: configForm.speed_body_template,
        }),
      })
      setConfigHealth({ ok: result.ok, openai_ok: result.openai_ok, smtp_ok: result.smtp_ok, error: null })
      setConfigFormLoaded(false)
      toast.success('Config saved')
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to save config')
    } finally {
      setSavingConfig(false)
    }
  }

  function addSmtpAccount() {
    setConfigForm((prev) => ({
      ...prev,
      smtp_accounts: [...(prev.smtp_accounts || []), createEmptySmtpAccount()],
    }))
    setSmtpTestResults({})
  }

  function removeSmtpAccount(index) {
    setConfigForm((prev) => {
      const current = [...(prev.smtp_accounts || [])]
      current.splice(index, 1)
      return {
        ...prev,
        smtp_accounts: current.length ? current : [createEmptySmtpAccount()],
      }
    })
    setShowSmtpPasswords((prev) => {
      const next = { ...prev }
      delete next[index]
      return next
    })
    setSmtpTestResults({})
  }

  function updateSmtpAccount(index, key, value) {
    setConfigForm((prev) => {
      const current = [...(prev.smtp_accounts || [])]
      const existing = current[index] || createEmptySmtpAccount()
      current[index] = { ...existing, [key]: value }
      return { ...prev, smtp_accounts: current }
    })
    setSmtpTestResults((prev) => {
      const next = { ...prev }
      delete next[index]
      return next
    })
  }

  function toggleSmtpPasswordVisibility(index) {
    setShowSmtpPasswords((prev) => ({ ...prev, [index]: !prev[index] }))
  }

  async function testSmtpAccount(index) {
    const account = (configForm.smtp_accounts || [])[index]
    if (!account) return

    setTestingSmtpIndex(index)
    try {
      const result = await fetchJson('/api/config/test-smtp', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          account_index: index,
          host: String(account.host || '').trim() || null,
          port: account.port ? Number(account.port) : 587,
          email: String(account.email || '').trim() || null,
          password: String(account.password || '').trim() || null,
        }),
      })

      setSmtpTestResults((prev) => ({
        ...prev,
        [index]: {
          ok: Boolean(result.ok),
          message: result.message || '',
          error: result.error || '',
        },
      }))
      if (result.ok) toast.success('SMTP test successful')
      else toast.error(result.message || 'SMTP test failed')
    } catch (error) {
      const message = error instanceof Error ? error.message : 'SMTP test failed'
      setSmtpTestResults((prev) => ({
        ...prev,
        [index]: {
          ok: false,
          message,
          error: message,
        },
      }))
      toast.error(message)
    } finally {
      setTestingSmtpIndex(null)
    }
  }

  function openTemplatePreview(mode) {
    const sample = {
      businessName: 'Apex Roofing',
      city: 'Dallas',
      niche: 'Roofer',
    }

    const firstAccount = (configForm.smtp_accounts || [])[0] || {}
    const senderName = String(firstAccount.from_name || firstAccount.email || 'Your Name').split('@')[0] || 'Your Name'

    if (mode === 'soft') {
      setTemplatePreview({
        mode: 'SOFT & HELPFUL',
        subject: `${sample.businessName} // quick question`,
        body: `Hi,\n\nI was checking out ${sample.businessName}'s site and ran a quick speed test — it's loading slow enough on mobile that Google is likely penalizing your ranking for it.\n\nFor ${sample.niche} businesses in ${sample.city}, a slow site typically means Google drops you below competitors with faster pages, even if your reviews are better.\n\nI fix this for local service businesses — usually takes less than a week and the ranking bump shows up within 30 days.\n\nIf helpful, I can send over a 2-minute video showing exactly what's slowing you down and how we'd fix it.\n\nBest, ${senderName}`,
      })
      return
    }

    if (mode === 'competitor') {
      setTemplatePreview({
        mode: 'THE COMPETITOR JAB',
        subject: `${sample.businessName} - quick question`,
        body: `Hi,\n\nI noticed that your main competitors are currently taking up most of the top spots on Google for ${sample.niche} in ${sample.city}, even though you have better local signals.\n\nThe main reason is that your site is missing a few key SEO tags and a tracking pixel, so Google is essentially "hiding" you from new customers.\n\nMy team and I help businesses reclaim those top spots and turn that traffic into actual booked jobs.\n\nIf useful, I can send over a 2-minute walkthrough with the exact fixes I’d start with.\n\nBest, ${senderName}`,
      })
      return
    }

    setTemplatePreview({
      mode: 'THE GHOST BUSINESS',
      subject: `question about ${sample.businessName}`,
      body: `Hi,\n\nI was looking for your services in ${sample.city} today but couldn't find a website for ${sample.businessName} anywhere.\n\nSince most people search on their phones now, you're likely losing dozens of high-ticket jobs every month to the few guys who actually show up on the map.\n\nI build high-converting landing pages that get businesses online and ranking in under 48 hours.\n\nIf helpful, I can send over a 2-minute video showing exactly what I'd build first.\n\nBest, ${senderName}`,
    })
  }

  async function downloadCsvExport(kind, fallbackFilename) {
    const token = getStoredValue('lf_token')
    const headers = token ? { Authorization: `Bearer ${token}` } : {}
    const response = await fetch(buildApiUrl(`/api/export-leads?kind=${encodeURIComponent(kind)}`), { headers })

    if (!response.ok) {
      const contentType = response.headers.get('content-type') || ''
      let detail = `Request failed (${response.status})`

      if (contentType.includes('application/json')) {
        const data = await response.json().catch(() => ({}))
        detail = typeof data.detail === 'string' ? data.detail : detail
      } else {
        const text = await response.text().catch(() => '')
        if (text) detail = text
      }

      const error = new Error(detail)
      error.status = response.status
      error.path = `/api/export-leads?kind=${kind}`
      throw error
    }

    const blob = await response.blob()
    const disposition = response.headers.get('content-disposition') || ''
    const filenameMatch = disposition.match(/filename="?([^";]+)"?/i)
    const filename = filenameMatch?.[1] || fallbackFilename
    const objectUrl = window.URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = objectUrl
    link.download = filename
    document.body.appendChild(link)
    link.click()
    link.remove()
    window.URL.revokeObjectURL(objectUrl)

    return Number(response.headers.get('X-Exported-Count') || 0)
  }

  async function exportTargets() {
    if (!canBulkExport) {
      toast('CSV exports unlock on The Growth and above.', { icon: '🔒' })
      return
    }

    setExportingTargets(true)
    try {
      const exported = await downloadCsvExport('target', 'target_leads.csv')
      toast.success(`Downloaded ${exported} target leads`)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Export failed')
    } finally {
      setExportingTargets(false)
    }
  }

  async function exportAI() {
    if (!canBulkExport) {
      toast('CSV exports unlock on The Growth and above.', { icon: '🔒' })
      return
    }

    setExportingAI(true)
    try {
      const exported = await downloadCsvExport('ai_mailer', 'ai_mailer_ready.csv')
      toast.success(`Downloaded ${exported} AI mailer leads`)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Export failed')
    } finally {
      setExportingAI(false)
    }
  }

  async function exportWebhookDestination(destination) {
    if (!featureAccess.webhooks) {
      toast('Webhook exports unlock on Business and Elite.', { icon: '🔒' })
      return
    }

    setWebhookExporting(destination)
    try {
      const result = await fetchJson('/api/export/webhook', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ destination }),
      })
      const exportedCount = Number(result?.exported_count || 0)
      toast.success(`${destination === 'hubspot' ? 'HubSpot' : 'Google Sheets'} export sent (${exportedCount})`)
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Webhook export failed'
      setLastError(message)
      toast.error(message)
    } finally {
      setWebhookExporting('')
    }
  }

  const planKey = String(user?.plan_key || '').toLowerCase().trim()
  const featureAccess = useMemo(
    () => resolveFeatureAccess(user?.plan_type || planKey || 'free', user?.feature_access),
    [planKey, user?.plan_type, user?.feature_access],
  )

  const refreshWeeklyReport = useCallback(async (options = {}) => {
    if (!featureAccess.advanced_reporting) {
      setWeeklyReport(null)
      return null
    }

    if (!options.silent) {
      setLoadingWeeklyReport(true)
    }
    try {
      const data = await fetchJson('/api/reporting/weekly-summary')
      setWeeklyReport(data)
      return data
    } catch (error) {
      if (Number(error?.status || 0) !== 403) {
        setLastError(error instanceof Error ? error.message : 'Could not load weekly report')
      }
      return null
    } finally {
      if (!options.silent) {
        setLoadingWeeklyReport(false)
      }
    }
  }, [featureAccess.advanced_reporting])

  const refreshMonthlyReport = useCallback(async (options = {}) => {
    if (!featureAccess.advanced_reporting) {
      setMonthlyReport(null)
      return null
    }

    if (!options.silent) {
      setLoadingMonthlyReport(true)
    }
    try {
      const data = await fetchJson('/api/reporting/monthly-summary')
      setMonthlyReport(data)
      return data
    } catch (error) {
      if (Number(error?.status || 0) !== 403) {
        setLastError(error instanceof Error ? error.message : 'Could not load monthly report')
      }
      return null
    } finally {
      if (!options.silent) {
        setLoadingMonthlyReport(false)
      }
    }
  }, [featureAccess.advanced_reporting])

  async function downloadMonthlyReportPdf() {
    if (!featureAccess.advanced_reporting) {
      toast('Monthly reports unlock on Business and Elite.', { icon: '🔒' })
      return
    }

    setLoadingMonthlyReport(true)
    try {
      const token = getStoredValue('lf_token')
      const headers = token ? { Authorization: `Bearer ${token}` } : {}
      const response = await fetch(buildApiUrl('/api/reporting/monthly-summary.pdf'), { headers })
      if (!response.ok) {
        const message = await response.text().catch(() => '')
        throw new Error(message || `Request failed (${response.status})`)
      }
      const blob = await response.blob()
      const objectUrl = window.URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = objectUrl
      link.download = 'monthly-summary.pdf'
      document.body.appendChild(link)
      link.click()
      link.remove()
      window.URL.revokeObjectURL(objectUrl)
      toast.success('Monthly PDF downloaded')
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not download PDF'
      setLastError(message)
      toast.error(message)
    } finally {
      setLoadingMonthlyReport(false)
    }
  }

  async function emailWeeklyReport() {
    if (!featureAccess.advanced_reporting) {
      toast('Weekly reports unlock on Business and Elite.', { icon: '🔒' })
      return
    }

    setSendingWeeklyReport(true)
    try {
      const result = await fetchJson('/api/reporting/weekly-summary/email', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      })
      toast.success(`Weekly summary emailed to ${result?.recipient || currentUserEmail || 'your account email'}`)
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not email weekly report'
      setLastError(message)
      toast.error(message)
    } finally {
      setSendingWeeklyReport(false)
    }
  }

  async function emailMonthlyReport() {
    if (!featureAccess.advanced_reporting) {
      toast('Monthly reports unlock on Business and Elite.', { icon: '🔒' })
      return
    }

    setSendingMonthlyReport(true)
    try {
      const result = await fetchJson('/api/reporting/monthly-summary/email', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      })
      toast.success(`Monthly summary emailed to ${result?.recipient || currentUserEmail || 'your account email'}`)
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not email report'
      setLastError(message)
      toast.error(message)
    } finally {
      setSendingMonthlyReport(false)
    }
  }

  const refreshClientFolders = useCallback(async (options = {}) => {
    if (!featureAccess.client_success_dashboard) {
      setClientFolders([])
      return []
    }

    if (!options.silent) {
      setLoadingClientFolders(true)
    }
    try {
      const data = await fetchJson('/api/client-folders')
      const items = Array.isArray(data.items) ? data.items : []
      setClientFolders(items)
      return items
    } catch (error) {
      if (Number(error?.status || 0) !== 403) {
        setLastError(error instanceof Error ? error.message : 'Could not load client folders')
      }
      return []
    } finally {
      if (!options.silent) {
        setLoadingClientFolders(false)
      }
    }
  }, [featureAccess.client_success_dashboard])

  const refreshClientDashboard = useCallback(async (options = {}) => {
    if (!featureAccess.client_success_dashboard) {
      setClientDashboard({
        total_clients: 0,
        active_clients: 0,
        folder_count: 0,
        unassigned_count: 0,
        pipeline: { scraped: 0, contacted: 0, replied: 0, won_paid: 0 },
        folders: [],
      })
      return null
    }

    if (!options.silent) {
      setLoadingClientDashboard(true)
    }
    try {
      const data = await fetchJson('/api/client-dashboard')
      setClientDashboard({
        total_clients: Number(data?.total_clients || data?.folder_count || 0),
        active_clients: Number(data?.active_clients || 0),
        folder_count: Number(data?.folder_count || 0),
        unassigned_count: Number(data?.unassigned_count || 0),
        pipeline: {
          scraped: Number(data?.pipeline?.scraped || 0),
          contacted: Number(data?.pipeline?.contacted || 0),
          replied: Number(data?.pipeline?.replied || 0),
          won_paid: Number(data?.pipeline?.won_paid || 0),
        },
        folders: Array.isArray(data?.folders) ? data.folders : [],
      })
      return data
    } catch (error) {
      if (Number(error?.status || 0) !== 403) {
        setLastError(error instanceof Error ? error.message : 'Could not load client dashboard')
      }
      return null
    } finally {
      if (!options.silent) {
        setLoadingClientDashboard(false)
      }
    }
  }, [featureAccess.client_success_dashboard])

  async function createClientFolder(e) {
    e.preventDefault()
    if (!featureAccess.client_success_dashboard) {
      toast('Client folders unlock on Business and Elite.', { icon: '🔒' })
      return
    }

    const folderName = String(clientFolderForm.name || '').trim()
    if (!folderName) {
      toast.error('Enter a client folder name first')
      return
    }

    setCreatingClientFolder(true)
    try {
      await fetchJson('/api/client-folders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: folderName,
          description: String(clientFolderForm.description || '').trim() || null,
        }),
      })
      setClientFolderForm({ name: '', description: '' })
      toast.success('Client folder created')
      await Promise.allSettled([
        refreshClientFolders({ silent: true }),
        refreshClientDashboard({ silent: true }),
      ])
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not create client folder'
      setLastError(message)
      toast.error(message)
    } finally {
      setCreatingClientFolder(false)
    }
  }

  async function assignLeadToClientFolder(leadId, clientFolderId) {
    if (!featureAccess.client_success_dashboard) {
      toast('Client folders unlock on Business and Elite.', { icon: '🔒' })
      return
    }

    setAssigningClientFolderLeadId(leadId)
    try {
      await fetchJson(`/api/leads/${leadId}/client-folder`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ client_folder_id: clientFolderId ? Number(clientFolderId) : null }),
      })
      setLeads((prev) => prev.map((lead) => (lead.id === leadId ? { ...lead, client_folder_id: clientFolderId ? Number(clientFolderId) : null } : lead)))
      await Promise.allSettled([
        refreshClientDashboard({ silent: true }),
        refreshClientFolders({ silent: true }),
      ])
      toast.success(clientFolderId ? 'Lead assigned to client folder' : 'Lead unassigned from client folder')
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not assign lead to client folder'
      setLastError(message)
      toast.error(message)
    } finally {
      setAssigningClientFolderLeadId(null)
    }
  }

  async function refreshStats() {
    try {
      const data = await fetchJson('/api/stats')
      setStats({
        total_leads: Number(data.total_leads || 0),
        emails_sent: Number(data.emails_sent || 0),
        opened_count: Number(data.opened_count || 0),
        opens_total: Number(data.opens_total || 0),
        open_rate: Number(data.open_rate || 0),
        paid_count: Number(data.paid_count || 0),
        total_revenue: Number(data.total_revenue || data.potential_revenue || 0),
        setup_revenue: Number(data.setup_revenue || data.total_revenue || data.potential_revenue || 0),
        setup_milestone: Number(data.setup_milestone || SETUP_MILESTONE_EUR),
        milestone_progress_pct: Number(data.milestone_progress_pct || 0),
        monthly_recurring_revenue: Number(data.monthly_recurring_revenue || 0),
        website_clients: Number(data.website_clients || 0),
        ads_clients: Number(data.ads_clients || 0),
        ads_and_website_clients: Number(data.ads_and_website_clients || 0),
        mrr_goal: Number(data.mrr_goal || MRR_GOAL_EUR),
        queued_mail_count: Number(data.queued_mail_count || 0),
        next_drip_at: data.next_drip_at || null,
        reply_rate: Number(data.reply_rate || 0),
        replies_count: Number(data.replies_count || 0),
        found_this_month: Number(data.found_this_month || 0),
        contacted_this_month: Number(data.contacted_this_month || 0),
        replied_this_month: Number(data.replied_this_month || 0),
        won_this_month: Number(data.won_this_month || 0),
        found_this_week: Number(data.found_this_week || 0),
        contacted_this_week: Number(data.contacted_this_week || 0),
        replied_this_week: Number(data.replied_this_week || 0),
        won_this_week: Number(data.won_this_week || 0),
        client_folder_count: Number(data.client_folder_count || 0),
        pipeline: data.pipeline || { scraped: 0, contacted: 0, replied: 0, won_paid: 0 },
      })
    } catch (error) {
      setLastError(error instanceof Error ? error.message : 'Unknown error while loading stats')
    }
  }

  async function fetchTaskState(force = false) {
    // Exponential backoff — skip if we are in a cooldown period
    if (!force && Date.now() < taskFetchBackoffUntilRef.current) return
    try {
      const data = await fetchJson('/api/tasks')
      taskFetchFailCountRef.current = 0
      taskFetchBackoffUntilRef.current = 0
      setTasks(data.tasks || {})
      setTaskHistory(Array.isArray(data.history) ? data.history : [])
    } catch (error) {
      const fails = taskFetchFailCountRef.current + 1
      taskFetchFailCountRef.current = fails
      // Cap delay at 5 minutes; 3s * 2^n
      const delayMs = Math.min(3000 * Math.pow(2, fails - 1), 5 * 60 * 1000)
      taskFetchBackoffUntilRef.current = Date.now() + delayMs
      setLastError(error instanceof Error ? error.message : 'Unknown error while loading tasks')
    }
  }

  async function refreshSavedSegments({ silent = false } = {}) {
    if (!silent) {
      setLoadingSavedSegments(true)
    }
    try {
      const data = await fetchJson('/api/saved-segments')
      setSavedSegments(Array.isArray(data?.items) ? data.items : [])
    } catch {
      // Keep the last successful snapshot if saved segment fetch fails.
    } finally {
      if (!silent) {
        setLoadingSavedSegments(false)
      }
    }
  }

  useEffect(() => {
    const initialRequests = [
      checkHealth(),
      refreshConfigHealth(),
      refreshStats(),
      fetchTaskState(),
      fetchPersonalGoal(),
    ]

    if (activeTab === 'leads') {
      initialRequests.push(refreshLeads(), refreshSavedSegments({ silent: true }))
    }
    if (activeTab === 'tasks' || activeTab === 'history' || activeTab === 'workers') {
      initialRequests.push(refreshWorkers(), refreshDeliveryTasks(), fetchRevenueLog())
    }
    if (activeTab === 'mail') {
      initialRequests.push(fetchMailerCampaignStats({ silent: true }))
    }
    if (activeTab === 'export') {
      initialRequests.push(refreshWeeklyReport({ silent: true }), refreshMonthlyReport({ silent: true }))
    }
    if (activeTab === 'clients') {
      initialRequests.push(refreshClientFolders({ silent: true }), refreshClientDashboard({ silent: true }))
    }

    void Promise.allSettled(initialRequests)

    const fastId = window.setInterval(() => {
      if (activeTab === 'leads' || activeTab === 'tasks' || activeTab === 'history') {
        void fetchTaskState()
      }
    }, 3000)

    const slowId = window.setInterval(() => {
      const requests = [checkHealth(), refreshConfigHealth(), refreshStats()]

      if (activeTab === 'leads') {
        requests.push(fetchTaskState(), refreshLeads({ silent: true }), refreshSavedSegments({ silent: true }))
      }
      if (activeTab === 'tasks' || activeTab === 'history' || activeTab === 'workers') {
        requests.push(fetchTaskState(), fetchRevenueLog(), refreshWorkers(), refreshDeliveryTasks())
      }
      if (activeTab === 'mail') {
        requests.push(fetchMailerCampaignStats({ silent: true }))
      }
      if (activeTab === 'export') {
        requests.push(refreshWeeklyReport({ silent: true }), refreshMonthlyReport({ silent: true }))
      }
      if (activeTab === 'clients') {
        requests.push(refreshClientFolders({ silent: true }), refreshClientDashboard({ silent: true }))
      }

      void Promise.allSettled(requests)
    }, 12000)

    return () => {
      window.clearInterval(fastId)
      window.clearInterval(slowId)
    }
  }, [activeTab, refreshClientDashboard, refreshClientFolders, refreshLeads, refreshMonthlyReport, refreshWeeklyReport])

  function applySavedSegment(segment) {
    const filters = normalizeSavedSegmentFilters(segment?.filters || {})
    setLeadSearch(filters.leadSearch)
    setLeadStatusFilter(filters.leadStatusFilter)
    setLeadQuickFilter(filters.leadQuickFilter)
    setLeadSortMode(filters.leadSortMode)
    setShowBlacklisted(filters.showBlacklisted)
    setAdvancedLeadFilters(filters.advancedLeadFilters)
    setLeadPage(0)
    setLeadFilterPanelOpen(true)
    toast.success(`Loaded segment: ${segment?.name || 'Saved segment'}`)
  }

  async function saveCurrentSegment() {
    const name = String(segmentNameDraft || '').trim()
    if (!name) {
      toast.error('Enter a segment name first')
      return
    }

    setSavingSegment(true)
    try {
      await fetchJson('/api/saved-segments', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name,
          filters: {
            leadSearch,
            leadStatusFilter,
            leadQuickFilter,
            leadSortMode,
            showBlacklisted,
            advancedLeadFilters,
          },
        }),
      })
      setSegmentNameDraft('')
      await refreshSavedSegments({ silent: true })
      toast.success(`Saved segment '${name}'`)
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not save segment'
      setLastError(message)
      toast.error(message)
    } finally {
      setSavingSegment(false)
    }
  }

  async function deleteLeadSegment(segmentId) {
    setDeletingSegmentId(segmentId)
    try {
      await fetchJson(`/api/saved-segments/${segmentId}`, { method: 'DELETE' })
      setSavedSegments((prev) => prev.filter((segment) => Number(segment.id) !== Number(segmentId)))
      toast.success('Segment removed')
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not delete segment'
      setLastError(message)
      toast.error(message)
    } finally {
      setDeletingSegmentId(null)
    }
  }

  async function refreshBlacklistEntries() {
    try {
      const data = await fetchJson('/api/blacklist')
      setBlacklistEntries(Array.isArray(data.items) ? data.items : [])
    } catch {
      // Keep the last successful snapshot if blacklist fetch fails.
    }
  }

  useEffect(() => {
    void refreshBlacklistEntries()
  }, [])

  // ──────────────────────────────────────────────────────────────────────

  async function fetchRevenueLog() {
    try {
      const data = await fetchJson('/api/revenue?limit=5')
      setRevenueLog(Array.isArray(data.items) ? data.items : [])
    } catch { /* silent */ }
  }

  async function refreshWorkers() {
    try {
      const data = await fetchJson('/api/workers')
      setWorkers(Array.isArray(data.items) ? data.items : [])
      setWorkerMetrics({
        total_team_cost: Number(data.metrics?.total_team_cost || 0),
        delivery_efficiency_days: Number(data.metrics?.delivery_efficiency_days || 0),
        net_agency_margin: Number(data.metrics?.net_agency_margin || 0),
      })
      setWorkerAudit(Array.isArray(data.audit) ? data.audit : [])
    } catch (error) {
      setLastError(error instanceof Error ? error.message : 'Could not load workers')
    }
  }

  async function refreshDeliveryTasks() {
    try {
      const data = await fetchJson('/api/delivery-tasks')
      const items = Array.isArray(data.items) ? data.items : []
      setDeliveryTasks(items)
      const orderedAutoIds = items.map((task) => `auto-${task.id}`)
      setTaskOrder((prev) => {
        const customIds = prev.filter((id) => !String(id).startsWith('auto-'))
        return [...customIds, ...orderedAutoIds]
      })
      setDeliverySummary({
        total: Number(data.summary?.total || 0),
        todo: Number(data.summary?.todo || 0),
        in_progress: Number(data.summary?.in_progress || 0),
        blocked: Number(data.summary?.blocked || 0),
        done: Number(data.summary?.done || 0),
      })
    } catch (error) {
      setLastError(error instanceof Error ? error.message : 'Could not load delivery tasks')
    }
  }

  async function createWorker(e) {
    e.preventDefault()
    setCreatingWorker(true)
    try {
      await fetchJson('/api/workers', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          worker_name: workerForm.workerName,
          role: workerForm.role,
          monthly_cost: Number(workerForm.monthlyCost || 0),
          status: workerForm.status,
          comms_link: workerForm.commsLink || null,
        }),
      })
      toast.success('New worker added')
      setWorkerForm(defaultWorkerForm)
      setShowHireWorkerForm(false)
      await refreshWorkers()
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to create worker'
      setLastError(message)
      toast.error(message)
    } finally {
      setCreatingWorker(false)
    }
  }

  function startEditWorker(worker) {
    setEditingWorkerId(worker.id)
    setWorkerEditForm({
      workerName: worker.worker_name || '',
      role: (worker.role || 'PPC').toUpperCase(),
      monthlyCost: String(worker.monthly_cost ?? ''),
      status: worker.status || 'Active',
      commsLink: worker.comms_link || '',
    })
  }

  function cancelEditWorker() {
    setEditingWorkerId(null)
    setWorkerEditForm(defaultWorkerForm)
  }

  async function saveWorker(workerId) {
    try {
      await fetchJson(`/api/workers/${workerId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          worker_name: workerEditForm.workerName,
          role: workerEditForm.role,
          monthly_cost: Number(workerEditForm.monthlyCost || 0),
          status: workerEditForm.status,
          comms_link: workerEditForm.commsLink,
        }),
      })
      toast.success('Worker updated')
      cancelEditWorker()
      await refreshWorkers()
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to update worker'
      setLastError(message)
      toast.error(message)
    }
  }

  async function deleteWorker(worker) {
    const confirmed = window.confirm(`Delete worker '${worker.worker_name}'? Assigned leads will be unassigned.`)
    if (!confirmed) return

    setDeletingWorkerId(worker.id)
    try {
      await fetchJson(`/api/workers/${worker.id}`, { method: 'DELETE' })
      toast.success('Worker deleted')
      await Promise.allSettled([refreshWorkers(), refreshLeads(), refreshDeliveryTasks()])
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to delete worker'
      setLastError(message)
      toast.error(message)
    } finally {
      setDeletingWorkerId(null)
    }
  }

  async function assignLeadToWorker(leadId, workerId) {
    setAssigningWorkerLeadId(leadId)
    try {
      await fetchJson(`/api/leads/${leadId}/assign-worker`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ worker_id: workerId ? Number(workerId) : null }),
      })
      toast.success(workerId ? 'Client assigned to worker' : 'Worker assignment removed')
      await Promise.allSettled([refreshLeads(), refreshWorkers(), refreshDeliveryTasks()])
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to assign worker'
      setLastError(message)
      toast.error(message)
    } finally {
      setAssigningWorkerLeadId(null)
    }
  }

  async function _updateDeliveryTask(taskId, payload) {
    setUpdatingDeliveryTaskId(taskId)
    try {
      await fetchJson(`/api/delivery-tasks/${taskId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      toast.success('Delivery task updated')
      await Promise.allSettled([refreshDeliveryTasks(), refreshWorkers()])
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to update delivery task'
      setLastError(message)
      toast.error(message)
    } finally {
      setUpdatingDeliveryTaskId(null)
    }
  }

  async function fetchQualifierData({ silent = false } = {}) {
    if (!canLeadScoring) {
      setQualifierData({ loading: false, data: null, error: '' })
      if (!silent) {
        toast('Lead Qualifier unlocks on The Hustler and above.', { icon: '🔒' })
      }
      return
    }

    try {
      setQualifierData((prev) => ({ ...prev, loading: true, error: '' }))
      const data = await fetchJson('/api/leads/qualify')
      setQualifierData({ loading: false, data, error: '' })
      if (!silent) toast.success('Lead Qualifier refreshed')
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Could not load qualifier data'
      setQualifierData({ loading: false, data: null, error: msg })
      if (!silent) toast.error(msg)
    }
  }

  async function generateColdOutreach(e) {
    e.preventDefault()
    const { businessName, city, niche, painPoint, competitors, monthlyLoss } = coldOutreachForm
    if (!businessName.trim() || !city.trim()) {
      toast.error('Business name and city are required')
      return
    }
    setColdOutreachLoading(true)
    setColdOutreachError('')
    try {
      const data = await fetchJson('/api/mailer/cold-outreach', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          business_name: businessName.trim(),
          city: city.trim(),
          niche: niche.trim() || null,
          pain_point: painPoint.trim() || null,
          competitors: competitors.split(',').map((c) => c.trim()).filter(Boolean),
          monthly_loss: monthlyLoss.trim() || null,
        }),
      })
      setColdOutreachResult({ subject: data.subject, body: data.body, generatedAt: data.generated_at })
      toast.success('Cold outreach email generated')
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Generation failed'
      setColdOutreachError(msg)
      toast.error(msg)
    } finally {
      setColdOutreachLoading(false)
    }
  }

  function copyColdOutreach() {
    const text = `Subject: ${coldOutreachResult.subject}\n\n${coldOutreachResult.body}`
    navigator.clipboard.writeText(text).then(() => toast.success('Copied to clipboard'))
  }

  function mapCountryCodeToScrape(countryCode) {
    const supported = ['US', 'SI', 'DE', 'AT']
    const code = String(countryCode || '').toUpperCase()
    return supported.includes(code) ? code : 'US'
  }

  const selectedSignalCountryCode = String(nicheAdvice.data?.selected_country_code || scrapeForm.country || 'US').toUpperCase()
  const selectedSignalCountryName = COUNTRIES.find((country) => country.code === selectedSignalCountryCode)?.name || selectedSignalCountryCode

  const marketCandidates = useMemo(() => {
    const recommendations = Array.isArray(nicheAdvice.data?.recommendations) ? nicheAdvice.data.recommendations : []
    const performance = Array.isArray(nicheAdvice.data?.performance_snapshot) ? nicheAdvice.data.performance_snapshot : []
    const countryInfo = getCountryTZInfo(selectedSignalCountryCode)
    const fallbackLocation = selectedSignalCountryCode === 'US' ? 'New York, NY' : `${countryInfo.city}, ${selectedSignalCountryCode}`

    const fromPerformance = performance.slice(0, 8).map((item) => {
      const rawKeyword = String(item?.keyword || '').trim()
      if (!rawKeyword) return null
      const lower = rawKeyword.toLowerCase()
      const splitIndex = lower.lastIndexOf(' in ')
      const serviceLabel = splitIndex > -1 ? rawKeyword.slice(0, splitIndex).trim() : rawKeyword
      const location = fallbackLocation
      const keyword = serviceLabel && serviceLabel !== rawKeyword ? `${serviceLabel} in ${location}` : rawKeyword

      return {
        keyword,
        location,
        country_code: selectedSignalCountryCode,
        reason: `Historical performance: ${Number(item?.reply_rate || 0).toFixed(1)}% reply rate across ${Number(item?.sent_count || 0)} sent emails, adapted for ${selectedSignalCountryName}.`,
        expected_reply_rate: Number(item?.reply_rate || 0),
      }
    }).filter(Boolean)

    const pool = [...recommendations, ...fromPerformance].filter((candidate) => {
      const candidateCode = String(candidate?.country_code || selectedSignalCountryCode).toUpperCase()
      return candidateCode === selectedSignalCountryCode
    })
    const dedup = new Map()

    pool.forEach((candidate) => {
      const keyword = String(candidate?.keyword || '').trim()
      if (!keyword) return
      const location = String(candidate?.location || '').trim() || 'US'
      const key = `${keyword.toLowerCase()}__${location.toLowerCase()}`
      const expectedReplyRate = Number(candidate?.expected_reply_rate || 0)

      const normalized = {
        keyword,
        location,
        country_code: String(candidate?.country_code || 'US').toUpperCase() || 'US',
        reason: String(candidate?.reason || 'AI is evaluating current market demand and historical outreach signals.').trim(),
        expected_reply_rate: Number.isFinite(expectedReplyRate) ? expectedReplyRate : 0,
      }

      const existing = dedup.get(key)
      if (!existing || normalized.expected_reply_rate > existing.expected_reply_rate) {
        dedup.set(key, normalized)
      }
    })

    return Array.from(dedup.values())
      .sort((a, b) => b.expected_reply_rate - a.expected_reply_rate)
      .slice(0, 8)
  }, [nicheAdvice.data, selectedSignalCountryCode, selectedSignalCountryName])

  const activeMarketPick = marketCandidates[marketPickIndex] || marketCandidates[0] || nicheAdvice.data?.top_pick || null

  useEffect(() => {
    if (marketCandidates.length === 0) {
      if (marketPickIndex !== 0) setMarketPickIndex(0)
      return
    }
    if (marketPickIndex >= marketCandidates.length) {
      setMarketPickIndex(0)
    }
  }, [marketCandidates.length, marketPickIndex])

  useEffect(() => {
    if (marketCandidates.length <= 1) return undefined
    const intervalId = window.setInterval(() => {
      setMarketPickIndex((prev) => (prev + 1) % marketCandidates.length)
    }, 6000)
    return () => window.clearInterval(intervalId)
  }, [marketCandidates.length])

  function applyRecommendedNiche(recommendation = activeMarketPick) {
    const topPick = recommendation || nicheAdvice.data?.top_pick
    if (!topPick?.keyword) {
      toast.error('No recommendation available yet')
      return
    }

    setScrapeForm((prev) => ({
      ...prev,
      keyword: String(topPick.keyword),
      country: mapCountryCodeToScrape(topPick.country_code),
    }))

    workflowRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    toast.success('Niche applied to Maps Scrape')
  }

  function openMainTab(tabName) {
    setActiveTab(tabName)

    if (tabName === 'mail' || tabName === 'config') {
      void loadConfigForm()
      if (tabName === 'mail') {
        void fetchMailerCampaignStats({ silent: true })
        if (configFormLoaded) {
          void previewMailTemplate({ silent: true })
        }
      }
    }

    if (tabName === 'blacklist') {
      setShowBlacklisted(true)
      setLeadStatusFilter('all')
    } else if (tabName === 'leads') {
      setShowBlacklisted(false)
    } else if (tabName === 'qualify') {
      if (canLeadScoring && !qualifierData.data && !qualifierData.loading) {
        void fetchQualifierData({ silent: true })
      }
    }

    setTimeout(() => {
      mainPanelRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
      if (tabName === 'leads') {
        leadSearchRef.current?.focus()
      }
    }, 60)
  }

  function savePersonalGoal(event) {
    event.preventDefault()
    const nextName = String(goalDraft.name || '').trim() || 'My Goal'
    const rawAmount = String(goalDraft.amount || '').replace(',', '.').trim()
    const nextAmount = Number(rawAmount)
    const nextCurrency = GOAL_CURRENCY_OPTIONS.includes(String(goalDraft.currency || '').toUpperCase())
      ? String(goalDraft.currency || '').toUpperCase()
      : DEFAULT_GOAL_CURRENCY
    if (!Number.isFinite(nextAmount) || nextAmount <= 0) {
      toast.error('Enter a valid goal amount')
      return
    }

    const normalizedAmount = Math.round(nextAmount)
    const nextPayload = {
      name: nextName,
      amount: normalizedAmount,
      currency: nextCurrency,
    }

    setGoalSettings(nextPayload)
    setGoalDraft({
      name: nextName,
      amount: String(normalizedAmount),
      currency: nextCurrency,
    })

    void (async () => {
      try {
        const data = await fetchJson('/api/auth/personal-goal', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(nextPayload),
        })
        const savedCurrencyRaw = String(data.currency || nextCurrency).toUpperCase().trim()
        const saved = {
          name: String(data.name || nextName).trim() || 'My Goal',
          amount: Number(data.amount || normalizedAmount),
          currency: GOAL_CURRENCY_OPTIONS.includes(savedCurrencyRaw) ? savedCurrencyRaw : DEFAULT_GOAL_CURRENCY,
        }
        setGoalSettings(saved)
        setGoalDraft({ name: saved.name, amount: String(Math.round(saved.amount)), currency: saved.currency })
        toast.success('Goal updated')
      } catch {
        toast.success('Goal updated locally')
      }
    })()
  }

  function resetPersonalGoal() {
    const defaults = {
      name: 'My Goal',
      amount: MRR_GOAL_EUR,
      currency: DEFAULT_GOAL_CURRENCY,
    }
    setGoalSettings(defaults)
    setGoalDraft({ name: defaults.name, amount: String(defaults.amount), currency: defaults.currency })

    void (async () => {
      try {
        await fetchJson('/api/auth/personal-goal', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(defaults),
        })
        toast.success('Goal reset')
      } catch {
        toast.success('Goal reset locally')
      }
    })()
  }

  async function fetchMailerCampaignStats({ silent = false } = {}) {
    try {
      setCampaignLoading(true)
      const data = await fetchJson('/api/mailer/campaign-stats')
      setCampaignStats({
        sent: Number(data.sent || 0),
        opened: Number(data.opened || 0),
        replied: Number(data.replied || 0),
        bounced: Number(data.bounced || 0),
        opens_total: Number(data.opens_total || 0),
        open_rate: Number(data.open_rate || 0),
        reply_rate: Number(data.reply_rate || 0),
        bounce_rate: Number(data.bounce_rate || 0),
        ab_breakdown: {
          A: Number(data.ab_breakdown?.A || 0),
          B: Number(data.ab_breakdown?.B || 0),
        },
        sequences: Array.isArray(data.sequences) ? data.sequences : [],
        saved_templates: Array.isArray(data.saved_templates) ? data.saved_templates : [],
        recent_events: Array.isArray(data.recent_events) ? data.recent_events : [],
      })
      if (!silent) toast.success('Campaign stats refreshed')
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not load campaign stats'
      if (!message.toLowerCase().includes('backend is not configured')) {
        setLastError(message)
        if (!silent) toast.error(message)
      }
    } finally {
      setCampaignLoading(false)
    }
  }

  async function saveCampaignSequence() {
    setSavingSequence(true)
    try {
      await fetchJson('/api/mailer/sequences', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...sequenceForm,
          step2_delay_days: Number(sequenceForm.step2_delay_days || 3),
          step3_delay_days: Number(sequenceForm.step3_delay_days || 7),
          active: Boolean(sequenceForm.active),
        }),
      })
      toast.success('Sequence saved')
      await fetchMailerCampaignStats({ silent: true })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to save campaign sequence'
      setLastError(message)
      toast.error(message)
    } finally {
      setSavingSequence(false)
    }
  }

  async function previewMailTemplate({ regenerate = false, silent = false, configOverride = null } = {}) {
    const previewConfig = configOverride || configForm
    setPreviewLoading(true)
    try {
      const data = await fetchJson('/api/mailer/preview', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          regenerate,
          mail_signature: previewConfig.mail_signature || '',
          ghost_subject_template: previewConfig.ghost_subject_template || '',
          ghost_body_template: previewConfig.ghost_body_template || '',
          golden_subject_template: previewConfig.golden_subject_template || '',
          golden_body_template: previewConfig.golden_body_template || '',
          competitor_subject_template: previewConfig.competitor_subject_template || '',
          competitor_body_template: previewConfig.competitor_body_template || '',
          speed_subject_template: previewConfig.speed_subject_template || '',
          speed_body_template: previewConfig.speed_body_template || '',
        }),
      })

      setMailPreview({
        subject: String(data.subject || ''),
        body: String(data.body || ''),
        generatedAt: data.generated_at || new Date().toISOString(),
      })

      const nextBalance = Number(data?.credits_balance)
      const nextLimit = Number(data?.credits_limit)
      if (Number.isFinite(nextBalance)) {
        setUser((prev) => ({
          ...prev,
          credits: Math.max(0, nextBalance),
          creditLimit: Number.isFinite(nextLimit) ? Math.max(1, nextLimit) : prev.creditLimit,
          credits_balance: Math.max(0, nextBalance),
          credits_limit: Number.isFinite(nextLimit) ? Math.max(1, nextLimit) : prev.credits_limit,
        }))
      }

      if (!silent) {
        toast.success(regenerate ? 'Preview regenerated' : 'Preview generated')
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Preview generation failed'
      setLastError(message)
      if (!silent) {
        toast.error(message)
      }
    } finally {
      setPreviewLoading(false)
    }
  }

  async function applyMailTemplatePack(packKey) {
    const pack = visibleMailTemplatePacks.find((item) => item.key === packKey)
    if (!pack) return

    const nextConfig = {
      ...configForm,
      ...pack.templates,
    }

    setConfigForm(nextConfig)
    setActiveMailPack(pack.key)
    await previewMailTemplate({ silent: true, configOverride: nextConfig })
    toast.success(`${pack.label} pack applied`)
  }

  async function submitSale(e) {
    e.preventDefault()
    setSubmittingSale(true)
    const amount = parseFloat(saleForm.amount)
    try {
      await fetchJson('/api/revenue', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          amount,
          service_type: saleForm.serviceType,
          lead_name: saleForm.leadName || null,
          lead_id: saleForm.leadId ? parseInt(saleForm.leadId, 10) : null,
          is_recurring: saleForm.isRecurring,
        }),
      })
      toast.success(`💰 +${formatCurrencyEur(amount)} logged!`)
      if (amount >= 1000) shootConfetti()
      setShowSaleModal(false)
      setSaleForm({ amount: '', serviceType: 'Google Ads Setup', leadName: '', leadId: '', isRecurring: false })
      await Promise.allSettled([refreshStats(), fetchRevenueLog()])
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to save sale')
    } finally {
      setSubmittingSale(false)
    }
  }

  async function startTask(action, endpoint, payload, retries = 0) {
    setPendingRequest(action)
    setLastError('')
    setLastResult('')
    if (action === 'enrich') {
      setEnrichRetrySeconds(0)
    }
    try {
      let data
      if (action === 'enrich') {
        const token = getStoredValue('lf_token')
        const response = await axios.post(
          endpoint,
          payload,
          {
            headers: {
              'Content-Type': 'application/json',
              ...(token ? { Authorization: `Bearer ${token}` } : {}),
            },
          },
        )
        data = response.data
      } else {
        data = await fetchJson(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
      }
      if (action !== 'scrape') {
        setLastResult(JSON.stringify(data, null, 2))
      }
      if (data.status === 'started') toast(`${taskLabels[action]} started`, { icon: '\u23F3' })
      const postStartRequests = [fetchTaskState(), refreshStats(), refreshConfigHealth()]
      if (action === 'scrape') {
        postStartRequests.push(refreshLeads({ silent: true }))
      }
      await Promise.allSettled(postStartRequests)
    } catch (error) {
      const axiosStatus = axios.isAxiosError(error) ? Number(error.response?.status || 0) : 0
      const fetchStatus = Number(error?.status || 0)
      const status = axiosStatus || fetchStatus

      const axiosMessage = axios.isAxiosError(error)
        ? String(error.response?.data?.error || error.response?.data?.detail || error.message || '')
        : ''
      const rawMessage = axiosMessage || (error instanceof Error ? error.message : 'Unknown API error')

      if (action === 'enrich' && status === 429) {
        const capacityMessage = 'Heavy traffic! We are processing other leads, please wait a moment.'
        setEnrichRetrySeconds(30)
        setLastError(capacityMessage)
        toast.error(capacityMessage)
        return
      }

      const friendlyMessage = getFriendlyAiError(endpoint, status, rawMessage)
      setLastError(friendlyMessage)
      toast.error(friendlyMessage)

      const shouldRetry = action === 'enrich' && retries < 1 && status >= 500
      if (shouldRetry) {
        window.setTimeout(() => {
          void startTask(action, endpoint, payload, retries + 1)
        }, 5000)
      }
    } finally {
      setPendingRequest('')
    }
  }

  async function retryTask(taskId) {
    setRetryingTaskId(taskId)
    setLastError('')
    try {
      const data = await fetchJson(`/api/tasks/${taskId}/retry`, { method: 'POST' })
      setLastResult(JSON.stringify(data, null, 2))
      toast('Retry started', { icon: '\uD83D\uDD01' })
      await Promise.allSettled([fetchTaskState(), refreshStats()])
    } catch (error) {
      setLastError(error instanceof Error ? error.message : 'Retry failed')
    } finally {
      setRetryingTaskId(null)
    }
  }

  async function createManualLead(event) {
    event.preventDefault()
    setPendingRequest('manualLead')
    setLastError('')
    try {
      const data = await fetchJson('/api/leads/manual', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ contact_name: manualLeadForm.contactName, email: manualLeadForm.email, business_name: manualLeadForm.businessName }),
      })
      setLastResult(JSON.stringify(data, null, 2))
      toast.success('Manual lead added')
      setManualLeadForm(defaultManualLead)
      await Promise.allSettled([refreshLeads(), refreshStats()])
    } catch (error) {
      setLastError(error instanceof Error ? error.message : 'Manual lead creation failed')
    } finally {
      setPendingRequest('')
    }
  }

  async function updateLeadStatus(leadId, nextStatus) {
    const wasPaid = nextStatus === 'Paid'
    setPendingStatusLeadId(leadId)
    setLastError('')
    try {
      await fetchJson(`/api/leads/${leadId}/status`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: nextStatus }),
      })
      toast.success(`Lead \u2192 ${nextStatus}`)
      if (wasPaid) shootConfetti()
      await Promise.allSettled([refreshLeads(), refreshStats(), refreshWorkers(), refreshDeliveryTasks()])
    } catch (error) {
      setLastError(error instanceof Error ? error.message : 'Lead status update failed')
    } finally {
      setPendingStatusLeadId(null)
    }
  }

  async function updateLeadTier(leadId, newTier) {
    setPendingTierLeadId(leadId)
    setLastError('')
    try {
      await fetchJson(`/api/leads/${leadId}/tier`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tier: newTier }),
      })
      toast.success(`Tier \u2192 ${newTier}`)
      await Promise.allSettled([refreshLeads(), refreshStats()])
    } catch (error) {
      setLastError(error instanceof Error ? error.message : 'Tier update failed')
    } finally {
      setPendingTierLeadId(null)
    }
  }

  async function blacklistLead(leadId) {
    setPendingBlacklistLeadId(leadId)
    setLastError('')
    try {
      const result = await fetchJson(`/api/leads/${leadId}/blacklist`, { method: 'POST' })
      toast.success(`Blacklisted ${result.business_name || 'lead'}`)
      await Promise.allSettled([refreshLeads(), refreshStats(), refreshBlacklistEntries()])
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Blacklist failed'
      setLastError(message)
      toast.error(message)
    } finally {
      setPendingBlacklistLeadId(null)
    }
  }

  async function addBlacklistEntry(e) {
    e?.preventDefault?.()
    const value = String(blacklistForm.value || '').trim()
    if (!value) {
      toast.error('Enter an email or domain first')
      return
    }

    setSubmittingBlacklistEntry(true)
    setLastError('')
    try {
      const result = await fetchJson('/api/blacklist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          kind: blacklistForm.kind,
          value,
          reason: String(blacklistForm.reason || '').trim() || 'Manual dashboard block',
        }),
      })
      toast.success(`Added ${result.value || value} to blacklist`)
      setBlacklistForm((prev) => ({ ...prev, value: '' }))
      await Promise.allSettled([refreshBlacklistEntries(), refreshLeads(), refreshStats()])
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not update blacklist'
      setLastError(message)
      toast.error(message)
    } finally {
      setSubmittingBlacklistEntry(false)
    }
  }

  async function removeBlacklistEntry(kind, value) {
    const entryValue = String(value || '').trim()
    const entryKind = String(kind || 'email').trim().toLowerCase()
    if (!entryValue) return

    const confirmed = window.confirm(`Remove ${entryValue} from the never-contact list?`)
    if (!confirmed) return

    const entryKey = `${entryKind}:${entryValue.toLowerCase()}`
    setPendingBlacklistEntryKey(entryKey)
    setLastError('')
    try {
      const result = await fetchJson(`/api/blacklist?kind=${encodeURIComponent(entryKind)}&value=${encodeURIComponent(entryValue)}`, {
        method: 'DELETE',
      })
      toast.success(`Removed ${result.value || entryValue} from blacklist`)
      await Promise.allSettled([refreshBlacklistEntries(), refreshLeads(), refreshStats()])
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not remove blacklist entry'
      setLastError(message)
      toast.error(message)
    } finally {
      setPendingBlacklistEntryKey('')
    }
  }

  async function unblacklistLead(lead) {
    if (!lead?.id) return
    const label = lead.business_name || lead.email || 'this lead'
    const confirmed = window.confirm(`Remove ${label} from the blacklist and restore outreach eligibility?`)
    if (!confirmed) return

    setPendingBlacklistLeadId(lead.id)
    setLastError('')
    try {
      const result = await fetchJson(`/api/leads/${lead.id}/blacklist`, { method: 'DELETE' })
      toast.success(`Reactivated ${result.business_name || label}`)
      await Promise.allSettled([refreshBlacklistEntries(), refreshLeads(), refreshStats()])
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not unblacklist lead'
      setLastError(message)
      toast.error(message)
    } finally {
      setPendingBlacklistLeadId(null)
    }
  }

  function copyEmail(email) {
    if (!email) return
    navigator.clipboard.writeText(email).then(
      () => toast('Email copied', { icon: '\uD83D\uDCCB' }),
      () => toast.error('Could not copy'),
    )
  }

  async function onScrapeSubmit(e) {
    e?.preventDefault?.()
    const keyword = String(scrapeForm.keyword || '').trim()
    if (!keyword || keyword.length < 2) {
      setLastError('Keyword must be at least 2 characters')
      toast.error('Keyword required (min 2 chars)')
      return
    }
    if (creditsBalance <= 0) {
      toast.error('Out of credits. Please upgrade or buy more credits.')
      return
    }
    if (scrapeForm.exportTargets && !canBulkExport) {
      toast('Auto-export unlocks on The Growth and above.', { icon: '🔒' })
      return
    }
    setPendingRequest('scrape')
    setLastError('')
    try {
      await fetchJson('/api/scrape', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keyword, results: Number(scrapeForm.results), country: scrapeForm.country, headless: Boolean(scrapeForm.headless), export_targets: Boolean(scrapeForm.exportTargets), min_rating: 3.5 }),
      })
      setTasks((prev) => ({
        ...prev,
        scrape: {
          ...(prev?.scrape || getIdleTask('scrape')),
          status: 'queued',
          running: true,
          last_request: {
            ...(prev?.scrape?.last_request || {}),
            keyword,
            results: Number(scrapeForm.results),
            country: scrapeForm.country,
          },
          result: {
            phase: 'queued',
            total_to_find: Number(scrapeForm.results || 0),
            current_found: 0,
            scanned_count: 0,
            inserted: 0,
          },
          error: null,
        },
      }))
      toast('Scrape started', { icon: '⏳' })
      void Promise.allSettled([fetchTaskState(true), refreshStats()])
    } catch (error) {
      setLastError(error instanceof Error ? error.message : 'Unknown API error')
    } finally {
      setPendingRequest('')
    }
  }
  function onEnrichSubmit(e) {
    e?.preventDefault?.()
    if (creditsBalance <= 0) {
      toast.error('Out of credits. Please upgrade or buy more credits.')
      return
    }
    void startTask('enrich', '/api/enrich', {
      limit: Number(enrichForm.limit),
      headless: Boolean(enrichForm.headless),
      skip_export: Boolean(enrichForm.skipExport),
      token: getStoredValue('lf_token') || undefined,
    })
  }
  function onMailerSubmit(e) {
    e?.preventDefault?.()
    if (creditsBalance <= 0) {
      toast.error('Out of credits. Please upgrade or buy more credits.')
      return
    }
    setShowMailerConfirm(true)
  }

  async function onMailerConfirm() {
    setShowMailerConfirm(false)
    setMailerStopRequested(false)
    const payload = {
      limit: Number(mailerForm.limit),
      delay_min: Number(mailerForm.delayMin) * 60,
      delay_max: Number(mailerForm.delayMax) * 60,
    }
    if (mailerScheduledHour !== 'now') {
      const tzInfo = getCountryTZInfo(scrapeForm.country || 'US')
      payload.start_after_hour_est = localHourToET(Number(mailerScheduledHour), tzInfo.tz)
    }
    void startTask('mailer', '/api/mailer/send', payload)
  }

  async function onMailerStop() {
    setMailerStopRequested(true)
    try {
      await fetchJson('/api/mailer/stop', { method: 'POST' })
      toast.error('Emergency Stop sent — stopping mailer now.', { duration: 5000 })
      await Promise.allSettled([fetchTaskState(), refreshStats()])
    } catch {
      setMailerStopRequested(false)
      toast.error('Failed to send stop signal.')
    }
  }

  function sendManualEmail(lead) {
    if (!lead?.email) {
      toast.error('Lead has no email')
      return
    }
    const subject = encodeURIComponent(`Quick idea for ${lead.business_name || 'your business'}`)
    window.location.href = `mailto:${lead.email}?subject=${subject}`
  }

  function setMeetingStatus(leadId) {
    void updateLeadStatus(leadId, 'Meeting Set')
  }

  function openEmailPreviewModal(lead) {
    setEmailPreviewLead({
      businessName: lead.business_name || 'Lead',
      subject: lead.generated_email_subject || '',
      body: lead.generated_email_body || '',
    })
  }

  function closeEmailPreviewModal() {
    setEmailPreviewLead(null)
  }

  function openAiSummaryModal(lead) {
    setAiSummaryPreviewLead({
      businessName: lead.business_name || 'Lead',
      summary: lead.ai_description || '',
      companyAudit: lead.company_audit || {},
      competitors: normalizeLeadInsightList(lead.competitor_snapshot, 3),
      intentSignals: normalizeLeadInsightList(lead.intent_signals, 6),
      techStack: normalizeLeadInsightList(lead.tech_stack, 5),
      achievements: normalizeLeadInsightList(lead.latest_achievements, 3),
      bestLeadScore: resolveBestLeadScore(lead),
      leadPriority: lead.lead_priority || '',
      employeeCount: Number(lead.employee_count || 0),
      sentimentScore: resolveLeadSignalScore(lead),
      qualificationScore: Number(lead.qualification_score || lead.lead_score_100 || 0),
      socialActivityScore: Number(lead.social_activity_score || 0),
      competitiveHook: lead.competitive_hook || '',
      mainOffer: lead.main_offer || '',
      googleMaps: lead.google_maps || {},
      websiteSignals: lead.website_signals || {},
      socialProfiles: lead.social_profiles || {
        linkedin: lead.linkedin_url || '',
        instagram: lead.instagram_url || '',
        facebook: lead.facebook_url || '',
      },
      socialMetrics: lead.social_metrics || {},
    })
  }

  function closeAiSummaryModal() {
    setAiSummaryPreviewLead(null)
  }

  function openTaskAiMessagePreview(item) {
    if (item.source !== 'auto') return
    const lead = item.linkedLead || (Number.isFinite(Number(item.rawTask?.lead_id)) ? leadsById.get(Number(item.rawTask.lead_id)) : null)
    if (!lead) {
      toast.error('Lead details are still loading. Try again in a moment.')
      return
    }

    const summary = String(lead.ai_description || '').trim()
    const subject = String(lead.generated_email_subject || '').trim()
    const body = String(lead.generated_email_body || '').trim()

    if (!summary && !subject && !body) {
      toast('No AI message has been generated for this lead yet.')
      return
    }

    setTaskAiPreviewLead({
      businessName: lead.business_name || item.title || 'Lead',
      summary,
      subject,
      body,
    })
  }

  function closeTaskAiMessagePreview() {
    setTaskAiPreviewLead(null)
  }

  async function copyTaskAiField(label, value) {
    const text = String(value || '').trim()
    if (!text) {
      toast('Nothing to copy yet.')
      return
    }

    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text)
      } else {
        const helper = document.createElement('textarea')
        helper.value = text
        helper.setAttribute('readonly', 'true')
        helper.style.position = 'fixed'
        helper.style.opacity = '0'
        document.body.appendChild(helper)
        helper.focus()
        helper.select()
        const copied = document.execCommand('copy')
        document.body.removeChild(helper)
        if (!copied) throw new Error('copy command failed')
      }
      toast.success(`${label} copied to clipboard`)
    } catch {
      toast.error(`Could not copy ${label.toLowerCase()}`)
    }
  }

  function taskPillClass(task) {
    const status = String(task?.status || '').toLowerCase().trim()
    if (task?.running || status === 'running') {
      return 'bg-blue-500/10 text-blue-300 border-blue-400/30'
    }
    if (status === 'stopped') {
      return 'bg-slate-700/25 text-slate-200 border-slate-500/40'
    }
    if (status === 'error' || status === 'failed' || status === 'generation_failed') {
      return 'bg-rose-500/10 text-rose-300 border-rose-400/35'
    }
    if (status === 'completed' || status === 'done') {
      return 'bg-emerald-500/10 text-emerald-300 border-emerald-400/35'
    }
    return 'bg-slate-600/20 text-slate-200 border-slate-500/30'
  }

  function submitLabel(action, running, pending) {
    if (running) return 'Running\u2026'
    if (pending) return 'Starting\u2026'
    if (action === 'scrape') return 'Start Scrape'
    if (action === 'enrich') return 'Start Enrichment'
    if (action === 'mailer') return 'Start Mailer'
    return 'Submit'
  }

  const mrrMilestone = useMemo(() => {
    if (revenueProgress >= 100) return '\uD83C\uDF89 Goal Reached!'
    if (revenueProgress >= 75) return '\uD83D\uDD25 Almost there'
    if (revenueProgress >= 50) return '\uD83D\uDE80 Halfway'
    if (revenueProgress >= 25) return '\u26A1 Gaining momentum'
    return '\uD83C\uDF31 Getting started'
  }, [revenueProgress])
  const toneProfile = useMemo(
    () => deriveToneProfile(mailPreview.subject, mailPreview.body),
    [mailPreview.subject, mailPreview.body],
  )
  const previewSenderName = currentUserName || configForm.smtp_accounts?.[0]?.from_name || currentUserEmail || 'Your sender name'
  const previewSenderEmail = currentUserEmail || configForm.smtp_accounts?.[0]?.email || 'sender@domain.com'
  const userInitial = String(displayName || currentUserEmail || 'U').trim().charAt(0).toUpperCase() || 'U'
  const normalizedSubscriptionStatus = String(user?.subscriptionStatus || '').toLowerCase().trim()
  const lifecycleSubscriptionStatus = String(user?.subscription_status || '').toLowerCase().trim()
  const canBulkExport = Boolean(featureAccess.bulk_export)
  const canLeadScoring = Boolean(featureAccess.ai_lead_scoring)
  const canAdvancedReporting = Boolean(featureAccess.advanced_reporting)
  const canClientSuccessDashboard = Boolean(featureAccess.client_success_dashboard)
  const cancelAtRaw = String(user?.subscription_cancel_at || '').trim()
  const cancelAtDate = cancelAtRaw ? new Date(cancelAtRaw) : null
  const cancelAtValid = Boolean(cancelAtDate && !Number.isNaN(cancelAtDate.getTime()))
  const cancelPending = Boolean(user?.subscription_cancel_at_period_end) && cancelAtValid && cancelAtDate > new Date()
  const hasActiveSubscription = Boolean(user?.isSubscribed)
    || (normalizedSubscriptionStatus
    ? ['active', 'paid', 'trialing'].includes(normalizedSubscriptionStatus)
    : false)
    || ['active', 'paid', 'trialing', 'cancelled_pending'].includes(lifecycleSubscriptionStatus)
    || (Boolean(user?.subscription_active) && planKey !== 'free')
  const fallbackPlanName = String(user?.currentPlanName || (hasActiveSubscription ? 'Pro Plan' : 'Free Plan')).trim()
    .replace(/\s*\((cancelled|canceled).*$/i, '')
  const currentPlanName = SUBSCRIPTION_PLAN_DETAILS[planKey]?.displayName || fallbackPlanName || (hasActiveSubscription ? 'Pro Plan' : 'Free Plan')
  const cancelUntilLabel = cancelPending && cancelAtValid ? cancelAtDate.toLocaleDateString() : ''
  const selectedTopUpPackage = useMemo(
    () => TOP_UP_PACKAGES.find((pkg) => pkg.id === selectedTopUpPackageId) || TOP_UP_PACKAGES[0],
    [selectedTopUpPackageId],
  )
  const rawCredits = user?.credits ?? user?.credits_balance
  const rawCreditLimit = user?.monthly_quota ?? user?.monthly_limit ?? user?.creditLimit ?? user?.credits_limit
  const creditsBalance = Number(rawCredits ?? 0)
  const baseCreditsLimit = Math.max(1, Number(rawCreditLimit ?? DEFAULT_FREE_CREDIT_LIMIT))
  const topupCreditsBalance = Math.max(0, Number(user?.topup_credits_balance ?? 0))
  const creditsLimit = Math.max(baseCreditsLimit, creditsBalance, baseCreditsLimit + topupCreditsBalance)
  const creditsPercent = Math.max(0, Math.min(100, Math.round((creditsBalance / creditsLimit) * 100)))
  const isOutOfCredits = creditsBalance <= 0
  const topupLabel = topupCreditsBalance > 0
    ? `${topupCreditsBalance.toLocaleString('en-US')} purchased top-up credits included`
    : ''
  const selectedUserNiche = String(user?.niche || qualifierData?.data?.selected_niche || getStoredValue('lf_niche') || '').trim()
  const visibleLiveMailTemplateCards = useMemo(
    () => resolveLiveMailTemplateCardsForNiche(selectedUserNiche),
    [selectedUserNiche],
  )
  const visibleMailTemplatePacks = useMemo(
    () => resolveMailTemplatePacksForNiche(selectedUserNiche),
    [selectedUserNiche],
  )
  useEffect(() => {
    if (!visibleLiveMailTemplateCards.some((card) => card.key === activeLiveMailTemplateKey)) {
      setActiveLiveMailTemplateKey(visibleLiveMailTemplateCards[0]?.key || 'ghost')
    }
  }, [visibleLiveMailTemplateCards, activeLiveMailTemplateKey])
  const visibleMainNavItems = useMemo(
    () => mainNavItems.filter((item) => item.tab !== 'clients' || canClientSuccessDashboard),
    [canClientSuccessDashboard],
  )
  useEffect(() => {
    if (!canBulkExport) {
      setScrapeForm((prev) => (prev.exportTargets ? { ...prev, exportTargets: false } : prev))
    }
  }, [canBulkExport])
  useEffect(() => {
    if (!canLeadScoring) {
      setQualifierData((prev) => (prev.data || prev.error ? { loading: false, data: null, error: '' } : prev))
    }
  }, [canLeadScoring])
  useEffect(() => {
    if (canAdvancedReporting) {
      void Promise.allSettled([
        refreshWeeklyReport({ silent: true }),
        refreshMonthlyReport({ silent: true }),
      ])
    } else {
      setWeeklyReport(null)
      setMonthlyReport(null)
    }
  }, [canAdvancedReporting, refreshMonthlyReport, refreshWeeklyReport])
  useEffect(() => {
    if (canClientSuccessDashboard) {
      void Promise.allSettled([
        refreshClientFolders({ silent: true }),
        refreshClientDashboard({ silent: true }),
      ])
    } else {
      setClientFolders([])
    }
  }, [canClientSuccessDashboard, refreshClientDashboard, refreshClientFolders])
  useEffect(() => {
    if (activeTab === 'clients' && !canClientSuccessDashboard) {
      setActiveTab('leads')
    }
  }, [activeTab, canClientSuccessDashboard])
  useEffect(() => {
    const frameId = window.requestAnimationFrame(() => setAnimatedCreditsPercent(creditsPercent))
    return () => window.cancelAnimationFrame(frameId)
  }, [creditsPercent])
  const isCreditsLow = creditsBalance / creditsLimit < 0.1
  const creditsLabelClass = isCreditsLow ? 'text-amber-300' : 'text-yellow-200'
  const resetLabel = useMemo(() => {
    if (hasActiveSubscription && planKey !== 'free') {
      return 'Paid plan credits do not reset monthly'
    }
    const days = Number(user?.next_reset_in_days)
    if (Number.isFinite(days)) {
      return days <= 0 ? 'Credits reset today' : `Credits reset in ${Math.max(0, Math.round(days))} day${Math.round(days) === 1 ? '' : 's'}`
    }
    const rawDate = String(user?.next_reset_at || '').trim()
    if (rawDate) {
      const parsed = new Date(rawDate)
      if (!Number.isNaN(parsed.getTime())) {
        return `Next reset: ${parsed.toLocaleDateString()}`
      }
      return `Next reset: ${rawDate}`
    }
    return 'Monthly credits active'
  }, [hasActiveSubscription, planKey, user?.next_reset_at, user?.next_reset_in_days])
  const estimatedOpportunityLabel = useMemo(() => {
    const expectedReplyRate = Number(activeMarketPick?.expected_reply_rate || nicheAdvice.data?.top_pick?.expected_reply_rate || 0)
    if (expectedReplyRate >= 8) return '$75k+'
    if (expectedReplyRate >= 5) return '$50k+'
    if (expectedReplyRate >= 3) return '$30k+'
    return '$15k+'
  }, [activeMarketPick?.expected_reply_rate, nicheAdvice.data?.top_pick?.expected_reply_rate])
  if (!hasSessionToken) {
    return (
      <div className="app-root min-h-screen flex items-center justify-center bg-[#07111f] px-6 text-slate-100">
        <Toaster
          position="top-right"
          toastOptions={{
            duration: 3500,
            style: { background: '#1e2a3a', color: '#e2e8f0', border: '1px solid rgba(255,255,255,0.08)', borderRadius: '14px' },
          }}
        />
        <div className="rounded-3xl border border-white/10 bg-white/5 px-6 py-5 text-center shadow-2xl backdrop-blur-sm">
          <p className="text-sm font-medium text-white">Session required</p>
          <p className="mt-1 text-sm text-slate-400">Redirecting to login…</p>
        </div>
      </div>
    )
  }
  return (
    <div className="app-root">
      <Toaster
        position="top-right"
        toastOptions={{
          duration: 3500,
          style: { background: '#1e2a3a', color: '#e2e8f0', border: '1px solid rgba(255,255,255,0.08)', borderRadius: '14px' },
        }}
      />

      <aside className="dashboard-sidebar hidden xl:flex">
        <div className="dashboard-sidebar-shell">
          <div className="mb-3 flex items-center gap-2.5 px-1 pt-1">
            <a href="/?stay=1" className="flex items-center gap-2.5 group" title="Go to landing page">
              <span className="flex h-7 w-7 flex-shrink-0 items-center justify-center rounded-xl group-hover:opacity-80 transition-opacity" style={{background: 'linear-gradient(135deg, #d9a406, #FFC107)'}}>
                <Zap className="h-3.5 w-3.5" style={{color: '#0a1422'}} />
              </span>
              <span className="text-[0.875rem] font-bold tracking-[-0.025em] text-white group-hover:text-yellow-300 transition-colors">Sni<span style={{color: '#FFC107'}}>ped</span></span>
            </a>
          </div>
          <div>
            <SidebarLeadFlowPanel
              isPaid={hasActiveSubscription}
              planName={currentPlanName}
              cancelPending={cancelPending}
              cancelUntilLabel={cancelUntilLabel}
              creditsBalance={creditsBalance}
              monthlyLimit={creditsLimit}
              creditsPercent={animatedCreditsPercent}
              creditsLabelClass={creditsLabelClass}
              resetLabel={resetLabel}
              topupLabel={topupLabel}
              onUpgrade={openPricingSection}
              onChangeSubscription={openPricingSection}
              onTopUp={handleTopUpClick}
            />
          </div>

          <div className="my-3 border-b border-[#FFC107]/20" />

          <div className="grid gap-2">
            {visibleMainNavItems.map((item) => {
              const Icon = item.icon
              return (
                <button
                  key={item.tab}
                  className={`topbar-nav w-full justify-start ${activeTab === item.tab ? 'topbar-nav-active' : ''}`}
                  type="button"
                  onClick={() => openMainTab(item.tab)}
                >
                  <Icon className="h-4 w-4" /> {item.label}
                </button>
              )
            })}
          </div>

          <button
            className="topbar-nav topbar-nav-sale mt-3 w-full justify-start"
            type="button"
            onClick={() => setShowSaleModal(true)}
          >
            <PlusCircle className="h-4 w-4" /> Add Sale
          </button>

          <button
            className="topbar-nav mt-2 w-full justify-start"
            type="button"
            onClick={() => window.location.assign('/settings')}
          >
            <Settings className="h-4 w-4" /> Settings
          </button>

          <div className="pb-2" />
        </div>
      </aside>

      <div className="flex w-full flex-col gap-3 px-8 pb-8 pt-1 xl:pl-[16rem]">
        <header className="topbar sticky top-4 z-40 xl:hidden">
          <div className="topbar-shell">
            <div className="flex flex-wrap items-center gap-3">
              <a href="/?stay=1" className="topbar-nav topbar-nav-active">
                <Zap className="h-4 w-4" style={{color:'#D4AF37'}} /> Sni<span style={{color:'#D4AF37'}}>ped</span>
              </a>
              {visibleMainNavItems.map((item) => {
                const Icon = item.icon
                return (
                  <button key={item.tab} className={`topbar-nav ${activeTab === item.tab ? 'topbar-nav-active' : ''}`} type="button" onClick={() => openMainTab(item.tab)}>
                    <Icon className="h-4 w-4" /> {item.label}
                  </button>
                )
              })}
              <button
                className="topbar-nav topbar-nav-sale"
                type="button"
                onClick={() => setShowSaleModal(true)}
              >
                <PlusCircle className="h-4 w-4" /> Add Sale
              </button>
              <button className="topbar-nav" type="button" onClick={() => window.location.assign('/settings')}>
                <Settings className="h-4 w-4" /> Settings
              </button>
            </div>

            <div className="flex flex-wrap items-center justify-between gap-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
              <div className="flex flex-wrap items-center gap-3">
                <StatusDot label="API" ok={health === 'online'} />
                <StatusDot label="SMTP" ok={configHealth.smtp_ok} />
                <StatusDot label="OpenAI" ok={configHealth.openai_ok} />
              </div>
              <div
                id="credits-display"
                className="ml-auto flex items-center gap-2 normal-case tracking-normal"
              >
                <button
                  className="topbar-nav h-8 min-h-0 px-2.5 text-[11px]"
                  type="button"
                  onClick={() => { void handleTopUpClick() }}
                  title="Top up credits"
                >
                  <PlusCircle className="h-3.5 w-3.5" />
                </button>
                <div className="flex items-center gap-1.5 rounded-full border border-cyan-500/30 bg-cyan-950/35 px-2.5 py-1">
                  <Zap className="h-3 w-3 text-amber-300" />
                  <span className={`text-[11px] font-semibold ${creditsLabelClass}`}>
                    {creditsBalance.toLocaleString('en-US')}
                  </span>
                </div>
                <div className="flex h-8 w-8 items-center justify-center rounded-full border border-white/15 bg-slate-800/70 text-[11px] font-semibold text-slate-200">
                  {userInitial}
                </div>
              </div>
            </div>
          </div>
        </header>

        <section className="hero-panel">
          <div>
            <p className="label-overline">Sniped · AI Lead Engine</p>

            {/* =========================================================
                HERO COPY — uncomment ONE option, comment the others.
                ========================================================= */}

            {/* Option 1 — Direct, money-oriented (DEFAULT) */}
            <h1 className="mt-3 max-w-4xl text-[2.6rem] font-bold leading-[1.15] tracking-tight text-white sm:text-5xl">
              Your AI-powered pipeline to predictable revenue.{' '}
              <span style={{color:'#D4AF37'}}>No more guessing.</span>
            </h1>
            <p className="mt-4 max-w-2xl text-base leading-7 text-slate-400">
              Sniped automatically finds, qualifies, and engages leads that are ready to buy.
              All from one control surface.
            </p>

            {/* Option 2 — Aggressive, growth-oriented
            <h1 className="mt-3 max-w-4xl text-[2.6rem] font-bold leading-[1.15] tracking-tight text-white sm:text-5xl">
              Turn cold data into warm leads faster than you can say{' '}
              <span style={{color:'#D4AF37'}}>&lsquo;Closing&rsquo;.</span>
            </h1>
            <p className="mt-4 max-w-2xl text-base leading-7 text-slate-400">
              Sniped: The AI machine that replaces an entire outbound agency.
              Just set, scrape, and sell.
            </p>
            */}

            {/* Option 3 — Minimalist, time-oriented
            <h1 className="mt-3 max-w-4xl text-[2.6rem] font-bold leading-[1.15] tracking-tight text-white sm:text-5xl">
              Stop hunting for leads.{' '}
              <span style={{color:'#D4AF37'}}>Start closing them with Sniped AI.</span>
            </h1>
            <p className="mt-4 max-w-2xl text-base leading-7 text-slate-400">
              AI does the work, you get the credit. Your streamlined control surface
              for automatic, high-converting outreach.
            </p>
            */}
          </div>
          <div className="hero-metrics">
            <div className="hero-metric">
              <span>Next drip</span>
              <strong>{countdown ?? '\u2014'}</strong>
            </div>
            <div className="hero-metric">
              <span>Queued mail</span>
              <strong>{stats.queued_mail_count}</strong>
            </div>
            <div className="hero-metric">
              <span>Digest ETA</span>
              <strong>{digestCountdown}</strong>
            </div>
          </div>
        </section>

        <section className="grid grid-cols-12 items-start gap-3">
          <section className="glass-card col-span-12 rounded-[24px] p-5">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div>
                <p className="label-overline">Performance Overview</p>
                <h2 className="mt-2 text-2xl font-semibold text-white">Revenue, replies, and pipeline health</h2>
                <p className="mt-2 text-sm leading-6 text-slate-400">Seven-day trend snapshots built from live lead and CRM activity.</p>
              </div>
              <button className="sidebar-btn" type="button" onClick={() => void refreshDashboard()} disabled={refreshingDashboard}>
                <RefreshCw className={`h-3.5 w-3.5 ${refreshingDashboard ? 'animate-spin' : ''}`} /> {refreshingDashboard ? 'Refreshing...' : 'Refresh Data'}
              </button>
            </div>
            {lastManualRefreshAt ? (
              <p className="mt-2 text-xs text-slate-500">Last manual refresh: {new Date(lastManualRefreshAt).toLocaleTimeString()}</p>
            ) : null}

            <div className="mt-5 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              <MetricSparkCard
                icon={<DollarSign className="h-5 w-5" />}
                label="Total Setup Revenue"
                value={formatCurrencyEur(stats.setup_revenue)}
                subtitle={`${stats.paid_count} paid clients · 1.300€/setup`}
                points={performanceSeries.revenue}
                tone="amber"
              />
              <MetricSparkCard
                icon={<TrendingUp className="h-5 w-5" />}
                label="Live MRR"
                value={formatCurrencyEur(agencyMrrForGoal)}
                subtitle={`${stats.website_clients || 0}w · ${stats.ads_clients || 0}ads · ${stats.ads_and_website_clients || 0}both`}
                points={performanceSeries.mrr}
                tone="cyan"
                live
              />
              <MetricSparkCard
                icon={<MessageCircle className="h-5 w-5" />}
                label="Reply Rate"
                value={`${Number(stats.reply_rate || 0).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 })}%`}
                subtitle={`Total Replies: ${Number(stats.replies_count || 0).toLocaleString()}`}
                points={performanceSeries.replies}
                tone={Number(stats.reply_rate || 0) > 5 ? 'emerald' : Number(stats.reply_rate || 0) < 2 ? 'amber' : 'slate'}
              />
              <MetricSparkCard
                icon={<Users className="h-5 w-5" />}
                label="Pipeline"
                value={stats.total_leads.toLocaleString()}
                subtitle={`${stats.emails_sent.toLocaleString()} emailed · ${stats.opened_count.toLocaleString()} opened`}
                points={performanceSeries.replies.map((value, index) => value + (performanceSeries.revenue[index] / 1200))}
                tone="violet"
              />
            </div>

            <div id="revenue-stat" className="mt-4 rounded-[18px] border border-cyan-500/20 bg-[linear-gradient(135deg,rgba(8,47,73,0.38),rgba(15,23,42,0.92))] p-4 shadow-[0_14px_38px_rgba(8,47,73,0.28)]">
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div className="max-w-3xl">
                  <p className="label-overline text-cyan-300">Revenue Opportunity Widget</p>
                  <h3 className="mt-2 text-2xl font-semibold leading-tight text-white">
                    Sniped has identified <span className="text-emerald-300">{formatCurrencyEur(totalOpportunityValue)}</span> in high-value opportunities for you.
                  </h3>
                  <p className="mt-2 text-sm leading-6 text-slate-300">
                    {hotOpportunityCount.toLocaleString()} hot leads above 7/10 · Average Deal Value {formatCurrencyEur(averageDealValue)}
                  </p>
                </div>
                <div className="rounded-2xl border border-cyan-400/20 bg-slate-950/45 px-4 py-3 text-right">
                  <p className="text-[11px] uppercase tracking-[0.16em] text-slate-400">Mailer Coverage</p>
                  <p className="mt-1 text-2xl font-semibold text-white">{hotLeadContactPct}%</p>
                  <p className="mt-1 text-xs text-slate-400">{contactedHotLeadCount}/{hotOpportunityCount} hot leads contacted</p>
                </div>
              </div>

              <div className="mt-4">
                <div className="mb-2 flex items-center justify-between text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                  <span>Contacted via Mailer</span>
                  <span>{remainingHotLeadCount} still untouched</span>
                </div>
                <div className="h-2.5 w-full overflow-hidden rounded-full bg-slate-800/85">
                  <div
                    className="h-full rounded-full bg-gradient-to-r from-cyan-400 via-sky-500 to-emerald-400 transition-[width] duration-500"
                    style={{ width: `${hotLeadContactPct}%` }}
                  />
                </div>
              </div>
            </div>

            {revenueLog.length > 0 && (
              <div className="mt-4 rounded-[18px] border border-emerald-500/20 bg-emerald-950/20 p-3">
                <p className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-emerald-400">Recent Wins</p>
                <div className="space-y-2">
                  {revenueLog.map((entry) => (
                    <div key={entry.id} className="flex items-center justify-between gap-3 rounded-xl border border-white/5 bg-white/[0.03] px-3 py-2.5">
                      <div className="flex items-center gap-2.5">
                        <span className="text-base leading-none">💰</span>
                        <span className="text-sm font-semibold text-white">
                          +{formatCurrencyEur(entry.amount)}
                        </span>
                        <span className="text-slate-400">·</span>
                        <span className="text-sm text-slate-300">{entry.lead_name || 'Direct'}</span>
                        <span className="text-slate-500">·</span>
                        <span className="text-xs text-slate-400">{entry.service_type}</span>
                      </div>
                      <div className="flex shrink-0 items-center gap-2">
                        {entry.is_recurring ? (
                          <span className="rounded-full border border-cyan-500/25 bg-cyan-500/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-cyan-400">MRR</span>
                        ) : null}
                        <span className="text-[11px] text-slate-600">
                          {entry.date ? new Date(entry.date).toLocaleDateString() : ''}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div className="mt-4 rounded-[18px] border border-slate-700/50 bg-slate-900/60 p-4 shadow-[0_12px_36px_rgba(2,6,23,0.32)]">
              <div className="flex flex-wrap items-end justify-between gap-4">
                <div>
                  <p className="label-overline">{goalSettings.name}</p>
                  <h3 className="mt-2 text-2xl font-semibold text-white">
                    {formatGoalCurrency(agencyMrrForGoal, goalSettings.currency)}
                    <span className="text-slate-500 text-lg font-medium"> / {formatGoalCurrency(goalSettings.amount || stats.mrr_goal || MRR_GOAL_EUR, goalSettings.currency)}</span>
                  </h3>
                  <p className="mt-1 text-sm leading-6 text-slate-400">Set your own goal name and target amount below.</p>
                </div>
                <div className="flex flex-col items-end gap-2">
                  <span className="milestone-label">{mrrMilestone}</span>
                  <span className="rounded-full border border-teal-500/20 bg-teal-500/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.16em] text-teal-300">
                    {revenueProgress}% complete
                  </span>
                </div>
              </div>

              <div className="group relative mt-5">
                <div className="goal-track">
                  <div className="goal-fill" style={{ width: `${revenueProgress}%` }} />
                </div>
                <div className="tooltip-card pointer-events-none absolute -top-11 left-1/2 hidden -translate-x-1/2 group-hover:block">
                  Remaining to {goalSettings.name}: {formatGoalCurrency(mrrRemaining, goalSettings.currency)}
                </div>
              </div>

              <div className="relative mt-2 flex justify-between">
                {[25, 50, 75, 100].map((pct) => (
                  <div key={pct} className="flex flex-col items-center">
                    <span className={`text-[10px] font-semibold ${revenueProgress >= pct ? 'text-teal-300' : 'text-slate-600'}`}>{pct}%</span>
                  </div>
                ))}
              </div>

              <div className="mt-4 flex flex-wrap items-center gap-2 text-xs font-semibold uppercase tracking-[0.14em]">
                <span className="text-slate-500 mr-1">Client mix</span>
                <span className="tier-chip tier-chip-std">Web only <strong>{tierSummary.standard}</strong></span>
                <span className="tier-chip tier-chip-prem">Ads only <strong>{tierSummary.premium_ads}</strong></span>
                <span className="tier-chip tier-chip-prem" style={{ borderColor: 'rgba(52,211,153,0.3)', color: '#6ee7b7' }}>Ads+Web <strong>{tierSummary.both}</strong></span>
              </div>

              <form className="mt-4 grid gap-3 rounded-2xl border border-cyan-500/25 bg-cyan-950/10 p-4 md:grid-cols-[1fr_200px_170px_auto]" onSubmit={savePersonalGoal}>
                <label className="field-label">
                  <span className="mb-1.5 block text-[11px] uppercase tracking-[0.12em] text-cyan-300">Goal Name</span>
                  <input
                    className="glass-input"
                    type="text"
                    placeholder="npr. BMW Fund"
                    value={goalDraft.name}
                    onChange={(e) => setGoalDraft((prev) => ({ ...prev, name: e.target.value }))}
                  />
                </label>
                <label className="field-label">
                  <span className="mb-1.5 block text-[11px] uppercase tracking-[0.12em] text-cyan-300">Target Amount (€)</span>
                  <input
                    className="glass-input"
                    type="number"
                    min="1"
                    step="1"
                    value={goalDraft.amount}
                    onChange={(e) => setGoalDraft((prev) => ({ ...prev, amount: e.target.value }))}
                    required
                  />
                </label>
                <label className="field-label">
                  <span className="mb-1.5 block text-[11px] uppercase tracking-[0.12em] text-cyan-300">Currency</span>
                  <div className="relative">
                    <select
                      className="glass-input appearance-none pr-8"
                      value={goalDraft.currency}
                      onChange={(e) => setGoalDraft((prev) => ({ ...prev, currency: e.target.value }))}
                    >
                      {GOAL_CURRENCY_OPTIONS.map((value) => <option key={value} value={value}>{value}</option>)}
                    </select>
                    <ChevronDown className="select-chevron" />
                  </div>
                </label>
                <div className="flex items-end">
                  <div className="flex w-full items-center gap-2 md:w-auto">
                    <button type="button" className="btn-ghost h-11 w-full md:w-auto" onClick={resetPersonalGoal}>
                      Reset Goal
                    </button>
                    <button type="submit" className="btn-primary h-11 w-full md:w-auto">
                      <Save className="h-4 w-4" /> Save Goal
                    </button>
                  </div>
                </div>
              </form>
            </div>
          </section>

          <aside className="col-span-12 grid grid-cols-12 gap-3">
            <div className="glass-card col-span-12 rounded-[20px] p-4 xl:col-span-8">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="label-overline">AI Signal Layer</p>
                  <h3 className="mt-1 text-lg font-semibold tracking-tight text-white">Market Intelligence</h3>
                </div>
                <button
                  className="sidebar-btn"
                  type="button"
                  onClick={() => void fetchNicheAdvice({ silent: false, forceRefresh: true })}
                  disabled={nicheAdvice.loading}
                >
                  <RefreshCw className={`h-3.5 w-3.5 ${nicheAdvice.loading ? 'animate-spin' : ''}`} /> Refresh
                </button>
              </div>

              {nicheAdvice.loading && !nicheAdvice.data ? (
                <div className="mt-4 flex items-center gap-2 text-sm text-slate-400">
                  <RefreshCw className="h-3.5 w-3.5 animate-spin" />
                  <span>Analyzing market signals…</span>
                </div>
              ) : nicheAdvice.error ? (
                <div className="mt-4 rounded-xl border border-amber-500/20 bg-amber-500/5 px-4 py-3">
                  <p className="text-sm font-semibold text-amber-300">AI Signal unavailable</p>
                  <p className="mt-1 text-xs text-slate-400">
                    {nicheAdvice.error.toLowerCase().includes('backend') || nicheAdvice.error.toLowerCase().includes('503')
                      ? 'The AI signal service is temporarily offline. Showing heuristic fallback data when available.'
                      : nicheAdvice.error}
                  </p>
                  <button
                    className="mt-2 text-xs text-cyan-400 hover:text-cyan-300 underline"
                    type="button"
                    onClick={() => void fetchNicheAdvice({ silent: false, forceRefresh: true })}
                  >
                    Try again
                  </button>
                </div>
              ) : (
                <>
                  <div className="mt-3 grid gap-3 sm:grid-cols-3">
                    <div className="rounded-xl border border-white/10 bg-slate-900/70 px-3 py-2">
                      <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Niche</p>
                      <p className="mt-1 inline-flex rounded-full border border-cyan-500/30 bg-cyan-500/10 px-2.5 py-1 text-xs font-semibold text-cyan-200">
                        {activeMarketPick?.keyword || 'Loading strategy...'}
                      </p>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-slate-900/70 px-3 py-2">
                      <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Location</p>
                      <p className="mt-1 text-sm font-semibold text-white">{activeMarketPick?.location || 'N/A'}</p>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-slate-900/70 px-3 py-2">
                      <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Estimated Opportunity</p>
                      <p className="mt-1 text-sm font-semibold text-emerald-300">{estimatedOpportunityLabel}</p>
                    </div>
                  </div>
                  <p className="mt-3 text-xs leading-6 text-slate-400">
                    {activeMarketPick?.reason || 'Analyzing trends, seasonality, and your historical reply-rate data.'}
                  </p>
                  <p className="mt-2 text-[11px] font-semibold uppercase tracking-[0.14em] text-cyan-300">
                    Expected Reply Rate: {Number(activeMarketPick?.expected_reply_rate || 0).toFixed(1)}%
                  </p>
                  {marketCandidates.length > 1 ? (
                    <p className="mt-1 text-[11px] text-slate-500">
                      Rotating {marketCandidates.length} opportunities · highest potential first
                    </p>
                  ) : null}
                  {nicheAdvice.data?.generated_at ? (
                    <p className="mt-1 text-[11px] text-slate-500">Updated: {new Date(nicheAdvice.data.generated_at).toLocaleTimeString()}</p>
                  ) : null}
                  {(nicheAdvice.data?.refresh_window_days || nicheAdvice.data?.refresh_window_hours) ? (
                    <p className="mt-1 text-[11px] text-slate-500">
                      {Number(nicheAdvice.data.refresh_window_days || 0) >= 7
                        ? `AI Signal is tuned for ${selectedSignalCountryName} and free plan gets a fresh update every 7 days.`
                        : `AI Signal is tuned for ${selectedSignalCountryName} and paid plans auto-refresh every hour. Manual refresh is instant.`}
                    </p>
                  ) : null}
                  <button className="mt-4 inline-flex items-center gap-2 rounded-xl border border-indigo-400/30 bg-gradient-to-r from-blue-600 to-indigo-600 px-4 py-2 text-xs font-semibold uppercase tracking-[0.14em] text-white shadow-[0_10px_30px_rgba(59,130,246,0.35)] transition hover:brightness-110" type="button" onClick={() => applyRecommendedNiche(activeMarketPick)}>
                    <Zap className="h-4 w-4" /> ACTIVATE CAMPAIGN
                  </button>
                </>
              )}
            </div>

            <div className="glass-card col-span-12 rounded-[20px] p-4 xl:col-span-4">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <p className="label-overline">Live Control</p>
                  <h2 className="mt-1 text-lg font-semibold tracking-tight text-white">Campaign Status</h2>
                </div>
                <TerminalSquare className="h-4 w-4 text-cyan-300" />
              </div>
              <div className="mt-3 grid gap-2">
                <div className="rounded-lg border border-white/10 bg-white/[0.03] px-3 py-2 text-xs text-slate-300">Queued Mail: <strong className="text-white">{stats.queued_mail_count}</strong></div>
                <div className="rounded-lg border border-white/10 bg-white/[0.03] px-3 py-2 text-xs text-slate-300">Replies: <strong className="text-white">{Number(stats.replies_count || 0)}</strong></div>
                <div className="rounded-lg border border-white/10 bg-white/[0.03] px-3 py-2 text-xs text-slate-300">Open Rate: <strong className="text-white">{Number(stats.open_rate || 0).toFixed(1)}%</strong></div>
                <div className="rounded-lg border border-white/10 bg-white/[0.03] px-3 py-2 text-xs text-slate-300">Last Refresh: <strong className="text-white">{lastManualRefreshAt ? new Date(lastManualRefreshAt).toLocaleTimeString() : 'Auto'}</strong></div>
              </div>

              <div className="activity-log activity-log-panel mt-3 max-h-[220px] overflow-auto rounded-xl border border-slate-700/50 bg-slate-950/90 p-3">
                {activityFeed.length === 0 ? (
                  <div className="text-xs text-slate-500">No activity yet. Start a campaign step to populate the feed.</div>
                ) : activityFeed.slice(0, 12).map((item, index) => (
                  <div key={`${item.at}-${index}`} className="activity-line">
                    <span className="activity-time">[{formatFeedTime(item.at)}]</span>
                    <span className="activity-text">{item.message}</span>
                  </div>
                ))}
              </div>
            </div>
          </aside>
        </section>

        {isOutOfCredits ? (
          <section className="glass-card mb-6 rounded-[24px] border border-rose-500/30 bg-rose-500/10 p-5 shadow-[0_12px_40px_rgba(244,63,94,0.18)]">
            <div className="flex flex-wrap items-center justify-between gap-4">
              <div>
                <p className="label-overline text-rose-300">Credits depleted</p>
                <h3 className="mt-1 text-lg font-semibold text-white">You’re out of credits.</h3>
                <p className="mt-1 text-sm text-slate-300">Free users get 50 credits. Each scraped lead uses 1 credit, and each sent email uses 1 credit.</p>
              </div>
              <div className="flex flex-wrap gap-3">
                <button className="btn-primary" type="button" onClick={openPricingSection}>
                  <Zap className="h-4 w-4" /> Upgrade Plan
                </button>
                <button className="btn-ghost" type="button" onClick={handleTopUpClick}>
                  <PlusCircle className="h-4 w-4" /> Buy Credits
                </button>
              </div>
            </div>
          </section>
        ) : null}

        <section className="glass-card rounded-[28px] p-7" ref={workflowRef}>
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <p className="label-overline">Campaign Workflow</p>
              <h2 className="mt-2 text-2xl font-semibold text-white">Scrape → Enrich → Mail</h2>
              <p className="mt-2 text-sm leading-6 text-slate-400">Each phase exposes the current queue and the next best action, without fragmenting the workflow into separate blocks.</p>
            </div>
            <div className="rounded-full border border-slate-700/50 bg-slate-900/70 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">
              {activeTasks.length > 0 ? `${activeTasks.length} worker${activeTasks.length !== 1 ? 's' : ''} active` : 'All systems idle'}
            </div>
          </div>

          <div className="workflow-grid mt-4">
            <WorkflowCard
              icon={<Database className="h-5 w-5" />}
              step="01"
              title="Search & Scrape"
              summary={`${workflowStats.scraped} leads waiting for enrichment`}
              status={scrapeTask.running ? 'Running' : 'Ready'}
              accent="cyan"
            >
              <div className="grid gap-3 sm:grid-cols-2">
                <label className="field-label">
                  <span className="mb-1.5 block">Keyword <span style={{ color: '#f87171' }}>*</span></span>
                  <input className="glass-input" type="text" value={scrapeForm.keyword} onChange={(e) => setScrapeForm({ ...scrapeForm, keyword: e.target.value })} placeholder="Roofers in Miami" minLength="2" required />
                </label>
                <label className="field-label">
                  <span className="mb-1.5 block">Results</span>
                  <input className="glass-input" type="number" min="1" max="500" value={scrapeForm.results} onChange={(e) => setScrapeForm({ ...scrapeForm, results: e.target.value })} />
                </label>
              </div>
              <div className="grid gap-3 sm:grid-cols-2">
                <label className="field-label">
                  <span className="mb-1.5 block">Country</span>
                  <div className="relative">
                    <CountrySelect value={scrapeForm.country} onChange={(v) => setScrapeForm({ ...scrapeForm, country: v })} />
                  </div>
                </label>
                <div className="grid gap-2">
                  <CheckboxField label="Headless" checked={scrapeForm.headless} onChange={(v) => setScrapeForm({ ...scrapeForm, headless: v })} />
                  <CheckboxField
                    label="Export targets"
                    checked={canBulkExport && scrapeForm.exportTargets}
                    onChange={(v) => setScrapeForm({ ...scrapeForm, exportTargets: v })}
                    disabled={!canBulkExport}
                    badge={!canBulkExport ? 'Growth+' : ''}
                  />
                  {!canBulkExport ? (
                    <p className="text-[11px] text-amber-300">Unlock The Growth to auto-export CSV targets after each scrape.</p>
                  ) : null}
                </div>
              </div>
              <button className="workflow-btn" type="button" disabled={pendingRequest === 'scrape' || scrapeTask.running} onClick={onScrapeSubmit}>
                <Database className="h-4 w-4" /> {submitLabel('scrape', scrapeTask.running, pendingRequest === 'scrape').replace('Start', 'Launch')}
              </button>

              {scrapeProgress.isVisible ? (
                <div className="scrape-progress-wrap">
                  <div className="scrape-progress-track">
                    <div
                      className={`scrape-progress-fill ${
                        scrapeProgress.status === 'completed' ? 'is-success'
                        : scrapeProgress.status === 'failed' ? 'is-error'
                        : scrapeProgress.isLoading ? 'is-loading'
                        : 'is-running'
                      }`}
                      style={scrapeProgress.isLoading ? {} : { width: `${scrapeProgress.percent}%` }}
                    />
                  </div>
                  {scrapeProgress.status === 'queued' ? (
                    <p className="scrape-progress-copy">
                      ⏳ Scrape queued, waiting for worker slot...
                    </p>
                  ) : null}
                  {scrapeProgress.isLoading ? (
                    <p className="scrape-progress-copy">
                      {scrapeProgress.statusMessage || '🌐 Launching browser and opening Google Maps... (cold start can take up to ~30s)'}
                    </p>
                  ) : null}
                  {scrapeProgress.status === 'running' && !scrapeProgress.isLoading ? (
                    <p className="scrape-progress-copy">
                      {scrapeProgress.statusMessage || (
                        <>
                          🔍 <span className="scrape-count-pulse">{scrapeProgress.currentFound}</span> / {scrapeProgress.totalToFind || Number(scrapeForm.results || 0)} leads found… (scanned {scrapeProgress.scannedCount})
                        </>
                      )}
                    </p>
                  ) : null}
                  {scrapeProgress.status === 'completed' ? (
                    <p className="scrape-progress-copy is-success">
                      ✅ Success! {scrapeProgress.inserted} leads added to database.
                    </p>
                  ) : null}
                  {scrapeProgress.status === 'failed' ? (
                    <p className="scrape-progress-copy is-error">
                      Stopped at {scrapeProgress.currentFound}. Check logs for details.
                    </p>
                  ) : null}
                </div>
              ) : null}
            </WorkflowCard>

            <WorkflowCard
              icon={<Sparkles className="h-5 w-5" />}
              step="02"
              title={<span className="flex items-center gap-2">AI Enrichment</span>}
              summary={`${workflowStats.scraped} raw leads need scoring and email discovery`}
              status={String(enrichTask.status || '').toLowerCase() === 'queued' ? 'Queued' : enrichTask.running ? 'Running' : 'Ready'}
              accent="teal"
            >
              <div className="grid gap-3 sm:grid-cols-2">
                <label className="field-label">
                  <span className="mb-1.5 block">Batch size</span>
                  <input className="glass-input" type="number" min="1" value={enrichForm.limit} onChange={(e) => setEnrichForm({ ...enrichForm, limit: e.target.value })} />
                </label>
                <div className="grid gap-2">
                  <CheckboxField label="Headless" checked={enrichForm.headless} onChange={(v) => setEnrichForm({ ...enrichForm, headless: v })} />
                  <CheckboxField label="Skip CSV export" checked={enrichForm.skipExport} onChange={(v) => setEnrichForm({ ...enrichForm, skipExport: v })} />
                </div>
              </div>
              <div className="workflow-meta">
                Total leads: {workflowStats.total} · Enriched: {workflowStats.enrichmentDone} · Not enriched: {workflowStats.notEnriched}
              </div>
              <div className="workflow-meta">{workflowStats.enriched} leads currently enriched and waiting for next step.</div>
              {enrichRetrySeconds > 0 ? (
                <div className="capacity-retry-banner" role="status" aria-live="polite">
                  Heavy traffic! We are processing other leads. Retry in <strong>{enrichRetrySeconds}s</strong>.
                </div>
              ) : null}
              <button className="workflow-btn" type="button" disabled={pendingRequest === 'enrich' || enrichTask.running || enrichRetrySeconds > 0} onClick={onEnrichSubmit}>
                {pendingRequest === 'enrich' || enrichTask.running ? (
                  <>
                    <RefreshCw className="h-4 w-4 animate-spin" /> AI is analyzing...
                  </>
                ) : enrichRetrySeconds > 0 ? (
                  <>
                    <RefreshCw className="h-4 w-4" /> Retry in {enrichRetrySeconds}s
                  </>
                ) : (
                  <>
                    <Sparkles className="h-4 w-4" /> {submitLabel('enrich', enrichTask.running, pendingRequest === 'enrich').replace('Start', 'Run')}
                  </>
                )}
              </button>
              {enrichProgress.isVisible ? (
                <div style={{ marginTop: '1.5rem' }}>
                  <div
                    style={{
                      height: '6px',
                      backgroundColor: '#1e293b',
                      borderRadius: '9999px',
                      overflow: 'hidden',
                      border: '1px solid rgba(148, 163, 184, 0.2)',
                    }}
                  >
                    <div
                      className={`scrape-progress-fill ${enrichProgress.status === 'completed' ? 'is-success' : enrichProgress.status === 'failed' ? 'is-error' : 'is-running'}`}
                      style={{ width: `${enrichProgress.percent}%` }}
                    />
                  </div>
                  {enrichProgress.status === 'running' ? (
                    <p className="scrape-progress-copy">
                      ✨ Processing <span className="scrape-count-pulse">{enrichProgress.processed}</span> / {enrichProgress.total} leads, please wait...
                      {enrichProgress.currentLead ? ` Now analyzing: ${enrichProgress.currentLead}` : ''}
                    </p>
                  ) : null}
                  {enrichProgress.status === 'completed' ? (
                    <p className="scrape-progress-copy is-success">
                      ✅ Success! {enrichProgress.processed} leads enriched, {enrichProgress.queued} queued for mailer.
                    </p>
                  ) : null}
                  {enrichProgress.status === 'failed' ? (
                    <p className="scrape-progress-copy is-error">
                      Stopped after processing {enrichProgress.processed}. Check logs for details.
                    </p>
                  ) : null}
                </div>
              ) : null}
            </WorkflowCard>

            <div id="automation-card">
              <WorkflowCard
                icon={<Send className="h-5 w-5" />}
                step="03"
                title={<span className="flex items-center gap-2">Launch Mailer</span>}
                summary={`${livePendingMailCount} leads waiting for outreach`}
                status={mailerProgress.status === 'stopping' ? 'Stopping' : mailerTask.running ? 'Sending' : mailerProgress.status === 'stopped' ? 'Stopped' : 'Ready'}
                accent="blue"
              >
              <div className="grid gap-3 sm:grid-cols-3">
                <label className="field-label">
                  <span className="mb-1.5 block">Limit</span>
                  <input className="glass-input" type="number" min="1" value={mailerForm.limit} onChange={(e) => setMailerForm({ ...mailerForm, limit: e.target.value })} />
                </label>
                <label className="field-label">
                  <span className="mb-1.5 block">Delay Min <span className="text-slate-500 font-normal">(min)</span></span>
                  <input className="glass-input" type="number" min="1" value={mailerForm.delayMin} onChange={(e) => setMailerForm({ ...mailerForm, delayMin: e.target.value })} />
                </label>
                <label className="field-label">
                  <span className="mb-1.5 block">Delay Max <span className="text-slate-500 font-normal">(min)</span></span>
                  <input className="glass-input" type="number" min="1" value={mailerForm.delayMax} onChange={(e) => setMailerForm({ ...mailerForm, delayMax: e.target.value })} />
                </label>
              </div>
              <p className="mt-2 text-xs text-slate-500">
                🛡️ <span className="text-slate-400">Safe sending:</span> 5–10 min minimum between emails. 10–20 min recommended to avoid spam filters and protect sender reputation.
              </p>
              <div className="workflow-meta">Reply rate is currently {Number(stats.reply_rate || 0).toFixed(1)}% with {Number(stats.replies_count || 0)} tracked replies.</div>
              <div className="flex items-center gap-3 flex-wrap">
                {mailerTask.running ? (
                  <button
                    className="workflow-btn"
                    style={{ background: 'linear-gradient(135deg,#dc2626,#991b1b)', boxShadow: '0 4px 20px rgba(220,38,38,0.45)' }}
                    type="button"
                    disabled={mailerStopRequested}
                    onClick={onMailerStop}
                  >
                    <span style={{ fontSize: '1rem' }}>⛔</span> {mailerStopRequested ? 'Stopping…' : 'Emergency Stop'}
                  </button>
                ) : (
                  <button
                    id="mailer-button"
                    className="workflow-btn"
                    type="button"
                    disabled={pendingRequest === 'mailer' || isOutOfCredits}
                    onClick={onMailerSubmit}
                  >
                    {isOutOfCredits ? <Lock className="h-4 w-4" /> : <Send className="h-4 w-4" />} {isOutOfCredits ? 'Out of Credits' : 'Launch Mailer'}
                  </button>
                )}
                <span className="text-sm font-semibold" style={{ color: livePendingMailCount > 0 ? '#60a5fa' : '#64748b' }}>
                  Pending: {livePendingMailCount}
                </span>
              </div>
              {!configForm.smtp_accounts?.[0]?.email ? (
                <p className="mt-2 text-xs text-amber-300">Before sending, set up your SMTP account in Mail -&gt; Settings.</p>
              ) : null}
              {mailerProgress.isVisible ? (
                <div style={{ marginTop: '1.5rem' }}>
                  <div
                    style={{
                      height: '6px',
                      backgroundColor: '#1e293b',
                      borderRadius: '9999px',
                      overflow: 'hidden',
                      border: '1px solid rgba(148, 163, 184, 0.2)',
                    }}
                  >
                    <div
                      className={`scrape-progress-fill ${mailerProgress.status === 'completed' || mailerProgress.status === 'stopped' ? 'is-success' : mailerProgress.status === 'failed' ? 'is-error' : 'is-running'}`}
                      style={{ width: `${mailerProgress.percent}%` }}
                    />
                  </div>
                  {mailerProgress.status === 'running' ? (
                    <p className="scrape-progress-copy">
                      ✉️ Sending in progress: <span className="scrape-count-pulse">{mailerProgress.sent}</span> / {mailerProgress.effectiveLimit} emails sent...
                    </p>
                  ) : null}
                  {mailerProgress.status === 'stopping' ? (
                    <p className="scrape-progress-copy">
                      ⏹ Stop requested. Mailer will halt before the next lead.
                    </p>
                  ) : null}
                  {mailerProgress.status === 'completed' ? (
                    <p className="scrape-progress-copy is-success">
                      ✅ Done! {mailerProgress.sent} sent, {mailerProgress.skipped} skipped, {mailerProgress.failed} failed.
                    </p>
                  ) : null}
                  {mailerProgress.status === 'stopped' ? (
                    <p className="scrape-progress-copy is-success">
                      ⏹ Stopped after {mailerProgress.sent} sent.
                    </p>
                  ) : null}
                  {mailerProgress.status === 'failed' ? (
                    <p className="scrape-progress-copy is-error">
                      {mailerProgress.stoppedByUser
                        ? `Emergency stop completed after ${mailerProgress.sent} sent.`
                        : `Stopped after ${mailerProgress.sent} sent. Check logs for details.`}
                    </p>
                  ) : null}
                </div>
              ) : null}
              </WorkflowCard>
            </div>
          </div>
        </section>

        {/* ── Mailer Confirmation Dialog ── */}
        {showMailerConfirm ? (
          <div
            className="fixed inset-0 z-50 flex items-center justify-center"
            style={{ background: 'rgba(0,0,0,0.7)', backdropFilter: 'blur(4px)' }}
            onClick={() => setShowMailerConfirm(false)}
          >
            <div
              className="glass-card rounded-3xl p-6 w-full max-w-md shadow-2xl"
              style={{ border: '1px solid rgba(255,255,255,0.1)' }}
              onClick={(e) => e.stopPropagation()}
            >
              <h3 className="text-lg font-bold text-white mb-1">Confirm Launch Mailer</h3>
              {(() => {
                const code = (scrapeForm.country || 'US').toUpperCase()
                const tzInfo = getCountryTZInfo(code)
                const countryObj = COUNTRIES.find((c) => c.code === code) || { code: 'US', name: 'United States' }
                const abbr = getTZAbbr(tzInfo.tz)
                const localTime = getLocalTimeStr(tzInfo.tz)
                return (
                  <>
                    <p className="text-sm text-slate-300 mb-1 flex items-center gap-2">
                      <img
                        src={`https://flagcdn.com/w20/${code.toLowerCase()}.png`}
                        width="20" height="14"
                        alt={code}
                        className="rounded-sm shrink-0"
                      />
                      <span className="text-white font-semibold">{countryObj.name}</span>
                      <span className="text-slate-500">·</span>
                      <span className="text-slate-400">{tzInfo.city} ({abbr})</span>
                    </p>
                    <p className="text-sm text-slate-400 mb-4">
                      Current local time:{' '}
                      <span className="font-semibold text-blue-400">{localTime}</span>
                      . Best sending window:{' '}
                      <span className="font-semibold text-green-400">09:00</span>
                      {' – '}
                      <span className="font-semibold text-green-400">11:00 {abbr}</span>.
                    </p>
                  </>
                )
              })()}
              <div className="mb-5">
                {(() => {
                  const code = (scrapeForm.country || 'US').toUpperCase()
                  const tzInfo = getCountryTZInfo(code)
                  const abbr = getTZAbbr(tzInfo.tz)
                  const hourOptions = Array.from({ length: 13 }, (_, i) => i + 7)
                  const isSelectedBest = mailerScheduledHour !== 'now' && Number(mailerScheduledHour) >= 9 && Number(mailerScheduledHour) <= 11
                  const etHour = mailerScheduledHour !== 'now' ? localHourToET(Number(mailerScheduledHour), tzInfo.tz) : null
                  const selectedLabel = mailerScheduledHour === 'now'
                    ? 'Now'
                    : `${String(mailerScheduledHour).padStart(2, '0')}:00 ${abbr}`
                  return (
                    <>
                      <p className="mb-1.5 text-sm text-slate-300">Start sending at ({tzInfo.city}):</p>
                      <div className="relative">
                        {mailerHourOpen && (
                          <div className="fixed inset-0 z-40" onClick={() => setMailerHourOpen(false)} />
                        )}
                        <button
                          type="button"
                          className="glass-input w-full flex items-center justify-between gap-2 text-left cursor-pointer relative z-50"
                          onClick={() => setMailerHourOpen((v) => !v)}
                        >
                          <span className="flex items-center gap-2">
                            {isSelectedBest && <span>⭐</span>}
                            <span className={isSelectedBest ? 'text-yellow-300 font-semibold' : 'text-slate-200'}>{selectedLabel}</span>
                            {isSelectedBest && <span className="text-green-400 text-xs font-medium">recommended</span>}
                          </span>
                          <ChevronDown className="h-4 w-4 text-slate-400 shrink-0" />
                        </button>
                        {mailerHourOpen && (
                          <div
                            className="absolute z-50 mt-1 w-full rounded-xl border border-white/10 bg-slate-900 shadow-2xl overflow-hidden"
                            style={{ top: '100%', left: 0 }}
                          >
                            <div className="max-h-52 overflow-y-auto">
                              <button
                                type="button"
                                className={`w-full flex items-center px-3 py-2.5 text-sm text-left transition-colors ${
                                  mailerScheduledHour === 'now' ? 'bg-yellow-500/10 text-yellow-300' : 'text-slate-200 hover:bg-white/5'
                                }`}
                                onClick={() => { setMailerScheduledHour('now'); setMailerHourOpen(false) }}
                              >
                                Now
                              </button>
                              {hourOptions.map((h) => {
                                const isBest = h >= 9 && h <= 11
                                const isSelected = mailerScheduledHour === String(h)
                                return (
                                  <button
                                    key={h}
                                    type="button"
                                    className={`w-full flex items-center gap-2 px-3 py-2.5 text-sm text-left transition-colors ${
                                      isSelected ? 'bg-yellow-500/10 text-yellow-300' : 'text-slate-200 hover:bg-white/5'
                                    }`}
                                    onClick={() => { setMailerScheduledHour(String(h)); setMailerHourOpen(false) }}
                                  >
                                    {isBest && <span className="text-yellow-400">⭐</span>}
                                    <span className={isBest ? 'font-medium' : ''}>
                                      {String(h).padStart(2, '0')}:00 {abbr}
                                    </span>
                                    {isBest && <span className="text-green-400 text-xs">recommended</span>}
                                  </button>
                                )
                              })}
                            </div>
                          </div>
                        )}
                      </div>
                      {mailerScheduledHour !== 'now' && etHour !== null && (
                        <p className="mt-2 text-xs text-yellow-400/80">
                          ⏰ Mailer will start at{' '}
                          <strong className="text-yellow-300">
                            {String(mailerScheduledHour).padStart(2, '0')}:00 {tzInfo.city}
                          </strong>{' '}
                          ({String(etHour).padStart(2, '0')}:00 ET)
                        </p>
                      )}
                      <p className="mt-2 text-xs text-slate-500">
                        {stats.queued_mail_count}{' '}
                        {stats.queued_mail_count === 1 ? 'lead waiting' : 'leads waiting'}{' '}
                        — sending up to <strong className="text-white">{mailerForm.limit}</strong> emails.
                      </p>
                    </>
                  )
                })()}
              </div>
              <div className="flex gap-3">
                <button
                  className="workflow-btn flex-1"
                  type="button"
                  onClick={onMailerConfirm}
                >
                  <Send className="h-4 w-4" /> Yes, Start Sending
                </button>
                <button
                  className="flex-1 rounded-2xl border border-white/10 bg-white/5 py-3 text-sm font-semibold text-slate-300 hover:bg-white/10 transition-colors"
                  type="button"
                  onClick={() => setShowMailerConfirm(false)}
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        ) : null}

        {/* ── Main CRM Panel ── */}
        <section className="glass-card rounded-3xl p-6" ref={mainPanelRef}>
          <div className="mb-5 flex flex-wrap items-center gap-3 border-b border-white/5 pb-4">
            <button className={`tab-btn ${activeTab === 'leads' ? 'tab-active' : ''}`} type="button" onClick={() => openMainTab('leads')}>Lead Management</button>
            <button className={`tab-btn ${activeTab === 'blacklist' ? 'tab-active' : ''}`} type="button" onClick={() => openMainTab('blacklist')}>Blacklist</button>
            <button className={`tab-btn ${activeTab === 'workers' ? 'tab-active' : ''}`} type="button" onClick={() => openMainTab('workers')}>Workers</button>
            <button className={`tab-btn ${activeTab === 'tasks' || activeTab === 'history' ? 'tab-active' : ''}`} type="button" onClick={() => openMainTab('tasks')}>Tasks</button>
            <button
              className={`tab-btn ${activeTab === 'mail' ? 'tab-active' : ''}`}
              type="button"
              onClick={() => openMainTab('mail')}
            >
              <Mail className="inline h-3.5 w-3.5 mr-1" />
              Mail
            </button>
            <button className={`tab-btn ${activeTab === 'export' ? 'tab-active' : ''}`} type="button" onClick={() => setActiveTab('export')}>
              <Download className="inline h-3.5 w-3.5 mr-1" />
              Export
              {!canBulkExport ? <span className="ml-1.5"><PremiumBadge label="Growth+" /></span> : null}
            </button>
            <button className={`tab-btn ${activeTab === 'qualify' ? 'tab-active' : ''}`} type="button" onClick={() => openMainTab('qualify')}>
              <Zap className="inline h-3.5 w-3.5 mr-1" />
              Lead Qualifier
              {!canLeadScoring ? <span className="ml-1.5"><PremiumBadge label="Hustler+" /></span> : null}
              {qualifierData.data?.total > 0 && (
                <span className="ml-1.5 inline-flex items-center justify-center rounded-full bg-amber-500/20 px-1.5 py-0.5 text-[10px] font-bold text-amber-400">
                  {qualifierData.data.total}
                </span>
              )}
            </button>
            <div className="ml-auto rounded-full bg-white/5 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-slate-400">
              {activeTab === 'leads' ? `${filteredLeads.length} visible • ${Math.max(leadServerTotal, filteredLeads.length)} total leads` : activeTab === 'blacklist' ? `${blacklistedLeads.length} blacklisted` : activeTab === 'workers' ? `${workers.length} workers` : activeTab === 'tasks' || activeTab === 'history' ? `${deliverySummary.total} task manager items • ${taskHistory.length} history entries` : activeTab === 'mail' ? 'Mailer editor' : activeTab === 'qualify' ? `${qualifierData.data?.total ?? 0} gold mines` : activeTab === 'export' ? 'Reporting & exports' : activeTab === 'clients' ? `${clientFolders.length} client folders` : activeTab === 'config' ? 'Platform settings' : null}
            </div>
          </div>

          {lastError
            && !lastError.toLowerCase().includes('backend_url')
            && !lastError.toLowerCase().includes('backend is not configured')
            && !lastError.toLowerCase().includes('please add your smtp account in settings.')
            ? <pre className="mb-4 overflow-auto rounded-2xl bg-rose-950/60 px-4 py-4 text-sm text-rose-300 ring-1 ring-rose-500/20">{lastError}</pre>
            : null}
          {scrapeSummary ? (
            <div className="mb-4">
              <div className="scrape-summary-card scrape-summary-card-wide">
                <div className="scrape-summary-pill">✅ TASK COMPLETED SUCCESSFULLY</div>

                <div className="scrape-summary-track" role="progressbar" aria-valuenow={100} aria-valuemin={0} aria-valuemax={100}>
                  <div className="scrape-summary-fill" style={{ width: '100%' }} />
                </div>

                <div className="scrape-summary-grid">
                  <div className="scrape-summary-item">
                    <Target className="h-4 w-4" />
                    <span className="label">Target</span>
                    <strong>{scrapeSummary.totalToFind}</strong>
                  </div>
                  <div className="scrape-summary-item is-new">
                    <Sparkles className="h-4 w-4" />
                    <span className="label">New Leads</span>
                    <strong>{scrapeSummary.inserted}</strong>
                  </div>
                  <div className="scrape-summary-item is-neutral">
                    <Copy className="h-4 w-4" />
                    <span className="label">Duplicates</span>
                    <strong>{scrapeSummary.duplicates}</strong>
                  </div>
                </div>

                <p className="scrape-summary-text">
                  Success! We found {scrapeSummary.scraped} businesses and successfully added {scrapeSummary.inserted} fresh leads to your pipeline. {scrapeSummary.duplicates} were already in your database and were skipped.
                </p>
              </div>
            </div>
          ) : null}

          {activeTab === 'leads' ? (
            <div className="space-y-5">
              {/* Manual lead form */}
              <form
                className="grid gap-4 rounded-2xl border border-white/5 bg-white/[0.03] p-4 lg:grid-cols-[1fr_1fr_1fr_auto]"
                onSubmit={createManualLead}
              >
                <label className="field-label">
                  <span className="mb-1.5 block">Ime kontakta</span>
                  <input className="glass-input" type="text" value={manualLeadForm.contactName} onChange={(e) => setManualLeadForm({ ...manualLeadForm, contactName: e.target.value })} required />
                </label>
                <label className="field-label">
                  <span className="mb-1.5 block">Email</span>
                  <input className="glass-input" type="email" value={manualLeadForm.email} onChange={(e) => setManualLeadForm({ ...manualLeadForm, email: e.target.value })} required />
                </label>
                <label className="field-label">
                  <span className="mb-1.5 block">Podjetje</span>
                  <input className="glass-input" type="text" value={manualLeadForm.businessName} onChange={(e) => setManualLeadForm({ ...manualLeadForm, businessName: e.target.value })} required />
                </label>
                <div className="flex items-end">
                  <button className="btn-primary" type="submit" disabled={pendingRequest === 'manualLead'}>
                    {pendingRequest === 'manualLead' ? 'Adding\u2026' : 'Add Lead'}
                  </button>
                </div>
              </form>

              {/* Search + filter row */}
              <div className="flex flex-wrap items-center gap-3">
                <div className="relative min-w-[220px] flex-1">
                  <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-500" />
                  <input
                    id="search-section"
                    ref={leadSearchRef}
                    className="glass-input pl-9"
                    type="text"
                    placeholder="Search lead, company, email..."
                    value={leadSearch}
                    onChange={(e) => setLeadSearch(e.target.value)}
                  />
                </div>
                <button
                  type="button"
                  className="btn-ghost px-3 py-2 text-sm"
                  onClick={() => setLeadFilterPanelOpen((prev) => !prev)}
                >
                  <Sparkles className="h-4 w-4" />
                  Filters
                </button>
                <div className="relative">
                  <select
                    className="glass-input appearance-none pr-8 text-sm"
                    value={leadStatusFilter}
                    onChange={(e) => setLeadStatusFilter(e.target.value)}
                  >
                    <option value="all">All statuses</option>
                    {leadStatusOptions.map((s) => <option key={s} value={s}>{s}</option>)}
                  </select>
                  <ChevronDown className="select-chevron" />
                </div>
                <div className="relative">
                  <select
                    className="glass-input appearance-none pr-8 text-sm"
                    value={leadSortMode}
                    onChange={(e) => setLeadSortMode(e.target.value)}
                  >
                    <option value="best">Best leads first</option>
                    <option value="score">Highest AI score</option>
                    <option value="recent">Most recent</option>
                    <option value="name">A–Z</option>
                  </select>
                  <ChevronDown className="select-chevron" />
                </div>
                <label className="inline-flex items-center gap-2 rounded-xl border border-white/10 bg-white/[0.03] px-3 py-2 text-xs font-semibold text-slate-300">
                  <input
                    type="checkbox"
                    checked={showBlacklisted}
                    onChange={(e) => setShowBlacklisted(e.target.checked)}
                  />
                  Show Blacklisted
                </label>
                {loadingLeads ? (
                  <span className="inline-flex items-center gap-2 rounded-xl border border-cyan-500/20 bg-cyan-500/10 px-3 py-2 text-xs font-semibold text-cyan-200">
                    <RefreshCw className="h-3.5 w-3.5 animate-spin" />
                    Loading page…
                  </span>
                ) : null}
                {/* Real-time scrape/enrich progress — GPU-composited, no layout shift */}
                <ScrapeProgressBadge />
              </div>

              <div className="flex flex-wrap gap-2">
                <button
                  type="button"
                  className={`btn-ghost px-3 py-1.5 text-xs ${leadQuickFilter === 'all' ? 'ring-1 ring-slate-400/70 text-slate-200' : ''}`}
                  onClick={() => setLeadQuickFilter('all')}
                >
                  All ({leadQuickCounts.total})
                </button>
                <button
                  type="button"
                  className={`btn-ghost px-3 py-1.5 text-xs ${leadQuickFilter === 'qualified' ? 'ring-1 ring-cyan-400/70 text-cyan-200' : ''}`}
                  onClick={() => setLeadQuickFilter((prev) => (prev === 'qualified' ? 'all' : 'qualified'))}
                >
                  Qualified ({leadQuickCounts.qualified})
                </button>
                <button
                  type="button"
                  className={`btn-ghost px-3 py-1.5 text-xs ${leadQuickFilter === 'not_qualified' ? 'ring-1 ring-amber-400/70 text-amber-200' : ''}`}
                  onClick={() => setLeadQuickFilter((prev) => (prev === 'not_qualified' ? 'all' : 'not_qualified'))}
                >
                  Not Qualified ({leadQuickCounts.notQualified})
                </button>
                <button
                  type="button"
                  className={`btn-ghost px-3 py-1.5 text-xs ${leadQuickFilter === 'mailed' ? 'ring-1 ring-emerald-400/70 text-emerald-200' : ''}`}
                  onClick={() => setLeadQuickFilter((prev) => (prev === 'mailed' ? 'all' : 'mailed'))}
                >
                  Mailed ({leadQuickCounts.mailed})
                </button>
                <button
                  type="button"
                  className={`btn-ghost px-3 py-1.5 text-xs ${leadQuickFilter === 'opened' ? 'ring-1 ring-cyan-400/70 text-cyan-200' : ''}`}
                  onClick={() => setLeadQuickFilter((prev) => (prev === 'opened' ? 'all' : 'opened'))}
                >
                  Opened ({leadQuickCounts.opened})
                </button>
                <button
                  type="button"
                  className={`btn-ghost px-3 py-1.5 text-xs ${leadQuickFilter === 'replied' ? 'ring-1 ring-emerald-400/70 text-emerald-200' : ''}`}
                  onClick={() => setLeadQuickFilter((prev) => (prev === 'replied' ? 'all' : 'replied'))}
                >
                  Replied ({leadQuickCounts.replied})
                </button>
              </div>

              <div className="grid gap-4 xl:grid-cols-[280px_minmax(0,1fr)]">
                <aside className={`${leadFilterPanelOpen ? 'block' : 'hidden'} xl:block rounded-[24px] border border-slate-700/50 bg-slate-900/70 p-4 shadow-[0_10px_40px_rgba(2,6,23,0.22)]`}>
                  <div className="mb-4 flex items-center justify-between gap-2">
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-[0.22em] text-cyan-300/80">Advanced filters</p>
                      <p className="text-sm text-slate-400">Multi-select pipeline segments</p>
                    </div>
                    <button
                      type="button"
                      className="text-xs font-semibold text-slate-400 transition hover:text-white"
                      onClick={() => setAdvancedLeadFilters({ industries: [], revenueBands: [], techStacks: [], highScoreOnly: false })}
                    >
                      Clear
                    </button>
                  </div>

                  <div className="mb-4 rounded-2xl border border-white/5 bg-white/[0.03] p-3">
                    <p className="text-xs font-semibold uppercase tracking-[0.2em] text-amber-300/80">Saved segments</p>
                    <div className="mt-2 flex gap-2">
                      <input
                        className="glass-input h-10 flex-1 text-sm"
                        type="text"
                        placeholder="Hot Shopify Leads"
                        value={segmentNameDraft}
                        onChange={(e) => setSegmentNameDraft(e.target.value)}
                      />
                      <button
                        type="button"
                        className="btn-primary px-3 py-2 text-sm"
                        disabled={savingSegment}
                        onClick={() => void saveCurrentSegment()}
                      >
                        <Save className="h-4 w-4" />
                        {savingSegment ? 'Saving…' : 'Save'}
                      </button>
                    </div>

                    <div className="mt-3 space-y-2">
                      {loadingSavedSegments ? (
                        <div className="rounded-xl border border-slate-700/60 bg-slate-800/50 px-3 py-2 text-xs text-slate-400">Loading saved segments…</div>
                      ) : savedSegments.length ? savedSegments.map((segment) => (
                        <div key={segment.id} className="flex items-start gap-2 rounded-xl border border-slate-700/60 bg-slate-800/50 p-2">
                          <button
                            type="button"
                            className="flex-1 text-left"
                            onClick={() => applySavedSegment(segment)}
                          >
                            <p className="text-sm font-semibold text-white">{segment.name}</p>
                            <p className="text-[11px] text-slate-400">{describeSavedSegment(segment)}</p>
                          </button>
                          <button
                            type="button"
                            className="copy-btn mt-0.5"
                            disabled={deletingSegmentId === segment.id}
                            onClick={() => void deleteLeadSegment(segment.id)}
                            title="Delete segment"
                          >
                            <Trash2 className="h-4 w-4" />
                          </button>
                        </div>
                      )) : (
                        <p className="text-xs text-slate-500">Save your favorite filter mix for one-click access.</p>
                      )}
                    </div>
                  </div>

                  <div className="space-y-4 text-sm">
                    <div className="space-y-2">
                      <div className="flex items-center gap-2 text-slate-200"><Briefcase className="h-4 w-4 text-cyan-300" /> Industry</div>
                      <div className="space-y-1.5">
                        {industryFilterOptions.length ? industryFilterOptions.slice(0, 8).map((industry) => (
                          <label key={industry} className="flex items-center gap-2 rounded-xl border border-white/5 bg-white/[0.02] px-2.5 py-2 text-xs text-slate-300">
                            <input
                              type="checkbox"
                              checked={advancedLeadFilters.industries.includes(industry)}
                              onChange={() => setAdvancedLeadFilters((prev) => ({
                                ...prev,
                                industries: prev.industries.includes(industry)
                                  ? prev.industries.filter((item) => item !== industry)
                                  : [...prev.industries, industry],
                              }))}
                            />
                            <span>{industry}</span>
                          </label>
                        )) : <p className="text-xs text-slate-500">Industry tags appear as leads load.</p>}
                      </div>
                    </div>

                    <div className="space-y-2">
                      <div className="flex items-center gap-2 text-slate-200"><DollarSign className="h-4 w-4 text-emerald-300" /> Revenue range</div>
                      <div className="flex flex-wrap gap-2">
                        {revenueFilterOptions.map((band) => (
                          <button
                            key={band}
                            type="button"
                            className={`rounded-full border px-3 py-1.5 text-xs font-semibold transition ${advancedLeadFilters.revenueBands.includes(band) ? 'border-emerald-400/50 bg-emerald-500/10 text-emerald-200' : 'border-slate-700 bg-slate-800/60 text-slate-300 hover:border-slate-500'}`}
                            onClick={() => setAdvancedLeadFilters((prev) => ({
                              ...prev,
                              revenueBands: prev.revenueBands.includes(band)
                                ? prev.revenueBands.filter((item) => item !== band)
                                : [...prev.revenueBands, band],
                            }))}
                          >
                            {band}
                          </button>
                        ))}
                      </div>
                    </div>

                    <div className="space-y-2">
                      <div className="flex items-center gap-2 text-slate-200"><Database className="h-4 w-4 text-violet-300" /> Tech stack</div>
                      <div className="flex flex-wrap gap-2">
                        {techStackFilterOptions.length ? techStackFilterOptions.map((stack) => (
                          <button
                            key={stack}
                            type="button"
                            className={`rounded-full border px-3 py-1.5 text-xs font-semibold transition ${advancedLeadFilters.techStacks.includes(stack) ? 'border-violet-400/50 bg-violet-500/10 text-violet-200' : 'border-slate-700 bg-slate-800/60 text-slate-300 hover:border-slate-500'}`}
                            onClick={() => setAdvancedLeadFilters((prev) => ({
                              ...prev,
                              techStacks: prev.techStacks.includes(stack)
                                ? prev.techStacks.filter((item) => item !== stack)
                                : [...prev.techStacks, stack],
                            }))}
                          >
                            {stack}
                          </button>
                        )) : <p className="text-xs text-slate-500">No tech-stack enrichment yet.</p>}
                      </div>
                    </div>

                    <label className="flex items-center justify-between gap-3 rounded-2xl border border-cyan-500/20 bg-cyan-500/10 px-3 py-2 text-sm text-cyan-100">
                      <span>Lead Score &gt; 80</span>
                      <input
                        type="checkbox"
                        checked={advancedLeadFilters.highScoreOnly}
                        onChange={(e) => setAdvancedLeadFilters((prev) => ({ ...prev, highScoreOnly: e.target.checked }))}
                      />
                    </label>
                  </div>
                </aside>

                <div className="space-y-4 min-w-0">
                  {/* Leads table */}
                  <div
                    id="leads-table"
                    className="hidden overflow-hidden rounded-[24px] border border-slate-700/50 bg-slate-900/70 shadow-[0_10px_40px_rgba(2,6,23,0.28)] lg:block"
                  >
                <div className="max-h-[68vh] overflow-auto">
                  <table className="apollo-table w-full table-fixed text-xs tracking-tight">
                    <colgroup>
                      <col style={{width: '22%'}} />
                      <col style={{width: '13%'}} />
                      <col style={{width: '11%'}} />
                      <col style={{width: '4%'}} />
                      <col style={{width: '4%'}} />
                      <col style={{width: '4%'}} />
                      <col style={{width: '5%'}} />
                      <col style={{width: '9%'}} />
                      <col style={{width: '11%'}} />
                      <col style={{width: '9%'}} />
                      <col style={{width: '8%'}} />
                    </colgroup>
                    <thead className="sticky top-0 bg-slate-900/95 backdrop-blur-xl">
                      <tr>
                        <th className="th-cell">Business</th>
                        <th className="th-cell">Email</th>
                        <th className="th-cell">Phone</th>
                        <th className="th-cell text-center">AI</th>
                        <th className="th-cell text-center">Mail</th>
                        <th className="th-cell">F·U</th>
                        <th id="lead-score-column" className="th-cell">Score</th>
                        <th className="th-cell">Tier</th>
                        <th className="th-cell">Status</th>
                        <th className="th-cell">Worker</th>
                        <th className="th-cell text-center">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-white/[0.04]">
                      {loadingLeads ? (
                        <tr key="lead-skeleton-row">
                          <td colSpan={11} className="td-cell">
                            {/* Fixed-height skeletons — CLS = 0, shimmer animation via CSS only */}
                            <LeadCardSkeletonList count={6} />
                          </td>
                        </tr>
                      ) : pagedLeads.length ? pagedLeads.map((lead) => {
                        const isProcessing = String(lead.enrichment_status || lead.status || '').toLowerCase() === 'processing'
                        const intentSignals = normalizeLeadInsightList(lead.intent_signals, 2)
                        const techStack = normalizeLeadInsightList(lead.tech_stack, 1)
                        const auditHighlights = normalizeLeadInsightList(lead.company_audit?.strengths, 2)
                        const bestLeadScore = resolveBestLeadScore(lead)
                        const pipelineStage = resolvePipelineStage(lead)
                        const socialLinks = [lead.linkedin_url, lead.instagram_url, lead.facebook_url].filter(Boolean)
                        return (
                        <tr key={lead.id} className="td-row">
                          {/* Business + Niche + Contact merged */}
                          <td className="td-cell">
                            <div className="flex flex-col gap-0.5 min-w-0">
                              <span className="font-semibold text-white truncate block">{lead.business_name || '—'}</span>
                              <span className="text-[10px] text-slate-500 truncate block">{lead.search_keyword || 'manual'}</span>
                              {lead.contact_name && <span className="text-[10px] text-slate-600 truncate block">{lead.contact_name}</span>}
                              <div className="mt-1 flex flex-wrap gap-1">
                                {bestLeadScore > 0 && (
                                  <span className="inline-flex items-center gap-1 rounded-full border border-cyan-500/30 bg-cyan-500/10 px-1.5 py-0.5 text-[9px] font-semibold text-cyan-200">
                                    <Sparkles className="h-2.5 w-2.5" /> Score {formatLeadScoreValue(bestLeadScore)}/10
                                  </span>
                                )}
                                {intentSignals.map((signal) => (
                                  <span key={`${lead.id}-${signal}`} className="inline-flex items-center rounded-full border border-emerald-500/30 bg-emerald-500/10 px-1.5 py-0.5 text-[9px] font-medium text-emerald-200">
                                    {signal}
                                  </span>
                                ))}
                                {techStack.map((stack) => (
                                  <span key={`${lead.id}-${stack}`} className="inline-flex items-center rounded-full border border-violet-500/30 bg-violet-500/10 px-1.5 py-0.5 text-[9px] font-medium text-violet-200">
                                    {stack}
                                  </span>
                                ))}
                              </div>
                              {auditHighlights.length > 0 && (
                                <span className="text-[10px] text-slate-400 truncate block">Audit: {auditHighlights.join(' • ')}</span>
                              )}
                              <div className="mt-1 flex flex-wrap items-center gap-1.5">
                                <span className={`inline-flex items-center rounded-full border px-1.5 py-0.5 text-[9px] font-semibold ${pipelineStageBadgeClass(pipelineStage)}`}>
                                  {pipelineStage}
                                </span>
                                {Number(lead.qualification_score || 0) > 0 && (
                                  <span className="inline-flex items-center rounded-full border border-amber-500/30 bg-amber-500/10 px-1.5 py-0.5 text-[9px] font-semibold text-amber-100">
                                    Q {Math.round(Number(lead.qualification_score || 0))}/100
                                  </span>
                                )}
                                {socialLinks.length > 0 && (
                                  <span className="inline-flex items-center rounded-full border border-sky-500/30 bg-sky-500/10 px-1.5 py-0.5 text-[9px] font-semibold text-sky-100">
                                    {socialLinks.length} socials
                                  </span>
                                )}
                              </div>
                              {canClientSuccessDashboard && clientFolders.length > 0 && (
                                <div className="relative mt-1.5">
                                  <select
                                    className="status-select w-full text-[10px]"
                                    value={lead.client_folder_id || ''}
                                    disabled={assigningClientFolderLeadId === lead.id || loadingClientFolders}
                                    onChange={(e) => void assignLeadToClientFolder(lead.id, e.target.value || null)}
                                  >
                                    <option value="">No client folder</option>
                                    {clientFolders.map((folder) => (
                                      <option key={folder.id} value={folder.id}>{folder.name}</option>
                                    ))}
                                  </select>
                                  <ChevronDown className="select-chevron" />
                                </div>
                              )}
                            </div>
                          </td>
                          {/* Email — truncated + copy */}
                          <td className="td-cell">
                            <div className="flex items-center gap-1 min-w-0">
                              <span className="text-slate-400 truncate block min-w-0 text-[11px]">{lead.email || '—'}</span>
                              {lead.email && (
                                <button type="button" className="copy-btn flex-shrink-0" onClick={() => copyEmail(lead.email)} title="Copy email">
                                  <Clipboard className="h-3 w-3" />
                                </button>
                              )}
                            </div>
                          </td>
                          {/* Phone — formatted + type badge */}
                          <td className="td-cell">
                            {lead.phone_formatted || lead.phone_number ? (
                              <div className="flex flex-col gap-0.5">
                                <span className="text-slate-300 text-[11px] font-mono whitespace-nowrap">
                                  {lead.phone_formatted || lead.phone_number}
                                </span>
                                {lead.phone_type && lead.phone_type !== 'unknown' && (
                                  <span className={`inline-block rounded-full px-1.5 py-0.5 text-[9px] font-semibold leading-none w-fit ${
                                    lead.phone_type === 'mobile'
                                      ? 'bg-emerald-900/50 text-emerald-400 border border-emerald-700/40'
                                      : 'bg-slate-800/70 text-slate-400 border border-slate-700/40'
                                  }`}>
                                    {lead.phone_type === 'mobile' ? '📱 mobile' : '🏢 office'}
                                  </span>
                                )}
                              </div>
                            ) : (
                              <span className="text-slate-700 text-xs">—</span>
                            )}
                          </td>
                          {/* AI Summary — icon only with tooltip */}
                          <td className="td-cell text-center">
                            {isProcessing ? (
                              <div className="mx-auto h-3 w-12 rounded bg-slate-600/40 animate-pulse" title="AI processing" />
                            ) : lead.ai_description ? (
                              <button
                                type="button"
                                className="icon-action-btn mx-auto"
                                onClick={() => openAiSummaryModal(lead)}
                                title={lead.ai_description}
                              >
                                <Eye className="h-3.5 w-3.5" />
                              </button>
                            ) : (
                              <span className="text-slate-700 text-xs">—</span>
                            )}
                          </td>
                          {/* Last AI Mail — icon only */}
                          <td className="td-cell text-center">
                            <button
                              type="button"
                              className="icon-action-btn mx-auto"
                              disabled={!lead.generated_email_body}
                              onClick={() => openEmailPreviewModal(lead)}
                              title={lead.generated_email_body ? 'View Generated Mail' : 'No email generated yet'}
                            >
                              <Mail className="h-3.5 w-3.5" />
                            </button>
                          </td>
                          {/* Follow-up — badge only */}
                          <td className="td-cell">
                            <span className="inline-flex items-center gap-0.5 rounded-full border border-slate-700/50 bg-slate-800/70 px-1.5 py-0.5 text-[10px] font-semibold text-slate-300 whitespace-nowrap">
                              ✉ x{Number(lead.follow_up_count || 0)}
                            </span>
                          </td>
                          {/* Score */}
                          <td className="td-cell">
                            {isProcessing ? (
                              <div className="h-4 w-10 rounded bg-slate-600/40 animate-pulse" title="Scoring in progress" />
                            ) : (
                              <div className="flex flex-col gap-0.5">
                                <div className="flex items-center gap-0.5">
                                  {Number(lead.ai_score || 0) >= 9 && (
                                    <span className="text-amber-400 text-xs leading-none">★</span>
                                  )}
                                  <span className={`score-orb ${scoreHeatTone(lead.ai_score)}`}>
                                    {lead.ai_score != null ? Number(lead.ai_score).toFixed(1) : '--'}
                                  </span>
                                </div>
                                {bestLeadScore > 0 && (
                                  <span className="text-[10px] font-semibold text-cyan-200">
                                    AI {formatLeadScoreValue(bestLeadScore)}/10{lead.lead_priority ? ` · ${lead.lead_priority}` : ''}
                                  </span>
                                )}
                                {Number(lead.qualification_score || 0) > 0 && (
                                  <span className="text-[10px] font-semibold text-amber-200">
                                    Qualified {Math.round(Number(lead.qualification_score || 0))}/100
                                  </span>
                                )}
                              </div>
                            )}
                          </td>
                          {/* Tier */}
                          <td className="td-cell">
                            <div className="relative">
                              <select
                                className="tier-select w-full"
                                value={normalizeTierValue(lead.client_tier)}
                                disabled={pendingTierLeadId === lead.id}
                                onChange={(e) => void updateLeadTier(lead.id, e.target.value)}
                              >
                                {tierOptions.map((t) => (
                                  <option key={t} value={t}>{t}</option>
                                ))}
                              </select>
                              <ChevronDown className="select-chevron" />
                            </div>
                          </td>
                          {/* Status */}
                          <td className="td-cell">
                            {isProcessing ? (
                              <span className="inline-flex items-center gap-1 rounded-full border border-cyan-500/30 bg-cyan-500/10 px-2 py-1 text-[10px] font-semibold text-cyan-200">
                                <RefreshCw className="h-3 w-3 animate-spin" /> processing
                              </span>
                            ) : (
                              <div className="space-y-1">
                                <div className="relative">
                                  <select
                                    className={`status-select w-full ${hasReply(lead) ? 'border-emerald-500/40' : hasOpenedMail(lead) ? 'border-cyan-500/30' : ''}`}
                                    value={normalizeLeadStatus(lead.status)}
                                    disabled={pendingStatusLeadId === lead.id}
                                    title={hasReply(lead) ? `↩ Reply: ${lead.contact_name || lead.email || 'contact'}` : hasOpenedMail(lead) ? `👁 Opened x${Number(lead.open_count || 0)}` : undefined}
                                    onChange={(e) => void updateLeadStatus(lead.id, e.target.value)}
                                  >
                                    {leadStatusOptions.map((s) => (<option key={s} value={s}>{s}</option>))}
                                  </select>
                                  <ChevronDown className="select-chevron" />
                                </div>
                                <div className="relative">
                                  <select
                                    className="status-select w-full border-dashed text-[10px]"
                                    value={pipelineStage}
                                    disabled={pendingStatusLeadId === lead.id}
                                    onChange={(e) => void updateLeadStatus(lead.id, e.target.value)}
                                  >
                                    {leadPipelineOptions.map((stage) => (<option key={stage} value={stage}>{stage}</option>))}
                                  </select>
                                  <ChevronDown className="select-chevron" />
                                </div>
                              </div>
                            )}
                          </td>
                          {/* Worker */}
                          <td className="td-cell">
                            <div className="relative">
                              <select
                                className="status-select w-full"
                                value={lead.worker_id || ''}
                                disabled={assigningWorkerLeadId === lead.id || String(lead.status || '').toLowerCase() !== 'paid'}
                                title={String(lead.status || '').toLowerCase() !== 'paid' ? 'Set status to Paid first' : 'Assign worker'}
                                onChange={(e) => void assignLeadToWorker(lead.id, e.target.value || null)}
                              >
                                <option value="">—</option>
                                {workers.map((worker) => (
                                  <option key={worker.id} value={worker.id}>{worker.worker_name}</option>
                                ))}
                              </select>
                              <ChevronDown className="select-chevron" />
                            </div>
                          </td>
                          {/* Actions — icon-only buttons */}
                          <td className="td-cell">
                            <div className="flex items-center justify-center gap-1">
                              <button type="button" className="icon-action-btn" onClick={() => sendManualEmail(lead)} title="Send Manual Email">
                                <Mail className="h-3.5 w-3.5" />
                              </button>
                              <button
                                type="button"
                                className="icon-action-btn"
                                disabled={pendingBlacklistLeadId === lead.id || isBlacklistedLeadStatus(lead.status)}
                                onClick={() => void blacklistLead(lead.id)}
                                title={pendingBlacklistLeadId === lead.id ? 'Blacklisting…' : 'Blacklist'}
                              >
                                <Ban className="h-3.5 w-3.5" />
                              </button>
                              <button type="button" className="icon-action-btn" onClick={() => setMeetingStatus(lead.id)} title="Set Meeting">
                                <PhoneCall className="h-3.5 w-3.5" />
                              </button>
                            </div>
                          </td>
                        </tr>
                        )}) : (
                        <tr>
                          <td colSpan={11} className="px-4 py-10 text-center text-sm text-slate-400">
                            No leads match the current filters yet.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

                  <div className="space-y-3 lg:hidden">
                    {loadingLeads ? (
                      /* Fixed-height mobile skeletons — CLS = 0 */
                      <LeadCardSkeletonList count={4} />
                    ) : pagedLeads.length ? pagedLeads.map((lead) => {
                      const bestLeadScore = resolveBestLeadScore(lead)
                      const pipelineStage = resolvePipelineStage(lead)
                      const techStack = normalizeLeadInsightList(lead.tech_stack, 2)
                      const socialCount = [lead.linkedin_url, lead.instagram_url, lead.facebook_url].filter(Boolean).length
                      return (
                        <article key={`mobile-${lead.id}`} className="rounded-[22px] border border-slate-700/50 bg-slate-900/70 p-4 shadow-[0_8px_24px_rgba(2,6,23,0.2)]">
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0">
                              <p className="truncate text-base font-semibold text-white">{lead.business_name || '—'}</p>
                              <p className="truncate text-xs text-slate-400">{titleCaseLeadLabel(deriveLeadIndustry(lead))} • {deriveLeadRevenueBand(lead)}</p>
                            </div>
                            <span className={`inline-flex items-center rounded-full border px-2 py-1 text-[10px] font-semibold ${pipelineStageBadgeClass(pipelineStage)}`}>
                              {pipelineStage}
                            </span>
                          </div>

                          <div className="mt-3 space-y-2 text-sm text-slate-300">
                            <p className="truncate">{lead.email || 'No email yet'}</p>
                            <p>{lead.phone_formatted || lead.phone_number || 'No phone yet'}</p>
                            <div className="flex flex-wrap gap-2">
                              <span className="inline-flex items-center rounded-full border border-cyan-500/30 bg-cyan-500/10 px-2 py-1 text-[10px] font-semibold text-cyan-200">Score {formatLeadScoreValue(bestLeadScore)}/10</span>
                              {Number(lead.qualification_score || 0) > 0 && <span className="inline-flex items-center rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-1 text-[10px] font-semibold text-amber-100">Q {Math.round(Number(lead.qualification_score || 0))}/100</span>}
                              {socialCount > 0 && <span className="inline-flex items-center rounded-full border border-sky-500/30 bg-sky-500/10 px-2 py-1 text-[10px] font-semibold text-sky-100">{socialCount} socials</span>}
                              {techStack.map((stack) => (
                                <span key={`${lead.id}-mobile-${stack}`} className="inline-flex items-center rounded-full border border-violet-500/30 bg-violet-500/10 px-2 py-1 text-[10px] font-medium text-violet-200">{stack}</span>
                              ))}
                            </div>
                          </div>

                          <div className="mt-4 grid gap-2">
                            <button type="button" className="btn-primary w-full justify-center py-3 text-sm" onClick={() => sendManualEmail(lead)}>
                              <Send className="h-4 w-4" />
                              Send Email
                            </button>
                            <div className="grid grid-cols-3 gap-2">
                              <button type="button" className="btn-ghost justify-center px-2 py-2 text-xs" onClick={() => openAiSummaryModal(lead)}>
                                <Eye className="h-4 w-4" />
                              </button>
                              <button type="button" className="btn-ghost justify-center px-2 py-2 text-xs" onClick={() => setMeetingStatus(lead.id)}>
                                <PhoneCall className="h-4 w-4" />
                              </button>
                              <button
                                type="button"
                                className="btn-ghost justify-center px-2 py-2 text-xs"
                                disabled={pendingBlacklistLeadId === lead.id || isBlacklistedLeadStatus(lead.status)}
                                onClick={() => void blacklistLead(lead.id)}
                              >
                                <Ban className="h-4 w-4" />
                              </button>
                            </div>
                          </div>
                        </article>
                      )
                    }) : (
                      <div className="rounded-[22px] border border-dashed border-slate-700 bg-slate-900/60 px-4 py-8 text-center text-sm text-slate-400">
                        No leads match the current filters yet.
                      </div>
                    )}
                  </div>

              {/* Pagination controls */}
                  {leadsPageCount > 1 && (
                    <div className="flex flex-wrap items-center justify-between gap-3 px-1 text-xs text-slate-400">
                      <span>
                        {leadPage * LEADS_PAGE_SIZE + 1}–{Math.min(leadPage * LEADS_PAGE_SIZE + pagedLeads.length, Math.max(leadServerTotal, filteredLeads.length))} of {Math.max(leadServerTotal, filteredLeads.length)} leads
                      </span>
                      <div className="flex gap-2">
                        <button
                          type="button"
                          className="btn-ghost px-3 py-1.5 text-xs disabled:opacity-40"
                          disabled={leadPage === 0}
                          onClick={() => setLeadPage((p) => Math.max(0, p - 1))}
                        >
                          ← Prev
                        </button>
                        <span className="flex items-center px-2 font-semibold text-slate-300">
                          {leadPage + 1} / {leadsPageCount}
                        </span>
                        <button
                          type="button"
                          className="btn-ghost px-3 py-1.5 text-xs disabled:opacity-40"
                          disabled={leadPage >= leadsPageCount - 1}
                          onClick={() => setLeadPage((p) => Math.min(leadsPageCount - 1, p + 1))}
                        >
                          Next →
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>
          ) : activeTab === 'blacklist' ? (
            <div className="space-y-4">
              <p className="text-sm text-slate-400">All blacklisted or unsubscribed leads are blocked from automation and drip sending for safety.</p>

              <div className="grid gap-4 xl:grid-cols-[1.15fr,0.85fr]">
                <form className="rounded-[24px] border border-slate-700/50 bg-slate-900/70 p-4 shadow-[0_10px_40px_rgba(2,6,23,0.28)]" onSubmit={(e) => void addBlacklistEntry(e)}>
                  <div className="mb-3">
                    <p className="text-sm font-semibold text-white">Never-contact list</p>
                    <p className="mt-1 text-xs text-slate-400">Add an email or domain and Sniped will automatically skip future outreach.</p>
                  </div>
                  <div className="grid gap-3 md:grid-cols-[120px,1fr]">
                    <select
                      className="glass-input"
                      value={blacklistForm.kind}
                      onChange={(e) => setBlacklistForm((prev) => ({ ...prev, kind: e.target.value }))}
                    >
                      <option value="email">Email</option>
                      <option value="domain">Domain</option>
                    </select>
                    <input
                      className="glass-input"
                      type={blacklistForm.kind === 'email' ? 'email' : 'text'}
                      placeholder={blacklistForm.kind === 'email' ? 'owner@example.com' : 'example.com'}
                      value={blacklistForm.value}
                      onChange={(e) => setBlacklistForm((prev) => ({ ...prev, value: e.target.value }))}
                    />
                  </div>
                  <div className="mt-3 flex flex-col gap-3 md:flex-row md:items-center">
                    <input
                      className="glass-input flex-1"
                      type="text"
                      placeholder="Reason (optional)"
                      value={blacklistForm.reason}
                      onChange={(e) => setBlacklistForm((prev) => ({ ...prev, reason: e.target.value }))}
                    />
                    <button className="btn-primary whitespace-nowrap" type="submit" disabled={submittingBlacklistEntry}>
                      <Ban className="h-4 w-4" /> {submittingBlacklistEntry ? 'Adding…' : 'Add to blacklist'}
                    </button>
                  </div>
                </form>

                <div className="rounded-[24px] border border-slate-700/50 bg-slate-900/70 p-4 shadow-[0_10px_40px_rgba(2,6,23,0.28)]">
                  <p className="text-sm font-semibold text-white">Recent do-not-contact entries</p>
                  <div className="mt-3 space-y-2">
                    {blacklistEntries.length === 0 ? (
                      <p className="text-xs text-slate-500">No manual entries yet.</p>
                    ) : blacklistEntries.slice(0, 6).map((entry, index) => {
                      const entryKey = `${String(entry.kind || 'email').toLowerCase()}:${String(entry.value || '').toLowerCase()}`
                      return (
                        <div key={`${entry.id || `${entry.kind}-${entry.value}`}-${index}`} className="rounded-2xl border border-white/10 bg-slate-950/60 px-3 py-2">
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0 flex-1">
                              <div className="flex items-center justify-between gap-3">
                                <span className="text-xs font-semibold uppercase tracking-[0.16em] text-rose-200">{entry.kind}</span>
                                <span className="text-[11px] text-slate-500">{entry.created_at ? new Date(entry.created_at).toLocaleString() : 'Saved'}</span>
                              </div>
                              <p className="mt-1 break-all text-sm text-white">{entry.value}</p>
                              <p className="mt-1 text-xs text-slate-400">{entry.reason || 'Manual dashboard block'}</p>
                            </div>
                            <button
                              type="button"
                              className="copy-btn text-rose-200 disabled:opacity-40"
                              title={pendingBlacklistEntryKey === entryKey ? 'Removing…' : 'Remove from blacklist'}
                              disabled={pendingBlacklistEntryKey === entryKey}
                              onClick={() => void removeBlacklistEntry(entry.kind, entry.value)}
                            >
                              <Trash2 className="h-3.5 w-3.5" />
                            </button>
                          </div>
                        </div>
                      )
                    })}
                  </div>
                </div>
              </div>

              <div className="overflow-hidden rounded-[24px] border border-slate-700/50 bg-slate-900/70 shadow-[0_10px_40px_rgba(2,6,23,0.28)]">
                <div className="max-h-[580px] overflow-auto">
                  <table className="apollo-table min-w-full text-sm">
                    <thead className="sticky top-0 bg-slate-900/95 backdrop-blur-xl">
                      <tr>
                        <th className="th-cell">Business</th>
                        <th className="th-cell">Contact</th>
                        <th className="th-cell">Email</th>
                        <th className="th-cell">Status</th>
                        <th className="th-cell">Last Updated</th>
                        <th className="th-cell">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-white/[0.04]">
                      {blacklistedLeads.length === 0 ? (
                        <tr>
                          <td className="td-cell text-slate-500" colSpan={6}>No blacklisted or unsubscribed leads yet.</td>
                        </tr>
                      ) : blacklistedLeads.map((lead) => (
                        <tr key={lead.id} className="td-row">
                          <td className="td-cell font-semibold text-white">{lead.business_name || '\u2014'}</td>
                          <td className="td-cell text-slate-300">{lead.contact_name || '\u2014'}</td>
                          <td className="td-cell">
                            <div className="flex items-center gap-2">
                              <span className="text-slate-400">{lead.email || '\u2014'}</span>
                              {lead.email ? (
                                <button type="button" className="copy-btn" onClick={() => copyEmail(lead.email)} title="Copy email">
                                  <Clipboard className="h-3.5 w-3.5" />
                                </button>
                              ) : null}
                            </div>
                          </td>
                          <td className="td-cell"><span className="status-badge badge-blacklisted">{normalizeLeadStatus(lead.status)}</span></td>
                          <td className="td-cell text-slate-500">{lead.status_updated_at ? new Date(lead.status_updated_at).toLocaleString() : '\u2014'}</td>
                          <td className="td-cell">
                            <button
                              type="button"
                              className="btn-ghost px-3 py-1.5 text-xs disabled:opacity-40"
                              disabled={pendingBlacklistLeadId === lead.id}
                              onClick={() => void unblacklistLead(lead)}
                            >
                              <Trash2 className="h-3.5 w-3.5" /> {pendingBlacklistLeadId === lead.id ? 'Removing…' : 'Unblacklist'}
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          ) : activeTab === 'workers' ? (
            <div className="space-y-5">
              <div className="grid gap-4 md:grid-cols-3">
                <article className="rounded-2xl border border-slate-700/50 bg-slate-900/60 p-4">
                  <p className="text-xs uppercase tracking-[0.14em] text-slate-500">Total Team Cost</p>
                  <p className="mt-2 text-2xl font-semibold text-white">{formatCurrencyEur(workerMetrics.total_team_cost)}</p>
                  <p className="mt-1 text-xs text-slate-500">Monthly payroll for all workers</p>
                </article>
                <article className="rounded-2xl border border-slate-700/50 bg-slate-900/60 p-4">
                  <p className="text-xs uppercase tracking-[0.14em] text-slate-500">Delivery Efficiency</p>
                  <p className="mt-2 text-2xl font-semibold text-white">{Number(workerMetrics.delivery_efficiency_days || 0).toFixed(1)} days</p>
                  <p className="mt-1 text-xs text-slate-500">Avg. time from paid contract to delivery completion</p>
                </article>
                <article className="rounded-2xl border border-slate-700/50 bg-slate-900/60 p-4">
                  <p className="text-xs uppercase tracking-[0.14em] text-slate-500">Net Agency Margin</p>
                  <p className={`mt-2 text-2xl font-semibold ${workerMetrics.net_agency_margin >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                    {formatCurrencyEur(workerMetrics.net_agency_margin)}
                  </p>
                  <p className="mt-1 text-xs text-slate-500">Generated profit minus monthly team cost</p>
                </article>
              </div>

              <div className="flex items-center justify-between">
                <h3 className="text-lg font-semibold text-white">Worker Database</h3>
                <button className="btn-primary" type="button" onClick={() => setShowHireWorkerForm((v) => !v)}>
                  <PlusCircle className="h-4 w-4" /> + Hire New Worker
                </button>
              </div>

              {showHireWorkerForm ? (
                <form className="grid gap-4 rounded-2xl border border-white/5 bg-white/[0.03] p-4 lg:grid-cols-5" onSubmit={createWorker}>
                  <label className="field-label">
                    <span className="mb-1.5 block">Worker Name</span>
                    <input className="glass-input" type="text" required value={workerForm.workerName} onChange={(e) => setWorkerForm({ ...workerForm, workerName: e.target.value })} />
                  </label>
                  <label className="field-label">
                    <span className="mb-1.5 block">Role</span>
                    <div className="relative">
                      <select className="glass-input appearance-none pr-8" value={workerForm.role} onChange={(e) => setWorkerForm({ ...workerForm, role: e.target.value })}>
                        <option value="PPC">PPC</option>
                        <option value="SEO">SEO</option>
                        <option value="DEV">Dev</option>
                      </select>
                      <ChevronDown className="select-chevron" />
                    </div>
                  </label>
                  <label className="field-label">
                    <span className="mb-1.5 block">Monthly Cost (€)</span>
                    <input className="glass-input" type="number" min="0" step="0.01" required value={workerForm.monthlyCost} onChange={(e) => setWorkerForm({ ...workerForm, monthlyCost: e.target.value })} />
                  </label>
                  <label className="field-label">
                    <span className="mb-1.5 block">Status</span>
                    <div className="relative">
                      <select className="glass-input appearance-none pr-8" value={workerForm.status} onChange={(e) => setWorkerForm({ ...workerForm, status: e.target.value })}>
                        <option value="Active">Active</option>
                        <option value="Idle">Idle</option>
                      </select>
                      <ChevronDown className="select-chevron" />
                    </div>
                  </label>
                  <label className="field-label">
                    <span className="mb-1.5 block">WhatsApp/Slack Link</span>
                    <input className="glass-input" type="url" placeholder="https://..." value={workerForm.commsLink} onChange={(e) => setWorkerForm({ ...workerForm, commsLink: e.target.value })} />
                  </label>
                  <div className="lg:col-span-5 flex justify-end gap-2">
                    <button className="btn-ghost" type="button" onClick={() => setShowHireWorkerForm(false)}>Cancel</button>
                    <button className="btn-primary" type="submit" disabled={creatingWorker}>{creatingWorker ? 'Hiring...' : 'Save Worker'}</button>
                  </div>
                </form>
              ) : null}

              <div className="overflow-hidden rounded-[24px] border border-slate-700/50 bg-slate-900/70 shadow-[0_10px_40px_rgba(2,6,23,0.28)]">
                <div className="max-h-[580px] overflow-auto">
                  <table className="apollo-table min-w-full text-sm">
                    <thead className="sticky top-0 bg-slate-900/95 backdrop-blur-xl">
                      <tr>
                        <th className="th-cell">Worker Name</th>
                        <th className="th-cell">Role</th>
                        <th className="th-cell">Assigned Clients</th>
                        <th className="th-cell">Monthly Cost (€)</th>
                        <th className="th-cell">Total Profit Generated (€)</th>
                        <th className="th-cell">Status</th>
                        <th className="th-cell">Profitability Metric</th>
                        <th className="th-cell">Hub</th>
                        <th className="th-cell">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-white/[0.04]">
                      {workers.length === 0 ? (
                        <tr>
                          <td className="td-cell text-slate-500" colSpan={9}>No workers yet. Click + Hire New Worker to add your first team member.</td>
                        </tr>
                      ) : workers.map((worker) => {
                        const editing = editingWorkerId === worker.id
                        return (
                          <tr key={worker.id} className="td-row">
                            <td className="td-cell font-semibold text-white">
                              {editing ? (
                                <input className="glass-input" value={workerEditForm.workerName} onChange={(e) => setWorkerEditForm({ ...workerEditForm, workerName: e.target.value })} />
                              ) : worker.worker_name}
                            </td>
                            <td className="td-cell text-slate-300">
                              {editing ? (
                                <select className="status-select" value={workerEditForm.role} onChange={(e) => setWorkerEditForm({ ...workerEditForm, role: e.target.value })}>
                                  <option value="PPC">PPC</option>
                                  <option value="SEO">SEO</option>
                                  <option value="DEV">DEV</option>
                                </select>
                              ) : worker.role}
                            </td>
                            <td className="td-cell">
                              <div className="flex flex-col gap-1">
                                <span className="text-slate-300">{(worker.assigned_clients || []).slice(0, 3).join(', ') || '—'}</span>
                                <span className="text-[11px] text-slate-500">{Number(worker.assigned_clients_count || 0)} clients</span>
                              </div>
                            </td>
                            <td className="td-cell text-slate-300">
                              {editing ? (
                                <input className="glass-input" type="number" min="0" step="0.01" value={workerEditForm.monthlyCost} onChange={(e) => setWorkerEditForm({ ...workerEditForm, monthlyCost: e.target.value })} />
                              ) : formatCurrencyEur(worker.monthly_cost || 0)}
                            </td>
                            <td className="td-cell text-slate-300">{formatCurrencyEur(worker.total_profit_generated || 0)}</td>
                            <td className="td-cell">
                              {editing ? (
                                <select className="status-select" value={workerEditForm.status} onChange={(e) => setWorkerEditForm({ ...workerEditForm, status: e.target.value })}>
                                  <option value="Active">Active</option>
                                  <option value="Idle">Idle</option>
                                </select>
                              ) : <span className={`status-badge ${String(worker.status || '').toLowerCase() === 'active' ? 'badge-paid' : 'badge-default'}`}>{worker.status}</span>}
                            </td>
                            <td className="td-cell">
                              <span className={`font-semibold ${Number(worker.profitability_metric || 0) >= 0 ? 'text-emerald-300 drop-shadow-[0_0_10px_rgba(16,185,129,0.45)]' : 'text-rose-300'}`}>
                                {formatCurrencyEur(worker.profitability_metric || 0)}
                              </span>
                            </td>
                            <td className="td-cell">
                              {editing ? (
                                <input className="glass-input" placeholder="https://..." value={workerEditForm.commsLink} onChange={(e) => setWorkerEditForm({ ...workerEditForm, commsLink: e.target.value })} />
                              ) : worker.comms_link ? (
                                <a className="quick-action-btn" href={worker.comms_link} target="_blank" rel="noreferrer">
                                  <MessageCircle className="h-3.5 w-3.5" /> Open
                                </a>
                              ) : (
                                <span className="text-xs text-slate-500">No link</span>
                              )}
                            </td>
                            <td className="td-cell">
                              <div className="flex flex-wrap gap-2">
                                {editing ? (
                                  <>
                                    <button type="button" className="quick-action-btn" onClick={() => void saveWorker(worker.id)}>
                                      <Save className="h-3.5 w-3.5" /> Save
                                    </button>
                                    <button type="button" className="quick-action-btn" onClick={cancelEditWorker}>Cancel</button>
                                  </>
                                ) : (
                                  <>
                                    <button type="button" className="quick-action-btn" onClick={() => startEditWorker(worker)}>Edit</button>
                                    <button type="button" className="quick-action-btn" disabled={deletingWorkerId === worker.id} onClick={() => void deleteWorker(worker)}>
                                      {deletingWorkerId === worker.id ? 'Deleting...' : 'Delete'}
                                    </button>
                                  </>
                                )}
                              </div>
                            </td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="rounded-2xl border border-slate-700/50 bg-slate-900/60 p-4">
                <h4 className="text-sm font-semibold uppercase tracking-[0.14em] text-slate-400">Worker Audit Log</h4>
                <div className="mt-3 max-h-[220px] overflow-auto space-y-2">
                  {workerAudit.length === 0 ? (
                    <p className="text-sm text-slate-500">No audit events yet.</p>
                  ) : workerAudit.map((event) => (
                    <div key={event.id} className="rounded-xl border border-white/5 bg-white/[0.03] px-3 py-2">
                      <p className="text-sm text-slate-200">{event.message || event.action}</p>
                      <p className="mt-1 text-[11px] text-slate-500">{event.created_at ? new Date(event.created_at).toLocaleString() : '—'} · {event.actor || 'system'}</p>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          ) : activeTab === 'tasks' || activeTab === 'history' ? (
            <div className="space-y-5">
              <div className="rounded-2xl border border-slate-700/50 bg-slate-900/70 p-5 shadow-[0_14px_50px_rgba(2,6,23,0.35)]">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <p className="text-xl font-bold text-white">
                      Welcome back, {displayName}. You have {highPriorityOpenCount} high-priority tasks today.
                    </p>
                    <p className="mt-1 text-sm text-slate-400">Personal Task Manager for outreach execution and follow-ups.</p>
                  </div>
                  <button
                    type="button"
                    className="inline-flex items-center gap-2 rounded-xl border border-cyan-400/35 bg-cyan-500/10 px-4 py-2 text-sm font-semibold text-cyan-200 transition hover:bg-cyan-500/20"
                    onClick={() => setShowCustomTaskForm((prev) => !prev)}
                  >
                    <PlusCircle className="h-4 w-4" /> Add Custom Task
                  </button>
                </div>

                {showCustomTaskForm && (
                  <form className="mt-4 grid gap-3 rounded-xl border border-slate-700/60 bg-slate-950/60 p-4 md:grid-cols-2" onSubmit={addCustomTask}>
                    <label className="field-label md:col-span-2">
                      <span className="mb-1.5 block">Task</span>
                      <input
                        className="glass-input"
                        type="text"
                        placeholder="Call the director at Solar d.o.o."
                        value={customTaskDraft.title}
                        onChange={(e) => setCustomTaskDraft((prev) => ({ ...prev, title: e.target.value }))}
                        required
                      />
                    </label>
                    <label className="field-label">
                      <span className="mb-1.5 block">Priority</span>
                      <div className="relative">
                        <select
                          className="glass-input appearance-none pr-8"
                          value={customTaskDraft.priority}
                          onChange={(e) => setCustomTaskDraft((prev) => ({ ...prev, priority: e.target.value }))}
                        >
                          {TASK_MANAGER_PRIORITIES.map((value) => <option key={value} value={value}>{value}</option>)}
                        </select>
                        <ChevronDown className="select-chevron" />
                      </div>
                    </label>
                    <label className="field-label">
                      <span className="mb-1.5 block">Status</span>
                      <div className="relative">
                        <select
                          className="glass-input appearance-none pr-8"
                          value={customTaskDraft.status}
                          onChange={(e) => setCustomTaskDraft((prev) => ({ ...prev, status: e.target.value }))}
                        >
                          {TASK_MANAGER_STATUSES.map((value) => <option key={value} value={value}>{value}</option>)}
                        </select>
                        <ChevronDown className="select-chevron" />
                      </div>
                    </label>
                    <label className="field-label md:col-span-2">
                      <span className="mb-1.5 block">Note</span>
                      <input
                        className="glass-input"
                        type="text"
                        placeholder="He said to call after 3 PM"
                        value={customTaskDraft.note}
                        onChange={(e) => setCustomTaskDraft((prev) => ({ ...prev, note: e.target.value }))}
                      />
                    </label>
                    <div className="md:col-span-2 flex items-center justify-end gap-2">
                      <button type="button" className="btn-ghost" onClick={() => setShowCustomTaskForm(false)}>Cancel</button>
                      <button type="submit" className="btn-primary">
                        <Save className="h-4 w-4" /> Save Task
                      </button>
                    </div>
                  </form>
                )}
              </div>

              {taskManagerItems.length === 0 ? (
                <div className="overflow-hidden rounded-[24px] border border-slate-700/50 bg-slate-900/70 shadow-[0_10px_40px_rgba(2,6,23,0.28)]">
                  <div className="px-6 py-14 text-center">
                    <div className="mx-auto relative mb-5 h-20 w-20">
                      <div className="absolute inset-0 rounded-full bg-cyan-500/10 blur-xl" />
                      <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2">
                        <Rocket className="h-14 w-14 text-cyan-300" />
                      </div>
                      <div className="absolute left-1/2 bottom-0 -translate-x-1/2 h-5 w-3 rounded-full bg-amber-400/70 blur-[1px] animate-pulse" />
                    </div>
                    <p className="text-lg font-semibold text-white">Ready for takeoff.</p>
                    <p className="mt-2 text-sm text-slate-400">Paid orders will trigger automated workers here.</p>
                  </div>
                </div>
              ) : (
                <DndContext
                  sensors={sensors}
                  collisionDetection={closestCenter}
                  onDragStart={onDragStart}
                  onDragCancel={onDragCancel}
                  onDragEnd={onTaskReorderEnd}
                  autoScroll
                >
                  <SortableContext items={taskManagerItems.map((item) => String(item.id))} strategy={verticalListSortingStrategy}>
                    <div className="grid gap-3">
                      {taskManagerItems.map((item) => (
                        <TaskManagerCard
                          key={item.id}
                          item={item}
                          keyboardMode={keyboardSorting}
                          isFading={Boolean(fadingTaskIds[String(item.id)])}
                          isUpdating={Boolean(item.source === 'auto' && updatingDeliveryTaskId === item.rawTask?.id)}
                          onToggleDone={toggleTaskDone}
                          onStatusChange={updateTaskStatus}
                          onNoteChange={updateTaskNote}
                          onDelete={deleteTask}
                          onPreviewAiMessage={openTaskAiMessagePreview}
                          onViewLeads={() => setActiveTab('leads')}
                          onDownload={() => setActiveTab('export')}
                        />
                      ))}
                    </div>
                  </SortableContext>
                </DndContext>
              )}

              <div className="overflow-hidden rounded-xl border border-slate-800 bg-slate-950/70 shadow-2xl shadow-blue-950/20">
                <div className="flex items-center justify-between gap-3 border-b border-slate-800 px-4 py-3">
                  <div>
                    <p className="font-semibold text-white">Task history</p>
                    <p className="text-xs text-slate-400">Completed, failed, and retried automation runs appear here.</p>
                  </div>
                </div>
                <div className="max-h-[420px] overflow-auto">
                  <table className="w-full text-sm">
                    <thead className="sticky top-0 bg-slate-950/95 backdrop-blur border-b border-slate-800">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-slate-400">Task</th>
                        <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-slate-400">Status</th>
                        <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-slate-400">Payload</th>
                        <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-slate-400">Result</th>
                        <th className="px-4 py-3 text-right text-xs font-semibold uppercase tracking-wider text-slate-400">Action</th>
                      </tr>
                    </thead>
                    <tbody>
                      {taskHistory.length === 0 ? (
                        <tr>
                          <td colSpan={5} className="px-4 py-10 text-center text-sm text-slate-400">
                            No task history yet.
                          </td>
                        </tr>
                      ) : taskHistory.map((task) => {
                        const taskName = taskLabels[task.task_type] || task.task_type
                        const status = String(task.status || 'idle').toUpperCase()
                        const statusRaw = String(task.status || '').toLowerCase()
                        const failed = statusRaw === 'failed' || statusRaw === 'error' || statusRaw === 'generation_failed'
                        const payloadText = String(formatTaskPayload(task.last_request, task.task_type) || '—').replace(/\n+/g, ' | ')
                        const resultText = task.error
                          ? String(task.error || '').replace(/\n+/g, ' | ')
                          : String(formatTaskResult(task.result, task.task_type, task.error) || '—').replace(/\n+/g, ' | ')

                        return (
                          <tr key={`task-history-${task.id}`} className="border-b border-slate-800 transition-colors hover:bg-blue-500/5">
                            <td className="px-4 py-3 align-top">
                              <p className="font-bold text-white">{taskName}</p>
                              <p className="mt-1 text-xs text-slate-400">{task.created_at ? new Date(task.created_at).toLocaleString() : '—'}</p>
                            </td>
                            <td className="px-4 py-3 align-top">
                              <span className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11px] font-semibold ${taskPillClass(task)}`}>
                                {(task.running || statusRaw === 'running') && <span className="h-1.5 w-1.5 rounded-full bg-blue-400 animate-pulse" />}
                                {status}
                              </span>
                            </td>
                            <td className="px-4 py-3 align-top">
                              <div title={payloadText} className="w-[220px] rounded-lg border border-slate-800 bg-slate-900 px-3 py-2 text-xs text-slate-300 truncate">
                                {payloadText || '—'}
                              </div>
                            </td>
                            <td className="px-4 py-3 align-top">
                              <div
                                title={resultText}
                                className={`w-[220px] rounded-lg border px-3 py-2 text-xs truncate ${
                                  failed ? 'border-rose-500/30 bg-rose-950/40 text-rose-300' : 'border-slate-800 bg-slate-900 text-slate-300'
                                }`}
                              >
                                {resultText || '—'}
                              </div>
                            </td>
                            <td className="px-4 py-3 align-top text-right">
                              <button
                                className="group inline-flex items-center gap-2 rounded-lg border border-blue-500/30 bg-blue-500/10 px-3 py-1.5 text-xs font-semibold text-blue-300 transition hover:bg-blue-500/20 disabled:cursor-not-allowed disabled:opacity-40"
                                type="button"
                                disabled={retryingTaskId === task.id || task.running || statusRaw !== 'failed'}
                                onClick={() => void retryTask(task.id)}
                              >
                                <RefreshCw className={`h-3.5 w-3.5 ${retryingTaskId === task.id ? 'animate-spin' : 'group-hover:animate-spin'}`} />
                                {retryingTaskId === task.id ? 'Retrying…' : 'Retry'}
                              </button>
                            </td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          ) : activeTab === 'mail' ? (
            <form className="space-y-6" onSubmit={saveConfig}>

              <div className="rounded-2xl border border-cyan-500/20 bg-slate-950/70 p-5">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <h3 className="text-base font-semibold text-white flex items-center gap-2">
                      <Activity className="h-4 w-4 text-cyan-400" /> Campaign Command Center
                    </h3>
                    <p className="mt-1 text-sm text-slate-400">
                      Track sent, opens, replies, bounces, A/B subject split, and the latest mailer signals in one place.
                    </p>
                  </div>
                  <button className="btn-ghost" type="button" disabled={campaignLoading} onClick={() => void fetchMailerCampaignStats()}>
                    <RefreshCw className={`h-4 w-4 ${campaignLoading ? 'animate-spin' : ''}`} />
                    {campaignLoading ? 'Refreshing…' : 'Refresh Analytics'}
                  </button>
                </div>

                <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                  {[
                    { label: 'Sent', value: campaignStats.sent, accent: 'text-white' },
                    { label: 'Opened', value: campaignStats.opened, accent: 'text-cyan-300' },
                    { label: 'Replied', value: campaignStats.replied, accent: 'text-emerald-300' },
                    { label: 'Bounced', value: campaignStats.bounced, accent: 'text-rose-300' },
                  ].map((card) => (
                    <div key={card.label} className="rounded-xl border border-white/10 bg-white/[0.03] px-4 py-3">
                      <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">{card.label}</p>
                      <p className={`mt-2 text-2xl font-semibold ${card.accent}`}>{card.value}</p>
                    </div>
                  ))}
                </div>

                <div className="mt-4 grid gap-4 xl:grid-cols-[1.05fr_0.95fr]">
                  <div className="rounded-xl border border-white/10 bg-slate-900/60 p-4">
                    <p className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-400">Performance snapshot</p>
                    <div className="mt-3 grid gap-3 sm:grid-cols-3">
                      <div className="rounded-xl border border-cyan-500/20 bg-cyan-500/5 px-3 py-3">
                        <p className="text-[11px] uppercase tracking-[0.12em] text-cyan-300">Open Rate</p>
                        <p className="mt-1 text-lg font-semibold text-white">{Number(campaignStats.open_rate || 0).toFixed(1)}%</p>
                      </div>
                      <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/5 px-3 py-3">
                        <p className="text-[11px] uppercase tracking-[0.12em] text-emerald-300">Reply Rate</p>
                        <p className="mt-1 text-lg font-semibold text-white">{Number(campaignStats.reply_rate || 0).toFixed(1)}%</p>
                      </div>
                      <div className="rounded-xl border border-rose-500/20 bg-rose-500/5 px-3 py-3">
                        <p className="text-[11px] uppercase tracking-[0.12em] text-rose-300">Bounce Rate</p>
                        <p className="mt-1 text-lg font-semibold text-white">{Number(campaignStats.bounce_rate || 0).toFixed(1)}%</p>
                      </div>
                    </div>
                    <div className="mt-4 flex flex-wrap items-center gap-2 text-sm text-slate-300">
                      <span className="rounded-full border border-violet-400/30 bg-violet-500/10 px-2.5 py-1 text-violet-200">A/B Subject Split</span>
                      <span>A: <strong className="text-white">{Number(campaignStats.ab_breakdown?.A || 0)}</strong></span>
                      <span>•</span>
                      <span>B: <strong className="text-white">{Number(campaignStats.ab_breakdown?.B || 0)}</strong></span>
                      <span>•</span>
                      <span>Total opens: <strong className="text-white">{Number(campaignStats.opens_total || 0)}</strong></span>
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-slate-900/60 p-4">
                    <p className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-400">Recent campaign signals</p>
                    {(campaignStats.recent_events || []).length ? (
                      <div className="mt-3 space-y-2">
                        {campaignStats.recent_events.slice(0, 6).map((event) => (
                          <div key={`campaign-event-${event.id}`} className="flex items-start justify-between gap-3 rounded-xl border border-white/10 bg-white/[0.02] px-3 py-2">
                            <div>
                              <p className="text-sm font-semibold text-white">{event.business_name || event.email || 'Campaign event'}</p>
                              <p className="text-xs text-slate-400 capitalize">{String(event.event_type || '').replace(/_/g, ' ')}</p>
                            </div>
                            <span className="text-[11px] text-slate-500">{event.occurred_at ? new Date(event.occurred_at).toLocaleDateString() : 'Now'}</span>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <p className="mt-3 text-sm text-slate-400">No campaign events yet — launch a mailer run or log reply/bounce events to start filling this feed.</p>
                    )}
                  </div>
                </div>
              </div>

              {/* ── Cold Outreach Generator ── */}
              <div className="rounded-2xl border border-amber-500/20 bg-amber-950/10 p-5">
                <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <h3 className="text-base font-semibold text-white flex items-center gap-2">
                      <Zap className="h-4 w-4 text-amber-400" /> Cold Outreach Generator
                    </h3>
                    <p className="mt-1 text-xs text-slate-400">
                      AI writes a short, punchy email for a specific business — under 100 words, with a pain point and PDF-CTA.
                    </p>
                  </div>
                  <span className="rounded-full border border-amber-400/30 bg-amber-500/10 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-amber-200">Draft AI</span>
                </div>

                <div className="grid gap-4 xl:grid-cols-2">
                  <div className="rounded-xl border border-white/10 bg-slate-950/50 p-4">
                    <p className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-400">Business context</p>
                    <div className="mt-3 grid gap-3 sm:grid-cols-2">
                      <label className="field-label">
                        <span className="mb-1.5 block">Business Name *</span>
                        <input className="glass-input" type="text" placeholder="e.g. Apex Roofing" value={coldOutreachForm.businessName} onChange={(e) => setColdOutreachForm({ ...coldOutreachForm, businessName: e.target.value })} required />
                      </label>
                      <label className="field-label">
                        <span className="mb-1.5 block">City *</span>
                        <input className="glass-input" type="text" placeholder="e.g. London" value={coldOutreachForm.city} onChange={(e) => setColdOutreachForm({ ...coldOutreachForm, city: e.target.value })} required />
                      </label>
                      <label className="field-label sm:col-span-2">
                        <span className="mb-1.5 block">Niche</span>
                        <input className="glass-input" type="text" placeholder="e.g. auto repair, dentist…" value={coldOutreachForm.niche} onChange={(e) => setColdOutreachForm({ ...coldOutreachForm, niche: e.target.value })} />
                      </label>
                      <label className="field-label sm:col-span-2">
                        <span className="mb-1.5 block">Competitors (comma-separated)</span>
                        <input className="glass-input" type="text" placeholder="e.g. Apex Roofing, Summit Builders" value={coldOutreachForm.competitors} onChange={(e) => setColdOutreachForm({ ...coldOutreachForm, competitors: e.target.value })} />
                      </label>
                      <label className="field-label sm:col-span-2">
                        <span className="mb-1.5 block">Pain Point (optional — AI turns it into a value statement)</span>
                        <input className="glass-input" type="text" placeholder="e.g. no website, 2 reviews vs. 150 competitors…" value={coldOutreachForm.painPoint} onChange={(e) => setColdOutreachForm({ ...coldOutreachForm, painPoint: e.target.value })} />
                      </label>
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-slate-950/50 p-4">
                    <p className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-400">Recipient & offer</p>
                    <div className="mt-3 grid gap-3">
                      <label className="field-label">
                        <span className="mb-1.5 block">Estimated Monthly Loss</span>
                        <input className="glass-input" type="text" placeholder="e.g. €3,000" value={coldOutreachForm.monthlyLoss} onChange={(e) => setColdOutreachForm({ ...coldOutreachForm, monthlyLoss: e.target.value })} />
                      </label>
                      <label className="field-label">
                        <span className="mb-1.5 block">Recipient Contact Name</span>
                        <input className="glass-input" type="text" placeholder="e.g. John Smith" value={coldOutreachForm.contactName} onChange={(e) => setColdOutreachForm({ ...coldOutreachForm, contactName: e.target.value })} />
                      </label>
                      <label className="field-label">
                        <span className="mb-1.5 block">Recipient Email</span>
                        <input className="glass-input" type="email" placeholder="e.g. john@example.com" value={coldOutreachForm.contactEmail} onChange={(e) => setColdOutreachForm({ ...coldOutreachForm, contactEmail: e.target.value })} />
                      </label>
                    </div>
                  </div>
                </div>

                <div className="mt-4 flex flex-wrap gap-3">
                  <button className="btn-primary" type="button" disabled={coldOutreachLoading} onClick={generateColdOutreach}>
                    <Sparkles className={`h-4 w-4 ${coldOutreachLoading ? 'animate-spin' : ''}`} />
                    {coldOutreachLoading ? 'Generating…' : 'Generate Email'}
                  </button>
                  {coldOutreachResult.subject && (
                    <button className="btn-ghost" type="button" onClick={copyColdOutreach}>
                      <Clipboard className="h-4 w-4" /> Copy
                    </button>
                  )}
                  {coldOutreachResult.subject && (
                    <button className="btn-ghost" type="button" onClick={() => { setColdOutreachResult({ subject: '', body: '', generatedAt: null }); setColdOutreachError('') }}>
                      Reset
                    </button>
                  )}
                </div>

                {coldOutreachError && (
                  <p className="mt-3 rounded-xl bg-rose-950/60 px-3 py-2 text-sm text-rose-300">{coldOutreachError}</p>
                )}

                {coldOutreachResult.subject && (
                  <div className="mt-4 overflow-hidden rounded-[20px] border border-white/10 bg-gradient-to-b from-slate-950 via-slate-950 to-slate-900">
                    <div className="border-b border-white/10 bg-white/[0.03] px-4 py-2.5 flex items-center gap-2">
                      <span className="h-2 w-2 rounded-full bg-rose-400/80" />
                      <span className="h-2 w-2 rounded-full bg-amber-300/80" />
                      <span className="h-2 w-2 rounded-full bg-emerald-400/80" />
                      <p className="ml-2 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Cold Outreach Draft</p>
                      {coldOutreachResult.generatedAt && (
                        <span className="ml-auto text-[10px] text-slate-600">{new Date(coldOutreachResult.generatedAt).toLocaleTimeString()}</span>
                      )}
                    </div>
                    <div className="space-y-3 px-4 py-4">
                      <div className="rounded-xl border border-white/10 bg-white/[0.02] px-3 py-2">
                        <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500 mb-1">Subject</p>
                        <p className="text-sm font-semibold text-white">{coldOutreachResult.subject}</p>
                      </div>
                      <div className="rounded-xl border border-white/10 bg-slate-950/70 p-4">
                        <pre className="whitespace-pre-wrap break-words font-sans text-[14px] leading-7 text-slate-200">{coldOutreachResult.body}</pre>
                      </div>
                    </div>
                  </div>
                )}
              </div>

              <div className="rounded-2xl border border-white/5 bg-white/[0.03] p-5">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
                  <div>
                    <h3 className="text-base font-semibold text-white flex items-center gap-2">
                      <Mail className="h-4 w-4 text-cyan-400" /> Template Studio
                    </h3>
                    <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-400">
                      Edit both live templates and follow-up templates here in one single place.
                    </p>
                  </div>
                  <div className="rounded-2xl border border-cyan-500/20 bg-cyan-500/5 px-4 py-3 text-xs text-slate-300">
                    <p className="font-semibold uppercase tracking-[0.14em] text-cyan-300">Supported placeholders</p>
                    <p className="mt-2">{'{BusinessName}'} {'{City}'} {'{Niche}'} {'{YourName}'}</p>
                  </div>
                </div>

                <div className="mt-5 space-y-4">
                  {selectedUserNiche && (
                    <div className="inline-flex items-center gap-2 rounded-xl border border-cyan-500/20 bg-cyan-500/5 px-3 py-2 text-xs text-cyan-100">
                      <span className="font-semibold uppercase tracking-[0.14em] text-cyan-300">Active Category</span>
                      <span className="text-slate-200">{selectedUserNiche}</span>
                    </div>
                  )}
                  <div className="flex flex-wrap gap-2">
                    <button
                      type="button"
                      className={`rounded-xl border px-3 py-2 text-sm font-semibold transition ${activeMailEditorTab === 'live' ? 'border-cyan-400/50 bg-cyan-500/10 text-cyan-100' : 'border-white/10 bg-slate-900/60 text-slate-300 hover:border-white/20 hover:bg-slate-900/80'}`}
                      onClick={() => setActiveMailEditorTab('live')}
                    >
                      Live Templates
                    </button>
                    <button
                      type="button"
                      className={`rounded-xl border px-3 py-2 text-sm font-semibold transition ${activeMailEditorTab === 'followup' ? 'border-violet-400/50 bg-violet-500/10 text-violet-100' : 'border-white/10 bg-slate-900/60 text-slate-300 hover:border-white/20 hover:bg-slate-900/80'}`}
                      onClick={() => setActiveMailEditorTab('followup')}
                    >
                      Follow-up Templates
                    </button>
                  </div>

                  {activeMailEditorTab === 'followup' ? (
                    <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-4">
                      <div className="flex items-start justify-between gap-3 mb-4">
                        <div>
                          <p className="text-sm font-semibold text-white">Follow-up Sequence</p>
                          <p className="mt-1 text-xs leading-5 text-slate-400">Edit subject and body for each follow-up step.</p>
                        </div>
                        <span className="rounded-full border border-violet-400/30 bg-violet-500/10 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-violet-200">3 steps</span>
                      </div>

                      <div className="flex flex-wrap gap-2 mb-4">
                        {[1, 2, 3].map((step) => (
                          <button
                            key={`followup-step-${step}`}
                            type="button"
                            className={`rounded-xl border px-3 py-2 text-sm font-semibold transition ${sequenceForm[`activeStep`] === step ? 'border-violet-400/50 bg-violet-500/10 text-violet-100' : 'border-white/10 bg-slate-950/60 text-slate-300 hover:border-white/20'}`}
                            onClick={() => setSequenceForm({ ...sequenceForm, activeStep: step })}
                          >
                            Step {step}
                          </button>
                        ))}
                      </div>

                      <div className="grid gap-3">
                        {sequenceForm.activeStep === 1 && (
                          <>
                            <label className="field-label">
                              <span className="mb-1.5 block">Subject</span>
                              <input className="glass-input" type="text" placeholder="Follow-up subject line" value={sequenceForm.step1_subject || sequenceForm.ab_subject_a || ''} onChange={(e) => setSequenceForm({ ...sequenceForm, ab_subject_a: e.target.value })} />
                            </label>
                            <label className="field-label">
                              <span className="mb-1.5 block">Body</span>
                              <textarea className="glass-input min-h-[180px]" placeholder="Follow-up email body" value={sequenceForm.step1_body} onChange={(e) => setSequenceForm({ ...sequenceForm, step1_body: e.target.value })} />
                            </label>
                          </>
                        )}

                        {sequenceForm.activeStep === 2 && (
                          <>
                            <label className="field-label">
                              <span className="mb-1.5 block">Subject</span>
                              <input className="glass-input" type="text" placeholder="Follow-up subject line" value={sequenceForm.step2_subject} onChange={(e) => setSequenceForm({ ...sequenceForm, step2_subject: e.target.value })} />
                            </label>
                            <label className="field-label">
                              <span className="mb-1.5 block">Body</span>
                              <textarea className="glass-input min-h-[180px]" placeholder="Follow-up email body" value={sequenceForm.step2_body} onChange={(e) => setSequenceForm({ ...sequenceForm, step2_body: e.target.value })} />
                            </label>
                            <label className="field-label">
                              <span className="mb-1.5 block">Delay after first email (days)</span>
                              <input className="glass-input" type="number" min="1" max="30" value={sequenceForm.step2_delay_days} onChange={(e) => setSequenceForm({ ...sequenceForm, step2_delay_days: e.target.value })} />
                            </label>
                          </>
                        )}

                        {sequenceForm.activeStep === 3 && (
                          <>
                            <label className="field-label">
                              <span className="mb-1.5 block">Subject</span>
                              <input className="glass-input" type="text" placeholder="Follow-up subject line" value={sequenceForm.step3_subject} onChange={(e) => setSequenceForm({ ...sequenceForm, step3_subject: e.target.value })} />
                            </label>
                            <label className="field-label">
                              <span className="mb-1.5 block">Body</span>
                              <textarea className="glass-input min-h-[180px]" placeholder="Follow-up email body" value={sequenceForm.step3_body} onChange={(e) => setSequenceForm({ ...sequenceForm, step3_body: e.target.value })} />
                            </label>
                            <label className="field-label">
                              <span className="mb-1.5 block">Delay after first email (days)</span>
                              <input className="glass-input" type="number" min="1" max="60" value={sequenceForm.step3_delay_days} onChange={(e) => setSequenceForm({ ...sequenceForm, step3_delay_days: e.target.value })} />
                            </label>
                          </>
                        )}
                      </div>

                      <div className="mt-4 flex flex-wrap items-center gap-3">
                        <button className="btn-primary" type="button" disabled={savingSequence} onClick={() => void saveCampaignSequence()}>
                          <Save className="h-4 w-4" />
                          {savingSequence ? 'Saving…' : 'Save Sequence'}
                        </button>
                        <p className="text-xs text-slate-500">Applied automatically when mailer sends follow-ups.</p>
                      </div>
                    </div>
                  ) : (
                    <>
                      <div className="flex flex-wrap gap-2">
                        {visibleLiveMailTemplateCards.map((card) => {
                          const Icon = templateCardIcons[card.key] || Mail
                          const isActive = activeLiveMailTemplateKey === card.key
                          return (
                            <button
                              key={card.key}
                              type="button"
                              className={`inline-flex items-center gap-2 rounded-xl border px-3 py-2 text-sm font-semibold transition ${isActive ? 'border-cyan-400/50 bg-cyan-500/10 text-cyan-100' : 'border-white/10 bg-slate-900/60 text-slate-300 hover:border-white/20 hover:bg-slate-900/80'}`}
                              onClick={() => setActiveLiveMailTemplateKey(card.key)}
                            >
                              <Icon className="h-4 w-4" />
                              {card.title}
                            </button>
                          )
                        })}
                      </div>

                      {(() => {
                        const activeCard = visibleLiveMailTemplateCards.find((card) => card.key === activeLiveMailTemplateKey) || visibleLiveMailTemplateCards[0]
                        const ActiveIcon = templateCardIcons[activeCard?.key] || Mail
                        return (
                          <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-4">
                            <div className="flex items-start justify-between gap-3">
                              <div>
                                <p className="text-sm font-semibold text-white">{activeCard?.title || 'Mail Template'}</p>
                                <p className="mt-1 text-xs leading-5 text-slate-400">{activeCard?.description || 'Edit the selected mail template here.'}</p>
                              </div>
                              <span className="inline-flex h-9 w-9 items-center justify-center rounded-xl border border-violet-400/30 bg-gradient-to-br from-indigo-600/30 to-violet-600/30 text-indigo-200">
                                <ActiveIcon className="h-4 w-4" />
                              </span>
                            </div>
                            <div className="mt-4 grid gap-3">
                              <label className="field-label">
                                <span className="mb-1.5 block">Subject</span>
                                <input
                                  className="glass-input focus-input font-mono"
                                  type="text"
                                  value={configForm[activeCard?.subjectKey] || ''}
                                  onChange={(e) => setConfigForm({ ...configForm, [activeCard.subjectKey]: e.target.value })}
                                />
                              </label>
                              <label className="field-label">
                                <span className="mb-1.5 block">Body</span>
                                <textarea
                                  className="glass-input focus-input min-h-[220px]"
                                  value={configForm[activeCard?.bodyKey] || ''}
                                  onChange={(e) => setConfigForm({ ...configForm, [activeCard.bodyKey]: e.target.value })}
                                />
                              </label>
                              <div className="placeholder-pills">
                                {templatePlaceholderTokens.map((token) => (
                                  <span key={`${activeCard?.key}-${token}`} className="placeholder-pill">{token}</span>
                                ))}
                              </div>
                            </div>
                          </div>
                        )
                      })()}
                    </>
                  )}
                </div>

                <div className="mt-4 rounded-2xl border border-white/10 bg-slate-950/60 p-4">
                  <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-400">Quick Template Packs</p>
                      <p className="mt-2 text-sm text-slate-400">Z enim klikom nalozis celoten set template-ov za drugacen ton kampanje.</p>
                    </div>
                    <p className="text-xs text-slate-500">Current: <span className="font-semibold text-slate-200">{activeMailPack || 'Custom mix'}</span></p>
                  </div>

                  <div className="mt-4 grid gap-3 lg:grid-cols-3">
                    {visibleMailTemplatePacks.map((pack) => (
                      <button
                        key={pack.key}
                        type="button"
                        className={`rounded-2xl border px-4 py-4 text-left transition ${activeMailPack === pack.key ? 'border-cyan-400/50 bg-cyan-500/10' : 'border-white/10 bg-slate-900/60 hover:border-white/20 hover:bg-slate-900/80'}`}
                        onClick={() => void applyMailTemplatePack(pack.key)}
                      >
                        <div className="flex items-center justify-between gap-3">
                          <p className="text-sm font-semibold text-white">{pack.label}</p>
                          <span className="rounded-full border border-white/10 px-2 py-1 text-[10px] uppercase tracking-[0.14em] text-slate-400">Pack</span>
                        </div>
                        <p className="mt-2 text-xs leading-5 text-slate-400">{pack.description}</p>
                      </button>
                    ))}
                  </div>
                </div>

                <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
                  <div className="space-y-4">
                    <label className="field-label">
                      <span className="mb-1.5 block">Optional Mail Footer</span>
                      <textarea
                        className="glass-input min-h-[120px]"
                        placeholder={'Example:\nGoFast\nwww.gofast.si'}
                        value={configForm.mail_signature}
                        onChange={(e) => setConfigForm({ ...configForm, mail_signature: e.target.value })}
                      />
                      <span className="mt-1 block text-[11px] text-slate-500">Phone numbers are stripped automatically. Leave empty if the template already ends with your name.</span>
                    </label>

                    <div className="rounded-xl border border-white/10 bg-slate-900/60 p-4">
                      <p className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-400">Routing Logic</p>
                      <div className="mt-3 space-y-2 text-sm text-slate-300">
                        {visibleLiveMailTemplateCards.map((card) => (
                          <p key={`routing-${card.key}`}>
                            <span className="font-semibold text-white">{card.title}</span> — {card.description}
                          </p>
                        ))}
                      </div>
                    </div>
                  </div>

                  <div className="rounded-2xl border border-cyan-500/20 bg-slate-900/70 p-4">
                    <p className="text-xs font-semibold uppercase tracking-[0.14em] text-cyan-300">Preview (sample lead)</p>
                    <p className="mt-2 text-sm leading-6 text-slate-400">
                      Preview uses the same backend pipeline as real sends. What you see here is what goes out in a campaign.
                    </p>
                    <div className="mt-3 grid gap-3 sm:grid-cols-2">
                      <div className="rounded-xl border border-white/10 bg-white/[0.03] px-3 py-2">
                        <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Generated</p>
                        <p className="mt-1 text-sm text-slate-300">{mailPreview.generatedAt ? new Date(mailPreview.generatedAt).toLocaleTimeString() : 'Not yet'}</p>
                      </div>
                      <div className="rounded-xl border border-violet-500/20 bg-violet-500/5 px-3 py-2">
                        <p className="text-[11px] uppercase tracking-[0.12em] text-violet-300">Tone of Voice</p>
                        <p className="mt-1 text-sm font-semibold text-white">{toneProfile.dominantLabel} · {toneProfile.dominantScore}%</p>
                        <div className="mail-tone-bars mt-2">
                          {Object.entries(toneProfile.scores).map(([label, score]) => (
                            <div key={label} className="tone-row">
                              <span>{label}</span>
                              <div className="tone-track"><div className="tone-fill" style={{ width: `${score}%` }} /></div>
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>

                    <div className="mt-4 overflow-hidden rounded-[24px] border border-white/10 bg-gradient-to-b from-slate-950 via-slate-950 to-slate-900 shadow-[0_24px_80px_rgba(8,15,32,0.45)]">
                      <div className="border-b border-white/10 bg-white/[0.03] px-4 py-3">
                        <div className="flex items-center gap-2">
                          <span className="h-2.5 w-2.5 rounded-full bg-rose-400/80" />
                          <span className="h-2.5 w-2.5 rounded-full bg-amber-300/80" />
                          <span className="h-2.5 w-2.5 rounded-full bg-emerald-400/80" />
                          <p className="ml-2 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Outgoing Draft</p>
                        </div>
                      </div>

                      <div className="space-y-4 px-4 py-4">
                        <div className="grid gap-2 rounded-2xl border border-white/10 bg-white/[0.02] p-3 text-sm text-slate-300">
                          <div className="flex items-start gap-3">
                            <span className="w-12 shrink-0 text-[11px] uppercase tracking-[0.12em] text-slate-500">From</span>
                            <div>
                              <p className="font-medium text-white">{previewSenderName}</p>
                              <p className="text-xs text-slate-500">{previewSenderEmail}</p>
                            </div>
                          </div>
                          <div className="flex items-start gap-3">
                            <span className="w-12 shrink-0 text-[11px] uppercase tracking-[0.12em] text-slate-500">To</span>
                            <div>
                              <p className="font-medium text-white">{coldOutreachForm.contactName || 'Recipient Name'}</p>
                              <p className="text-xs text-slate-500">{coldOutreachForm.contactEmail || 'recipient@domain.com'}</p>
                            </div>
                          </div>
                          <div className="flex items-start gap-3">
                            <span className="w-12 shrink-0 text-[11px] uppercase tracking-[0.12em] text-slate-500">Subject</span>
                            {previewLoading ? <span className="preview-skeleton h-5 w-full max-w-[260px]" /> : <p className="font-medium text-white">{mailPreview.subject || 'Generate preview to inspect current template output.'}</p>}
                          </div>
                        </div>

                        <div className="rounded-2xl border border-white/10 bg-slate-950/70 p-4">
                          {previewLoading ? (
                            <div className="space-y-2">
                              <span className="preview-skeleton h-4 w-[88%]" />
                              <span className="preview-skeleton h-4 w-[80%]" />
                              <span className="preview-skeleton h-4 w-[92%]" />
                              <span className="preview-skeleton h-4 w-[72%]" />
                              <span className="preview-skeleton h-4 w-[86%]" />
                            </div>
                          ) : (
                            <pre className="min-h-[260px] whitespace-pre-wrap break-words font-sans text-[14px] leading-7 text-slate-200">{mailPreview.body || 'Preview body will appear here.'}</pre>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="mt-5 flex flex-wrap items-center gap-3">
                  <button className="btn-primary" type="submit" disabled={savingConfig}>
                    <Save className="h-4 w-4" />
                    {savingConfig ? 'Saving…' : 'Save Mail Settings'}
                  </button>
                  <button className="btn-ghost" type="button" disabled={previewLoading} onClick={() => void previewMailTemplate()}>
                    <Eye className="h-4 w-4" />
                    {previewLoading ? 'Generating…' : 'Generate Preview'}
                  </button>
                  <button className="btn-ghost" type="button" disabled={previewLoading} onClick={() => void previewMailTemplate({ regenerate: true })}>
                    <RefreshCw className={`h-4 w-4 ${previewLoading ? 'animate-spin' : ''}`} />
                    Regenerate Preview
                  </button>
                  <p className="text-xs text-slate-500">Saved templates apply to both preview and real sends.</p>
                </div>
              </div>
            </form>
          ) : activeTab === 'config' ? (
            <form className="max-w-2xl space-y-6" onSubmit={saveConfig}>
              <div>
                <h3 className="text-base font-semibold text-white mb-4 flex items-center gap-2">
                  <Settings className="h-4 w-4 text-cyan-400" /> OpenAI
                </h3>
                <p className="text-xs text-slate-500">API key is configured from environment-backed server settings.</p>
              </div>
              <div>
                <h3 className="text-base font-semibold text-white mb-4 flex items-center gap-2">
                  <Mail className="h-4 w-4 text-cyan-400" /> SMTP
                </h3>
                <label className="field-label mb-4 block">
                  <span className="mb-1.5 block">Open Tracking Base URL (public HTTPS)</span>
                  <input
                    className="glass-input"
                    type="text"
                    placeholder="https://your-domain.com"
                    value={configForm.open_tracking_base_url || ''}
                    onChange={(e) => setConfigForm({ ...configForm, open_tracking_base_url: e.target.value })}
                  />
                  <span className="mt-1 block text-[11px] text-slate-500">
                    Tracking pixel uses this URL: /api/track/open/TOKEN
                  </span>
                </label>
                <label className="field-label mb-4 block">
                  <span className="mb-1.5 block">Rotating Proxies <span className="text-slate-500">(optional)</span></span>
                  <textarea
                    className="glass-input min-h-[120px] font-mono text-xs"
                    placeholder={"http://user:pass@host1:port1\nhttp://user:pass@host2:port2\n...one proxy per line"}
                    value={configForm.proxy_urls || ''}
                    onChange={(e) => setConfigForm({ ...configForm, proxy_urls: e.target.value })}
                    rows={5}
                  />
                  <span className="mt-1 block text-[11px] text-slate-500">
                    One proxy per line — e.g. <code className="text-slate-400">http://user:pass@host:port</code>. The scraper rotates through all of them to avoid Google Maps bans.
                  </span>
                </label>
                <div className="grid gap-4 sm:grid-cols-2">
                  <label className="field-label block">
                    <span className="mb-1.5 block">HubSpot webhook URL</span>
                    <input
                      className="glass-input"
                      type="url"
                      placeholder="https://hooks.zapier.com/..."
                      value={configForm.hubspot_webhook_url || ''}
                      onChange={(e) => setConfigForm({ ...configForm, hubspot_webhook_url: e.target.value })}
                    />
                  </label>
                  <label className="field-label block">
                    <span className="mb-1.5 block">Google Sheets webhook URL</span>
                    <input
                      className="glass-input"
                      type="url"
                      placeholder="https://hooks.zapier.com/..."
                      value={configForm.google_sheets_webhook_url || ''}
                      onChange={(e) => setConfigForm({ ...configForm, google_sheets_webhook_url: e.target.value })}
                    />
                  </label>
                  <div className="sm:col-span-2 rounded-2xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-3 text-sm text-slate-200">
                    <p className="font-semibold text-white">Report destination</p>
                    <p className="mt-1 text-xs text-slate-300">
                      Weekly and monthly summaries are sent automatically to the email used to sign in:
                      <span className="ml-1 font-semibold text-cyan-200">{currentUserEmail || 'your account email'}</span>
                    </p>
                  </div>
                </div>
                <div className="grid gap-3 sm:grid-cols-2">
                  <label className="flex cursor-pointer items-center gap-3 rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm text-slate-300 transition hover:bg-white/[0.06]">
                    <input
                      type="checkbox"
                      className="h-4 w-4 rounded border-slate-600 bg-slate-900 text-cyan-400"
                      checked={Boolean(configForm.auto_weekly_report_email)}
                      onChange={(e) => setConfigForm({ ...configForm, auto_weekly_report_email: e.target.checked })}
                    />
                    <span>Enable automatic weekly summaries</span>
                  </label>
                  <label className="flex cursor-pointer items-center gap-3 rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm text-slate-300 transition hover:bg-white/[0.06]">
                    <input
                      type="checkbox"
                      className="h-4 w-4 rounded border-slate-600 bg-slate-900 text-cyan-400"
                      checked={Boolean(configForm.auto_monthly_report_email)}
                      onChange={(e) => setConfigForm({ ...configForm, auto_monthly_report_email: e.target.checked })}
                    />
                    <span>Enable automatic monthly summaries</span>
                  </label>
                </div>
                <div className="space-y-4">
                  {(configForm.smtp_accounts || []).map((account, index) => (
                    <div key={`smtp-account-${index}`} className="rounded-2xl border border-white/10 bg-slate-900/40 p-4 space-y-3">
                      <div className="flex items-center justify-between">
                        <p className="text-sm font-semibold text-slate-200">SMTP Account #{index + 1}</p>
                        <button
                          type="button"
                          className="text-xs text-rose-300 hover:text-rose-200 disabled:opacity-50"
                          onClick={() => removeSmtpAccount(index)}
                          disabled={(configForm.smtp_accounts || []).length <= 1}
                        >
                          Remove
                        </button>
                      </div>
                      <div className="grid gap-4 sm:grid-cols-2">
                        <label className="field-label">
                          <span className="mb-1.5 block">Host</span>
                          <input
                            className="glass-input"
                            type="text"
                            placeholder="smtp.gmail.com"
                            value={account.host || ''}
                            onChange={(e) => updateSmtpAccount(index, 'host', e.target.value)}
                          />
                        </label>
                        <label className="field-label">
                          <span className="mb-1.5 block">Port</span>
                          <input
                            className="glass-input"
                            type="number"
                            value={account.port || 587}
                            onChange={(e) => updateSmtpAccount(index, 'port', e.target.value)}
                          />
                        </label>
                        <label className="field-label">
                          <span className="mb-1.5 block">Email</span>
                          <input
                            className="glass-input"
                            type="email"
                            placeholder="you@gmail.com"
                            value={account.email || ''}
                            onChange={(e) => updateSmtpAccount(index, 'email', e.target.value)}
                          />
                        </label>
                        <label className="field-label">
                          <span className="mb-1.5 block">Password / App password</span>
                          <div className="relative">
                            <input
                              className="glass-input pr-10 font-mono"
                              type={showSmtpPasswords[index] ? 'text' : 'password'}
                              placeholder={account.password_set ? 'Leave blank to keep existing' : 'Enter password'}
                              value={account.password || ''}
                              onChange={(e) => updateSmtpAccount(index, 'password', e.target.value)}
                              autoComplete="new-password"
                            />
                            <button
                              type="button"
                              className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-300"
                              onClick={() => toggleSmtpPasswordVisibility(index)}
                            >
                              {showSmtpPasswords[index] ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                            </button>
                          </div>
                        </label>
                        <label className="field-label sm:col-span-2">
                          <span className="mb-1.5 block">From Name</span>
                          <input
                            className="glass-input"
                            type="text"
                            placeholder="Your Name"
                            value={account.from_name || ''}
                            onChange={(e) => updateSmtpAccount(index, 'from_name', e.target.value)}
                          />
                        </label>
                      </div>
                      <div className="flex flex-wrap items-center gap-3">
                        <button
                          type="button"
                          className="btn-secondary"
                          onClick={() => testSmtpAccount(index)}
                          disabled={testingSmtpIndex === index}
                        >
                          {testingSmtpIndex === index ? 'Testing…' : 'Test Connection'}
                        </button>
                        {smtpTestResults[index] ? (
                          <div className={`text-xs ${smtpTestResults[index].ok ? 'text-emerald-400' : 'text-rose-400'}`}>
                            <span className="flex items-center gap-1.5">
                              <span>{smtpTestResults[index].ok ? '✓' : '!'}</span>
                              <span>{smtpTestResults[index].ok ? (smtpTestResults[index].message || 'Connected') : (smtpTestResults[index].message || 'Connection failed')}</span>
                            </span>
                            {!smtpTestResults[index].ok && smtpTestResults[index].error ? (
                              <span className="mt-1 block text-[11px] text-rose-300/90">{smtpTestResults[index].error}</span>
                            ) : null}
                          </div>
                        ) : null}
                      </div>
                    </div>
                  ))}
                  <button type="button" className="btn-secondary" onClick={addSmtpAccount}>
                    <PlusCircle className="h-4 w-4" /> Add SMTP Account
                  </button>
                </div>
              </div>
              <div>
                <h3 className="text-base font-semibold text-white mb-4 flex items-center gap-2">
                  <Send className="h-4 w-4 text-cyan-400" /> Sending Strategy
                </h3>
                <label className="field-label max-w-sm">
                  <span className="mb-1.5 block">Strategy</span>
                  <select
                    className="glass-input"
                    value={configForm.sending_strategy || 'round_robin'}
                    onChange={(e) => setConfigForm({ ...configForm, sending_strategy: e.target.value })}
                  >
                    <option value="round_robin">Round-robin</option>
                    <option value="random">Random</option>
                  </select>
                </label>
              </div>
              <div>
                <h3 className="text-base font-semibold text-white mb-4 flex items-center gap-2">
                  <Eye className="h-4 w-4 text-cyan-400" /> Template Preview
                </h3>
                <div className="flex flex-wrap gap-3">
                  <button type="button" className="btn-ghost" onClick={() => openTemplatePreview('soft')}>Preview Soft</button>
                  <button type="button" className="btn-ghost" onClick={() => openTemplatePreview('competitor')}>Preview Competitor</button>
                  <button type="button" className="btn-ghost" onClick={() => openTemplatePreview('ghost')}>Preview Ghost</button>
                </div>
                {templatePreview.body ? (
                  <div className="mt-4 rounded-2xl border border-cyan-500/20 bg-slate-900/70 p-4">
                    <p className="text-xs font-semibold uppercase tracking-[0.14em] text-cyan-300">{templatePreview.mode}</p>
                    <div className="mt-3 rounded-xl border border-white/10 bg-slate-950/70 px-3 py-2">
                      <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Subject</p>
                      <p className="mt-1 text-sm font-semibold text-white">{templatePreview.subject}</p>
                    </div>
                    <div className="mt-3 rounded-xl border border-white/10 bg-slate-950/70 px-3 py-2">
                      <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Body</p>
                      <pre className="code-block mt-1 whitespace-pre-wrap break-words">{templatePreview.body}</pre>
                    </div>
                  </div>
                ) : null}
              </div>
              <div className="flex items-center gap-4">
                <button className="btn-primary" type="submit" disabled={savingConfig}>
                  <Save className="h-4 w-4" />
                  {savingConfig ? 'Saving…' : 'Save Config'}
                </button>
                <div className="flex gap-3 text-xs">
                  <span className={`flex items-center gap-1.5 ${configHealth.openai_ok ? 'text-emerald-400' : 'text-rose-400'}`}>
                    <span className={`h-1.5 w-1.5 rounded-full ${configHealth.openai_ok ? 'bg-emerald-400' : 'bg-rose-400'}`} />
                    OpenAI {configHealth.openai_ok ? 'OK' : 'Not set'}
                  </span>
                  <span className={`flex items-center gap-1.5 ${configHealth.smtp_ok ? 'text-emerald-400' : 'text-rose-400'}`}>
                    <span className={`h-1.5 w-1.5 rounded-full ${configHealth.smtp_ok ? 'bg-emerald-400' : 'bg-rose-400'}`} />
                    SMTP {configHealth.smtp_ok ? 'OK' : 'Not set'}
                  </span>
                </div>
              </div>
            </form>
          ) : activeTab === 'qualify' ? (
            <div className="space-y-6">
              {!canLeadScoring ? (
                <LockedFeatureNotice
                  title="Lead Qualifier is locked on your current plan"
                  description="Upgrade to The Hustler or above to unlock Gold Mine scoring, priority buckets, and instant qualifier refreshes."
                />
              ) : null}
              {/* Header */}
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <h2 className="text-lg font-bold text-white flex items-center gap-2">
                    <Zap className="h-5 w-5 text-amber-400" />
                    Expert Lead Qualifier
                  </h2>
                  <p className="text-sm text-slate-400 mt-0.5">
                    Gold Mine opportunities — businesses losing money today that you can fix.
                  </p>
                  {qualifierData.data?.context_benchmark && (
                    <p className="text-[11px] text-slate-500 mt-1">
                      Niche: {qualifierData.data?.selected_niche || 'N/A'} • Avg score: {qualifierData.data?.context_benchmark?.niche_avg_score ?? 0}
                    </p>
                  )}
                  {qualifierData.data?.scope === 'legacy_fallback' && (
                    <p className="text-[11px] text-amber-400/80 mt-1">
                      Showing legacy leads (user-scoped leads not found yet).
                    </p>
                  )}
                  {qualifierLossInsight ? (
                    <div className="mt-3 rounded-xl border border-amber-400/30 bg-amber-500/10 px-3 py-2.5">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.12em] text-amber-300">Opportunity Alert</p>
                      <p className="mt-1 text-sm font-semibold text-amber-100">
                        Your qualified leads are losing an estimated {formatCurrencyEur(qualifierLossInsight.estimatedMonthlyLoss)}/month due to {qualifierLossInsight.finding}.
                      </p>
                      <p className="mt-1 text-[11px] text-amber-200/80">
                        Based on {qualifierLossInsight.leadCount} lead{qualifierLossInsight.leadCount === 1 ? '' : 's'} in {qualifierLossInsight.niche}.
                      </p>
                    </div>
                  ) : null}
                </div>
                <button
                  className="btn-ghost"
                  type="button"
                  disabled={qualifierData.loading || !canLeadScoring}
                  onClick={() => void fetchQualifierData()}
                >
                  {canLeadScoring ? <RefreshCw className={`h-3.5 w-3.5 ${qualifierData.loading ? 'animate-spin' : ''}`} /> : <Lock className="h-3.5 w-3.5" />}
                  {qualifierData.loading ? 'Analyzing…' : canLeadScoring ? 'Refresh' : 'Locked on Hustler+'}
                </button>
              </div>

              {qualifierData.error ? (
                <p className="rounded-xl bg-rose-950/60 p-3 text-sm text-rose-300">{qualifierData.error}</p>
              ) : !qualifierData.data && !qualifierData.loading ? (
                <div className="rounded-2xl border border-white/5 bg-white/[0.03] p-10 text-center">
                  <Zap className="mx-auto h-8 w-8 text-slate-600 mb-3" />
                  <p className="text-slate-400 text-sm">{canLeadScoring ? 'Click Refresh to qualify your leads.' : 'Upgrade to The Hustler to unlock AI lead scoring and Gold Mine buckets.'}</p>
                </div>
              ) : qualifierData.loading && !qualifierData.data ? (
                <div className="rounded-2xl border border-white/5 bg-white/[0.03] p-10 text-center">
                  <RefreshCw className="mx-auto h-8 w-8 text-slate-600 animate-spin mb-3" />
                  <p className="text-slate-400 text-sm">Analyzing leads…</p>
                </div>
              ) : (
                <>
                  {/* Summary pills */}
                  <div className="grid gap-3 sm:grid-cols-3">
                    <div className="glass-card rounded-2xl p-4 flex items-center gap-3">
                      <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-rose-500/15">
                        <Target className="h-5 w-5 text-rose-400" />
                      </div>
                      <div>
                        <p className="text-2xl font-bold text-white">{qualifierData.data?.counts?.ghost ?? qualifierData.data?.counts?.no_website ?? 0}</p>
                        <p className="text-xs text-slate-400 font-semibold uppercase tracking-wide">The Ghost</p>
                        <p className="text-[10px] text-rose-400 mt-0.5">Priority #1</p>
                      </div>
                    </div>
                    <div className="glass-card rounded-2xl p-4 flex items-center gap-3">
                      <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-amber-500/15">
                        <Search className="h-5 w-5 text-amber-400" />
                      </div>
                      <div>
                        <p className="text-2xl font-bold text-white">{qualifierData.data?.counts?.invisible_giant ?? qualifierData.data?.counts?.invisible_local ?? 0}</p>
                        <p className="text-xs text-slate-400 font-semibold uppercase tracking-wide">Invisible Giant</p>
                        <p className="text-[10px] text-amber-400 mt-0.5">Big offline, tiny online</p>
                      </div>
                    </div>
                    <div className="glass-card rounded-2xl p-4 flex items-center gap-3">
                      <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-teal-500/15">
                        <TrendingUp className="h-5 w-5 text-teal-400" />
                      </div>
                      <div>
                        <p className="text-2xl font-bold text-white">{qualifierData.data?.counts?.tech_debt ?? qualifierData.data?.counts?.low_authority ?? 0}</p>
                        <p className="text-xs text-slate-400 font-semibold uppercase tracking-wide">Tech Debt</p>
                        <p className="text-[10px] text-teal-400 mt-0.5">Stack and UX drag</p>
                      </div>
                    </div>
                  </div>

                  {/* No Website bucket */}
                  {((qualifierData.data?.ghost?.length ?? qualifierData.data?.no_website?.length) ?? 0) > 0 && (
                    <div>
                      <div className="mb-3 flex items-center gap-2">
                        <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-rose-500/20 text-xs font-bold text-rose-400">1</span>
                        <h3 className="font-semibold text-white text-sm">The Ghost — Gold Mine #1</h3>
                        <span className="rounded-full bg-rose-500/15 px-2 py-0.5 text-xs font-bold text-rose-400">
                          {(qualifierData.data?.ghost ?? qualifierData.data?.no_website ?? []).length} leads
                        </span>
                      </div>
                      <div className="space-y-3">
                        {(qualifierData.data?.ghost ?? qualifierData.data?.no_website ?? []).map((lead) => (
                          <QualifierLeadCard key={lead.id} lead={lead} accentClass="border-rose-500/20 bg-rose-950/10" badgeClass="bg-rose-500/15 text-rose-400" />
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Invisible Local bucket */}
                  {((qualifierData.data?.invisible_giant?.length ?? qualifierData.data?.invisible_local?.length) ?? 0) > 0 && (
                    <div>
                      <div className="mb-3 flex items-center gap-2">
                        <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-amber-500/20 text-xs font-bold text-amber-400">2</span>
                        <h3 className="font-semibold text-white text-sm">The Invisible Giant</h3>
                        <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-xs font-bold text-amber-400">
                          {(qualifierData.data?.invisible_giant ?? qualifierData.data?.invisible_local ?? []).length} leads
                        </span>
                      </div>
                      <div className="space-y-3">
                        {(qualifierData.data?.invisible_giant ?? qualifierData.data?.invisible_local ?? []).map((lead) => (
                          <QualifierLeadCard key={lead.id} lead={lead} accentClass="border-amber-500/20 bg-amber-950/10" badgeClass="bg-amber-500/15 text-amber-400" />
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Low Authority bucket */}
                  {((qualifierData.data?.tech_debt?.length ?? qualifierData.data?.low_authority?.length) ?? 0) > 0 && (
                    <div>
                      <div className="mb-3 flex items-center gap-2">
                        <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-teal-500/20 text-xs font-bold text-teal-400">3</span>
                        <h3 className="font-semibold text-white text-sm">Tech Debt</h3>
                        <span className="rounded-full bg-teal-500/15 px-2 py-0.5 text-xs font-bold text-teal-400">
                          {(qualifierData.data?.tech_debt ?? qualifierData.data?.low_authority ?? []).length} leads
                        </span>
                      </div>
                      <div className="space-y-3">
                        {(qualifierData.data?.tech_debt ?? qualifierData.data?.low_authority ?? []).map((lead) => (
                          <QualifierLeadCard key={lead.id} lead={lead} accentClass="border-teal-500/20 bg-teal-950/10" badgeClass="bg-teal-500/15 text-teal-400" />
                        ))}
                      </div>
                    </div>
                  )}

                  {qualifierData.data?.total === 0 && (
                    <div className="rounded-2xl border border-white/5 bg-white/[0.03] p-10 text-center">
                      <CheckCircle2 className="mx-auto h-8 w-8 text-emerald-500 mb-3" />
                      <p className="text-slate-300 font-semibold">No Gold Mine leads found.</p>
                      <p className="text-slate-500 text-sm mt-1">All current leads are either qualified or filtered out. Scrape more niche-specific results.</p>
                    </div>
                  )}
                </>
              )}
            </div>
          ) : activeTab === 'export' ? (
            <div className="space-y-5">
              <p className="text-sm text-slate-400">Download CSVs, send leads into HubSpot or Google Sheets, and generate monthly Business/Elite reports.</p>

              <div className="grid gap-4 xl:grid-cols-4">
                <div className="glass-card rounded-2xl p-5 flex flex-col gap-3">
                  <div>
                    <p className="font-semibold text-white text-sm flex items-center gap-2">CSV Exports {!canBulkExport ? <PremiumBadge label="Growth+" /> : null}</p>
                    <p className="text-xs text-slate-400 mt-1">Download filtered lead files directly in your browser.</p>
                  </div>
                  <button className="btn-primary" type="button" disabled={!canBulkExport || exportingTargets} onClick={exportTargets}>
                    <Download className="h-4 w-4" />
                    {exportingTargets ? 'Downloading…' : canBulkExport ? 'Export target_leads.csv' : 'Locked on Growth+'}
                  </button>
                  <button className="btn-secondary" type="button" disabled={!canBulkExport || exportingAI} onClick={exportAI}>
                    <Download className="h-4 w-4" />
                    {exportingAI ? 'Downloading…' : canBulkExport ? 'Export ai_mailer_ready.csv' : 'Locked on Growth+'}
                  </button>
                </div>

                <div className="glass-card rounded-2xl p-5 flex flex-col gap-3">
                  <div>
                    <p className="font-semibold text-white text-sm flex items-center gap-2">Advanced Exports {!canAdvancedReporting ? <PremiumBadge label="Business+" /> : null}</p>
                    <p className="text-xs text-slate-400 mt-1">Push qualified leads into HubSpot or Google Sheets through your webhook/Zapier URLs.</p>
                  </div>
                  {!canAdvancedReporting ? (
                    <LockedFeatureNotice
                      title="CRM sync is locked on your current plan"
                      description="Upgrade to Business or Elite to unlock HubSpot, Google Sheets, Zapier, and webhook exports."
                    />
                  ) : null}
                  <button className="btn-primary" type="button" disabled={!canAdvancedReporting || webhookExporting === 'hubspot'} onClick={() => void exportWebhookDestination('hubspot')}>
                    <Send className="h-4 w-4" />
                    {webhookExporting === 'hubspot' ? 'Sending…' : 'Export to HubSpot'}
                  </button>
                  <button className="btn-secondary" type="button" disabled={!canAdvancedReporting || webhookExporting === 'google_sheets'} onClick={() => void exportWebhookDestination('google_sheets')}>
                    <Send className="h-4 w-4" />
                    {webhookExporting === 'google_sheets' ? 'Sending…' : 'Export to Google Sheets'}
                  </button>
                </div>

                <div className="glass-card rounded-2xl p-5 flex flex-col gap-3">
                  <div>
                    <p className="font-semibold text-white text-sm flex items-center gap-2">Weekly Summary {!canAdvancedReporting ? <PremiumBadge label="Business+" /> : null}</p>
                    <p className="text-xs text-slate-400 mt-1">{weeklyReport?.period_label || 'Last 7 days'} of new leads, outreach, replies, and wins.</p>
                  </div>
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <div className="rounded-xl border border-slate-700/60 bg-slate-900/60 px-3 py-2">
                      <p className="text-slate-400">Found</p>
                      <p className="mt-1 text-lg font-semibold text-white">{Number(weeklyReport?.found_this_week ?? stats.found_this_week ?? 0)}</p>
                    </div>
                    <div className="rounded-xl border border-slate-700/60 bg-slate-900/60 px-3 py-2">
                      <p className="text-slate-400">Contacted</p>
                      <p className="mt-1 text-lg font-semibold text-white">{Number(weeklyReport?.contacted_this_week ?? stats.contacted_this_week ?? 0)}</p>
                    </div>
                    <div className="rounded-xl border border-slate-700/60 bg-slate-900/60 px-3 py-2">
                      <p className="text-slate-400">Replied</p>
                      <p className="mt-1 text-lg font-semibold text-emerald-200">{Number(weeklyReport?.replied_this_week ?? stats.replied_this_week ?? 0)}</p>
                    </div>
                    <div className="rounded-xl border border-slate-700/60 bg-slate-900/60 px-3 py-2">
                      <p className="text-slate-400">Won</p>
                      <p className="mt-1 text-lg font-semibold text-amber-200">{Number(weeklyReport?.won_this_week ?? stats.won_this_week ?? 0)}</p>
                    </div>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <button className="btn-secondary" type="button" disabled={!canAdvancedReporting || loadingWeeklyReport} onClick={() => void refreshWeeklyReport()}>
                      <RefreshCw className={`h-4 w-4 ${loadingWeeklyReport ? 'animate-spin' : ''}`} />
                      Refresh
                    </button>
                    <button className="btn-primary" type="button" disabled={!canAdvancedReporting || sendingWeeklyReport} onClick={emailWeeklyReport}>
                      <Mail className="h-4 w-4" />
                      {sendingWeeklyReport ? 'Sending…' : 'Email weekly summary'}
                    </button>
                  </div>
                </div>

                <div className="glass-card rounded-2xl p-5 flex flex-col gap-3">
                  <div>
                    <p className="font-semibold text-white text-sm flex items-center gap-2">Monthly Summary {!canAdvancedReporting ? <PremiumBadge label="Business+" /> : null}</p>
                    <p className="text-xs text-slate-400 mt-1">{monthlyReport?.month_label || 'This month'} summary of found, contacted, replied, and won leads.</p>
                  </div>
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <div className="rounded-xl border border-slate-700/60 bg-slate-900/60 px-3 py-2">
                      <p className="text-slate-400">Found</p>
                      <p className="mt-1 text-lg font-semibold text-white">{Number(monthlyReport?.found_this_month ?? stats.found_this_month ?? 0)}</p>
                    </div>
                    <div className="rounded-xl border border-slate-700/60 bg-slate-900/60 px-3 py-2">
                      <p className="text-slate-400">Contacted</p>
                      <p className="mt-1 text-lg font-semibold text-white">{Number(monthlyReport?.contacted_this_month ?? stats.contacted_this_month ?? 0)}</p>
                    </div>
                    <div className="rounded-xl border border-slate-700/60 bg-slate-900/60 px-3 py-2">
                      <p className="text-slate-400">Replied</p>
                      <p className="mt-1 text-lg font-semibold text-emerald-200">{Number(monthlyReport?.replied_this_month ?? stats.replied_this_month ?? 0)}</p>
                    </div>
                    <div className="rounded-xl border border-slate-700/60 bg-slate-900/60 px-3 py-2">
                      <p className="text-slate-400">Won</p>
                      <p className="mt-1 text-lg font-semibold text-amber-200">{Number(monthlyReport?.won_this_month ?? stats.won_this_month ?? 0)}</p>
                    </div>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <button className="btn-secondary" type="button" disabled={!canAdvancedReporting || loadingMonthlyReport} onClick={() => void refreshMonthlyReport()}>
                      <RefreshCw className={`h-4 w-4 ${loadingMonthlyReport ? 'animate-spin' : ''}`} />
                      Refresh
                    </button>
                    <button className="btn-secondary" type="button" disabled={!canAdvancedReporting || loadingMonthlyReport} onClick={downloadMonthlyReportPdf}>
                      <Download className="h-4 w-4" /> PDF
                    </button>
                    <button className="btn-primary" type="button" disabled={!canAdvancedReporting || sendingMonthlyReport} onClick={emailMonthlyReport}>
                      <Mail className="h-4 w-4" />
                      {sendingMonthlyReport ? 'Sending…' : 'Email monthly summary'}
                    </button>
                  </div>
                  <p className="text-[11px] text-slate-500">
                    Sent automatically to: {String(currentUserEmail || 'your account email')}
                  </p>
                </div>
              </div>
            </div>
          ) : activeTab === 'clients' ? (
            <div className="space-y-5">
              {!canClientSuccessDashboard ? (
                <LockedFeatureNotice
                  title="Client Success Dashboard is locked on your current plan"
                  description="Upgrade to Business or Elite to manage multiple client folders, track agency pipeline, and review won accounts at a glance."
                />
              ) : null}

              <div className="grid gap-4 md:grid-cols-4">
                <div className="glass-card rounded-2xl p-4">
                  <p className="text-xs uppercase tracking-[0.14em] text-slate-400">Client folders</p>
                  <p className="mt-2 text-2xl font-semibold text-white">{Number(clientDashboard?.folder_count ?? clientDashboard?.total_clients ?? clientFolders.length ?? 0)}</p>
                </div>
                <div className="glass-card rounded-2xl p-4">
                  <p className="text-xs uppercase tracking-[0.14em] text-slate-400">Unassigned</p>
                  <p className="mt-2 text-2xl font-semibold text-white">{Number(clientDashboard?.unassigned_count ?? 0)}</p>
                </div>
                <div className="glass-card rounded-2xl p-4">
                  <p className="text-xs uppercase tracking-[0.14em] text-slate-400">Contacted</p>
                  <p className="mt-2 text-2xl font-semibold text-cyan-200">{Number(clientDashboard?.pipeline?.contacted ?? stats.pipeline?.contacted ?? 0)}</p>
                </div>
                <div className="glass-card rounded-2xl p-4">
                  <p className="text-xs uppercase tracking-[0.14em] text-slate-400">Won</p>
                  <p className="mt-2 text-2xl font-semibold text-amber-200">{Number(clientDashboard?.pipeline?.won_paid ?? stats.pipeline?.won_paid ?? 0)}</p>
                </div>
              </div>

              <div className="grid gap-4 xl:grid-cols-[1.1fr,1.6fr]">
                <form className="glass-card rounded-2xl p-5 space-y-3" onSubmit={createClientFolder}>
                  <div>
                    <p className="font-semibold text-white">Create client folder</p>
                    <p className="text-xs text-slate-400 mt-1">Group multiple leads under one agency client and track their pipeline together.</p>
                  </div>
                  <label className="field-label block">
                    <span className="mb-1.5 block">Client name</span>
                    <input
                      className="glass-input"
                      type="text"
                      placeholder="Acme Dental"
                      value={clientFolderForm.name}
                      onChange={(e) => setClientFolderForm((prev) => ({ ...prev, name: e.target.value }))}
                    />
                  </label>
                  <label className="field-label block">
                    <span className="mb-1.5 block">Notes</span>
                    <textarea
                      className="glass-input min-h-[96px]"
                      placeholder="Priority account, offer angle, retention notes…"
                      value={clientFolderForm.description}
                      onChange={(e) => setClientFolderForm((prev) => ({ ...prev, description: e.target.value }))}
                    />
                  </label>
                  <button className="btn-primary" type="submit" disabled={!canClientSuccessDashboard || creatingClientFolder}>
                    <PlusCircle className="h-4 w-4" />
                    {creatingClientFolder ? 'Creating…' : 'Add client folder'}
                  </button>
                </form>

                <div className="glass-card rounded-2xl p-5 space-y-3">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <p className="font-semibold text-white">Client success overview</p>
                      <p className="text-xs text-slate-400 mt-1">Track each client folder from scraped to won (paid).</p>
                    </div>
                    <button className="btn-ghost" type="button" disabled={!canClientSuccessDashboard || loadingClientDashboard} onClick={() => void Promise.allSettled([refreshClientFolders(), refreshClientDashboard()])}>
                      <RefreshCw className={`h-4 w-4 ${loadingClientDashboard ? 'animate-spin' : ''}`} /> Refresh
                    </button>
                  </div>

                  {loadingClientFolders && clientFolders.length === 0 ? (
                    <p className="text-sm text-slate-400">Loading client folders…</p>
                  ) : clientFolders.length === 0 ? (
                    <div className="rounded-2xl border border-dashed border-slate-700/70 bg-slate-900/40 p-6 text-center text-sm text-slate-400">
                      No client folders yet. Create your first agency folder to start organizing leads.
                    </div>
                  ) : (
                    <div className="space-y-3">
                      {clientFolders.map((folder) => (
                        <div key={folder.id} className="rounded-2xl border border-slate-700/70 bg-slate-900/50 p-4">
                          <div className="flex flex-wrap items-start justify-between gap-3">
                            <div>
                              <p className="font-semibold text-white">{folder.name}</p>
                              <p className="text-[11px] text-slate-400 mt-1">{folder.notes || 'No notes yet.'}</p>
                            </div>
                            <span className="rounded-full border border-slate-700/70 bg-slate-800/80 px-2.5 py-1 text-[11px] text-slate-300">
                              {Number(folder.lead_count || 0)} lead{Number(folder.lead_count || 0) === 1 ? '' : 's'}
                            </span>
                          </div>
                          <div className="mt-3 grid gap-2 sm:grid-cols-4 text-xs">
                            <div className="rounded-xl bg-slate-950/80 px-3 py-2">
                              <p className="text-slate-500">Contacted</p>
                              <p className="mt-1 font-semibold text-cyan-200">{Number(folder.contacted_count || 0)}</p>
                            </div>
                            <div className="rounded-xl bg-slate-950/80 px-3 py-2">
                              <p className="text-slate-500">Replied</p>
                              <p className="mt-1 font-semibold text-emerald-200">{Number(folder.replied_count || 0)}</p>
                            </div>
                            <div className="rounded-xl bg-slate-950/80 px-3 py-2">
                              <p className="text-slate-500">Won</p>
                              <p className="mt-1 font-semibold text-amber-200">{Number(folder.won_paid_count || 0)}</p>
                            </div>
                            <div className="rounded-xl bg-slate-950/80 px-3 py-2">
                              <p className="text-slate-500">Top lead</p>
                              <p className="mt-1 font-semibold text-white">{folder.top_leads?.[0]?.business_name || '—'}</p>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </div>
          ) : (
            <div className="overflow-hidden rounded-xl border border-slate-800 bg-slate-950/70 shadow-2xl shadow-blue-950/20">
              <div className="max-h-[620px] overflow-auto">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-slate-950/95 backdrop-blur border-b border-slate-800">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-slate-400">Task</th>
                      <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-slate-400">Status</th>
                      <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-slate-400">Payload</th>
                      <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-slate-400">Result</th>
                      <th className="px-4 py-3 text-right text-xs font-semibold uppercase tracking-wider text-slate-400">Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {taskHistory.map((task) => {
                      const taskName = taskLabels[task.task_type] || task.task_type
                      const status = String(task.status || 'idle').toUpperCase()
                      const statusRaw = String(task.status || '').toLowerCase()
                      const failed = statusRaw === 'failed' || statusRaw === 'error' || statusRaw === 'generation_failed'
                      const payloadText = String(formatTaskPayload(task.last_request, task.task_type) || '—').replace(/\n+/g, ' | ')
                      const resultText = task.error
                        ? String(task.error || '').replace(/\n+/g, ' | ')
                        : String(formatTaskResult(task.result, task.task_type, task.error) || '—').replace(/\n+/g, ' | ')

                      return (
                        <tr key={task.id} className="border-b border-slate-800 transition-colors hover:bg-blue-500/5">
                          <td className="px-4 py-3 align-top">
                            <p className="font-bold text-white">{taskName}</p>
                            <p className="mt-1 text-xs text-slate-400">{task.created_at ? new Date(task.created_at).toLocaleString() : '\u2014'}</p>
                          </td>
                          <td className="px-4 py-3 align-top">
                            <span className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11px] font-semibold ${taskPillClass(task)}`}>
                              {(task.running || statusRaw === 'running') && <span className="h-1.5 w-1.5 rounded-full bg-blue-400 animate-pulse" />}
                              {status}
                            </span>
                          </td>
                          <td className="px-4 py-3 align-top">
                            <div
                              title={payloadText}
                              className="w-[220px] rounded-lg border border-slate-800 bg-slate-900 px-3 py-2 text-xs text-slate-300 truncate"
                            >
                              {payloadText || '—'}
                            </div>
                          </td>
                          <td className="px-4 py-3 align-top">
                            <div
                              title={resultText}
                              className={`w-[220px] rounded-lg border px-3 py-2 text-xs truncate ${
                                failed ? 'border-rose-500/30 bg-rose-950/40 text-rose-300' : 'border-slate-800 bg-slate-900 text-slate-300'
                              }`}
                            >
                              {resultText || '—'}
                            </div>
                          </td>
                          <td className="px-4 py-3 align-top text-right">
                            <button
                              className="group inline-flex items-center gap-2 rounded-lg border border-blue-500/30 bg-blue-500/10 px-3 py-1.5 text-xs font-semibold text-blue-300 transition hover:bg-blue-500/20 disabled:cursor-not-allowed disabled:opacity-40"
                              type="button"
                              disabled={retryingTaskId === task.id || task.running || statusRaw !== 'failed'}
                              onClick={() => void retryTask(task.id)}
                            >
                              <RefreshCw className={`h-3.5 w-3.5 ${retryingTaskId === task.id ? 'animate-spin' : 'group-hover:animate-spin'}`} />
                              {retryingTaskId === task.id ? 'Retrying\u2026' : 'Retry'}
                            </button>
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </section>
      </div>

      {/* ── Top Up Credits Modal ── */}
      <TopUpCreditsModal
        isOpen={showTopUpModal}
        selectedPackageId={selectedTopUpPackageId}
        selectedPackage={selectedTopUpPackage}
        packages={TOP_UP_PACKAGE_OPTIONS}
        loadingPackageId={topUpLoadingPackageId}
        preparingPackageId={topUpPreparingPackageId}
        onClose={closeTopUpModal}
        onPackageChange={handleTopUpPackageChange}
        onProceed={handleTopUpProceed}
      />


      {/* ── Add Sale Modal ── */}
      {showSaleModal && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center p-4"
          style={{ background: 'rgba(2,6,23,0.78)', backdropFilter: 'blur(6px)' }}
          onClick={(e) => { if (e.target === e.currentTarget) setShowSaleModal(false) }}
        >
          <div className="glass-card w-full max-w-md rounded-[28px] p-7 shadow-2xl">
            <div className="mb-6 flex items-center justify-between">
              <div>
                <p className="label-overline">Revenue Tracker</p>
                <h2 className="mt-1.5 text-2xl font-semibold text-white">Log a Sale</h2>
              </div>
              <button
                type="button"
                className="rounded-full p-2 text-slate-400 transition hover:bg-white/10 hover:text-white"
                onClick={() => setShowSaleModal(false)}
                aria-label="Close"
              >
                ✕
              </button>
            </div>

            <form className="space-y-4" onSubmit={submitSale}>
              <label className="field-label block">
                <span className="mb-1.5 block">Amount (€)</span>
                <input
                  className="glass-input"
                  type="number"
                  min="0.01"
                  step="0.01"
                  placeholder="1200"
                  required
                  value={saleForm.amount}
                  onChange={(e) => setSaleForm({ ...saleForm, amount: e.target.value })}
                  autoFocus
                />
              </label>

              <label className="field-label block">
                <span className="mb-1.5 block">Service Type</span>
                <div className="relative">
                  <select
                    className="glass-input appearance-none pr-8"
                    value={saleForm.serviceType}
                    onChange={(e) => setSaleForm({ ...saleForm, serviceType: e.target.value })}
                  >
                    <option>Google Ads Setup</option>
                    <option>Monthly Retainer</option>
                    <option>Web Design</option>
                    <option>SEO Package</option>
                    <option>Social Media Management</option>
                    <option>Consulting</option>
                    <option>Custom</option>
                  </select>
                  <ChevronDown className="select-chevron" />
                </div>
              </label>

              <label className="field-label block">
                <span className="mb-1.5 block">Lead / Company Name</span>
                <input
                  className="glass-input"
                  type="text"
                  list="lead-names-datalist"
                  placeholder="Select or type a name…"
                  value={saleForm.leadName}
                  onChange={(e) => {
                    const typed = e.target.value
                    const matched = leads.find((l) => (l.business_name || '') === typed)
                    setSaleForm({ ...saleForm, leadName: typed, leadId: matched ? String(matched.id) : '' })
                  }}
                />
                <datalist id="lead-names-datalist">
                  {leads.filter((l) => l.business_name).map((l) => (
                    <option key={l.id} value={l.business_name} />
                  ))}
                </datalist>
              </label>

              <label className="flex cursor-pointer items-center gap-3 rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3.5 text-sm font-medium text-slate-300 transition hover:bg-white/[0.07]">
                <div
                  className="relative h-5 w-9 shrink-0 rounded-full transition-colors"
                  style={{ background: saleForm.isRecurring ? '#14b8a6' : '#334155' }}
                  onClick={() => setSaleForm({ ...saleForm, isRecurring: !saleForm.isRecurring })}
                >
                  <span
                    className="absolute top-0.5 h-4 w-4 rounded-full bg-white shadow transition-transform"
                    style={{ left: saleForm.isRecurring ? '18px' : '2px' }}
                  />
                </div>
                <span>Recurring? <span className="text-slate-500">(adds to MRR)</span></span>
              </label>

              <div className="flex gap-3 pt-2">
                <button
                  type="submit"
                  className="btn-primary flex-1"
                  disabled={submittingSale || !saleForm.amount}
                >
                  <PlusCircle className="h-4 w-4" />
                  {submittingSale ? 'Logging…' : 'Log Sale'}
                </button>
                <button
                  type="button"
                  className="btn-ghost"
                  onClick={() => setShowSaleModal(false)}
                >
                  Cancel
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {emailPreviewLead && (
        <div
          className="fixed inset-0 z-[60] flex items-center justify-center p-4"
          style={{ background: 'rgba(2,6,23,0.78)', backdropFilter: 'blur(6px)' }}
          onClick={(e) => { if (e.target === e.currentTarget) closeEmailPreviewModal() }}
        >
          <div className="w-full max-w-3xl rounded-3xl border border-cyan-500/20 bg-[#0b1220] p-6 shadow-[0_24px_80px_rgba(2,132,199,0.25)]">
            <div className="mb-4 flex items-start justify-between gap-4">
              <h3 className="text-lg font-semibold text-white">
                Sent/Generated Email for {emailPreviewLead.businessName}
              </h3>
              <button
                type="button"
                className="rounded-full p-2 text-slate-400 transition hover:bg-white/10 hover:text-white"
                onClick={closeEmailPreviewModal}
                aria-label="Close"
              >
                ✕
              </button>
            </div>

            <div className="rounded-xl border border-white/10 bg-slate-950/60 px-4 py-3">
              <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Subject</p>
              <p className="mt-1 text-sm font-semibold text-white">
                {emailPreviewLead.subject || 'Subject not stored (body-only history).'}
              </p>
            </div>

            <div className="mt-3 rounded-xl border border-white/10 bg-slate-900/70 px-4 py-3">
              <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Body</p>
              {emailPreviewLead.body ? (
                <pre className="code-block mt-1 whitespace-pre-wrap break-words text-slate-100">{emailPreviewLead.body}</pre>
              ) : (
                <p className="mt-1 text-sm text-slate-400">No email generated for this lead yet.</p>
              )}
            </div>
          </div>
        </div>
      )}

      {aiSummaryPreviewLead && (
        <div
          className="fixed inset-0 z-[60] flex items-center justify-center p-4"
          style={{ background: 'rgba(2,6,23,0.78)', backdropFilter: 'blur(6px)' }}
          onClick={(e) => { if (e.target === e.currentTarget) closeAiSummaryModal() }}
        >
          <div className="w-full max-w-3xl rounded-3xl border border-cyan-500/20 bg-[#0b1220] p-6 shadow-[0_24px_80px_rgba(2,132,199,0.25)]">
            <div className="mb-4 flex items-start justify-between gap-4">
              <h3 className="text-lg font-semibold text-white">
                AI Summary for {aiSummaryPreviewLead.businessName}
              </h3>
              <button
                type="button"
                className="rounded-full p-2 text-slate-400 transition hover:bg-white/10 hover:text-white"
                onClick={closeAiSummaryModal}
                aria-label="Close"
              >
                ✕
              </button>
            </div>

            <div className="grid gap-3 md:grid-cols-4">
              <div className="rounded-xl border border-cyan-500/20 bg-cyan-500/10 px-3 py-2">
                <p className="text-[11px] uppercase tracking-[0.12em] text-cyan-200/80">Lead score</p>
                <p className="mt-1 text-lg font-semibold text-white">{formatLeadScoreValue(aiSummaryPreviewLead.bestLeadScore)}/10</p>
              </div>
              <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-3 py-2">
                <p className="text-[11px] uppercase tracking-[0.12em] text-emerald-200/80">AI signal</p>
                <p className="mt-1 text-lg font-semibold text-white">{formatLeadScoreValue(aiSummaryPreviewLead.sentimentScore)}/10</p>
              </div>
              <div className="rounded-xl border border-violet-500/20 bg-violet-500/10 px-3 py-2">
                <p className="text-[11px] uppercase tracking-[0.12em] text-violet-200/80">Team size</p>
                <p className="mt-1 text-lg font-semibold text-white">{aiSummaryPreviewLead.employeeCount || '—'}</p>
              </div>
              <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 px-3 py-2">
                <p className="text-[11px] uppercase tracking-[0.12em] text-amber-200/80">Priority</p>
                <p className="mt-1 text-sm font-semibold text-white">{aiSummaryPreviewLead.leadPriority || 'Qualified'}</p>
              </div>
            </div>

            <div className="mt-3 grid gap-3 md:grid-cols-4">
              <div className="rounded-xl border border-sky-500/20 bg-sky-500/10 px-3 py-2">
                <p className="text-[11px] uppercase tracking-[0.12em] text-sky-200/80">Qualification</p>
                <p className="mt-1 text-lg font-semibold text-white">{Math.round(Number(aiSummaryPreviewLead.qualificationScore || 0)) || '—'}/100</p>
              </div>
              <div className="rounded-xl border border-fuchsia-500/20 bg-fuchsia-500/10 px-3 py-2">
                <p className="text-[11px] uppercase tracking-[0.12em] text-fuchsia-200/80">Social activity</p>
                <p className="mt-1 text-lg font-semibold text-white">{Number(aiSummaryPreviewLead.socialActivityScore || 0).toFixed(1)}/10</p>
              </div>
              <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-3 py-2">
                <p className="text-[11px] uppercase tracking-[0.12em] text-emerald-200/80">Google claimed</p>
                <p className="mt-1 text-sm font-semibold text-white">{aiSummaryPreviewLead.googleMaps?.claimed == null ? 'Unknown' : aiSummaryPreviewLead.googleMaps?.claimed ? 'Yes' : 'No'}</p>
              </div>
              <div className="rounded-xl border border-slate-500/20 bg-slate-500/10 px-3 py-2">
                <p className="text-[11px] uppercase tracking-[0.12em] text-slate-300/80">Website maturity</p>
                <p className="mt-1 text-sm font-semibold text-white">{aiSummaryPreviewLead.websiteSignals?.modern_design ? 'Modern' : 'Basic / unclear'}</p>
              </div>
            </div>

            <div className="mt-3 rounded-xl border border-white/10 bg-slate-900/70 px-4 py-3">
              <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Full description</p>
              {aiSummaryPreviewLead.summary ? (
                <pre className="code-block mt-1 whitespace-pre-wrap break-words text-slate-100">{aiSummaryPreviewLead.summary}</pre>
              ) : (
                <p className="mt-1 text-sm text-slate-400">No AI summary available for this lead yet.</p>
              )}
              {aiSummaryPreviewLead.competitiveHook && (
                <p className="mt-2 text-sm text-cyan-100">{aiSummaryPreviewLead.competitiveHook}</p>
              )}
            </div>

            <div className="mt-3 grid gap-3 md:grid-cols-2">
              <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/5 px-4 py-3">
                <p className="text-[11px] uppercase tracking-[0.12em] text-emerald-200/80">Company strengths</p>
                <ul className="mt-2 space-y-1 text-sm text-slate-100">
                  {normalizeLeadInsightList(aiSummaryPreviewLead.companyAudit?.strengths, 3).length ? normalizeLeadInsightList(aiSummaryPreviewLead.companyAudit?.strengths, 3).map((item) => (
                    <li key={`strength-${item}`}>• {item}</li>
                  )) : <li className="text-slate-400">No strengths detected yet.</li>}
                </ul>
              </div>
              <div className="rounded-xl border border-rose-500/20 bg-rose-500/5 px-4 py-3">
                <p className="text-[11px] uppercase tracking-[0.12em] text-rose-200/80">Weaknesses to target</p>
                <ul className="mt-2 space-y-1 text-sm text-slate-100">
                  {normalizeLeadInsightList(aiSummaryPreviewLead.companyAudit?.weaknesses, 3).length ? normalizeLeadInsightList(aiSummaryPreviewLead.companyAudit?.weaknesses, 3).map((item) => (
                    <li key={`weakness-${item}`}>• {item}</li>
                  )) : <li className="text-slate-400">No weaknesses detected yet.</li>}
                </ul>
              </div>
            </div>

            <div className="mt-3 grid gap-3 md:grid-cols-2">
              <div className="rounded-xl border border-white/10 bg-slate-950/60 px-4 py-3">
                <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Intent signals</p>
                <div className="mt-2 flex flex-wrap gap-2">
                  {aiSummaryPreviewLead.intentSignals.length ? aiSummaryPreviewLead.intentSignals.map((signal) => (
                    <span key={`intent-${signal}`} className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-1 text-[11px] text-emerald-100">{signal}</span>
                  )) : <span className="text-sm text-slate-400">No intent signals yet.</span>}
                </div>
                {aiSummaryPreviewLead.techStack.length > 0 && (
                  <div className="mt-3">
                    <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Tech stack</p>
                    <div className="mt-2 flex flex-wrap gap-2">
                      {aiSummaryPreviewLead.techStack.map((item) => (
                        <span key={`stack-${item}`} className="rounded-full border border-violet-500/30 bg-violet-500/10 px-2 py-1 text-[11px] text-violet-100">{item}</span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
              <div className="rounded-xl border border-white/10 bg-slate-950/60 px-4 py-3">
                <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Competitor snapshot</p>
                <ul className="mt-2 space-y-1 text-sm text-slate-100">
                  {aiSummaryPreviewLead.competitors.length ? aiSummaryPreviewLead.competitors.map((item) => (
                    <li key={`competitor-${item}`}>• {item}</li>
                  )) : <li className="text-slate-400">No competitor context available yet.</li>}
                </ul>
                {aiSummaryPreviewLead.achievements.length > 0 && (
                  <div className="mt-3">
                    <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Recent wins / updates</p>
                    <ul className="mt-2 space-y-1 text-sm text-slate-100">
                      {aiSummaryPreviewLead.achievements.map((item) => (
                        <li key={`achievement-${item}`}>• {item}</li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            </div>

            <div className="mt-3 grid gap-3 md:grid-cols-2">
              <div className="rounded-xl border border-white/10 bg-slate-950/60 px-4 py-3">
                <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Social profiles</p>
                <div className="mt-2 flex flex-wrap gap-2">
                  {Object.entries(aiSummaryPreviewLead.socialProfiles || {}).filter(([, value]) => value).length ? Object.entries(aiSummaryPreviewLead.socialProfiles || {}).filter(([, value]) => value).map(([platform, value]) => (
                    <a key={`social-${platform}`} href={value} target="_blank" rel="noreferrer" className="rounded-full border border-sky-500/30 bg-sky-500/10 px-2 py-1 text-[11px] text-sky-100 hover:bg-sky-500/20">
                      {platform}
                    </a>
                  )) : <span className="text-sm text-slate-400">No social profiles found yet.</span>}
                </div>
                <div className="mt-3 space-y-1 text-sm text-slate-300">
                  {Object.entries(aiSummaryPreviewLead.socialMetrics || {}).length ? Object.entries(aiSummaryPreviewLead.socialMetrics || {}).map(([platform, metrics]) => (
                    <p key={`metric-${platform}`}>
                      <span className="font-semibold text-white">{platform}:</span>{' '}
                      {Number(metrics?.follower_count || 0) > 0 ? `${Number(metrics.follower_count).toLocaleString()} followers` : 'followers n/a'}
                      {' · '}
                      {metrics?.last_active_days != null ? `${metrics.last_active_days}d ago` : 'recency n/a'}
                      {' · '}
                      {metrics?.active ? 'active' : 'stale'}
                    </p>
                  )) : null}
                </div>
              </div>
              <div className="rounded-xl border border-white/10 bg-slate-950/60 px-4 py-3">
                <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Website signals</p>
                <div className="mt-2 flex flex-wrap gap-2">
                  <span className={`rounded-full border px-2 py-1 text-[11px] ${aiSummaryPreviewLead.websiteSignals?.has_pixel ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-100' : 'border-slate-700/50 bg-slate-800/60 text-slate-300'}`}>Pixel {aiSummaryPreviewLead.websiteSignals?.has_pixel ? 'detected' : 'missing'}</span>
                  <span className={`rounded-full border px-2 py-1 text-[11px] ${aiSummaryPreviewLead.websiteSignals?.has_contact_form ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-100' : 'border-slate-700/50 bg-slate-800/60 text-slate-300'}`}>Contact form {aiSummaryPreviewLead.websiteSignals?.has_contact_form ? 'present' : 'missing'}</span>
                  <span className={`rounded-full border px-2 py-1 text-[11px] ${aiSummaryPreviewLead.websiteSignals?.modern_design ? 'border-cyan-500/30 bg-cyan-500/10 text-cyan-100' : 'border-slate-700/50 bg-slate-800/60 text-slate-300'}`}>Design {aiSummaryPreviewLead.websiteSignals?.modern_design ? 'modern' : 'dated'}</span>
                </div>
                {aiSummaryPreviewLead.mainOffer && (
                  <p className="mt-3 text-sm text-slate-300"><span className="font-semibold text-white">Main offer:</span> {aiSummaryPreviewLead.mainOffer}</p>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {taskAiPreviewLead && (
        <div
          className="fixed inset-0 z-[60] flex items-center justify-center p-4"
          style={{ background: 'rgba(2,6,23,0.78)', backdropFilter: 'blur(6px)' }}
          onClick={(e) => { if (e.target === e.currentTarget) closeTaskAiMessagePreview() }}
        >
          <div className="w-full max-w-3xl rounded-3xl border border-cyan-500/20 bg-[#0b1220] p-6 shadow-[0_24px_80px_rgba(2,132,199,0.25)]">
            <div className="mb-4 flex items-start justify-between gap-4">
              <h3 className="text-lg font-semibold text-white">
                Preview AI Message for {taskAiPreviewLead.businessName}
              </h3>
              <button
                type="button"
                className="rounded-full p-2 text-slate-400 transition hover:bg-white/10 hover:text-white"
                onClick={closeTaskAiMessagePreview}
                aria-label="Close"
              >
                ✕
              </button>
            </div>

            <div className="rounded-xl border border-white/10 bg-slate-900/70 px-4 py-3">
              <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">AI insight</p>
              {taskAiPreviewLead.summary ? (
                <pre className="code-block mt-1 whitespace-pre-wrap break-words text-slate-100">{taskAiPreviewLead.summary}</pre>
              ) : (
                <p className="mt-1 text-sm text-slate-400">No AI insight is stored for this lead yet.</p>
              )}
            </div>

            <div className="mt-3 rounded-xl border border-white/10 bg-slate-950/60 px-4 py-3">
              <div className="flex items-start justify-between gap-3">
                <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Generated subject</p>
                <button
                  type="button"
                  className="quick-action-btn"
                  disabled={!taskAiPreviewLead.subject}
                  onClick={() => void copyTaskAiField('Subject', taskAiPreviewLead.subject)}
                >
                  <Copy className="h-3.5 w-3.5" /> Copy subject
                </button>
              </div>
              <p className="mt-1 text-sm font-semibold text-white">
                {taskAiPreviewLead.subject || 'Subject not generated yet.'}
              </p>
            </div>

            <div className="mt-3 rounded-xl border border-white/10 bg-slate-900/70 px-4 py-3">
              <div className="flex items-start justify-between gap-3">
                <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Generated message</p>
                <button
                  type="button"
                  className="quick-action-btn"
                  disabled={!taskAiPreviewLead.body}
                  onClick={() => void copyTaskAiField('Message', taskAiPreviewLead.body)}
                >
                  <Copy className="h-3.5 w-3.5" /> Copy message
                </button>
              </div>
              {taskAiPreviewLead.body ? (
                <pre className="code-block mt-1 whitespace-pre-wrap break-words text-slate-100">{taskAiPreviewLead.body}</pre>
              ) : (
                <p className="mt-1 text-sm text-slate-400">Message body is not generated yet.</p>
              )}
            </div>
          </div>
        </div>
      )}
      <Footer />
    </div>
  )
}

function StatusDot({ label, ok }) {
  return (
    <span className="inline-flex items-center gap-2">
      <span className={`pulse-dot ${ok ? 'dot-green' : 'dot-red'}`} />
      <span>{label}</span>
    </span>
  )
}

function MetricSparkCard({ icon, label, value, subtitle, points, tone = 'cyan', live = false }) {
  return (
    <article className={`metric-card metric-card-${tone}`}>
      <div className="flex items-start justify-between gap-3">
        <div className="metric-icon">{icon}</div>
        {live ? (
          <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/25 bg-emerald-500/10 px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-emerald-300">
            <span className="live-dot" /> Live
          </span>
        ) : null}
      </div>
      <p className="mt-5 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">{label}</p>
      <h3 className="mt-2 text-3xl font-semibold tracking-tight text-white">{value}</h3>
      <svg className="sparkline mt-2" viewBox="0 0 160 44" preserveAspectRatio="none" aria-hidden="true">
        <polyline fill="none" stroke="currentColor" strokeWidth="3" points={buildSparkPoints(points)} />
      </svg>
      <p className="mt-2 text-sm text-slate-500">{subtitle}</p>
    </article>
  )
}

function WorkflowCard({ icon, step, title, summary, status, accent, children }) {
  return (
    <article className={`workflow-card workflow-card-${accent}`}>
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="workflow-step">Step {step}</div>
          <h3 className="mt-3 text-xl font-semibold text-white">{title}</h3>
          <p className="mt-2 text-sm leading-6 text-slate-400">{summary}</p>
        </div>
        <div className="workflow-icon">{icon}</div>
      </div>
      <div className="mt-4 inline-flex rounded-full border border-slate-700/50 bg-slate-900/80 px-3 py-1 text-xs font-semibold uppercase tracking-[0.14em] text-slate-300">
        {status}
      </div>
      <div className="workflow-content">{children}</div>
    </article>
  )
}

function PremiumBadge({ label = 'Premium' }) {
  return (
    <span className="inline-flex items-center gap-1 rounded-full border border-amber-400/30 bg-amber-500/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-amber-200">
      <Lock className="h-3 w-3" /> {label}
    </span>
  )
}

function LockedFeatureNotice({ title, description }) {
  return (
    <div className="rounded-2xl border border-amber-400/30 bg-amber-500/10 p-4">
      <div className="flex items-start gap-3">
        <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-amber-500/15 text-amber-300">
          <Lock className="h-4 w-4" />
        </div>
        <div>
          <p className="text-sm font-semibold text-amber-100">{title}</p>
          <p className="mt-1 text-xs leading-5 text-amber-200/80">{description}</p>
        </div>
      </div>
    </div>
  )
}

function CheckboxField({ label, checked, onChange, disabled = false, badge = '' }) {
  return (
    <label className={`flex items-center gap-3 rounded-2xl border border-white/5 bg-white/[0.04] px-4 py-3 text-sm font-medium text-slate-300 transition ${disabled ? 'cursor-not-allowed opacity-70' : 'cursor-pointer hover:bg-white/[0.07]'}`}>
      <input
        className="h-4 w-4 rounded border-slate-600 bg-slate-800 text-teal-500 focus:ring-teal-500"
        type="checkbox"
        checked={checked}
        disabled={disabled}
        onChange={(e) => onChange(e.target.checked)}
      />
      <span className="flex items-center gap-2">
        <span>{label}</span>
        {badge ? <PremiumBadge label={badge} /> : null}
      </span>
    </label>
  )
}

export default App
