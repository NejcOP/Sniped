import confetti from 'canvas-confetti'
import axios from 'axios'
import {
  Activity,
  AlertTriangle,
  Ban,
  Bell,
  Briefcase,
  Building2,
  CheckCircle2,
  ChevronDown,
  Clipboard,
  Copy,
  Database,
  DollarSign,
  Download,
  ExternalLink,
  Eye,
  EyeOff,
  Facebook,
  Globe,
  Info,
  Instagram,
  LayoutDashboard,
  Linkedin,
  Lock,
  Mail,
  MapPin,
  MessageCircle,
  Phone,
  PlusCircle,
  RefreshCw,
  Reply,
  RotateCcw,
  Save,
  Search,
  Send,
  Settings,
  Shield,
  UserCheck,
  UserX,
  KeyRound,
  LogIn,
  Sparkles,
  Rocket,
  Star,
  Twitter,
  Youtube,
  Target,
  TerminalSquare,
  Trash2,
  GripVertical,
  TrendingUp,
  User,
  Users,
  Zap,
} from 'lucide-react'
import { DndContext, KeyboardSensor, PointerSensor, closestCenter, useDraggable, useDroppable, useSensor, useSensors } from '@dnd-kit/core'
import { SortableContext, arrayMove, sortableKeyboardCoordinates, useSortable, verticalListSortingStrategy } from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'
import { AnimatePresence, motion as Motion } from 'framer-motion'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import toast, { Toaster } from 'react-hot-toast'
import Footer from './Footer'
import { clearUserSession, getStoredValue } from './authStorage'
import { appToasterProps } from './toastTheme'
// ── Performance modules ────────────────────────────────────────────────────
import { useDebounce } from './hooks/useDebounce'
import { invalidateLeadsCache } from './hooks/useLeadsCache'
import { LeadCardSkeletonList, StatCardSkeletonList } from './components/SkeletonLoaders'
import { ScrapeProgressBar, ScrapeProgressBadge } from './components/ScrapeProgressBar'
import OnboardingWizard from './components/OnboardingWizard'
import { snipedEmailTemplates } from './sniped-email-templates'

const MRR_GOAL_EUR = 16000
const SETUP_MILESTONE_EUR = 6500
const DEFAULT_AVERAGE_DEAL_VALUE = 1000
const LEADS_PAGE_SIZE = 50
const SCRAPE_CREDIT_COST_PER_LEAD = 1
const ENRICH_CREDIT_COST_PER_LEAD = 2
const LOW_CREDITS_THRESHOLD = 20
const ONBOARDING_COMPLETED_KEY = 'lf_onboarding_completed_v1'
const ONBOARDING_DISMISSED_KEY = 'lf_onboarding_dismissed_v1'
const ENRICH_TASK_SNAPSHOT_KEY = 'lf_enrich_task_snapshot_v1'
const ENRICH_TASK_SNAPSHOT_TTL_MS = 30 * 60 * 1000
const CREDITS_SWR_CACHE_KEY = 'lf_credits_swr_cache_v1'
const SCRAPE_ACTIVE_TASK_ID_KEY = 'lf_active_scrape_task_id_v1'
const BYPASS_LEAD_FILTERS = false
const LEAD_QUICK_FILTER_VALUES = new Set(['all', 'qualified', 'not_qualified', 'mailed', 'opened', 'replied'])
const LEADS_QUERY_STALE_TIME_MS = 0
const LEADS_QUERY_GC_TIME_MS = 10 * 60_000
const PROFILE_QUERY_BASE_KEY = 'user-profile'
const USER_CREDITS_QUERY_BASE_KEY = 'user-credits'

const AI_QUICK_FILTERS = [
  { key: 'high_priority', label: '🚀 High Priority', prompt: 'Show high priority leads with score above 8' },
  { key: 'website_fix', label: '🛠️ Website Fix', prompt: 'Show leads with weak SEO, no HTTPS, or slow loading website' },
  { key: 'social_gaps', label: '📱 Social Gaps', prompt: 'Show leads with no socials or missing Instagram/LinkedIn' },
]

const QUALIFIER_LOSS_MULTIPLIER_RULES = [
  { terms: ['dentist', 'dental', 'orthodont'], multiplier: 1.55 },
  { terms: ['lawyer', 'legal', 'attorney'], multiplier: 1.65 },
  { terms: ['clinic', 'medical', 'medspa', 'dermatology', 'surgery'], multiplier: 1.75 },
  { terms: ['plumber', 'hvac', 'roof', 'electrician'], multiplier: 1.35 },
  { terms: ['restaurant', 'hotel', 'salon', 'spa', 'gym', 'fitness'], multiplier: 1.2 },
]

const QUALIFIER_FINDING_MODELS = [
  {
    key: 'no_website',
    countKeys: ['no_website', 'ghost'],
    listKeys: ['no_website', 'ghost'],
    finding: 'missing website foundation',
    perLeadLoss: 900,
  },
  {
    key: 'traffic_opportunity',
    countKeys: ['traffic_opportunity', 'invisible_local'],
    listKeys: ['traffic_opportunity', 'invisible_local'],
    finding: 'untapped traffic opportunity',
    perLeadLoss: 700,
  },
  {
    key: 'competitor_gap',
    countKeys: ['competitor_gap', 'invisible_giant'],
    listKeys: ['competitor_gap', 'invisible_giant'],
    finding: 'competitor ranking pressure',
    perLeadLoss: 650,
  },
  {
    key: 'site_speed',
    countKeys: ['site_speed', 'tech_debt', 'low_authority'],
    listKeys: ['site_speed', 'tech_debt', 'low_authority'],
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
const TAB_QUERY_KEYS = new Set(['leads', 'blacklist', 'workers', 'tasks', 'history', 'mail', 'qualify', 'export', 'clients', 'config', 'admin'])

function normalizeTabParam(raw, fallback = 'leads') {
  const tab = String(raw || '').toLowerCase().trim()
  if (tab === 'active') return 'tasks'
  if (tab === 'delivery') return 'tasks'
  if (tab === 'task') return 'tasks'
  if (tab === 'history') return 'tasks'
  if (TAB_QUERY_KEYS.has(tab)) return tab
  return fallback
}

function normalizeLeadQuickFilterParam(raw, fallback = 'all') {
  const value = String(raw || '').trim().toLowerCase()
  if (!value) return fallback
  return LEAD_QUICK_FILTER_VALUES.has(value) ? value : fallback
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
  { tab: 'admin', label: 'ADMIN CENTER', icon: Shield },
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
  { id: 'credits_3000', credits: 3000, priceUsd: 59.00, badge: '' },
  { id: 'credits_5000', credits: 5000, priceUsd: 99.00, badge: 'MOST POPULAR' },
  { id: 'credits_10000', credits: 10000, priceUsd: 169.00, badge: 'BEST VALUE' },
  { id: 'credits_25000', credits: 25000, priceUsd: 349.00, badge: '' },
  { id: 'credits_50000', credits: 50000, priceUsd: 699.00, badge: '' },
  { id: 'credits_100000', credits: 100000, priceUsd: 1119.00, badge: '' },
  { id: 'credits_250000', credits: 250000, priceUsd: 2119.00, badge: '' },
  { id: 'credits_500000', credits: 500000, priceUsd: 3499.00, badge: '' },
]
function compactPkgCredits(n) {
  const v = Number(n || 0)
  if (v >= 1000000) return `${(v / 1000000).toFixed(2).replace(/\.?0+$/, '')}M`
  if (v >= 1000) return `${Math.round(v / 1000)}k`
  return String(v)
}
const TOP_UP_PACKAGE_OPTIONS = TOP_UP_PACKAGES.map((pkg) => ({
  ...pkg,
  label: `${compactPkgCredits(pkg.credits)} Credits - $${Number(pkg.priceUsd || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
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

const creditIntegerFormatter = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 0,
})

function formatCreditAmount(value, options = {}) {
  const {
    thousandThreshold = 1000,
    thousandDecimals = 0,
    thousandMode = 'round',
    millionThreshold = 1000000,
    millionDecimals = 2,
    millionMode = 'round',
  } = options
  const numericValue = Number(value || 0)
  if (!Number.isFinite(numericValue)) return '0'

  const applyMode = (rawNumber, decimals, mode) => {
    const safeDecimals = Math.max(0, Number(decimals || 0))
    const factor = 10 ** safeDecimals
    if (mode === 'floor') {
      const scaled = rawNumber * factor
      const floored = rawNumber >= 0 ? Math.floor(scaled) : Math.ceil(scaled)
      return floored / factor
    }
    return Number(rawNumber.toFixed(safeDecimals))
  }

  const absValue = Math.abs(numericValue)
  if (absValue >= millionThreshold) {
    const compactMillions = applyMode(numericValue / 1000000, millionDecimals, millionMode)
    return `${compactMillions.toLocaleString('en-US', { minimumFractionDigits: Math.max(0, millionDecimals), maximumFractionDigits: Math.max(0, millionDecimals) })}M`
  }

  if (absValue >= thousandThreshold) {
    const compactThousands = applyMode(numericValue / 1000, thousandDecimals, thousandMode)
    return `${compactThousands.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: Math.max(0, thousandDecimals) })}k`
  }

  return creditIntegerFormatter.format(Math.round(numericValue))
}

const SidebarBillingCard = memo(function SidebarBillingCard({
  isPaid,
  planName,
  isLoading = false,
  cancelPending = false,
  cancelUntilLabel = '',
  onUpgrade,
  onChangeSubscription,
}) {
  const resolvedPlanName = String(planName || 'Free Plan').trim() || 'Free Plan'
  const statusText = isPaid ? 'Subscription active' : 'You are currently on the free tier'
  const actionLabel = isPaid ? 'Change Plans' : 'Upgrade Plan'

  if (isLoading) {
    return (
      <div className="rounded-2xl border border-slate-700/70 bg-[linear-gradient(180deg,#0D1117_0%,#0B1220_100%)] p-3.5 shadow-[0_14px_30px_rgba(2,6,23,0.45)]">
        <div className="space-y-3 animate-pulse">
          <div className="space-y-1.5">
            <div className="h-3 w-24 rounded bg-slate-700/70" />
            <div className="rounded-xl border border-[#FFC107]/15 bg-[#111827]/80 px-3 py-2.5 space-y-2">
              <div className="h-4 w-28 rounded bg-slate-700/70" />
              <div className="h-3 w-40 rounded bg-slate-700/60" />
            </div>
          </div>
          <div className="h-9 w-full rounded-xl bg-slate-700/70" />
        </div>
      </div>
    )
  }

  return (
    <div className="rounded-2xl border border-slate-700/70 bg-[linear-gradient(180deg,#0D1117_0%,#0B1220_100%)] p-3.5 shadow-[0_14px_30px_rgba(2,6,23,0.45)]">
      <div className="space-y-3">
        <div className="space-y-1.5">
          <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-400">Current Plan</p>
          <div className="rounded-xl border border-[#FFC107]/15 bg-[#111827]/80 px-3 py-2.5">
            <p className="text-[1rem] font-semibold leading-tight text-[#FFC107]">{resolvedPlanName}</p>
            <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px] leading-relaxed">
              <span className={cancelPending ? 'font-semibold text-amber-200' : 'text-slate-400'}>{statusText}</span>
              {cancelPending && cancelUntilLabel && (
                <span className="text-slate-400">scheduled to end on {cancelUntilLabel}</span>
              )}
            </div>
          </div>
        </div>

        <button
          type="button"
          className="inline-flex w-full items-center justify-center gap-1.5 rounded-xl border border-[#FFC107]/80 bg-gradient-to-r from-[#d9a406] to-[#FFC107] px-3 py-2 text-xs font-semibold text-[#0a1422] shadow-[0_8px_20px_rgba(255,193,7,0.28)] transition-all duration-200 hover:brightness-105"
          onClick={isPaid ? onChangeSubscription : onUpgrade}
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
  billingLoading,
  cancelPending,
  cancelUntilLabel,
  creditsBalanceLabel,
  creditsLimitLabel,
  creditsPercent,
  creditsLabelClass,
  creditsLoading,
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
        isLoading={billingLoading}
        cancelPending={cancelPending}
        cancelUntilLabel={cancelUntilLabel}
        onUpgrade={onUpgrade}
        onChangeSubscription={onChangeSubscription}
      />

      <div className="rounded-xl border border-slate-700/70 bg-[#0D1117] p-3 shadow-[0_14px_30px_rgba(2,6,23,0.45)]">
        <div className="mb-2 pb-1 text-center">
          <p className="inline-flex items-baseline justify-center gap-1.5 whitespace-nowrap text-sm font-semibold text-white">
            <span>Credits&nbsp;</span>
            {creditsLoading ? (
              <span className="inline-flex items-center gap-1.5">
                <span className="h-[1.05rem] w-14 rounded-md bg-slate-700/70 animate-pulse" />
                <span className="h-[0.95rem] w-2 rounded-sm bg-slate-700/60 animate-pulse" />
                <span className="h-[0.85rem] w-12 rounded-md bg-slate-700/70 animate-pulse" />
              </span>
            ) : (
              <span className={`inline-flex items-baseline gap-1 ${creditsLabelClass}`}>
                <span className="text-[1.05rem] font-semibold text-yellow-200">
                  {creditsBalanceLabel}
                </span>
                <span className="text-[0.95rem] text-slate-500">/</span>
                <span className="text-[0.85rem] font-semibold text-slate-400">
                  {creditsLimitLabel}
                </span>
              </span>
            )}
          </p>
        </div>
        <div className="mt-3 h-2 w-full overflow-hidden rounded-xl bg-slate-700/70">
          {creditsLoading ? (
            <div className="h-full w-full rounded-xl bg-slate-600/70 animate-pulse" />
          ) : (
            <div
              className="h-full rounded-xl bg-gradient-to-r from-[#d9a406] to-[#FFC107] transition-[width] duration-500 ease-out"
              style={{ width: `${creditsPercent}%` }}
            />
          )}
        </div>
        <div className="mt-2 flex items-center gap-2 text-[11px] text-slate-400">
          <span className="h-1.5 w-1.5 rounded-full bg-slate-500" />
          {creditsLoading ? <span className="h-[11px] w-40 rounded-md bg-slate-700/70 animate-pulse" /> : resetLabel}
        </div>
        {creditsLoading ? (
          <div className="mt-1 flex items-center gap-2 text-[11px] text-[#FFE082]">
            <span className="h-1.5 w-1.5 rounded-full bg-[#FFC107]" />
            <span className="h-[11px] w-36 rounded-md bg-slate-700/70 animate-pulse" />
          </div>
        ) : topupLabel ? (
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
                      {pkg.label || `${compactPkgCredits(pkg.credits)} Credits - $${formatUsd(pkg.priceUsd)}`}
                    </option>
                  ))}
                </select>
                <ChevronDown className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[#FFC107]" />
              </div>

              <div className="mt-4 rounded-xl border border-[#FFC107]/25 bg-[#111827] p-3">
                <div className="flex items-center justify-between text-sm">
                  <span className="text-slate-300">Selected package</span>
                  <span className="font-semibold text-[#FFE082]">
                    {compactPkgCredits(selectedPackage?.credits)} credits
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

const defaultScrape = { keyword: '', results: 25, country: 'US', headless: true, exportTargets: true, speedMode: false }
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

const SNIPED_TEMPLATE_KEY_TO_GAP = {
  ghost: 'No Website',
  golden: 'Traffic Opportunity',
  competitor: 'Competitor Gap',
  speed: 'Site Speed',
}

const SNIPED_GAP_TO_CONFIG_KEYS = {
  'No Website': { subjectKey: 'ghost_subject_template', bodyKey: 'ghost_body_template' },
  'Traffic Opportunity': { subjectKey: 'golden_subject_template', bodyKey: 'golden_body_template' },
  'Competitor Gap': { subjectKey: 'competitor_subject_template', bodyKey: 'competitor_body_template' },
  'Site Speed': { subjectKey: 'speed_subject_template', bodyKey: 'speed_body_template' },
}

function mapQualifierGapToTemplateKey(gap) {
  const normalized = String(gap || '').trim().toLowerCase()
  if (normalized === 'no_website') return 'ghost'
  if (normalized === 'traffic_opportunity') return 'golden'
  if (normalized === 'competitor_gap') return 'competitor'
  if (normalized === 'site_speed') return 'speed'
  return 'ghost'
}

const TEMPLATE_NICHE_ALIAS_RULES = [
  { key: 'Paid Ads Agency', terms: ['paid ads', 'ads agency', 'google ads', 'meta ads', 'ppc'] },
  { key: 'Web Design & Dev', terms: ['web design', 'web dev', 'website', 'design', 'developer', 'dev agency'] },
  { key: 'SEO & Content', terms: ['seo', 'content', 'organic', 'ranking'] },
  { key: 'Lead Gen Agency', terms: ['lead gen', 'lead generation', 'outbound', 'prospecting'] },
  { key: 'B2B Service Provider', terms: ['b2b', 'service provider', 'consulting', 'agency services'] },
]

function resolveTemplateNicheKey(rawNiche) {
  const raw = String(rawNiche || '').trim()
  const normalized = raw.toLowerCase()
  if (!normalized) return 'Web Design & Dev'

  const direct = Object.keys(snipedEmailTemplates).find((key) => key.toLowerCase() === normalized)
  if (direct) return direct

  const viaTerms = TEMPLATE_NICHE_ALIAS_RULES.find((rule) => rule.terms.some((term) => normalized.includes(term)))
  if (viaTerms) return viaTerms.key

  return 'Web Design & Dev'
}

function resolveSnipedTemplateForSelection(rawNiche, templateKey) {
  const niche = resolveTemplateNicheKey(rawNiche)
  const gap = SNIPED_TEMPLATE_KEY_TO_GAP[String(templateKey || '').trim()] || 'No Website'
  const nicheTemplates = snipedEmailTemplates[niche]
  const list = Array.isArray(nicheTemplates?.templates) ? nicheTemplates.templates : []
  return list.find((item) => String(item?.gap || '').trim() === gap) || null
}

function buildSnipedTemplateSeedPayload() {
  const slug = (value) => String(value || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '')
  const items = []
  for (const [niche, nicheData] of Object.entries(snipedEmailTemplates || {})) {
    const templates = Array.isArray(nicheData?.templates) ? nicheData.templates : []
    const nicheCategory = `manus_${slug(niche) || 'general'}`
    for (const template of templates) {
      const gap = String(template?.gap || '').trim() || 'No Website'
      const subject = String(template?.subject || '').trim()
      const body = String(template?.body || '').trim()
      const followup = String(template?.followup || '').trim()
      if (subject && body) {
        items.push({
          name: `${niche} - ${gap} - Live`,
          category: nicheCategory,
          prompt_text: `${niche} / ${gap} / initial`,
          subject_template: subject,
          body_template: body,
        })
      }
      if (followup) {
        items.push({
          name: `${niche} - ${gap} - Follow-up`,
          category: nicheCategory,
          prompt_text: `${niche} / ${gap} / followup`,
          subject_template: `Follow-up: ${subject || gap}`,
          body_template: followup,
        })
      }
    }
  }
  return items
}

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
  const niche = resolveTemplateNicheKey(rawNiche)
  const nicheMeta = liveMailTemplateCardMetaByNiche[niche]
  if (!nicheMeta) return liveMailTemplateCards
  return liveMailTemplateCards.map((card) => ({
    ...card,
    ...(nicheMeta[card.key] || {}),
  }))
}

function replaceTemplatePlaceholders(text, vars) {
  const payload = String(text || '')
  const map = {
    '{BusinessName}': String(vars?.BusinessName || ''),
    '{City}': String(vars?.City || ''),
    '{Niche}': String(vars?.Niche || ''),
    '{YourName}': String(vars?.YourName || ''),
  }
  return Object.entries(map).reduce((acc, [token, value]) => acc.split(token).join(value), payload)
}

function readStoredNumber(...candidates) {
  for (const candidate of candidates) {
    const raw = String(candidate ?? '').trim()
    if (!raw) continue
    const value = Number(raw)
    if (Number.isFinite(value)) return value
  }
  return undefined
}

function readCreditsSwrCache() {
  try {
    if (typeof window === 'undefined') return null
    const raw = window.localStorage.getItem(CREDITS_SWR_CACHE_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw)
    if (!parsed || typeof parsed !== 'object') return null
    return parsed
  } catch {
    return null
  }
}

function writeCreditsSwrCache(cache) {
  try {
    if (typeof window === 'undefined') return
    window.localStorage.setItem(CREDITS_SWR_CACHE_KEY, JSON.stringify(cache))
  } catch {
    // Ignore storage write failures.
  }
}

function getIdleTask(taskType) {
  return { id: null, task_type: taskType, status: 'idle', running: false, created_at: null, started_at: null, finished_at: null, last_request: null, result: null, error: null }
}

function readEnrichTaskSnapshot() {
  try {
    if (typeof window === 'undefined') return null
    const raw = window.localStorage.getItem(ENRICH_TASK_SNAPSHOT_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw)
    if (!parsed || typeof parsed !== 'object') return null
    return parsed
  } catch {
    return null
  }
}

function writeEnrichTaskSnapshot(snapshot) {
  try {
    if (typeof window === 'undefined') return
    window.localStorage.setItem(ENRICH_TASK_SNAPSHOT_KEY, JSON.stringify(snapshot))
  } catch {
    // Ignore storage failures.
  }
}

function clearEnrichTaskSnapshot() {
  try {
    if (typeof window === 'undefined') return
    window.localStorage.removeItem(ENRICH_TASK_SNAPSHOT_KEY)
  } catch {
    // Ignore storage failures.
  }
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

const REQUEST_ABORT_REGISTRY = new Map()
const RESPONSE_CACHE_REGISTRY = new Map()
let AUTH_REDIRECT_IN_PROGRESS = false
const ENDPOINT_CACHE_RULES = [
  { prefix: '/api/config', ttlMs: 45000 },
  { prefix: '/api/workers', ttlMs: 12000 },
]

function buildLoginRedirectPath() {
  try {
    if (typeof window === 'undefined') return '/login'
    const rawSearch = new URLSearchParams(window.location.search || '')
    const activeTab = String(rawSearch.get('tab') || '').trim().toLowerCase()
    const cleanAppTarget = activeTab ? `/app?tab=${encodeURIComponent(activeTab)}` : '/app'
    return `/login?redirect=${encodeURIComponent(cleanAppTarget)}`
  } catch {
    return '/login'
  }
}

async function forceLogoutToLogin(reason = 'auth_failed') {
  void reason
  if (AUTH_REDIRECT_IN_PROGRESS) return
  AUTH_REDIRECT_IN_PROGRESS = true

  try {
    if (typeof window !== 'undefined' && window?.supabase?.auth?.signOut) {
      await window.supabase.auth.signOut()
    }
  } catch {
    // Ignore Supabase sign-out failures; local session cleanup still runs.
  }

  try {
    clearUserSession()
    localStorage.removeItem('lf_credits_swr_cache_v1')
  } catch {
    // Ignore browser storage errors.
  }

  try {
    if (typeof window !== 'undefined') {
      const nextUrl = buildLoginRedirectPath()
      if (!window.location.pathname.startsWith('/login')) {
        window.location.replace(nextUrl)
      }
    }
  } catch {
    // Ignore redirect edge-cases.
  }
}

function isAuthInvalidError(error) {
  const status = Number(error?.status || 0)
  if (status === 401) return true
  const detail = String(error?.message || error?.detail || '').toLowerCase()
  return detail.includes('authentication required')
    || detail.includes('invalid or expired session token')
    || detail.includes('authenticated user does not exist')
    || detail.includes('profile')
}

if (!axios.__LF_AUTH_INTERCEPTOR_ATTACHED__) {
  axios.__LF_AUTH_INTERCEPTOR_ATTACHED__ = true
  axios.interceptors.response.use(
    (response) => response,
    async (error) => {
      const status = Number(error?.response?.status || 0)
      if (status === 401) {
        await forceLogoutToLogin('axios_401')
      }
      return Promise.reject(error)
    },
  )
}

function getEndpointCacheTtl(pathname) {
  const normalized = String(pathname || '').toLowerCase()
  const rule = ENDPOINT_CACHE_RULES.find((entry) => normalized.startsWith(entry.prefix))
  return Number(rule?.ttlMs || 0)
}

function getAbortGroupForRequest(pathname, method, explicitAbortKey) {
  if (explicitAbortKey) return String(explicitAbortKey)
  if (String(method || '').toUpperCase() !== 'GET') return ''
  const normalized = String(pathname || '').toLowerCase()
  if (normalized.startsWith('/api/leads/qualify')) return 'qualifier-refresh'
  if (normalized.startsWith('/api/leads')) return 'leads-list'
  if (normalized.startsWith('/api/workers')) return 'workers-list'
  if (normalized.startsWith('/api/config')) return 'config-load'
  if (normalized.startsWith('/api/scrape')) return 'scrape-list'
  return ''
}

function invalidateResponseCacheByPrefix(pathPrefix) {
  const normalizedPrefix = String(pathPrefix || '').toLowerCase()
  if (!normalizedPrefix) return
  for (const key of Array.from(RESPONSE_CACHE_REGISTRY.keys())) {
    if (key.toLowerCase().includes(`:${normalizedPrefix}`)) {
      RESPONSE_CACHE_REGISTRY.delete(key)
    }
  }
}

function abortRequestGroup(groupKey) {
  const key = String(groupKey || '').trim()
  if (!key) return
  const controller = REQUEST_ABORT_REGISTRY.get(key)
  if (controller) {
    controller.abort()
    REQUEST_ABORT_REGISTRY.delete(key)
  }
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
  const abortKey = String(options?.abortKey || '').trim()
  const bypassCache = Boolean(options?.bypassCache)
  const timeoutMs = Math.max(0, Number(options?.timeoutMs || 0))
  const externalSignal = options?.signal
  const requestOptions = { ...(options || {}) }
  delete requestOptions.abortKey
  delete requestOptions.bypassCache
  delete requestOptions.timeoutMs
  const isDynamicPollingEndpoint =
    method === 'GET' && (
      normalizedPath === '/api/tasks'
      || normalizedPath.startsWith('/api/tasks/')
      || normalizedPath === '/api/task'
      || normalizedPath === '/api/stats'
    )

  const pathnameOnly = normalizedPath.split('?')[0] || normalizedPath
  const cacheTtlMs = !isDynamicPollingEndpoint && method === 'GET' && !bypassCache
    ? getEndpointCacheTtl(pathnameOnly)
    : 0
  const responseCacheKey = `${method}:${normalizedPath}`
  if (cacheTtlMs > 0) {
    const cached = RESPONSE_CACHE_REGISTRY.get(responseCacheKey)
    if (cached && (Date.now() - Number(cached.ts || 0)) < cacheTtlMs) {
      return cached.data
    }
  }

  const controller = new AbortController()
  const abortGroup = getAbortGroupForRequest(pathnameOnly, method, abortKey)
  if (abortGroup) {
    abortRequestGroup(abortGroup)
    REQUEST_ABORT_REGISTRY.set(abortGroup, controller)
  }
  if (externalSignal) {
    if (externalSignal.aborted) {
      controller.abort()
    } else {
      externalSignal.addEventListener('abort', () => controller.abort(), { once: true })
    }
  }
  let timeoutHandle = null
  if (timeoutMs > 0) {
    timeoutHandle = window.setTimeout(() => {
      controller.abort()
    }, timeoutMs)
  }

  try {
    const response = await fetch(requestUrl, {
      ...requestOptions,
      headers,
      signal: controller.signal,
      ...(isDynamicPollingEndpoint ? { cache: 'no-store' } : {}),
    })
    const data = await response.json().catch(() => ({}))
    if (!response.ok) {
      const detail = typeof data.detail === 'string' ? data.detail : `Request failed (${response.status})`
      const error = new Error(detail)
      error.status = response.status
      error.path = requestUrl
      if (Number(response.status || 0) === 401) {
        await forceLogoutToLogin('fetch_401')
      }
      throw error
    }

    if (cacheTtlMs > 0) {
      RESPONSE_CACHE_REGISTRY.set(responseCacheKey, { ts: Date.now(), data })
    }

    if (method !== 'GET') {
      if (pathnameOnly.startsWith('/api/config')) invalidateResponseCacheByPrefix('/api/config')
      if (pathnameOnly.startsWith('/api/workers')) invalidateResponseCacheByPrefix('/api/workers')
      if (pathnameOnly.startsWith('/api/leads')) invalidateResponseCacheByPrefix('/api/leads')
    }
    return data
  } finally {
    if (timeoutHandle) window.clearTimeout(timeoutHandle)
    if (abortGroup && REQUEST_ABORT_REGISTRY.get(abortGroup) === controller) {
      REQUEST_ABORT_REGISTRY.delete(abortGroup)
    }
  }
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
    || endpoint.includes('/api/ai/market-intelligence')
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

function formatCommunicationTime(raw) {
  if (!raw) return 'Just now'
  const date = new Date(raw)
  if (Number.isNaN(date.getTime())) return 'Just now'
  return date.toLocaleString()
}

function extractCommunicationBody(item) {
  const plain = String(item?.body_text || '').trim()
  if (plain) return plain
  const html = String(item?.body_html || '')
  if (!html) return ''
  return html
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/\s+/g, ' ')
    .trim()
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

function deriveManusTemplateToneProfile(templateKey) {
  const key = String(templateKey || '').trim().toLowerCase()
  const presets = {
    ghost: { Professional: 90, Helpful: 82, Urgent: 44 },
    golden: { Professional: 91, Helpful: 80, Urgent: 48 },
    competitor: { Professional: 90, Helpful: 78, Urgent: 46 },
    speed: { Professional: 89, Helpful: 84, Urgent: 43 },
  }
  const scores = presets[key]
  if (!scores) return null
  return {
    dominantLabel: 'Professional',
    dominantScore: Number(scores.Professional || 90),
    scores,
  }
}

function renderTemplateWithPlaceholderHighlights(text) {
  const value = String(text || '')
  if (!value) return null
  const placeholderRegex = /(\{BusinessName\}|\{City\}|\{Niche\}|\{YourName\})/g
  return value.split(placeholderRegex).map((part, idx) => {
    const isPlaceholder = templatePlaceholderTokens.includes(part)
    if (!isPlaceholder) {
      return <span key={`preview-plain-${idx}`}>{part}</span>
    }
    return (
      <span
        key={`preview-ph-${idx}`}
        className="rounded bg-cyan-500/15 px-1 py-0.5 font-semibold text-cyan-200 ring-1 ring-cyan-400/35"
      >
        {part}
      </span>
    )
  })
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
  const score = resolveBestLeadScore(lead)
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

function parseLeadEnrichmentData(lead) {
  const raw = lead?.enrichment_data ?? lead?.enrichmentData ?? null
  if (!raw) return {}
  if (typeof raw === 'object' && !Array.isArray(raw)) return raw
  if (typeof raw === 'string') {
    try {
      const parsed = JSON.parse(raw)
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) return parsed
    } catch {
      return {}
    }
  }
  return {}
}

function resolveLeadScoreBreakdown(lead) {
  const payload = parseLeadEnrichmentData(lead)
  const rows = Array.isArray(payload?.score_breakdown) ? payload.score_breakdown : []
  return rows
    .map((item) => {
      const impact = Number(item?.impact ?? 0)
      const label = String(item?.label || '').trim()
      const detail = String(item?.detail || '').trim()
      if (!Number.isFinite(impact) || !label) return null
      return {
        impact: Math.round(impact * 10) / 10,
        type: impact >= 0 ? 'positive' : 'negative',
        label,
        detail,
      }
    })
    .filter(Boolean)
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

function PipelineDropColumn({ stage, children }) {
  const { setNodeRef, isOver } = useDroppable({ id: stage })
  return (
    <section
      ref={setNodeRef}
      className={`min-h-[240px] rounded-2xl border p-3 transition ${isOver ? 'border-cyan-400/70 bg-cyan-500/10' : 'border-slate-700/50 bg-slate-900/70'}`}
    >
      {children}
    </section>
  )
}

function PipelineLeadCard({ lead, onOpenDetails, pendingStatusLeadId }) {
  const draggableId = `pipeline-lead-${lead.id}`
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useDraggable({
    id: draggableId,
    data: { leadId: Number(lead.id) },
  })
  const style = {
    transform: CSS.Translate.toString(transform),
    transition,
  }
  return (
    <article
      ref={setNodeRef}
      style={style}
      className={`rounded-xl border border-white/10 bg-slate-950/70 p-3 ${isDragging ? 'opacity-80 shadow-[0_16px_34px_rgba(6,182,212,0.22)]' : ''}`}
    >
      <div className="flex items-start justify-between gap-2">
        <button
          type="button"
          className="min-w-0 text-left"
          onClick={() => onOpenDetails(lead)}
        >
          <p className="truncate text-sm font-semibold text-white">{lead.business_name || 'Unknown business'}</p>
          <p className="truncate text-xs text-slate-400">{lead.email || lead.contact_name || 'No email yet'}</p>
        </button>
        <button
          type="button"
          className="inline-flex h-7 w-7 items-center justify-center rounded-lg border border-slate-600/70 bg-slate-900/80 text-slate-300"
          title="Drag to another stage"
          {...listeners}
          {...attributes}
        >
          <GripVertical className="h-4 w-4" />
        </button>
      </div>
      <div className="mt-2 flex items-center gap-2 text-[11px] text-slate-400">
        <span className="inline-flex items-center rounded-full border border-cyan-500/30 bg-cyan-500/10 px-2 py-0.5 font-semibold text-cyan-200">
          Score {formatLeadScoreValue(resolveBestLeadScore(lead))}/10
        </span>
        {pendingStatusLeadId === lead.id ? (
          <span className="inline-flex items-center gap-1 text-cyan-300"><RefreshCw className="h-3 w-3 animate-spin" /> Saving</span>
        ) : null}
      </div>
    </article>
  )
}

function normalizeLeadScoreTen(rawScore) {
  const numeric = Number(rawScore || 0)
  if (!Number.isFinite(numeric) || numeric <= 0) return 0
  const normalized = numeric > 10 ? numeric / 10 : numeric
  return Math.max(0, Math.min(10, Math.round(normalized * 10) / 10))
}

function normalizeLeadTechSignals(lead) {
  const signals = []
  const rawTech = lead?.tech_stack
  if (Array.isArray(rawTech)) {
    signals.push(...rawTech)
  } else if (typeof rawTech === 'string') {
    signals.push(...rawTech.split(/[\n,;|]/))
  }

  const websiteSignals = lead?.website_signals && typeof lead.website_signals === 'object' ? lead.website_signals : {}
  if (websiteSignals.cms) signals.push(websiteSignals.cms)
  if (websiteSignals.platform) signals.push(websiteSignals.platform)

  const audit = lead?.company_audit && typeof lead.company_audit === 'object' ? lead.company_audit : {}
  if (Array.isArray(audit.tech_stack)) signals.push(...audit.tech_stack)

  return signals
    .map((item) => String(item || '').trim().toLowerCase())
    .filter(Boolean)
}

function deriveLeadTechScoreAdjustment(lead) {
  const websiteUrl = String(lead?.website_url || lead?.website || '').trim().toLowerCase()
  const hasWebsite = Boolean(websiteUrl)
  const techSignals = normalizeLeadTechSignals(lead)

  const modernIndicators = [
    'shopify', 'wordpress', 'webflow', 'next.js', 'react', 'gatsby', 'wix', 'squarespace',
    'hubspot', 'ga4', 'google analytics', 'google tag manager', 'gtm', 'meta pixel',
  ]
  const datedIndicators = ['joomla', 'drupal', 'flash', 'magento 1', 'legacy', 'outdated']

  const modernHits = modernIndicators.reduce((acc, token) => (techSignals.some((signal) => signal.includes(token)) ? acc + 1 : acc), 0)
  const datedHits = datedIndicators.reduce((acc, token) => (techSignals.some((signal) => signal.includes(token)) ? acc + 1 : acc), 0)

  let adjustment = hasWebsite ? 0.4 : -2.2
  adjustment += Math.min(1.6, modernHits * 0.35)
  adjustment -= Math.min(1.8, datedHits * 0.45)

  if (!lead?.email) adjustment -= 0.35
  return Math.max(-3, Math.min(2.2, adjustment))
}

function formatLeadScoreValue(rawScore) {
  const normalized = normalizeLeadScoreTen(rawScore)
  if (normalized <= 0) return '0'
  return Number.isInteger(normalized) ? String(normalized) : normalized.toFixed(1)
}

function resolveBestLeadScore(lead) {
  const techAdjustment = deriveLeadTechScoreAdjustment(lead)

  const aiScore = Number(lead?.ai_score ?? 0)
  if (Number.isFinite(aiScore) && aiScore > 0) {
    return normalizeLeadScoreTen(aiScore + techAdjustment)
  }

  const directScore = Number(lead?.best_lead_score ?? lead?.lead_score_100 ?? lead?.score_100 ?? 0)
  if (Number.isFinite(directScore) && directScore > 0) {
    return normalizeLeadScoreTen(directScore + techAdjustment)
  }

  const aiSentiment = Number(lead?.ai_sentiment_score ?? (aiScore <= 10 ? aiScore * 10 : aiScore) ?? 0)
  const employeeCount = Number(lead?.employee_count ?? 0)
  const emailComponent = lead?.email ? 40 : 8
  const sizeComponent = employeeCount >= 100 ? 30 : employeeCount >= 40 ? 26 : employeeCount >= 15 ? 22 : employeeCount >= 5 ? 16 : 10
  const fallbackScore = Math.min(100, emailComponent + sizeComponent + Math.max(0, Math.min(100, aiSentiment)) * 0.3)
  return normalizeLeadScoreTen(fallbackScore + (techAdjustment * 10))
}

function resolveLeadSignalScore(lead) {
  const rawSignal = Number(lead?.ai_sentiment_score ?? lead?.lead_score_100 ?? lead?.score_100 ?? 0)
  if (Number.isFinite(rawSignal) && rawSignal > 0) {
    return normalizeLeadScoreTen(rawSignal)
  }
  return resolveBestLeadScore(lead)
}

function resolveLeadTrendPoints(lead) {
  const raw = Array.isArray(lead?.score_trend_points) ? lead.score_trend_points : []
  const points = raw
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value) && value >= 0)
  if (points.length >= 2) return points.slice(-8)

  const fallback = Number(resolveBestLeadScore(lead) || 0)
  if (fallback <= 0) return []
  return [Math.max(0, fallback - 0.4), fallback]
}

function buildTrendSparklinePath(points, width = 54, height = 15) {
  const safe = Array.isArray(points) ? points.filter((value) => Number.isFinite(Number(value))) : []
  if (!safe.length) return ''
  const numeric = safe.map((value) => Number(value))
  const min = Math.min(...numeric)
  const max = Math.max(...numeric)
  const span = Math.max(0.01, max - min)
  const step = numeric.length > 1 ? width / (numeric.length - 1) : width
  return numeric
    .map((value, index) => {
      const x = (index * step).toFixed(2)
      const y = (height - ((value - min) / span) * height).toFixed(2)
      return `${index === 0 ? 'M' : 'L'}${x} ${y}`
    })
    .join(' ')
}

function resolveLeadTrendMeta(lead) {
  const points = resolveLeadTrendPoints(lead)
  const first = Number(points[0] || 0)
  const last = Number(points[points.length - 1] || 0)
  const fallbackDelta = Number((last - first).toFixed(1))
  const rawDelta = Number(lead?.score_trend_delta)
  const delta = Number.isFinite(rawDelta) ? rawDelta : fallbackDelta
  const direction = String(lead?.score_trend_direction || '').toLowerCase()
  if (direction === 'up' || direction === 'down' || direction === 'flat') {
    return { points, delta, direction }
  }
  if (delta > 0.2) return { points, delta, direction: 'up' }
  if (delta < -0.2) return { points, delta, direction: 'down' }
  return { points, delta, direction: 'flat' }
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

function resolveLeadTemplateKey(lead) {
  if (!lead) return 'ghost'
  const hasWebsite = Boolean(String(lead?.website_url || '').trim())
  if (!hasWebsite) return 'ghost'

  const speedSignals = [
    Number(lead?.website_performance_score || 0),
    Number(lead?.website_score || 0),
    Number(lead?.page_speed_score || 0),
  ].filter((value) => Number.isFinite(value) && value > 0)
  if (speedSignals.some((value) => value < 60)) return 'speed'

  const leadScore = Number(resolveBestLeadScore(lead) || 0)
  if (leadScore >= 8.5) return 'golden'
  return 'competitor'
}

function resolveLeadCityValue(lead) {
  const direct = String(lead?.city || '').trim()
  if (direct) return direct
  const fromAddress = String(lead?.address || '').split(',')[0]?.trim()
  if (fromAddress) return fromAddress
  const keyword = String(lead?.search_keyword || '').trim()
  const lower = keyword.toLowerCase()
  const marker = lower.lastIndexOf(' in ')
  if (marker >= 0) {
    return keyword.slice(marker + 4).trim() || 'Local Area'
  }
  return 'Local Area'
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

function QualifierLeadCard({ lead, accentClass, badgeClass, onGenerateEmail, onAddToPipeline, onSkip }) {
  const stars = typeof lead.rating === 'number' ? `${lead.rating.toFixed(1)}★` : '—'
  const [hookCopied, setHookCopied] = useState(false)
  const scoreBreakdown = Array.isArray(lead?.score_breakdown) ? lead.score_breakdown : []
  const qualifierScore = Number(lead?.qualifier_score || 0)
  const tierCode = String(lead?.tier_code || '').toLowerCase()
  const tierLabel = String(lead?.tier_label || '').trim() || 'Follow up in 30 days'
  const isBleeding = tierCode === 'bleeding'
  const tierBadgeClass = isBleeding
    ? 'border-rose-400/45 bg-rose-500/20 text-rose-100'
    : tierCode === 'warm'
      ? 'border-amber-400/40 bg-amber-500/20 text-amber-100'
      : 'border-emerald-400/40 bg-emerald-500/20 text-emerald-100'
  const cardClass = isBleeding
    ? `${accentClass} animate-pulse shadow-[0_0_0_1px_rgba(244,63,94,0.35),0_0_36px_rgba(244,63,94,0.2)]`
    : accentClass

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
    <div className={`rounded-2xl border p-4 ${cardClass}`}>
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="flex-1 min-w-0">
          <p className="font-semibold text-white text-sm truncate">{lead.business_name}</p>
          <p className="text-xs text-slate-400 mt-0.5 truncate">{lead.city || lead.address || '—'}</p>
        </div>
        <div className="flex flex-shrink-0 flex-wrap gap-1.5">
          <span className={`rounded-full border px-2 py-0.5 text-[11px] font-semibold ${tierBadgeClass}`}>
            {isBleeding ? '🔴 BLEEDING' : tierCode === 'warm' ? '🟡 WARM' : '🟢 NURTURE'}
          </span>
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
      <div className="mt-3 rounded-xl border border-white/10 bg-white/[0.02] px-3 py-2.5">
        <div className="flex items-center justify-between gap-2 text-xs">
          <p className="font-semibold uppercase tracking-wide text-slate-400">Tier Action</p>
          <p className="font-semibold text-white">{tierLabel}</p>
        </div>
        <p className="mt-1 text-sm text-slate-300">Total score: <strong className="text-white">{qualifierScore}/10</strong></p>
      </div>
      {lead.urgency_signal && (
        <div className="mt-3 rounded-xl border border-rose-400/25 bg-rose-500/10 px-3 py-2.5">
          <p className="text-xs font-semibold uppercase tracking-wide text-rose-300 mb-1">Why Contact NOW</p>
          <p className="text-sm text-rose-100 leading-relaxed">{lead.urgency_signal}</p>
        </div>
      )}
      {scoreBreakdown.length > 0 && (
        <div className="mt-3 overflow-hidden rounded-xl border border-white/10 bg-slate-950/70">
          <div className="border-b border-white/10 px-3 py-2 text-xs font-semibold uppercase tracking-wide text-slate-400">Score Breakdown</div>
          <div className="divide-y divide-white/5">
            {scoreBreakdown.map((entry, index) => (
              <div key={`${lead.id}-score-${index}`} className="grid grid-cols-[1fr_auto] items-center gap-3 px-3 py-2 text-sm">
                <span className="text-slate-300">{entry.signal}</span>
                <span className="font-semibold text-cyan-200">+{entry.points}</span>
              </div>
            ))}
            <div className="grid grid-cols-[1fr_auto] items-center gap-3 px-3 py-2 text-sm font-semibold">
              <span className="text-white">Total</span>
              <span className="text-white">{qualifierScore}/10</span>
            </div>
          </div>
        </div>
      )}
      {lead.pitch_angle && (
        <div className="mt-3 rounded-xl border border-cyan-400/20 bg-cyan-900/10 px-3 py-2.5">
          <p className="text-xs font-semibold uppercase tracking-wide text-cyan-300/80 mb-1">AI Pitch Angle</p>
          <p className="text-sm text-slate-200 leading-relaxed">{lead.pitch_angle}</p>
        </div>
      )}
      {lead.opportunity_pitch && (
        <div className="mt-3 rounded-xl border border-amber-400/20 bg-amber-500/10 px-3 py-2.5">
          <p className="text-xs font-semibold uppercase tracking-wide text-amber-300/90 mb-1">Gold Mine Opportunity</p>
          <p className="text-sm text-amber-100 leading-relaxed">{lead.opportunity_pitch}</p>
        </div>
      )}
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
      <div className="mt-4 grid gap-2 sm:grid-cols-3">
        <button type="button" className="btn-primary justify-center" onClick={() => onGenerateEmail?.(lead)}>
          📧 Generate Email
        </button>
        <button type="button" className="btn-secondary justify-center" onClick={() => onAddToPipeline?.(lead)}>
          ➕ Add to Pipeline
        </button>
        <button type="button" className="btn-ghost justify-center" onClick={() => onSkip?.(lead)}>
          ⏭ Skip
        </button>
      </div>
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
  const queryClient = useQueryClient()
  const sessionToken = getStoredValue('lf_token')
  const hasSessionToken = Boolean(sessionToken)
  const displayName = getStoredValue('lf_display_name') || getStoredValue('lf_email') || 'there'
  const currentUserEmail = getStoredValue('lf_email') || ''
  const currentUserName = getStoredValue('lf_display_name') || getStoredValue('lf_contact_name') || ''
  const initialCachedCredits = readStoredNumber(getStoredValue('lf_credits_balance'), getStoredValue('lf_credits'))
  const initialCachedTopupCredits = readStoredNumber(getStoredValue('lf_topup_credits_balance'))
  const initialCachedCreditsLimit = readStoredNumber(getStoredValue('lf_credits_limit'))
  const [user, setUser] = useState(() => ({
    credits: initialCachedCredits,
    credits_balance: initialCachedCredits,
    topup_credits_balance: initialCachedTopupCredits,
    credits_limit: initialCachedCreditsLimit,
    monthly_limit: initialCachedCreditsLimit,
    monthly_quota: initialCachedCreditsLimit,
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
    email: String(currentUserEmail || '').trim().toLowerCase(),
    is_admin: String(getStoredValue('lf_is_admin') || '').trim().toLowerCase() === 'true' || String(currentUserEmail || '').trim().toLowerCase() === 'info@sniped.io',
    last_login_at: null,
  }))
  const [profileHydrated, setProfileHydrated] = useState(() => !hasSessionToken)
  const [profileLoadedFromApi, setProfileLoadedFromApi] = useState(() => !hasSessionToken)
  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()
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
  const [statsHydrated, setStatsHydrated] = useState(false)
  const [tasks, setTasks] = useState({})
  const [taskHistory, setTaskHistory] = useState([])
  const [adminOverview, setAdminOverview] = useState({
    stats: { total_users: 0, total_revenue: 0, total_leads: 0 },
    scraper: { health: 'unknown', last_status: 'unknown', last_error: '', last_updated_at: null },
    users: [],
    transactions: [],
    top_scrapers: [],
    lead_quality: { success_rate: 0, successful: 0, attempted: 0 },
    logs: [],
    notification: { active: false, message: '', updated_at: null },
    ai_signals: { enabled: true, updated_at: null, updated_by: '' },
  })
  const [adminLoading, setAdminLoading] = useState(false)
  const [adminSection, setAdminSection] = useState('users')
  const [adminPlanForm, setAdminPlanForm] = useState({ userId: '', planKey: 'growth' })
  const [globalNoticeForm, setGlobalNoticeForm] = useState({ message: '', active: true })
  const [aiSignalsEnabledForm, setAiSignalsEnabledForm] = useState(true)
  const [globalBanner, setGlobalBanner] = useState({ active: false, message: '', updated_at: null })
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
  const [seedingTemplatePack, setSeedingTemplatePack] = useState(false)
  const [manualLeadForm, setManualLeadForm] = useState(defaultManualLead)
  const [pendingRequest, setPendingRequest] = useState('')
  const [pendingStatusLeadId, setPendingStatusLeadId] = useState(null)
  const [pendingTierLeadId, setPendingTierLeadId] = useState(null)
  const [retryingTaskId, setRetryingTaskId] = useState(null)
  const [, setLastResult] = useState('')
  const setLastError = useCallback((msg) => {
    const text = String(msg || '').trim()
    if (!text) return
    console.error('[dashboard-error]', text)
  }, [])
  const [enrichRetrySeconds, setEnrichRetrySeconds] = useState(0)
  const [enrichRunRequested, setEnrichRunRequested] = useState(false)
  const [enrichTaskSnapshot, setEnrichTaskSnapshot] = useState(() => readEnrichTaskSnapshot())
  const [isAnalyzing, setIsAnalyzing] = useState(false)
  const [activeTab, setActiveTab] = useState(initialTabResolved)
  // (job queue removed — direct execution)
  const [leadSearch, setLeadSearch] = useState('')
  // useDebounce replaces the manual setTimeout useEffect — avoids CPU spikes on every keystroke
  const debouncedLeadSearch = useDebounce(leadSearch, 300)
  const [leadPage, setLeadPage] = useState(0)
  const [leadStatusFilter, setLeadStatusFilter] = useState('all')
  const [leadQuickFilter, setLeadQuickFilter] = useState(() => normalizeLeadQuickFilterParam(searchParams.get('filter'), 'all'))
  const [leadSortMode, setLeadSortMode] = useState('best')
  const [showBlacklisted, setShowBlacklisted] = useState(false)
  const [loadingLeads, setLoadingLeads] = useState(false)
  const [leadServerTotal, setLeadServerTotal] = useState(0)
  const leadReplyNotifySnapshotRef = useRef(new Map())
  const leadReplyNotifyPrimedRef = useRef(false)
  const [lastLeadsApiPayload, setLastLeadsApiPayload] = useState(null)
  const [leadFilterPanelOpen, setLeadFilterPanelOpen] = useState(false)
  const [leadsViewMode, setLeadsViewMode] = useState('table')
  const [aiFilterPrompt, setAiFilterPrompt] = useState('')
  const [aiFilterLoading, setAiFilterLoading] = useState(false)
  const [aiFilterLeadIds, setAiFilterLeadIds] = useState([])
  const [aiFilterApplied, setAiFilterApplied] = useState(false)
  const [aiFilterToolbarOpen, setAiFilterToolbarOpen] = useState(false)
  const [aiFilterInputFocused, setAiFilterInputFocused] = useState(false)
  const [selectedLeadIds, setSelectedLeadIds] = useState([])
  const [shareReportStateByLeadId, setShareReportStateByLeadId] = useState({})
  const [onboardingWizardOpen, setOnboardingWizardOpen] = useState(false)
  const [onboardingLaunching, setOnboardingLaunching] = useState(false)
  const [aiFilterSummary, setAiFilterSummary] = useState('')
  const [advancedLeadFilters, setAdvancedLeadFilters] = useState({
    industries: [],
    revenueBands: [],
    techStacks: [],
    highScoreOnly: false,
  })
  const [savedSegments, setSavedSegments] = useState([])
  const [loadingSavedSegments, setLoadingSavedSegments] = useState(false)
  const templatePackSeededRef = useRef(false)

  const seedTemplatePackToSavedTemplates = useCallback(async ({ silent = false } = {}) => {
    const templates = buildSnipedTemplateSeedPayload()
    if (!templates.length) {
      if (!silent) toast.error('No template defaults found to seed.')
      return { inserted: 0, skipped: 0 }
    }

    setSeedingTemplatePack(true)
    try {
      const data = await fetchJson('/api/mailer/templates/seed-defaults', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ templates }),
        timeoutMs: 15000,
      })
      const inserted = Number(data?.inserted || 0)
      const skipped = Number(data?.skipped || 0)
      if (!silent) {
        if (inserted > 0) {
          toast.success(`Template pack synced: ${inserted} added, ${skipped} skipped.`)
        } else {
          toast.success(`Template pack is up to date (${skipped} existing).`)
        }
      }
      return { inserted, skipped }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Template pack sync failed'
      setLastError(message)
      if (!silent) toast.error(message)
      throw error
    } finally {
      setSeedingTemplatePack(false)
    }
  }, [setLastError])

  useEffect(() => {
    if (!hasSessionToken || templatePackSeededRef.current) return
    templatePackSeededRef.current = true
    seedTemplatePackToSavedTemplates({ silent: true }).catch(() => {
      // Best-effort sync only; seed failures should not block dashboard UX.
    })
  }, [hasSessionToken, seedTemplatePackToSavedTemplates])

  const refreshLeads = useCallback(async (options = {}) => {
    const silent = options?.silent !== undefined ? options.silent : true
    // Allow callers to explicitly override filter values (e.g. right after setState when
    // the new state value hasn't propagated through the closure yet).
    const effectiveQuickFilter = options?.quickFilter !== undefined ? options.quickFilter : leadQuickFilter
    const effectiveStatusFilter = options?.statusFilter !== undefined ? options.statusFilter : leadStatusFilter
    if (!silent) {
      setLoadingLeads(true)
    }

    const applyLeadsPayload = (data) => {
      const rawItems = Array.isArray(data)
        ? data
        : Array.isArray(data?.items)
          ? data.items
          : Array.isArray(data?.leads)
            ? data.leads
            : Array.isArray(data?.data)
              ? data.data
              : []
      const items = rawItems.map((lead) => ({
        ...lead,
        business_name: lead?.business_name || lead?.name || '',
        contact_name: lead?.contact_name || lead?.contact || '',
        website_url: lead?.website_url || lead?.website || '',
        maps_url: lead?.maps_url || '',
        phone_number: lead?.phone_number || lead?.phone || '',
      }))
      setLastLeadsApiPayload(data)
      setLeads(items)
      setLeadServerTotal(Number(data?.total || data?.count || data?.total_count || items.length || 0))
    }

    try {
      const params = new URLSearchParams({
        limit: String(LEADS_PAGE_SIZE),
        page: String(leadPage + 1),
        sort: String(leadSortMode || 'recent'),
        include_blacklisted: showBlacklisted ? '1' : '0',
        _ts: String(Date.now()),
      })
      if (effectiveStatusFilter !== 'all') {
        params.set('status', effectiveStatusFilter)
      }
      if (effectiveQuickFilter !== 'all') {
        params.set('quick_filter', effectiveQuickFilter)
      }
      if (debouncedLeadSearch.trim()) {
        params.set('search', debouncedLeadSearch.trim())
      }
      const queryKey = ['leads-list', params.toString()]
      const cachedData = queryClient.getQueryData(queryKey)
      if (cachedData) {
        applyLeadsPayload(cachedData)
      }

      const data = await queryClient.fetchQuery({
        queryKey,
        queryFn: () => fetchJson(`/api/leads?${params.toString()}`),
        staleTime: LEADS_QUERY_STALE_TIME_MS,
        gcTime: LEADS_QUERY_GC_TIME_MS,
      })
      applyLeadsPayload(data)
    } catch (error) {
      if (error?.name === 'AbortError') return
      console.error('[leads] fetch failed:', error)
    } finally {
      if (!silent) {
        setLoadingLeads(false)
      }
    }
  }, [debouncedLeadSearch, leadPage, leadQuickFilter, leadSortMode, leadStatusFilter, queryClient, showBlacklisted])

  useEffect(() => {
    const nextSnapshot = new Map()
    const newlyReplied = []
    for (const lead of (Array.isArray(leads) ? leads : [])) {
      const leadId = Number(lead?.id || 0)
      if (!leadId) continue
      const marker = hasReply(lead)
        ? (String(lead?.reply_detected_at || '').trim() || 'replied')
        : ''
      const prevMarker = String(leadReplyNotifySnapshotRef.current.get(leadId) || '')
      if (leadReplyNotifyPrimedRef.current && marker && !prevMarker) {
        newlyReplied.push(lead)
      }
      nextSnapshot.set(leadId, marker)
    }
    leadReplyNotifySnapshotRef.current = nextSnapshot
    if (!leadReplyNotifyPrimedRef.current) {
      leadReplyNotifyPrimedRef.current = true
      return
    }
    if (!newlyReplied.length) return

    newlyReplied.slice(0, 3).forEach((lead) => {
      const who = lead?.business_name || lead?.contact_name || lead?.email || `Lead #${lead?.id}`
      toast.success(`Reply detected from ${who}`)
    })

    if (typeof window === 'undefined' || !('Notification' in window)) return
    const firstLead = newlyReplied[0]
    const title = 'New lead reply detected'
    const body = `${firstLead?.business_name || firstLead?.contact_name || firstLead?.email || 'A lead'} moved to Replied.`
    const showNativeNotification = () => {
      try {
        const n = new Notification(title, { body })
        n.onclick = () => window.focus()
      } catch {
        // Ignore browser notification errors and keep toast fallback.
      }
    }
    if (Notification.permission === 'granted') {
      showNativeNotification()
      return
    }
    if (Notification.permission === 'default') {
      void Notification.requestPermission().then((permission) => {
        if (permission === 'granted') {
          showNativeNotification()
        }
      })
    }
  }, [leads])
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
  const [aiCostReport, setAiCostReport] = useState(null)
  const [loadingAiCostReport, setLoadingAiCostReport] = useState(false)
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
    developer_webhook_url: '',
    developer_score_drop_threshold: 6,
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
  const [configSettingsTab, setConfigSettingsTab] = useState('platform')
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
  const [showLowCreditsModal, setShowLowCreditsModal] = useState(false)
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
  const [mailPreviewRaw, setMailPreviewRaw] = useState({ subject: '', body: '' })
  const [leadEmailDraft, setLeadEmailDraft] = useState({ leadId: null, templateKey: 'ghost', subject: '', body: '' })
  const [emailPreviewLead, setEmailPreviewLead] = useState(null)
  const [aiSummaryPreviewLead, setAiSummaryPreviewLead] = useState(null)
  const [leadDetailsPreviewLead, setLeadDetailsPreviewLead] = useState(null)
  const [leadEmailHistory, setLeadEmailHistory] = useState({ loading: false, error: '', items: [] })
  const [showLeadScoreBreakdown, setShowLeadScoreBreakdown] = useState(false)
  const [taskAiPreviewLead, setTaskAiPreviewLead] = useState(null)
  const [activeLiveMailTemplateKey, setActiveLiveMailTemplateKey] = useState(liveMailTemplateCards[0]?.key || 'ghost')
  const [activeMailEditorTab, setActiveMailEditorTab] = useState('live')
  const [showMailerConfirm, setShowMailerConfirm] = useState(false)
  const [mailerScheduledHour, setMailerScheduledHour] = useState('now')
  const [mailerHourOpen, setMailerHourOpen] = useState(false)
  const [mailerStopRequested, setMailerStopRequested] = useState(false)
  const [activeScrapeTaskId, setActiveScrapeTaskId] = useState(() => {
    try {
      const raw = Number(window.localStorage.getItem(SCRAPE_ACTIVE_TASK_ID_KEY) || 0)
      return Number.isFinite(raw) && raw > 0 ? Math.trunc(raw) : null
    } catch {
      return null
    }
  })
  const [scrapeSuccessLeadsFound, setScrapeSuccessLeadsFound] = useState(null)
  const previousTasksRef = useRef({})
  const leadSearchRef = useRef(null)
  const workflowRef = useRef(null)
  const mainPanelRef = useRef(null)
  const pendingDeletesRef = useRef({})
  const checkoutRedirectHandledRef = useRef('')
  const scrapeSuccessResetTimerRef = useRef(null)
  const leadEmailHistoryRealtimeChannelRef = useRef(null)

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
  const enrichRunRequestedRef = useRef(enrichRunRequested)
  const enrichTaskSnapshotRef = useRef(enrichTaskSnapshot)
  const tasksRef = useRef(tasks)
  const activeScrapeTaskIdRef = useRef(activeScrapeTaskId)
  const scrapeStatusRef = useRef(String(tasks?.scrape?.status || 'idle').toLowerCase().trim())
  const scrape500RetryTimerRef = useRef(null)
  const refreshUserProfileRef = useRef(null)

  useEffect(() => {
    enrichRunRequestedRef.current = enrichRunRequested
  }, [enrichRunRequested])

  useEffect(() => {
    enrichTaskSnapshotRef.current = enrichTaskSnapshot
  }, [enrichTaskSnapshot])

  useEffect(() => {
    tasksRef.current = tasks
  }, [tasks])

  useEffect(() => {
    scrapeStatusRef.current = String(tasks?.scrape?.status || 'idle').toLowerCase().trim()
  }, [tasks])

  useEffect(() => {
    activeScrapeTaskIdRef.current = activeScrapeTaskId
    try {
      if (Number.isFinite(Number(activeScrapeTaskId)) && Number(activeScrapeTaskId) > 0) {
        window.localStorage.setItem(SCRAPE_ACTIVE_TASK_ID_KEY, String(Math.trunc(Number(activeScrapeTaskId))))
      } else {
        window.localStorage.removeItem(SCRAPE_ACTIVE_TASK_ID_KEY)
      }
    } catch {
      // Ignore storage errors in constrained browsers.
    }
  }, [activeScrapeTaskId])

  useEffect(() => () => {
    if (scrapeSuccessResetTimerRef.current) {
      window.clearTimeout(scrapeSuccessResetTimerRef.current)
      scrapeSuccessResetTimerRef.current = null
    }
    if (scrape500RetryTimerRef.current) {
      window.clearTimeout(scrape500RetryTimerRef.current)
      scrape500RetryTimerRef.current = null
    }
  }, [])

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
  const enrichTaskView = useMemo(() => {
    const liveStatus = String(enrichTask?.status || 'idle').toLowerCase().trim()
    if (['queued', 'running', 'completed', 'failed', 'stopped'].includes(liveStatus)) {
      return enrichTask
    }

    const snapshot = enrichTaskSnapshot && typeof enrichTaskSnapshot === 'object' ? enrichTaskSnapshot : null
    if (!snapshot) return enrichTask

    const savedAt = Number(snapshot.saved_at || 0)
    if (!Number.isFinite(savedAt) || (Date.now() - savedAt) > ENRICH_TASK_SNAPSHOT_TTL_MS) {
      return enrichTask
    }

    const snapshotStatus = String(snapshot.status || 'idle').toLowerCase().trim()
    if (!['queued', 'running', 'completed', 'failed', 'stopped'].includes(snapshotStatus)) {
      return enrichTask
    }

    return {
      ...enrichTask,
      ...snapshot,
      status: snapshotStatus,
      running: ['queued', 'running'].includes(snapshotStatus),
    }
  }, [enrichTask, enrichTaskSnapshot])
  const scrapeTaskStateRef = useRef({ id: null, status: 'idle' })

  useEffect(() => {
    if (!mailerTask.running) {
      setMailerStopRequested(false)
    }
  }, [mailerTask.running])

  useEffect(() => {
    const status = String(enrichTaskView.status || 'idle').toLowerCase()
    if (!enrichTaskView.running && ['completed', 'failed', 'stopped'].includes(status)) {
      setEnrichRunRequested(false)
    }
  }, [enrichTaskView.running, enrichTaskView.status])

  useEffect(() => {
    const status = String(enrichTask.status || 'idle').toLowerCase().trim()
    const snapshotCandidate = {
      id: enrichTask.id || null,
      task_type: 'enrich',
      status,
      running: Boolean(enrichTask.running),
      created_at: enrichTask.created_at || null,
      started_at: enrichTask.started_at || null,
      finished_at: enrichTask.finished_at || null,
      last_request: enrichTask.last_request || null,
      result: enrichTask.result || null,
      error: enrichTask.error || null,
      saved_at: Date.now(),
    }

    if (['queued', 'running', 'completed', 'failed', 'stopped'].includes(status)) {
      setEnrichTaskSnapshot(snapshotCandidate)
      writeEnrichTaskSnapshot(snapshotCandidate)
      return
    }

    const savedAt = Number(enrichTaskSnapshot?.saved_at || 0)
    if (savedAt > 0 && (Date.now() - savedAt) > ENRICH_TASK_SNAPSHOT_TTL_MS) {
      setEnrichTaskSnapshot(null)
      clearEnrichTaskSnapshot()
    }
  }, [enrichTask, enrichTaskSnapshot])

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

  useEffect(() => {
    const fromUrl = normalizeLeadQuickFilterParam(searchParams.get('filter'), 'all')
    setLeadQuickFilter((prev) => (prev === fromUrl ? prev : fromUrl))
  }, [searchParams])

  useEffect(() => {
    if (activeTab !== 'leads') return
    const current = normalizeLeadQuickFilterParam(searchParams.get('filter'), 'all')
    if (current === leadQuickFilter) return
    const next = new URLSearchParams(searchParams)
    if (leadQuickFilter === 'all') {
      next.delete('filter')
    } else {
      next.set('filter', leadQuickFilter)
    }
    setSearchParams(next, { replace: true })
  }, [activeTab, leadQuickFilter, searchParams, setSearchParams])

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
    const taskError = String(scrapeTask.error || '').trim()
    const requestedFromTask = Number(scrapeTask.last_request?.results || 0)
    const requestedFromForm = Number(scrapeForm.results || 0)
    const totalToFind = Number(result.total_to_find || requestedFromTask || requestedFromForm || 0)
    const currentFound = Number(result.current_found || (status === 'completed' ? result.scraped || 0 : 0))
    const currentInDb = Number(result.inserted || 0)
    const progressCurrent = Math.max(currentInDb, currentFound)
    const scannedCount = Number(result.scanned_count || 0)
    const inserted = Number(result.inserted || (status === 'completed' ? result.scraped || 0 : 0))
    const phase = String(result.phase || '')
    // When the backend orphan-resets a running task the error contains this phrase.
    const isOrphanReset = taskError.toLowerCase().includes('worker not active in current process')
    // Show a friendlier message for auto-reset; otherwise use the task's own message.
    const statusMessage = isOrphanReset
      ? `Reset by server — ${progressCurrent} lead${progressCurrent !== 1 ? 's' : ''} saved`
      : String(result.status_message || taskError || '').trim()
    // isLoading = scraper launched but Maps hasn't returned any card yet
    const isLoading = (status === 'running' || status === 'queued' || status === 'processing' || status === 'pending') && currentFound === 0 && scannedCount === 0

    let percent = 0
    if (totalToFind > 0) {
      percent = Math.min(100, Math.round((progressCurrent / totalToFind) * 100))
    }
    if (status === 'completed') percent = 100

    return {
      status,
      totalToFind,
      currentFound,
      currentInDb,
      progressCurrent,
      scannedCount,
      inserted,
      percent,
      phase,
      statusMessage,
      isLoading,
      isOrphanReset,
      // Keep bar visible on 'stopped' so progress is not lost if server reset the task.
      isVisible: ['queued', 'running', 'processing', 'pending', 'completed', 'failed', 'stopped'].includes(status),
    }
  }, [scrapeTask, scrapeForm.results])

  // Auto-refresh leads table whenever the backend saves a new lead during a live scrape.
  const scrapeInsertedCountRef = useRef(0)
  useEffect(() => {
    const status = String(scrapeTask.status || 'idle').toLowerCase()
    const isActive = ['queued', 'running', 'processing', 'pending'].includes(status)
    const insertedNow = Number(scrapeTask.result?.inserted || 0)
    if (isActive && insertedNow > scrapeInsertedCountRef.current) {
      scrapeInsertedCountRef.current = insertedNow
      void refreshLeads({ silent: true })
    }
    if (!isActive) {
      scrapeInsertedCountRef.current = 0
    }
  }, [scrapeTask.status, scrapeTask.result?.inserted, refreshLeads])

  const scrapeRuntimeStatus = String(scrapeTask.status || 'idle').toLowerCase().trim()
  const scrapeIsActive = ['queued', 'running', 'processing', 'pending'].includes(scrapeRuntimeStatus)
  const scrapeCardStatusLabel = scrapeIsActive
    ? 'RUNNING'
    : scrapeRuntimeStatus === 'completed'
      ? 'SUCCESS'
      : scrapeRuntimeStatus === 'failed'
        ? 'FAILED'
        : 'READY'
  const scrapeButtonLocked = pendingRequest === 'scrape' || scrapeIsActive || Boolean(scrapeSuccessLeadsFound)

  const enrichProgress = useMemo(() => {
    const status = String(enrichTaskView.status || 'idle').toLowerCase()
    const result = enrichTaskView.result && typeof enrichTaskView.result === 'object' ? enrichTaskView.result : {}
    const requestedLimit = Number(result.effective_limit || enrichTaskView.last_request?.limit || enrichForm.limit || 50)
    const totalFromTask = Number(result.total || requestedLimit || 0)
    const processed = Number(result.processed || 0)
    const queued = Number(result.queued_for_mail || 0)
    const withEmail = Number(result.with_email || 0)
    const currentLead = String(result.current_lead || '').trim()
    const statusMessage = String(result.status_message || '').trim()
    const currentPhase = String(result.current_phase || '').trim().toLowerCase()

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
      currentPhase,
      statusMessage,
      percent,
      isVisible: ['queued', 'running', 'completed', 'failed'].includes(status),
    }
  }, [enrichTaskView, enrichForm.limit])

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
  useEffect(() => { setLeadPage(0) }, [debouncedLeadSearch, leadStatusFilter, leadQuickFilter, leadSortMode, showBlacklisted, advancedLeadFilters, aiFilterLeadIds])

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

  const isAiFilterActive = aiFilterApplied

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

    if (isAiFilterActive) {
      const idSet = new Set(aiFilterLeadIds.map((id) => Number(id)).filter((id) => Number.isFinite(id)))
      result = result.filter((lead) => idSet.has(Number(lead?.id)))
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
  }, [leads, debouncedLeadSearch, leadStatusFilter, leadQuickFilter, leadSortMode, showBlacklisted, advancedLeadFilters, isAiFilterActive, aiFilterLeadIds])

  const leadsPageCount = Math.max(1, Math.ceil(Math.max(leadServerTotal, filteredLeads.length) / LEADS_PAGE_SIZE))
  const pagedLeads = useMemo(() => {
    const start = leadPage * LEADS_PAGE_SIZE
    return filteredLeads.slice(start, start + LEADS_PAGE_SIZE)
  }, [filteredLeads, leadPage])

  const kanbanColumns = useMemo(() => {
    const buckets = {
      Scraped: [],
      Contacted: [],
      Replied: [],
      'Won (Paid)': [],
    }
    for (const lead of pagedLeads) {
      const stage = resolvePipelineStage(lead)
      if (buckets[stage]) {
        buckets[stage].push(lead)
      } else {
        buckets.Scraped.push(lead)
      }
    }
    return buckets
  }, [pagedLeads])

  const handleLeadPipelineDragEnd = (event) => {
    const activeId = String(event?.active?.id || '')
    const overId = String(event?.over?.id || '')
    if (!activeId.startsWith('pipeline-lead-')) return
    if (!leadPipelineOptions.includes(overId)) return

    const leadId = Number(activeId.replace('pipeline-lead-', ''))
    if (!Number.isFinite(leadId)) return
    const currentLead = (Array.isArray(leads) ? leads : []).find((item) => Number(item?.id) === leadId)
    if (!currentLead) return

    const currentStage = resolvePipelineStage(currentLead)
    if (currentStage === overId) return
    void updateLeadStatus(leadId, overId)
  }

  const hasSearchOrAiFilter = Boolean(debouncedLeadSearch.trim()) || isAiFilterActive
  const emptyLeadsMessage = hasSearchOrAiFilter
    ? 'No leads found matching your criteria. Try adjusting your search or AI filter.'
    : 'No leads match the current filters yet.'

  const hasAnyLeadFiltersActive = Boolean(
    debouncedLeadSearch.trim()
    || leadStatusFilter !== 'all'
    || leadQuickFilter !== 'all'
    || showBlacklisted
    || advancedLeadFilters.industries.length
    || advancedLeadFilters.revenueBands.length
    || advancedLeadFilters.techStacks.length
    || advancedLeadFilters.highScoreOnly
    || isAiFilterActive,
  )

  const leadFilterSignature = [
    leadStatusFilter,
    leadQuickFilter,
    debouncedLeadSearch.trim().toLowerCase(),
    showBlacklisted ? '1' : '0',
    advancedLeadFilters.industries.join('|'),
    advancedLeadFilters.revenueBands.join('|'),
    advancedLeadFilters.techStacks.join('|'),
    advancedLeadFilters.highScoreOnly ? '1' : '0',
    isAiFilterActive ? aiFilterLeadIds.join('|') : '',
  ].join('::')

  useEffect(() => {
    if (leadPage <= 0) return
    const maxPageIndex = Math.max(0, Math.ceil(filteredLeads.length / LEADS_PAGE_SIZE) - 1)
    if (leadPage > maxPageIndex) {
      setLeadPage(maxPageIndex)
    }
  }, [filteredLeads.length, leadPage])

  const leadQuickCountSource = useMemo(() => {
    let visible = [...leads]
    if (!showBlacklisted) {
      visible = visible.filter((l) => !isBlacklistedLeadStatus(l.status))
    }
    if (leadStatusFilter !== 'all') {
      visible = visible.filter((l) => String(l.status || 'pending').toLowerCase() === leadStatusFilter.toLowerCase())
    }
    if (debouncedLeadSearch.trim()) {
      const q = debouncedLeadSearch.trim().toLowerCase()
      visible = visible.filter(
        (l) => (l.business_name || '').toLowerCase().includes(q)
          || (l.contact_name || '').toLowerCase().includes(q)
          || (l.email || '').toLowerCase().includes(q),
      )
    }
    if (advancedLeadFilters.industries.length > 0) {
      visible = visible.filter((lead) => advancedLeadFilters.industries.includes(deriveLeadIndustry(lead)))
    }
    if (advancedLeadFilters.revenueBands.length > 0) {
      visible = visible.filter((lead) => advancedLeadFilters.revenueBands.includes(deriveLeadRevenueBand(lead)))
    }
    if (advancedLeadFilters.techStacks.length > 0) {
      visible = visible.filter((lead) => {
        const stackSet = new Set(normalizeLeadInsightList(lead.tech_stack, 5))
        return advancedLeadFilters.techStacks.some((stack) => stackSet.has(stack))
      })
    }
    if (advancedLeadFilters.highScoreOnly) {
      visible = visible.filter((lead) => resolveBestLeadScore(lead) >= 8)
    }
    if (isAiFilterActive) {
      const idSet = new Set(aiFilterLeadIds.map((id) => Number(id)).filter((id) => Number.isFinite(id)))
      visible = visible.filter((lead) => idSet.has(Number(lead?.id)))
    }
    return visible
  }, [leads, showBlacklisted, leadStatusFilter, debouncedLeadSearch, advancedLeadFilters, isAiFilterActive, aiFilterLeadIds])

  const leadQuickCounts = useMemo(() => {
    const visible = leadQuickCountSource
    return {
      total: visible.length,
      qualified: visible.filter((l) => isQualifiedLead(l)).length,
      notQualified: visible.filter((l) => !isQualifiedLead(l)).length,
      mailed: visible.filter((l) => hasSentMail(l)).length,
      opened: visible.filter((l) => hasOpenedMail(l)).length,
      replied: visible.filter((l) => hasReply(l)).length,
    }
  }, [leadQuickCountSource])

  const selectedUserNiche = String(user?.niche || qualifierData?.data?.selected_niche || getStoredValue('lf_niche') || '').trim()

  const getEligibleEnrichmentLeadIds = useCallback((limitValue) => {
    const requested = Number(limitValue)
    const normalizedBatchSize = Math.max(1, Math.min(Number.isFinite(requested) ? Math.floor(requested) : 50, 200))
    const doneStatuses = new Set([
      'enriched',
      'queued_mail',
      'emailed',
      'interested',
      'replied',
      'meeting set',
      'zoom scheduled',
      'closed',
      'paid',
      'invalid_email',
    ])
    return leads
      .filter((lead) => {
        const status = String(lead.status || '').toLowerCase().trim()
        const enrichmentStatus = String(lead.enrichment_status || '').toLowerCase().trim()
        if (!lead?.id) return false
        if (lead.enriched_at != null && String(lead.enriched_at).trim() !== '') return false
        if (doneStatuses.has(status)) return false
        return ['pending', 'failed', '', 'processing'].includes(enrichmentStatus) || status === 'scraped'
      })
      .map((lead) => Number(lead.id))
      .filter((id) => Number.isFinite(id) && id > 0)
      .slice(0, normalizedBatchSize)
  }, [leads])

  const selectedLeadIdSet = useMemo(
    () => new Set(selectedLeadIds.map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0)),
    [selectedLeadIds],
  )

  const selectedLeadRows = useMemo(
    () => filteredLeads.filter((lead) => selectedLeadIdSet.has(Number(lead?.id))),
    [filteredLeads, selectedLeadIdSet],
  )

  const selectedLeadForEmailDraft = useMemo(
    () => (selectedLeadRows.length === 1 ? selectedLeadRows[0] : null),
    [selectedLeadRows],
  )

  const resolvedLeadDraftPreview = useMemo(() => {
    if (!selectedLeadForEmailDraft || Number(leadEmailDraft.leadId || 0) !== Number(selectedLeadForEmailDraft.id || 0)) {
      return { subject: '', body: '' }
    }

    const leadNiche = deriveLeadIndustry(selectedLeadForEmailDraft) || selectedUserNiche || 'Local Business'
    const vars = {
      BusinessName: selectedLeadForEmailDraft.business_name || selectedLeadForEmailDraft.name || 'Business Name',
      City: resolveLeadCityValue(selectedLeadForEmailDraft),
      Niche: leadNiche,
      YourName: currentUserName || 'Your Name',
    }

    return {
      subject: replaceTemplatePlaceholders(leadEmailDraft.subject, vars),
      body: replaceTemplatePlaceholders(leadEmailDraft.body, vars),
    }
  }, [leadEmailDraft.body, leadEmailDraft.leadId, leadEmailDraft.subject, selectedLeadForEmailDraft, selectedUserNiche, currentUserName])

  const enrichmentEligibleLeadIds = useMemo(
    () => getEligibleEnrichmentLeadIds(enrichForm.limit),
    [getEligibleEnrichmentLeadIds, enrichForm.limit],
  )

  const requiredScrapeCredits = Math.max(1, Number(scrapeForm.results || 1)) * SCRAPE_CREDIT_COST_PER_LEAD
  const requiredEnrichCredits = Math.max(1, enrichmentEligibleLeadIds.length) * ENRICH_CREDIT_COST_PER_LEAD
  const pageLeadIds = useMemo(
    () => pagedLeads.map((lead) => Number(lead?.id)).filter((id) => Number.isFinite(id) && id > 0),
    [pagedLeads],
  )
  const areAllPageLeadsSelected = pageLeadIds.length > 0 && pageLeadIds.every((id) => selectedLeadIdSet.has(id))

  const clearAiFilter = useCallback(() => {
    setAiFilterLeadIds([])
    setAiFilterApplied(false)
    setAiFilterSummary('')
    setAiFilterPrompt('')
    setAiFilterLoading(false)
  }, [])

  const clearAllLeadFilters = useCallback(() => {
    setLeadSearch('')
    setLeadStatusFilter('all')
    setLeadQuickFilter('all')
    setShowBlacklisted(false)
    setAdvancedLeadFilters({ industries: [], revenueBands: [], techStacks: [], highScoreOnly: false })
    setLeadPage(0)
    clearAiFilter()
  }, [clearAiFilter])

  useEffect(() => {
    const allowed = new Set(filteredLeads.map((lead) => Number(lead?.id)).filter((id) => Number.isFinite(id) && id > 0))
    setSelectedLeadIds((prev) => prev.filter((id) => allowed.has(Number(id))))
  }, [filteredLeads])

  useEffect(() => {
    if (!selectedLeadForEmailDraft) return
    const leadId = Number(selectedLeadForEmailDraft.id || 0)
    if (!leadId) return
    if (Number(leadEmailDraft.leadId || 0) === leadId) return

    const templateKey = resolveLeadTemplateKey(selectedLeadForEmailDraft)
    const leadNiche = deriveLeadIndustry(selectedLeadForEmailDraft) || selectedUserNiche
    const template = resolveSnipedTemplateForSelection(leadNiche, templateKey)

    setLeadEmailDraft({
      leadId,
      templateKey,
      subject: String(template?.subject || ''),
      body: String(template?.body || ''),
    })
  }, [selectedLeadForEmailDraft, selectedUserNiche, leadEmailDraft.leadId])

  const toggleLeadSelection = useCallback((leadId) => {
    const normalizedId = Number(leadId)
    if (!Number.isFinite(normalizedId) || normalizedId <= 0) return
    setSelectedLeadIds((prev) => (
      prev.includes(normalizedId)
        ? prev.filter((id) => id !== normalizedId)
        : [...prev, normalizedId]
    ))
  }, [])

  const toggleSelectAllPageLeads = useCallback(() => {
    setSelectedLeadIds((prev) => {
      const current = new Set(prev)
      const allSelected = pageLeadIds.length > 0 && pageLeadIds.every((id) => current.has(id))
      if (allSelected) {
        pageLeadIds.forEach((id) => current.delete(id))
      } else {
        pageLeadIds.forEach((id) => current.add(id))
      }
      return Array.from(current)
    })
  }, [pageLeadIds])

  const clearSelectedLeads = useCallback(() => {
    setSelectedLeadIds([])
  }, [])

  const planKey = String(user?.plan_key || '').toLowerCase().trim()
  const featureAccess = useMemo(
    () => resolveFeatureAccess(user?.plan_type || planKey || 'free', user?.feature_access),
    [planKey, user?.plan_type, user?.feature_access],
  )
  const canBulkExport = Boolean(featureAccess.bulk_export)
  const canLeadScoring = Boolean(featureAccess.ai_lead_scoring)
  const canAdvancedReporting = Boolean(featureAccess.advanced_reporting)
  const canClientSuccessDashboard = Boolean(featureAccess.client_success_dashboard)
  const isAdmin = String(user?.email || currentUserEmail || '').trim().toLowerCase() === 'info@sniped.io'
  const isAdminUser = Boolean(user?.is_admin || isAdmin)

  const bulkExportSelectedCsv = useCallback(() => {
    if (!selectedLeadRows.length) {
      toast('Select at least one lead first.', { icon: 'ℹ️' })
      return
    }
    if (!canBulkExport) {
      toast('CSV exports unlock on The Growth and above.', { icon: '🔒' })
      return
    }
    const headers = [
      'id', 'business_name', 'contact_name', 'email', 'phone_number', 'website_url', 'maps_url', 'status', 'ai_score', 'qualification_score', 'pipeline_stage',
    ]
    const escapeCsv = (value) => {
      const text = String(value ?? '')
      if (text.includes('"') || text.includes(',') || text.includes('\n')) {
        return `"${text.replace(/"/g, '""')}"`
      }
      return text
    }
    const rows = selectedLeadRows.map((lead) => headers.map((key) => escapeCsv(lead?.[key])).join(','))
    const csv = [headers.join(','), ...rows].join('\n')
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const objectUrl = window.URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = objectUrl
    link.download = `selected_leads_${selectedLeadRows.length}.csv`
    document.body.appendChild(link)
    link.click()
    link.remove()
    window.URL.revokeObjectURL(objectUrl)
    toast.success(`Exported ${selectedLeadRows.length} selected leads`) 
  }, [selectedLeadRows, canBulkExport])

  const bulkAiFilterSelected = useCallback(async () => {
    if (!selectedLeadRows.length) {
      toast('Select at least one lead first.', { icon: 'ℹ️' })
      return
    }
    const prompt = window.prompt('AI Filter prompt for selected leads:', aiFilterPrompt || 'Show only high priority leads with strong buying intent')
    if (!prompt || !String(prompt).trim()) return

    setAiFilterLoading(true)
    try {
      const response = await fetchJson('/api/leads/ai-filter', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          prompt: String(prompt).trim(),
          limit: 5000,
          include_blacklisted: showBlacklisted,
        }),
      })
      const returnedIds = new Set(
        (Array.isArray(response?.lead_ids) ? response.lead_ids : [])
          .map((id) => Number(id))
          .filter((id) => Number.isFinite(id) && id > 0),
      )
      const selectedSet = new Set(selectedLeadRows.map((lead) => Number(lead?.id)).filter((id) => Number.isFinite(id) && id > 0))
      const intersected = Array.from(returnedIds).filter((id) => selectedSet.has(id))
      setAiFilterLeadIds(intersected)
      setAiFilterApplied(true)
      setAiFilterPrompt(String(prompt).trim())
      setAiFilterSummary(String(response?.assistant_message || `AI filter matched ${intersected.length} of ${selectedLeadRows.length} selected leads.`))
      setLeadPage(0)
      if (intersected.length > 0) {
        toast.success(`AI filter matched ${intersected.length}/${selectedLeadRows.length} selected leads`)
      } else {
        toast('I couldn\'t find leads matching that specific criteria. Try a broader search!', { icon: 'ℹ️' })
      }
    } catch (error) {
      setAiFilterApplied(false)
      toast.error(error instanceof Error ? error.message : 'Bulk AI filter failed')
    } finally {
      setAiFilterLoading(false)
    }
  }, [selectedLeadRows, aiFilterPrompt, showBlacklisted])

  const bulkDeleteSelectedLeads = useCallback(async () => {
    const ids = selectedLeadRows.map((lead) => Number(lead?.id)).filter((id) => Number.isFinite(id) && id > 0)
    if (!ids.length) {
      toast('Select at least one lead first.', { icon: 'ℹ️' })
      return
    }
    const confirmed = window.confirm(`Delete ${ids.length} selected leads? This cannot be undone.`)
    if (!confirmed) return

    try {
      const result = await fetchJson('/api/leads/bulk-delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ lead_ids: ids }),
      })
      const deletedCount = Number(result?.deleted || 0)
      toast.success(`Deleted ${deletedCount} leads`)
      setSelectedLeadIds([])
      await Promise.allSettled([refreshLeads(), refreshStats()])
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Bulk delete failed')
    }
  }, [selectedLeadRows, refreshLeads])

  const runAiFilter = useCallback(async (rawPrompt) => {
    const prompt = String(rawPrompt || aiFilterPrompt || '').trim()
    if (!prompt) {
      toast.error('Type an AI filter prompt first')
      return
    }

    setAiFilterLoading(true)
    setAiFilterLeadIds([])
    setAiFilterApplied(false)
    try {
      const response = await fetchJson('/api/leads/ai-filter', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          prompt,
          limit: 5000,
          include_blacklisted: showBlacklisted,
        }),
      })
      const ids = Array.isArray(response?.lead_ids) ? response.lead_ids.map((id) => Number(id)).filter((id) => Number.isFinite(id)) : []
      setAiFilterLeadIds(ids)
      setAiFilterApplied(true)
      setAiFilterSummary(String(response?.assistant_message || `I found ${ids.length} lead(s) matching your request.`))
      setAiFilterPrompt(prompt)
      setLeadPage(0)
      if (ids.length > 0) {
        toast.success(`AI interpreted your filter (${ids.length} potential leads)`)
      } else {
        toast('I couldn\'t find leads matching that specific criteria. Try a broader search!', { icon: 'ℹ️' })
      }
    } catch (error) {
      setAiFilterApplied(false)
      const message = error instanceof Error ? error.message : 'AI filter failed'
      setLastError(message)
      toast.error(message)
    } finally {
      setAiFilterLoading(false)
    }
  }, [aiFilterPrompt, showBlacklisted, setLastError])

  const hasLeadsForOnboarding = useMemo(
    () => Math.max(Number(leadServerTotal || 0), Number(leads?.length || 0)) > 0,
    [leadServerTotal, leads],
  )

  const closeOnboardingWizard = useCallback((completed = false) => {
    setOnboardingWizardOpen(false)
    if (typeof window === 'undefined') return
    if (completed) {
      window.localStorage.setItem(ONBOARDING_COMPLETED_KEY, '1')
      window.localStorage.removeItem(ONBOARDING_DISMISSED_KEY)
      return
    }
    window.localStorage.setItem(ONBOARDING_DISMISSED_KEY, '1')
  }, [])

  useEffect(() => {
    if (loadingLeads) return
    if (hasLeadsForOnboarding) return
    if (typeof window === 'undefined') return

    const completed = window.localStorage.getItem(ONBOARDING_COMPLETED_KEY) === '1'
    const dismissed = window.localStorage.getItem(ONBOARDING_DISMISSED_KEY) === '1'
    if (completed || dismissed) return

    setOnboardingWizardOpen(true)
  }, [loadingLeads, hasLeadsForOnboarding])

  useEffect(() => {
    if (!hasLeadsForOnboarding) return
    if (typeof window === 'undefined') return
    window.localStorage.setItem(ONBOARDING_COMPLETED_KEY, '1')
    window.localStorage.removeItem(ONBOARDING_DISMISSED_KEY)
    setOnboardingWizardOpen(false)
  }, [hasLeadsForOnboarding])

  const refreshLeadsRef = useRef(refreshLeads)

  useEffect(() => {
    refreshLeadsRef.current = refreshLeads
  }, [refreshLeads])

  useEffect(() => {
    if (activeTab !== 'leads') return
    setLeadStatusFilter('all')
    setLeadQuickFilter('all')
    setLeadSearch('')
    setLeadPage(0)
    clearAiFilter()
    void refreshLeadsRef.current({ silent: false })
  }, [activeTab, clearAiFilter])

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
      ? ['running', 'queued', 'processing', 'pending'].includes(previous.status) && currentStatus === 'completed'
      : currentStatus === 'completed'

    if (transitionedToCompleted) {
      void refreshLeads({ silent: false })
    }

    if (['completed', 'failed', 'cancelled', 'stopped'].includes(currentStatus)) {
      void refreshUserProfileRef.current?.()
    }

    if (Number(scrapeTask.id || 0) > 0) {
      setActiveScrapeTaskId(Number(scrapeTask.id))
    }

    if (['completed', 'failed', 'cancelled', 'stopped'].includes(currentStatus)) {
      const tracked = Number(activeScrapeTaskIdRef.current || 0)
      if (tracked > 0 && tracked === Number(scrapeTask.id || 0)) {
        window.setTimeout(() => {
          if (activeScrapeTaskIdRef.current === tracked) {
            setActiveScrapeTaskId(null)
          }
        }, 4000)
      }
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

  useEffect(() => {
    if (enrichRetrySeconds <= 0) return undefined
    const id = window.setInterval(() => {
      setEnrichRetrySeconds((prev) => (prev <= 1 ? 0 : prev - 1))
    }, 1000)
    return () => window.clearInterval(id)
  }, [enrichRetrySeconds])

  const fetchNicheAdvice = useCallback(async ({ silent = false, forceRefresh = false, countryCode = null } = {}) => {
    try {
      const selectedCountry = String(countryCode || scrapeForm.country || 'US').toUpperCase()
      setNicheAdvice((prev) => ({ ...prev, loading: true, error: '' }))
      const params = new URLSearchParams({ country: selectedCountry })
      if (forceRefresh) params.set('refresh', '1')
      const contextKeyword = String(scrapeForm.keyword || '').trim()
      if (contextKeyword) params.set('context_keyword', contextKeyword)

      let data = null
      const retryDelays = [0, 900, 1800]
      for (let attempt = 0; attempt < retryDelays.length; attempt += 1) {
        try {
          if (retryDelays[attempt] > 0) {
            await sleep(retryDelays[attempt])
          }
          data = await fetchJson(`/api/ai/market-intelligence?${params.toString()}`, {
            abortKey: 'market-intelligence',
            bypassCache: forceRefresh,
            timeoutMs: 35000,
          })
          break
        } catch {
          // Retry below.
        }
      }

      if (!data) throw new Error('Could not load market intelligence')

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
      const message = error instanceof Error ? error.message : 'Could not load market intelligence'
      setNicheAdvice((prev) => ({ loading: false, data: prev.data || null, error: message }))
      if (!silent) {
        toast.error(message)
      }
    }
  }, [scrapeForm.country, scrapeForm.keyword])

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
    if (activeTab !== 'leads') return undefined
    if (!nicheAdvice.error) return undefined
    const retryId = window.setTimeout(() => {
      void fetchNicheAdvice({ silent: true, countryCode: scrapeForm.country })
    }, 12000)
    return () => window.clearTimeout(retryId)
  }, [activeTab, nicheAdvice.error, fetchNicheAdvice, scrapeForm.country])

  useEffect(() => {
    const previousTasks = previousTasksRef.current
    for (const taskType of Object.keys(taskLabels)) {
      const cur = tasks[taskType]
      const prev = previousTasks[taskType]
      if (!cur || !prev) continue
      const wasRunning = ['queued', 'running', 'processing', 'pending'].includes(String(prev.status || '').toLowerCase())
      const isCompleted = String(cur.status || '').toLowerCase() === 'completed'
      const isFailed = String(cur.status || '').toLowerCase() === 'failed'
      const sameTask = cur.id === prev.id
      if (wasRunning && sameTask && isCompleted) {
        toast.success(`${taskLabels[taskType]} completed`)
        if (taskType === 'scrape') {
          const inserted = Number(cur.result?.inserted || 0)
          const leadsFound = Number(cur.result?.scraped || cur.result?.current_found || inserted || 0)
          setScrapeSuccessLeadsFound(Math.max(0, leadsFound))
          // Re-fetch immediately when success state appears so Lead Management reflects latest DB rows.
          void refreshLeads({ silent: true, quickFilter: 'all', statusFilter: 'all' })
          if (scrapeSuccessResetTimerRef.current) {
            window.clearTimeout(scrapeSuccessResetTimerRef.current)
          }
          scrapeSuccessResetTimerRef.current = window.setTimeout(() => {
            setScrapeSuccessLeadsFound(null)
            scrapeSuccessResetTimerRef.current = null
          }, 3000)
          if (inserted > 0) shootConfetti()
          if (inserted > 0) {
            // Clear any active filters so newly scraped rows are visible immediately,
            // then fetch the fresh list from the server.
            setLeadStatusFilter('all')
            setLeadQuickFilter('all')
            setLeadSearch('')
            setLeadPage(0)
            void refreshLeads({ silent: true, quickFilter: 'all', statusFilter: 'all' })
          }
          setLastResult('')
        } else if (cur.result) {
          if (taskType === 'enrich') {
            const processedCount = Number(cur.result?.processed ?? cur.result?.enriched_count ?? cur.result?.updated_rows ?? 0)
            const rawBalance = cur.result?.credits_balance
            const nextBalance = Number(rawBalance)
            const nextLimit = Number(cur.result?.credits_limit)
            const hasConcreteBalance = rawBalance !== null && rawBalance !== undefined && Number.isFinite(nextBalance)
            if (processedCount > 0 && hasConcreteBalance) {
              setUser((prevUser) => ({
                ...prevUser,
                credits: Math.max(0, nextBalance),
                credits_balance: Math.max(0, nextBalance),
                creditLimit: Number.isFinite(nextLimit) ? Math.max(1, nextLimit) : prevUser.creditLimit,
                credits_limit: Number.isFinite(nextLimit) ? Math.max(1, nextLimit) : prevUser.credits_limit,
              }))
              toast.success(`Credits remaining: ${formatCreditAmount(Math.max(0, nextBalance), { thousandDecimals: 1, thousandMode: 'floor', millionDecimals: 2, millionMode: 'floor' })}`)
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
        if (taskType === 'scrape') {
          if (scrapeSuccessResetTimerRef.current) {
            window.clearTimeout(scrapeSuccessResetTimerRef.current)
            scrapeSuccessResetTimerRef.current = null
          }
          setScrapeSuccessLeadsFound(null)
        }
        toast.error(`${taskLabels[taskType]} failed`)
        if (cur.error) setLastError(String(cur.error))
        void Promise.allSettled([refreshLeads(), refreshStats(), refreshConfigHealth()])
      }
    }
    previousTasksRef.current = tasks
  }, [refreshLeads, tasks, setLastError])

  async function refreshDashboard() {
    setRefreshingDashboard(true)
    try {
      await Promise.allSettled([
        checkHealth(),
        refreshConfigHealth(),
        refreshLeads(),
        refreshStats(),
        refreshCreditsBalance({ timeoutMs: 2500 }),
        fetchTaskState(),
        fetchRevenueLog(),
        refreshSignalLayer({ forceRefresh: true, silentNiche: true }),
        refreshWorkers(),
        refreshDeliveryTasks(),
        refreshUserProfile(),
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
        developer_webhook_url: data.developer_webhook_url || '',
        developer_score_drop_threshold: Number.isFinite(Number(data.developer_score_drop_threshold)) ? Number(data.developer_score_drop_threshold) : 6,
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

  const applyUserProfileData = useCallback((data) => {
    if (!data || typeof data !== 'object') return
    setUser((prev) => {
      const resolvedFeatureAccess = resolveFeatureAccess(
        data?.plan_type ?? data?.plan_key ?? prev?.plan_type ?? prev?.plan_key ?? 'free',
        data?.feature_access ?? prev?.feature_access,
      )
      return {
        ...prev,
        ...data,
        credits: Number(data?.credits_balance ?? 0),
        creditLimit: Number(data?.monthly_quota ?? data?.monthly_limit ?? data?.credits_limit ?? DEFAULT_FREE_CREDIT_LIMIT),
        credits_balance: Number(data?.credits_balance ?? 0),
        credits_limit: Number(data?.monthly_quota ?? data?.monthly_limit ?? data?.credits_limit ?? DEFAULT_FREE_CREDIT_LIMIT),
        monthly_limit: Number(data?.monthly_quota ?? data?.monthly_limit ?? data?.credits_limit ?? DEFAULT_FREE_CREDIT_LIMIT),
        monthly_quota: Number(data?.monthly_quota ?? data?.monthly_limit ?? data?.credits_limit ?? DEFAULT_FREE_CREDIT_LIMIT),
        topup_credits_balance: Number(data?.topup_credits_balance ?? 0),
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
        email: String(data?.email ?? prev.email ?? '').trim().toLowerCase(),
        is_admin: Boolean(data?.is_admin ?? prev.is_admin ?? false),
        last_login_at: data?.last_login_at ?? prev.last_login_at ?? null,
      }
    })
    localStorage.setItem('lf_credits', String(Number(data?.credits_balance ?? 0)))
    localStorage.setItem('lf_credits_balance', String(Number(data?.credits_balance ?? 0)))
    localStorage.setItem('lf_topup_credits_balance', String(Number(data?.topup_credits_balance ?? 0)))
    localStorage.setItem('lf_credits_limit', String(Number(data?.monthly_quota ?? data?.monthly_limit ?? data?.credits_limit ?? DEFAULT_FREE_CREDIT_LIMIT)))
    localStorage.setItem('lf_plan_key', String(data?.plan_key ?? 'free').toLowerCase().trim() || 'free')
    localStorage.setItem('lf_plan_name', String(data?.currentPlanName ?? 'Free Plan').trim() || 'Free Plan')
    localStorage.setItem('lf_is_subscribed', String(Boolean(data?.isSubscribed ?? data?.subscription_active ?? false)))
    localStorage.setItem('lf_average_deal_value', String(Number(data?.average_deal_value ?? DEFAULT_AVERAGE_DEAL_VALUE)))
    localStorage.setItem('lf_niche', String(data?.niche ?? '').trim())
    localStorage.setItem('lf_is_admin', String(Boolean(data?.is_admin ?? false)))
  }, [])

  const applyCreditsData = useCallback((data) => {
    if (!data || typeof data !== 'object') return
    const nextBalance = Number(data?.credits_balance)
    const nextLimit = Number(data?.credits_limit)
    const nextTopup = Math.max(0, Number(data?.topup_credits_balance || 0))
    if (!Number.isFinite(nextBalance)) return

    const safeBalance = Math.max(0, nextBalance)
    const safeLimit = Number.isFinite(nextLimit) ? Math.max(1, nextLimit) : DEFAULT_FREE_CREDIT_LIMIT
    setUser((prev) => ({
      ...prev,
      credits: safeBalance,
      credits_balance: safeBalance,
      credits_limit: safeLimit,
      monthly_limit: safeLimit,
      monthly_quota: safeLimit,
      topup_credits_balance: nextTopup,
    }))
    localStorage.setItem('lf_credits', String(safeBalance))
    localStorage.setItem('lf_credits_balance', String(safeBalance))
    localStorage.setItem('lf_credits_limit', String(safeLimit))
    localStorage.setItem('lf_topup_credits_balance', String(nextTopup))
    writeCreditsSwrCache({
      credits_balance: safeBalance,
      credits_limit: safeLimit,
      topup_credits_balance: nextTopup,
      updated_at: Date.now(),
    })
  }, [])

  const fetchUserProfileApi = useCallback(async () => {
    const token = getStoredValue('lf_token')
    if (!token) return null
    return fetchJson('/api/auth/profile', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token }),
    })
  }, [])

  const profileQuery = useQuery({
    queryKey: [PROFILE_QUERY_BASE_KEY, sessionToken || 'anon'],
    enabled: hasSessionToken,
    queryFn: fetchUserProfileApi,
    staleTime: 15_000,
    gcTime: 10 * 60_000,
    refetchInterval: 30_000,
    retry: 1,
  })

  useEffect(() => {
    if (!hasSessionToken) return
    if (profileQuery.data) {
      applyUserProfileData(profileQuery.data)
      setProfileLoadedFromApi(true)
      setProfileHydrated(true)
      return
    }
    if (profileQuery.error && isAuthInvalidError(profileQuery.error)) {
      void forceLogoutToLogin('profile_missing_or_invalid')
      return
    }
    if (!profileQuery.isFetching) {
      setProfileHydrated(true)
    }
  }, [applyUserProfileData, hasSessionToken, profileQuery.data, profileQuery.error, profileQuery.isFetching])

  const creditsQuery = useQuery({
    queryKey: [USER_CREDITS_QUERY_BASE_KEY, sessionToken || 'anon'],
    enabled: hasSessionToken,
    queryFn: () => fetchJson('/api/user/credits', { method: 'GET', timeoutMs: 4500 }),
    staleTime: 10_000,
    gcTime: 10 * 60_000,
    refetchInterval: 20_000,
    retry: 1,
    initialData: () => {
      const cache = readCreditsSwrCache()
      if (!cache || typeof cache !== 'object') return undefined
      const cachedBalance = Number(cache?.credits_balance)
      const cachedLimit = Number(cache?.credits_limit)
      if (!Number.isFinite(cachedBalance) && !Number.isFinite(cachedLimit)) return undefined
      return {
        credits_balance: Number.isFinite(cachedBalance) ? Math.max(0, cachedBalance) : 0,
        credits_limit: Number.isFinite(cachedLimit) ? Math.max(1, cachedLimit) : DEFAULT_FREE_CREDIT_LIMIT,
        topup_credits_balance: Math.max(0, Number(cache?.topup_credits_balance || 0)),
      }
    },
  })

  useEffect(() => {
    if (!hasSessionToken) return
    if (creditsQuery.data) {
      applyCreditsData(creditsQuery.data)
    }
  }, [applyCreditsData, creditsQuery.data, hasSessionToken])

  const refreshUserProfile = useCallback(async () => {
    if (!hasSessionToken) return null
    try {
      const data = await queryClient.fetchQuery({
        queryKey: [PROFILE_QUERY_BASE_KEY, sessionToken || 'anon'],
        queryFn: fetchUserProfileApi,
        staleTime: 0,
      })
      if (data) {
        applyUserProfileData(data)
        setProfileLoadedFromApi(true)
        setProfileHydrated(true)
      }
      return data
    } catch (error) {
      if (isAuthInvalidError(error)) {
        await forceLogoutToLogin('profile_missing_or_invalid')
        return null
      }
      setProfileHydrated(true)
      return null
    }
  }, [applyUserProfileData, fetchUserProfileApi, hasSessionToken, queryClient, sessionToken])

  refreshUserProfileRef.current = refreshUserProfile

  const refreshAdminOverview = useCallback(async ({ silent = false } = {}) => {
    if (!isAdminUser) return null
    if (!silent) setAdminLoading(true)
    try {
      const data = await fetchJson('/api/admin/overview')
      setAdminOverview({
        stats: {
          total_users: Number(data?.stats?.total_users || 0),
          total_revenue: Number(data?.stats?.total_revenue || data?.stats?.mrr || 0),
          total_leads: Number(data?.stats?.total_leads || 0),
        },
        scraper: {
          health: String(data?.scraper?.health || 'unknown').toLowerCase(),
          last_status: String(data?.scraper?.last_status || 'unknown').toLowerCase(),
          last_error: String(data?.scraper?.last_error || ''),
          last_updated_at: data?.scraper?.last_updated_at || null,
        },
        users: Array.isArray(data?.users) ? data.users : [],
        transactions: Array.isArray(data?.transactions) ? data.transactions : [],
        top_scrapers: Array.isArray(data?.top_scrapers) ? data.top_scrapers : [],
        lead_quality: {
          success_rate: Number(data?.lead_quality?.success_rate || 0),
          successful: Number(data?.lead_quality?.successful || 0),
          attempted: Number(data?.lead_quality?.attempted || 0),
        },
        logs: Array.isArray(data?.logs) ? data.logs : [],
        notification: {
          active: Boolean(data?.notification?.active),
          message: String(data?.notification?.message || ''),
          updated_at: data?.notification?.updated_at || null,
        },
        ai_signals: {
          enabled: Boolean(data?.ai_signals?.enabled ?? true),
          updated_at: data?.ai_signals?.updated_at || null,
          updated_by: String(data?.ai_signals?.updated_by || ''),
        },
      })
      setGlobalNoticeForm({
        message: String(data?.notification?.message || ''),
        active: Boolean(data?.notification?.active),
      })
      setAiSignalsEnabledForm(Boolean(data?.ai_signals?.enabled ?? true))
      return data
    } catch (error) {
      if (!silent) {
        toast.error(error?.message || 'Failed to load admin dashboard')
      }
      return null
    } finally {
      if (!silent) setAdminLoading(false)
    }
  }, [isAdminUser])

  const adminToggleBlock = useCallback(async (userRow, blocked) => {
    if (!isAdminUser) return
    const reason = blocked ? window.prompt('Reason for block (optional):', userRow?.blocked_reason || '') || '' : ''
    setAdminLoading(true)
    try {
      await fetchJson(`/api/admin/users/${encodeURIComponent(userRow.id)}/block`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ blocked, reason }),
      })
      toast.success(blocked ? 'User blocked' : 'User unblocked')
      await refreshAdminOverview({ silent: true })
    } catch (error) {
      toast.error(error?.message || 'Failed to update block state')
    } finally {
      setAdminLoading(false)
    }
  }, [isAdminUser, refreshAdminOverview])

  const adminImpersonate = useCallback(async (userRow) => {
    if (!isAdminUser) return
    const confirmed = window.confirm(`Impersonate ${userRow?.email || 'this user'}?`)
    if (!confirmed) return
    setAdminLoading(true)
    try {
      const data = await fetchJson(`/api/admin/users/${encodeURIComponent(userRow.id)}/impersonate`, {
        method: 'POST',
      })
      localStorage.setItem('lf_token', String(data?.token || ''))
      localStorage.setItem('lf_email', String(data?.email || ''))
      toast.success(`Now impersonating ${String(data?.email || '')}`)
      window.location.assign('/app')
    } catch (error) {
      toast.error(error?.message || 'Impersonation failed')
    } finally {
      setAdminLoading(false)
    }
  }, [isAdminUser])

  const adminResetPassword = useCallback(async (userRow) => {
    if (!isAdminUser) return
    const confirmed = window.confirm(`Send password reset email to ${userRow?.email || 'user'}?`)
    if (!confirmed) return
    setAdminLoading(true)
    try {
      await fetchJson(`/api/admin/users/${encodeURIComponent(userRow.id)}/reset-password`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      })
      toast.success('Password reset email sent')
    } catch (error) {
      toast.error(error?.message || 'Failed to send reset email')
    } finally {
      setAdminLoading(false)
    }
  }, [isAdminUser])

  const adminUpdatePlan = useCallback(async (event) => {
    event.preventDefault()
    if (!isAdminUser) return
    if (!adminPlanForm.userId) {
      toast.error('Select a user')
      return
    }
    setAdminLoading(true)
    try {
      await fetchJson(`/api/admin/users/${encodeURIComponent(adminPlanForm.userId)}/plan`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ plan_key: adminPlanForm.planKey }),
      })
      toast.success('Plan updated')
      await refreshAdminOverview({ silent: true })
    } catch (error) {
      toast.error(error?.message || 'Failed to update plan')
    } finally {
      setAdminLoading(false)
    }
  }, [adminPlanForm, isAdminUser, refreshAdminOverview])

  const adminRestartScrapers = useCallback(async () => {
    if (!isAdminUser) return
    setAdminLoading(true)
    try {
      await fetchJson('/api/admin/scrapers/restart', { method: 'POST' })
      toast.success('Scraper restart signal sent')
      await refreshAdminOverview({ silent: true })
    } catch (error) {
      toast.error(error?.message || 'Failed to restart scrapers')
    } finally {
      setAdminLoading(false)
    }
  }, [isAdminUser, refreshAdminOverview])

  const adminSaveGlobalNotification = useCallback(async (event) => {
    event.preventDefault()
    if (!isAdminUser) return
    setAdminLoading(true)
    try {
      const data = await fetchJson('/api/admin/notification', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: String(globalNoticeForm.message || '').trim(),
          active: Boolean(globalNoticeForm.active),
        }),
      })
      setGlobalBanner(data?.notification || { active: false, message: '', updated_at: null })
      toast.success('Global notification updated')
      await refreshAdminOverview({ silent: true })
    } catch (error) {
      toast.error(error?.message || 'Failed to update notification')
    } finally {
      setAdminLoading(false)
    }
  }, [globalNoticeForm, isAdminUser, refreshAdminOverview])

  const adminSaveAiSignalsToggle = useCallback(async (event) => {
    event.preventDefault()
    if (!isAdminUser) return
    setAdminLoading(true)
    try {
      await fetchJson('/api/admin/ai-signals', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled: Boolean(aiSignalsEnabledForm) }),
      })
      toast.success(`AI Signals ${aiSignalsEnabledForm ? 'enabled' : 'disabled'} globally`)
      await refreshAdminOverview({ silent: true })
    } catch (error) {
      toast.error(error?.message || 'Failed to update AI signals toggle')
    } finally {
      setAdminLoading(false)
    }
  }, [aiSignalsEnabledForm, isAdminUser, refreshAdminOverview])

  const refreshGlobalNotification = useCallback(async () => {
    try {
      const data = await fetchJson('/api/system/notification')
      setGlobalBanner({
        active: Boolean(data?.active),
        message: String(data?.message || ''),
        updated_at: data?.updated_at || null,
      })
    } catch {
      // Keep quiet for non-critical banner polling.
    }
  }, [])

  useEffect(() => {
    void refreshGlobalNotification()
    const timer = window.setInterval(() => {
      void refreshGlobalNotification()
    }, 30000)
    return () => window.clearInterval(timer)
  }, [refreshGlobalNotification])

  const syncBillingStateAfterCheckout = useCallback(async (rawPlanKey = '') => {
    const normalizedPlanKey = String(rawPlanKey || '').trim().toLowerCase()
    const expectedPlan = SUBSCRIPTION_PLAN_DETAILS[normalizedPlanKey]

    const retryDelays = [0, 1500, 3500, 6000, 9000]
    for (const delayMs of retryDelays) {
      if (delayMs > 0) {
        await sleep(delayMs)
      }
      const data = await refreshUserProfile()
      if (!expectedPlan) {
        if (data) {
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
  }, [refreshUserProfile])

  useEffect(() => {
    const checkoutStatus = String(searchParams.get('checkout') || '').trim().toLowerCase()
    const topupStatus = String(searchParams.get('topup') || '').trim().toLowerCase()
    const paymentStatus = String(searchParams.get('payment') || '').trim().toLowerCase()
    const storedCheckoutPlanKey = (() => {
      try {
        return String(window.localStorage.getItem('lf_pending_checkout_plan') || '').trim().toLowerCase()
      } catch {
        return ''
      }
    })()
    const storedTopupCredits = (() => {
      try {
        return Number(window.localStorage.getItem('lf_pending_topup_credits') || 0)
      } catch {
        return 0
      }
    })()
    const checkoutPlanKey = String(searchParams.get('plan') || storedCheckoutPlanKey || '').trim().toLowerCase()
    const topupCreditsParam = Number(searchParams.get('topup_credits') || storedTopupCredits || 0)
    const inferredTopupSuccess = !topupStatus && paymentStatus === 'success' && storedTopupCredits > 0
    const inferredCheckoutSuccess = !checkoutStatus && paymentStatus === 'success' && Boolean(checkoutPlanKey) && !inferredTopupSuccess
    const resolvedCheckoutStatus = checkoutStatus || (inferredCheckoutSuccess ? 'success' : '')
    const resolvedTopupStatus = topupStatus || (inferredTopupSuccess ? 'success' : '')

    if (!resolvedCheckoutStatus && !resolvedTopupStatus && !paymentStatus) {
      checkoutRedirectHandledRef.current = ''
      return
    }

    const redirectSignature = [
      resolvedCheckoutStatus,
      resolvedTopupStatus,
      paymentStatus,
      checkoutPlanKey,
      String(topupCreditsParam),
    ].join('|')
    if (checkoutRedirectHandledRef.current === redirectSignature) {
      return
    }
    checkoutRedirectHandledRef.current = redirectSignature

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
      nextParams.delete('payment')
      setSearchParams(nextParams, { replace: true })
      try {
        window.localStorage.removeItem('lf_pending_checkout_plan')
        window.localStorage.removeItem('lf_pending_topup_package')
        window.localStorage.removeItem('lf_pending_topup_credits')
      } catch {
        // Ignore storage failures.
      }
    }

    const runCheckoutRedirectSync = async () => {
      if (resolvedCheckoutStatus === 'success') {
        const nextPlanName = SUBSCRIPTION_PLAN_DETAILS[checkoutPlanKey]?.displayName || 'your subscription'
        toast.success(`Payment successful — ${nextPlanName} is now active`)
        await syncBillingStateAfterCheckout(checkoutPlanKey)
      } else if (resolvedCheckoutStatus === 'cancel') {
        toast('Subscription checkout cancelled', { icon: 'ℹ️' })
      }

      if (resolvedTopupStatus === 'success') {
        toast.success('Top-up payment received')
        await syncBillingStateAfterCheckout('')
      } else if (resolvedTopupStatus === 'cancel') {
        toast('Top-up checkout cancelled', { icon: 'ℹ️' })
      }

      if (paymentStatus === 'success' && resolvedCheckoutStatus !== 'success' && resolvedTopupStatus !== 'success') {
        toast.success('Payment successful. Billing updated.')
        await syncBillingStateAfterCheckout('')
      } else if (paymentStatus === 'cancelled' && resolvedCheckoutStatus !== 'cancel' && resolvedTopupStatus !== 'cancel') {
        toast('Payment cancelled', { icon: 'ℹ️' })
      }

      finalizeCheckoutRedirect()
    }

    void runCheckoutRedirectSync().catch((error) => {
      checkoutRedirectHandledRef.current = ''
      const message = error instanceof Error ? error.message : 'Could not finalize checkout state'
      setLastError(message)
      toast.error(message)
    })
    return () => {
      cancelled = true
    }
  }, [
    searchParams,
    setSearchParams,
    syncBillingStateAfterCheckout,
    setLastError,
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
    const selectedPackage = TOP_UP_PACKAGES.find((pkg) => pkg.id === normalizedPackageId)
    const selectedCredits = Math.max(0, Number(selectedPackage?.credits || 0))
    setTopUpLoadingPackageId(normalizedPackageId)
    try {
      const checkoutUrl = await requestTopUpCheckoutUrl(normalizedPackageId, { markPreparing: true })
      if (checkoutUrl) {
        try {
          window.localStorage.setItem('lf_pending_topup_package', normalizedPackageId)
          window.localStorage.setItem('lf_pending_topup_credits', String(selectedCredits))
        } catch {
          // Ignore storage failures.
        }
        navigateToCheckoutWithFallback(checkoutUrl)
        return
      }
      toast.error('Could not open Stripe checkout.')
    } catch (error) {
      try {
        window.localStorage.removeItem('lf_pending_topup_package')
        window.localStorage.removeItem('lf_pending_topup_credits')
      } catch {
        // Ignore storage failures.
      }
      const message = error instanceof Error ? error.message : 'Top-up checkout failed.'
      setLastError(message)
      toast.error(message)
    } finally {
      setTopUpLoadingPackageId('')
    }
  }, [navigateToCheckoutWithFallback, requestTopUpCheckoutUrl, setLastError])

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
          developer_webhook_url: String(configForm.developer_webhook_url || '').trim() || null,
          developer_score_drop_threshold: Number(configForm.developer_score_drop_threshold || 0),
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
        body: JSON.stringify({ target: destination, kind: 'target' }),
      })
      const exportedCount = Number(result?.exported ?? result?.exported_count ?? 0)
      toast.success(`${destination === 'hubspot' ? 'HubSpot' : 'Google Sheets'} export sent (${exportedCount})`)
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Webhook export failed'
      setLastError(message)
      toast.error(message)
    } finally {
      setWebhookExporting('')
    }
  }

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
      console.error('[weekly-report] fetch failed:', error)
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
      console.error('[monthly-report] fetch failed:', error)
      return null
    } finally {
      if (!options.silent) {
        setLoadingMonthlyReport(false)
      }
    }
  }, [featureAccess.advanced_reporting])

  const refreshAiCostReport = useCallback(async (options = {}) => {
    if (!featureAccess.advanced_reporting) {
      setAiCostReport(null)
      return null
    }

    if (!options.silent) {
      setLoadingAiCostReport(true)
    }
    try {
      const data = await fetchJson('/api/reporting/ai-costs?limit=8')
      setAiCostReport(data)
      return data
    } catch (error) {
      console.error('[ai-cost-report] fetch failed:', error)
      return null
    } finally {
      if (!options.silent) {
        setLoadingAiCostReport(false)
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
      console.error('[client-folders] fetch failed:', error)
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
      console.error('[client-dashboard] fetch failed:', error)
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
      console.error('[stats] fetch failed:', error)
    } finally {
      setStatsHydrated(true)
    }
  }

  const refreshCreditsBalance = useCallback(async (options = {}) => {
    if (!hasSessionToken) return null
    try {
      const timeoutMs = Number(options?.timeoutMs || 4500)
      const data = await queryClient.fetchQuery({
        queryKey: [USER_CREDITS_QUERY_BASE_KEY, sessionToken || 'anon'],
        queryFn: () => fetchJson('/api/user/credits', { method: 'GET', timeoutMs }),
        staleTime: 0,
      })
      applyCreditsData(data)
      return data
    } catch {
      return null
    }
  }, [applyCreditsData, hasSessionToken, queryClient, sessionToken])

  const fetchTaskState = useCallback(async (force = false) => {
    // Exponential backoff — skip if we are in a cooldown period
    if (!force && Date.now() < taskFetchBackoffUntilRef.current) return
    try {
      const trackedScrapeId = Number(activeScrapeTaskIdRef.current || 0)
      let trackedTaskMissing = false
      const trackedTaskPromise = trackedScrapeId > 0
        ? fetchJson(`/api/tasks/${trackedScrapeId}`).catch((error) => {
          if (Number(error?.status || 0) === 404) {
            trackedTaskMissing = true
          }
          return null
        })
        : Promise.resolve(null)
      const data = await fetchJson('/api/tasks')
      const trackedTask = await trackedTaskPromise
      taskFetchFailCountRef.current = 0
      taskFetchBackoffUntilRef.current = 0
      setTasks((prev) => {
        const incoming = data.tasks && typeof data.tasks === 'object' ? data.tasks : {}
        const history = Array.isArray(data.history) ? data.history : []
        const next = { ...incoming }

        const trackedScrapeId = Number(activeScrapeTaskIdRef.current || prev?.scrape?.id || 0)
        if (trackedScrapeId > 0) {
          const explicitTrackedCandidate = trackedTask
            && Number(trackedTask?.id || 0) === trackedScrapeId
            && String(trackedTask?.task_type || '').toLowerCase() === 'scrape'
              ? {
                ...trackedTask,
                task_type: 'scrape',
                running: ['queued', 'running', 'processing', 'pending'].includes(String(trackedTask.status || '').toLowerCase()),
              }
              : null
          const nextScrapeCandidate = next?.scrape && Number(next.scrape?.id || 0) === trackedScrapeId ? next.scrape : null
          const historyScrapeCandidate = history.find((entry) => Number(entry?.id || 0) === trackedScrapeId)
          const prevScrapeCandidate = prev?.scrape && Number(prev.scrape?.id || 0) === trackedScrapeId
            ? prev.scrape
            : null

          if (explicitTrackedCandidate) {
            next.scrape = explicitTrackedCandidate
          } else if (nextScrapeCandidate) {
            next.scrape = nextScrapeCandidate
          } else if (historyScrapeCandidate && String(historyScrapeCandidate.task_type || '').toLowerCase() === 'scrape') {
            next.scrape = {
              ...historyScrapeCandidate,
              task_type: 'scrape',
              running: ['queued', 'running', 'processing', 'pending'].includes(String(historyScrapeCandidate.status || '').toLowerCase()),
            }
          } else if (prevScrapeCandidate) {
            const prevStatus = String(prevScrapeCandidate.status || '').toLowerCase().trim()
            if (['queued', 'running', 'processing', 'pending'].includes(prevStatus)) {
              next.scrape = prevScrapeCandidate
            }
          }
        }

        const prevScrape = prev?.scrape
        const nextScrape = next?.scrape
        const prevScrapeStatus = String(prevScrape?.status || '').toLowerCase().trim()
        const nextScrapeStatus = String(nextScrape?.status || '').toLowerCase().trim()
        const prevScrapeSticky = ['queued', 'running', 'processing', 'pending', 'failed'].includes(prevScrapeStatus)
        const nextScrapeMissingOrIdle = !nextScrape || !nextScrapeStatus || nextScrapeStatus === 'idle'
        if (prevScrapeSticky && nextScrapeMissingOrIdle) {
          next.scrape = prevScrape
        }
        return next
      })
      if (trackedTaskMissing) {
        setActiveScrapeTaskId(null)
      }
      setTaskHistory(Array.isArray(data.history) ? data.history : [])
    } catch (error) {
      const fails = taskFetchFailCountRef.current + 1
      taskFetchFailCountRef.current = fails
      const statusCode = Number(error?.status || 0)
      const liveScrapeStatus = String(tasksRef.current?.scrape?.status || '').toLowerCase().trim()
      const liveEnrichStatus = String(tasksRef.current?.enrich?.status || '').toLowerCase().trim()
      const snapshotEnrichStatus = String(enrichTaskSnapshotRef.current?.status || '').toLowerCase().trim()
      const scrapePossiblyActive = ['queued', 'running', 'processing', 'pending'].includes(liveScrapeStatus)
      const enrichPossiblyActive = enrichRunRequestedRef.current
        || ['queued', 'running'].includes(liveEnrichStatus)
        || ['queued', 'running'].includes(snapshotEnrichStatus)

      // Keep polling tighter while enrichment may still be active.
      const maxBackoffMs = (scrapePossiblyActive || enrichPossiblyActive) ? 15000 : 5 * 60 * 1000
      const delayMs = Math.min(3000 * Math.pow(2, fails - 1), maxBackoffMs)
      taskFetchBackoffUntilRef.current = Date.now() + delayMs

      // Transient backend hiccup: retry quickly without dropping progress UI state.
      if (statusCode >= 500 && (scrapePossiblyActive || Number(activeScrapeTaskIdRef.current || 0) > 0)) {
        if (scrape500RetryTimerRef.current) {
          window.clearTimeout(scrape500RetryTimerRef.current)
        }
        scrape500RetryTimerRef.current = window.setTimeout(() => {
          scrape500RetryTimerRef.current = null
          void fetchTaskState(true)
        }, 2000)
      }
      console.error('[tasks] fetch failed:', error)
    }
  }, [])

  useEffect(() => {
    const id = window.setInterval(() => {
      const trackedTaskId = Number(activeScrapeTaskIdRef.current || 0)
      const status = String(scrapeStatusRef.current || 'idle').toLowerCase().trim()
      const isTerminal = ['completed', 'failed', 'cancelled', 'stopped', 'idle'].includes(status)
      if (trackedTaskId <= 0 && isTerminal) return
      void fetchTaskState(true)
    }, 2000)

    return () => window.clearInterval(id)
  }, [fetchTaskState])

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

  const fetchMailerCampaignStats = useCallback(async ({ silent = false } = {}) => {
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
  }, [setLastError])

  const refreshSignalLayer = useCallback(async ({ forceRefresh = true, silentNiche = false } = {}) => {
    await Promise.allSettled([
      refreshCreditsBalance({ timeoutMs: 2500 }),
      fetchMailerCampaignStats({ silent: true }),
      fetchNicheAdvice({ silent: silentNiche, forceRefresh }),
    ])
  }, [refreshCreditsBalance, fetchMailerCampaignStats, fetchNicheAdvice])

  useEffect(() => {
    if (activeTab !== 'leads') {
      abortRequestGroup('leads-list')
    }
    if (activeTab !== 'workers' && activeTab !== 'tasks' && activeTab !== 'history') {
      abortRequestGroup('workers-list')
    }
  }, [activeTab])

  useEffect(() => {
    const initialRequests = [
      checkHealth(),
      refreshConfigHealth(),
      refreshStats(),
      refreshCreditsBalance({ timeoutMs: 2500 }),
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
      initialRequests.push(refreshWeeklyReport({ silent: true }), refreshMonthlyReport({ silent: true }), refreshAiCostReport({ silent: true }))
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
      const requests = [checkHealth(), refreshConfigHealth(), refreshStats(), refreshCreditsBalance({ timeoutMs: 2500 })]

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
        requests.push(refreshWeeklyReport({ silent: true }), refreshMonthlyReport({ silent: true }), refreshAiCostReport({ silent: true }))
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
  }, [activeTab, fetchMailerCampaignStats, fetchTaskState, refreshAiCostReport, refreshClientDashboard, refreshClientFolders, refreshCreditsBalance, refreshLeads, refreshMonthlyReport, refreshWeeklyReport])

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
      if (error?.name === 'AbortError') return
      console.error('[workers] fetch failed:', error)
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
      console.error('[delivery-tasks] fetch failed:', error)
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
      const niche = String(selectedUserNiche || '').trim()
      const endpoint = niche
        ? `/api/leads/qualify?niche=${encodeURIComponent(niche)}`
        : '/api/leads/qualify'
      let data = null
      let lastError = null
      for (let attempt = 0; attempt < 2; attempt += 1) {
        try {
          data = await fetchJson(endpoint, {
            abortKey: 'qualifier-refresh',
            bypassCache: true,
            timeoutMs: 45000,
          })
          break
        } catch (attemptError) {
          lastError = attemptError
          if (attempt < 1) {
            await new Promise((resolve) => window.setTimeout(resolve, 450))
            continue
          }
        }
      }
      if (!data) throw lastError || new Error('Could not load qualifier data')
      setQualifierData({ loading: false, data, error: '' })
      if (!silent) toast.success('Lead Qualifier refreshed')
    } catch (err) {
      const rawMsg = err instanceof Error ? err.message : 'Could not load qualifier data'
      const msg = String(rawMsg || '').toLowerCase().includes('aborted')
        ? 'Analysis request timed out. Retried automatically; showing latest available qualification results.'
        : rawMsg
      setQualifierData((prev) => ({ loading: false, data: prev.data || null, error: msg }))
      if (!silent) toast.error(msg)
    }
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

    const basePool = [...recommendations, ...fromPerformance]
    const pool = basePool.filter((candidate) => {
      const candidateCode = String(candidate?.country_code || selectedSignalCountryCode).toUpperCase()
      return candidateCode === selectedSignalCountryCode
    })
    const effectivePool = pool.length > 0 ? pool : basePool
    const dedup = new Map()

    effectivePool.forEach((candidate) => {
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
    if (tabName === 'admin' && !isAdminUser) {
      toast.error('Admin access required')
      return
    }
    setActiveTab(tabName)

    if (tabName === 'mail' || tabName === 'config') {
      void loadConfigForm()
      if (tabName === 'mail') {
        void fetchMailerCampaignStats({ silent: true })
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

  function handleMainNavigation(tabName) {
    if (tabName === 'admin') {
      navigate('/admin')
      openMainTab('admin')
      return
    }
    if (window.location.pathname === '/admin') {
      navigate('/app')
    }
    openMainTab(tabName)
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
      setIsAnalyzing(true)
    }
    try {
      let data
      if (action === 'enrich') {
        const token = getStoredValue('lf_token')
        const headers = {
          'Content-Type': 'application/json',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        }
        const directBaseRaw = String(import.meta.env.VITE_DIRECT_RAILWAY_URL || import.meta.env.VITE_API_BASE_URL || '').trim()
        const directBase = directBaseRaw ? directBaseRaw.replace(/\/$/, '') : ''
        const directUrl = directBase && endpoint.startsWith('/api') ? `${directBase}${endpoint}` : ''

        let response
        try {
          response = await axios.post(endpoint, payload, { headers })
        } catch (primaryError) {
          const status = axios.isAxiosError(primaryError) ? Number(primaryError.response?.status || 0) : 0
          const shouldTryDirect = Boolean(
            directUrl
            && directUrl !== endpoint
            && (status >= 500 || status === 0),
          )
          if (!shouldTryDirect) {
            throw primaryError
          }
          response = await axios.post(directUrl, payload, { headers })
        }
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

      const creditErrorText = String(rawMessage || friendlyMessage || '').toLowerCase()
      if (status === 403 && (creditErrorText.includes('credit') || creditErrorText.includes('out of credits') || creditErrorText.includes('insufficient credits'))) {
        void handleTopUpClick()
      }

      const shouldRetry = action === 'enrich' && retries < 1 && status >= 500
      if (shouldRetry) {
        window.setTimeout(() => {
          void startTask(action, endpoint, payload, retries + 1)
        }, 5000)
      } else if (action === 'enrich') {
        setEnrichRunRequested(false)
      }
    } finally {
      if (action === 'enrich') {
        setIsAnalyzing(false)
      }
      setPendingRequest('')
    }
  }

  function resetEnrichUiState() {
    setEnrichRetrySeconds(0)
    setEnrichRunRequested(false)
    setIsAnalyzing(false)
    setPendingRequest((prev) => (prev === 'enrich' ? '' : prev))
    setTasks((prev) => ({
      ...prev,
      enrich: {
        ...(prev?.enrich || getIdleTask('enrich')),
        running: false,
        status: 'idle',
      },
    }))
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
      const message = error instanceof Error ? error.message : 'Retry failed'
      setLastError(message)
      toast.error(message)
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
      const message = error instanceof Error ? error.message : 'Manual lead creation failed'
      setLastError(message)
      toast.error(message)
    } finally {
      setPendingRequest('')
    }
  }

  async function updateLeadStatus(leadId, nextStatus) {
    const normalizedNextStatus = String(nextStatus || '').trim().toLowerCase()
    const wasPaid = ['paid', 'won', 'won paid', 'won (paid)'].includes(normalizedNextStatus)
    setPendingStatusLeadId(leadId)
    setLastError('')
    try {
      await fetchJson(`/api/leads/${leadId}/status`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: nextStatus }),
      })
      toast.success(`Lead \u2192 ${nextStatus}`)
      if (wasPaid) {
        shootConfetti()
        const lead = (Array.isArray(leads) ? leads : []).find((item) => Number(item?.id) === Number(leadId))
        const estimateInput = window.prompt('Congrats! What is the estimated deal value?')
        const estimate = Number(String(estimateInput || '').replace(',', '.').trim())
        if (Number.isFinite(estimate) && estimate > 0) {
          try {
            await fetchJson('/api/revenue/won-deal', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                lead_id: Number(leadId),
                amount: estimate,
                currency: 'EUR',
                note: `Captured from ${nextStatus} transition`,
              }),
            })
            toast.success(`Estimated deal value saved: ${formatCurrencyEur(estimate)}`)
          } catch (error) {
            const message = error instanceof Error ? error.message : 'Could not save estimated deal value'
            toast.error(message)
          }
        } else if (String(estimateInput || '').trim()) {
          toast.error('Please enter a valid positive number for deal value')
        }
        if (!lead && !estimateInput) {
          // no-op: user dismissed prompt and lead may be stale while list is refreshing
        }
      }
      await Promise.allSettled([refreshLeads(), refreshStats(), refreshWorkers(), refreshDeliveryTasks()])
      return true
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Lead status update failed'
      setLastError(message)
      toast.error(message)
      return false
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
      const message = error instanceof Error ? error.message : 'Tier update failed'
      setLastError(message)
      toast.error(message)
    } finally {
      setPendingTierLeadId(null)
    }
  }

  async function removeLeadFromActiveView(lead) {
    if (!lead?.id) return
    const confirmed = window.confirm(`Remove ${lead.business_name || 'this lead'} from active view?`)
    if (!confirmed) return

    setPendingBlacklistLeadId(lead.id)
    setLastError('')
    try {
      await fetchJson(`/api/leads/${lead.id}/status`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: 'Blacklisted' }),
      })
      setLeads((prev) => prev.filter((item) => Number(item.id) !== Number(lead.id)))
      toast.success('Lead removed from active view')
      await Promise.allSettled([refreshLeads(), refreshStats(), refreshBlacklistEntries()])
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not remove lead'
      setLastError(message)
      toast.error(message)
    } finally {
      setPendingBlacklistLeadId(null)
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

  async function generateShareableLeadReport(lead) {
    if (!lead?.id) return
    const leadId = Number(lead.id)
    if (!Number.isFinite(leadId) || leadId <= 0) return

    setShareReportStateByLeadId((prev) => ({
      ...prev,
      [leadId]: {
        ...(prev[leadId] || {}),
        generating: true,
      },
    }))

    try {
      const result = await fetchJson(`/api/leads/${leadId}/report/share`, { method: 'POST' })
      const shareUrl = String(result?.share_url || '').trim()
      if (!shareUrl) throw new Error('Share link could not be generated')

      setShareReportStateByLeadId((prev) => ({
        ...prev,
        [leadId]: {
          ...(prev[leadId] || {}),
          shareUrl,
          isActive: true,
          generating: false,
          revoking: false,
        },
      }))

      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(shareUrl)
      }
      toast.success('Shareable report link copied')
    } catch (error) {
      setShareReportStateByLeadId((prev) => ({
        ...prev,
        [leadId]: {
          ...(prev[leadId] || {}),
          generating: false,
        },
      }))
      const message = error instanceof Error ? error.message : 'Could not generate share link'
      toast.error(message)
    }
  }

  function openLeadGapReportPreview(lead) {
    if (!lead?.id) return
    const leadId = Number(lead.id)
    if (!Number.isFinite(leadId) || leadId <= 0) return
    const reportUrl = `/api/leads/${leadId}/report`
    const win = window.open(reportUrl, '_blank', 'noopener,noreferrer')
    if (!win) {
      toast.error('Popup blocked. Please allow popups for this site.')
    }
  }

  async function revokeShareableLeadReport(lead) {
    if (!lead?.id) return
    const leadId = Number(lead.id)
    if (!Number.isFinite(leadId) || leadId <= 0) return

    const previous = shareReportStateByLeadId[leadId] || { isActive: Boolean(lead?.has_active_report_share) }
    setShareReportStateByLeadId((prev) => ({
      ...prev,
      [leadId]: {
        ...(prev[leadId] || {}),
        isActive: false,
        revoking: true,
      },
    }))

    try {
      await fetchJson(`/api/leads/${leadId}/report/share`, { method: 'DELETE' })
      setShareReportStateByLeadId((prev) => ({
        ...prev,
        [leadId]: {
          ...(prev[leadId] || {}),
          isActive: false,
          revoking: false,
          revokedFlash: true,
        },
      }))
      window.setTimeout(() => {
        setShareReportStateByLeadId((prev) => ({
          ...prev,
          [leadId]: {
            ...(prev[leadId] || {}),
            revokedFlash: false,
          },
        }))
      }, 2000)
      toast.success('Share link revoked')
    } catch (error) {
      setShareReportStateByLeadId((prev) => ({
        ...prev,
        [leadId]: {
          ...previous,
          revoking: false,
        },
      }))
      const message = error instanceof Error ? error.message : 'Could not revoke share link'
      toast.error(message)
    }
  }

  async function onScrapeSubmit(e, overrides = null) {
    e?.preventDefault?.()
    const keyword = String(overrides?.keyword ?? scrapeForm.keyword ?? '').trim()
    const results = Number(overrides?.results ?? scrapeForm.results)
    const country = String(overrides?.country ?? scrapeForm.country ?? 'US')
    const headless = Boolean(overrides?.headless ?? scrapeForm.headless)
    const exportTargets = Boolean(overrides?.exportTargets ?? scrapeForm.exportTargets)
    const speedMode = Boolean(overrides?.speedMode ?? scrapeForm.speedMode)
    if (!keyword || keyword.length < 2) {
      setLastError('Keyword must be at least 2 characters')
      toast.error('Keyword required (min 2 chars)')
      return
    }
    if (hasCreditsValue && normalizedCreditsBalance < requiredScrapeCredits) {
      setShowLowCreditsModal(true)
      return
    }
    if (exportTargets && !canBulkExport) {
      toast('Auto-export unlocks on The Growth and above.', { icon: '🔒' })
      return
    }
    setPendingRequest('scrape')
    setLastError('')
    try {
      const response = await fetchJson('/api/scrape', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keyword, results, country, headless, export_targets: exportTargets, min_rating: 3.5, speed_mode: speedMode }),
      })
      const launchedTaskId = Number(response?.task_id || 0)
      if (Number.isFinite(launchedTaskId) && launchedTaskId > 0) {
        setActiveScrapeTaskId(launchedTaskId)
      }
      setTasks((prev) => ({
        ...prev,
        scrape: {
          ...(prev?.scrape || getIdleTask('scrape')),
          id: (Number.isFinite(launchedTaskId) && launchedTaskId > 0) ? launchedTaskId : prev?.scrape?.id || null,
          status: 'queued',
          running: true,
          last_request: {
            ...(prev?.scrape?.last_request || {}),
            keyword,
            results,
            country,
          },
          result: {
            phase: 'queued',
            total_to_find: Number(results || 0),
            current_found: 0,
            scanned_count: 0,
            inserted: 0,
          },
          error: null,
        },
      }))
      toast('Scrape started', { icon: '⏳' })
      void Promise.allSettled([fetchTaskState(true), refreshStats(), refreshUserProfile()])
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown API error'
      const normalizedMessage = String(message || '').toLowerCase()
      const status = error?.status ?? error?.statusCode ?? 0
      if (Number(status) === 403 && (normalizedMessage.includes('out of credits') || normalizedMessage.includes('insufficient credits'))) {
        setShowLowCreditsModal(true)
      }
      setLastError(message)
      toast.error(message)
    } finally {
      setPendingRequest('')
    }
  }

  async function runOnboardingMagicSearch({ niche, location }) {
    const normalizedNiche = String(niche || '').trim()
    const normalizedLocation = String(location || '').trim()
    if (!normalizedNiche || !normalizedLocation) {
      toast.error('Please fill niche and location first.')
      return
    }

    const keyword = `${normalizedNiche} in ${normalizedLocation}`
    setOnboardingLaunching(true)
    setScrapeForm((prev) => ({ ...prev, keyword }))
    setAiFilterPrompt(`Find ${normalizedNiche} in ${normalizedLocation} with slow websites`)
    openMainTab('leads')

    try {
      await onScrapeSubmit(null, { keyword })
      closeOnboardingWizard(true)
      toast.success('Magic Search launched. Watching live results now…')
    } finally {
      setOnboardingLaunching(false)
    }
  }
  function onEnrichSubmit(e) {
    e?.preventDefault?.()
    const requested = Number(enrichForm.limit)
    const normalizedBatchSize = Math.max(1, Math.min(Number.isFinite(requested) ? Math.floor(requested) : 50, 200))
    if (normalizedBatchSize !== requested) {
      setEnrichForm((prev) => ({ ...prev, limit: normalizedBatchSize }))
    }

    const selectedLeadIds = getEligibleEnrichmentLeadIds(normalizedBatchSize)

    if (!selectedLeadIds.length) {
      toast('No eligible leads to enrich in current view.', { icon: 'ℹ️' })
      setEnrichRunRequested(false)
      return
    }

    const requiredCredits = selectedLeadIds.length * ENRICH_CREDIT_COST_PER_LEAD
    if (creditsBalance < requiredCredits) {
      toast.error(`Not enough credits for enrichment. Need ${creditIntegerFormatter.format(requiredCredits)}, available ${creditsBalanceLabel}.`)
      void handleTopUpClick()
      setEnrichRunRequested(false)
      return
    }

    setEnrichRunRequested(true)

    setTasks((prev) => ({
      ...prev,
      enrich: {
        ...(prev?.enrich || getIdleTask('enrich')),
        status: 'queued',
        running: true,
        last_request: {
          ...(prev?.enrich?.last_request || {}),
          limit: normalizedBatchSize,
          lead_ids: selectedLeadIds,
          headless: Boolean(enrichForm.headless),
          skip_export: Boolean(enrichForm.skipExport),
        },
        result: {
          processed: 0,
          with_email: 0,
          total: selectedLeadIds.length,
          current_lead: null,
          status_message: 'Queued for enrichment...',
        },
        error: null,
      },
    }))

    const selectedNiche = String(user?.niche || getStoredValue('lf_niche') || '').trim()

    void startTask('enrich', '/api/enrich', {
      limit: normalizedBatchSize,
      lead_ids: selectedLeadIds,
      headless: Boolean(enrichForm.headless),
      skip_export: Boolean(enrichForm.skipExport),
      token: getStoredValue('lf_token') || undefined,
      user_niche: selectedNiche || undefined,
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

  async function moveLeadToMailer(lead) {
    const status = String(lead?.status || '').toLowerCase().trim()
    const statusAlreadyMoved = new Set(['emailed', 'interested', 'replied', 'meeting set', 'zoom scheduled', 'closed', 'paid'])

    if (!statusAlreadyMoved.has(status)) {
      const ok = await updateLeadStatus(lead.id, 'Emailed')
      if (!ok) return
    }

    setActiveTab('mail')
    if (lead.generated_email_body) {
      openEmailPreviewModal(lead)
      toast.success('Lead moved to Mailer and draft opened')
    } else if (!lead?.email) {
      toast('Lead has no email yet — enrich it first for AI mail generation', { icon: '⚠️' })
    } else {
      toast.success('Lead moved to Mailer')
    }
  }

  function generateQualifierLeadEmail(lead) {
    if (!lead) return
    const templateKey = mapQualifierGapToTemplateKey(lead.gold_mine_gap)
    const template = resolveSnipedTemplateForSelection(selectedUserNiche, templateKey)
    if (!template) {
      toast.error('No matching template found for this lead.')
      return
    }

    const vars = {
      BusinessName: lead.business_name || 'Business Name',
      City: lead.city || resolveLeadCityValue(lead),
      Niche: deriveLeadIndustry(lead) || selectedUserNiche || 'Local Business',
      YourName: currentUserName || 'Nejc',
    }

    const subject = replaceTemplatePlaceholders(String(template.subject || ''), vars)
    const body = replaceTemplatePlaceholders(String(template.body || ''), vars)

    setActiveTab('mail')
    setActiveMailEditorTab('live')
    setActiveLiveMailTemplateKey(templateKey)
    setEmailPreviewLead({
      businessName: lead.business_name || 'Lead',
      subject,
      body,
    })
    toast.success('Email draft generated from highest-gap template')
  }

  async function addQualifierLeadToPipeline(lead) {
    if (!lead?.id) return
    const ok = await updateLeadStatus(lead.id, 'queued_mail')
    if (ok) toast.success('Lead added to pipeline')
  }

  async function skipQualifierLead(lead) {
    if (!lead?.id) return
    const ok = await updateLeadStatus(lead.id, 'low_priority')
    if (ok) toast.success('Lead skipped for now')
  }

  const loadLeadEmailHistory = useCallback(async (leadId, options = {}) => {
    const numericLeadId = Number(leadId || 0)
    if (!Number.isFinite(numericLeadId) || numericLeadId <= 0) {
      setLeadEmailHistory({ loading: false, error: '', items: [] })
      return
    }

    const silent = Boolean(options?.silent)
    if (!silent) {
      setLeadEmailHistory((prev) => ({ ...prev, loading: true, error: '' }))
    }

    try {
      const response = await fetchJson(`/api/leads/${numericLeadId}/email-history?limit=120`, {
        bypassCache: true,
        timeoutMs: 15000,
        abortKey: `lead-email-history-${numericLeadId}`,
      })
      const items = Array.isArray(response?.items) ? response.items.slice() : []
      items.sort((a, b) => {
        const aTs = new Date(a?.timestamp || a?.created_at || 0).getTime() || 0
        const bTs = new Date(b?.timestamp || b?.created_at || 0).getTime() || 0
        return aTs - bTs
      })
      setLeadEmailHistory({ loading: false, error: '', items })
    } catch (error) {
      const detail = String(error?.message || 'Failed to load email history.').trim() || 'Failed to load email history.'
      setLeadEmailHistory((prev) => ({
        loading: false,
        error: detail,
        items: Array.isArray(prev?.items) ? prev.items : [],
      }))
    }
  }, [])

  useEffect(() => {
    const leadId = Number(leadDetailsPreviewLead?.id || 0)
    if (!Number.isFinite(leadId) || leadId <= 0) {
      setLeadEmailHistory({ loading: false, error: '', items: [] })
      return
    }
    void loadLeadEmailHistory(leadId)
  }, [leadDetailsPreviewLead?.id, loadLeadEmailHistory])

  useEffect(() => {
    const leadId = Number(leadDetailsPreviewLead?.id || 0)
    const supabaseClient = window?.supabase
    if (!Number.isFinite(leadId) || leadId <= 0 || !supabaseClient || typeof supabaseClient.channel !== 'function') {
      return undefined
    }

    const channelName = `lead-email-history-${leadId}-${Date.now()}`
    const channel = supabaseClient
      .channel(channelName)
      .on(
        'postgres_changes',
        { event: '*', schema: 'public', table: 'communications', filter: `lead_id=eq.${leadId}` },
        () => {
          void loadLeadEmailHistory(leadId, { silent: true })
        },
      )
      .subscribe()

    leadEmailHistoryRealtimeChannelRef.current = channel

    return () => {
      const existing = leadEmailHistoryRealtimeChannelRef.current
      if (existing && typeof supabaseClient.removeChannel === 'function') {
        supabaseClient.removeChannel(existing)
      }
      leadEmailHistoryRealtimeChannelRef.current = null
    }
  }, [leadDetailsPreviewLead?.id, loadLeadEmailHistory])

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

  function openLeadDetailsModal(lead) {
    setLeadDetailsPreviewLead(lead || null)
    setShowLeadScoreBreakdown(false)
  }

  function closeLeadDetailsModal() {
    setLeadDetailsPreviewLead(null)
    setShowLeadScoreBreakdown(false)
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
  const toneProfile = useMemo(() => {
    const hasManusTemplate = Boolean(resolveSnipedTemplateForSelection(selectedUserNiche, activeLiveMailTemplateKey))
    if (hasManusTemplate) {
      const preset = deriveManusTemplateToneProfile(activeLiveMailTemplateKey)
      if (preset) return preset
    }
    return deriveToneProfile(mailPreview.subject, mailPreview.body)
  }, [activeLiveMailTemplateKey, mailPreview.subject, mailPreview.body, selectedUserNiche])
  const previewSenderName = currentUserName || configForm.smtp_accounts?.[0]?.from_name || currentUserEmail || 'Your sender name'
  const previewSenderEmail = currentUserEmail || configForm.smtp_accounts?.[0]?.email || 'sender@domain.com'
  const userInitial = String(displayName || currentUserEmail || 'U').trim().charAt(0).toUpperCase() || 'U'
  const normalizedSubscriptionStatus = String(user?.subscriptionStatus || '').toLowerCase().trim()
  const lifecycleSubscriptionStatus = String(user?.subscription_status || '').toLowerCase().trim()

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
  const rawCredits = user?.credits_balance
  const rawCreditLimit = user?.monthly_quota ?? user?.monthly_limit ?? user?.credits_limit
  const hasCreditsValue = rawCredits !== null && rawCredits !== undefined && Number.isFinite(Number(rawCredits))
  const creditsBalance = hasCreditsValue ? Math.max(0, Number(rawCredits)) : undefined
  const baseCreditsLimit = Math.max(1, Number(rawCreditLimit ?? DEFAULT_FREE_CREDIT_LIMIT))
  const topupCreditsBalance = Math.max(0, Number(user?.topup_credits_balance ?? 0))
  const normalizedCreditsBalance = hasCreditsValue ? Number(creditsBalance) : 0
  const creditsLimit = baseCreditsLimit
  const creditsPercent = hasCreditsValue
    ? Math.max(0, Math.min(100, Math.round((normalizedCreditsBalance / creditsLimit) * 100)))
    : 0
  const creditsReady = !hasSessionToken || profileLoadedFromApi || hasCreditsValue
  const creditsBalanceLabel = hasCreditsValue ? formatCreditAmount(normalizedCreditsBalance, {
    thousandDecimals: 1,
    thousandMode: 'floor',
    millionDecimals: 2,
    millionMode: 'floor',
  }) : '...'
  const creditsLimitLabel = formatCreditAmount(creditsLimit, {
    thousandDecimals: 0,
    thousandMode: 'round',
    millionDecimals: 2,
    millionMode: 'round',
  })
  const requiredEnrichCreditsLabel = creditIntegerFormatter.format(requiredEnrichCredits)
  const isCreditsLoading = !hasCreditsValue && hasSessionToken && !profileLoadedFromApi
  const isOutOfCredits = hasCreditsValue && normalizedCreditsBalance === 0
  const scrapeHasInsufficientCredits = hasCreditsValue && normalizedCreditsBalance < requiredScrapeCredits
  const canRunEnrich = enrichmentEligibleLeadIds.length > 0 && (!hasCreditsValue || normalizedCreditsBalance >= requiredEnrichCredits)
  const isLowOnCredits = hasCreditsValue && normalizedCreditsBalance > 0 && normalizedCreditsBalance <= LOW_CREDITS_THRESHOLD
  const topupLabel = topupCreditsBalance > 0
    ? `+ ${formatCreditAmount(topupCreditsBalance, { thousandDecimals: 0, millionDecimals: 2 })} top-up credits`
    : ''
  const visibleLiveMailTemplateCards = useMemo(
    () => resolveLiveMailTemplateCardsForNiche(selectedUserNiche),
    [selectedUserNiche],
  )

  const applySnipedTemplateSelection = useCallback((templateKey) => {
    const selectedCard = liveMailTemplateCards.find((card) => card.key === templateKey) || liveMailTemplateCards[0]
    if (!selectedCard) return

    const matched = resolveSnipedTemplateForSelection(selectedUserNiche, selectedCard.key)
    if (!matched) return

    setConfigForm((prev) => {
      return {
        ...prev,
        [selectedCard.subjectKey]: String(matched.subject || ''),
        [selectedCard.bodyKey]: String(matched.body || ''),
      }
    })
    setSequenceForm((prev) => ({
      ...prev,
      step2_body: String(matched.followup || ''),
      step2_subject: String(prev.step2_subject || '').trim() || `Following up with {BusinessName}`,
    }))
  }, [selectedUserNiche])

  useEffect(() => {
    if (!visibleLiveMailTemplateCards.some((card) => card.key === activeLiveMailTemplateKey)) {
      setActiveLiveMailTemplateKey(visibleLiveMailTemplateCards[0]?.key || 'ghost')
    }
  }, [visibleLiveMailTemplateCards, activeLiveMailTemplateKey])

  useEffect(() => {
    applySnipedTemplateSelection(activeLiveMailTemplateKey)
  }, [applySnipedTemplateSelection, activeLiveMailTemplateKey, selectedUserNiche])

  useEffect(() => {
    const activeCard = liveMailTemplateCards.find((card) => card.key === activeLiveMailTemplateKey) || liveMailTemplateCards[0]
    if (!activeCard) return

    const matched = resolveSnipedTemplateForSelection(selectedUserNiche, activeCard.key)

    const sampleVars = {
      BusinessName: 'Apex Roofing',
      City: 'London',
      Niche: 'Roofing',
      YourName: 'Nejc',
    }

    const followupStep = Number(sequenceForm.activeStep || 2)
    const followupSubjectByStep = {
      1: String(sequenceForm.step1_subject || sequenceForm.ab_subject_a || ''),
      2: String(sequenceForm.step2_subject || ''),
      3: String(sequenceForm.step3_subject || ''),
    }
    const followupBodyByStep = {
      1: String(sequenceForm.step1_body || ''),
      2: String(sequenceForm.step2_body || ''),
      3: String(sequenceForm.step3_body || ''),
    }

    const rawSubject = activeMailEditorTab === 'followup'
      ? (followupSubjectByStep[followupStep] || String(matched?.subject || ''))
      : String(configForm[activeCard.subjectKey] || String(matched?.subject || ''))

    const rawBody = activeMailEditorTab === 'followup'
      ? (followupBodyByStep[followupStep] || (followupStep === 2 ? String(matched?.followup || '') : ''))
      : String(configForm[activeCard.bodyKey] || String(matched?.body || ''))

    const signature = String(configForm.mail_signature || '').trim()
    const payloadBody = signature ? `${rawBody}\n\n${signature}` : rawBody
    setMailPreviewRaw({
      subject: rawSubject,
      body: payloadBody,
    })

    setMailPreview({
      subject: replaceTemplatePlaceholders(rawSubject, sampleVars),
      body: replaceTemplatePlaceholders(payloadBody, sampleVars),
      generatedAt: new Date().toISOString(),
    })
  }, [
    activeLiveMailTemplateKey,
    activeMailEditorTab,
    configForm,
    sequenceForm.activeStep,
    sequenceForm.step1_body,
    sequenceForm.step1_subject,
    sequenceForm.step2_body,
    sequenceForm.step2_subject,
    sequenceForm.step3_body,
    sequenceForm.step3_subject,
    sequenceForm.ab_subject_a,
    selectedUserNiche,
  ])
  const visibleMainNavItems = useMemo(
    () => mainNavItems.filter((item) => {
      if (item.tab === 'clients') return canClientSuccessDashboard
      if (item.tab === 'admin') return isAdminUser
      return true
    }),
    [canClientSuccessDashboard, isAdminUser],
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
        refreshAiCostReport({ silent: true }),
      ])
    } else {
      setWeeklyReport(null)
      setMonthlyReport(null)
      setAiCostReport(null)
    }
  }, [canAdvancedReporting, refreshAiCostReport, refreshMonthlyReport, refreshWeeklyReport])
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
    if (activeTab === 'admin' && !isAdminUser) {
      setActiveTab('leads')
    }
  }, [activeTab, isAdminUser])
  useEffect(() => {
    if (!isAdminUser || activeTab !== 'admin') return undefined
    void refreshAdminOverview()
    const timerId = window.setInterval(() => {
      void refreshAdminOverview({ silent: true })
    }, 15000)
    return () => window.clearInterval(timerId)
  }, [activeTab, isAdminUser, refreshAdminOverview])
  useEffect(() => {
    const targetPercent = creditsReady ? creditsPercent : 0
    const frameId = window.requestAnimationFrame(() => setAnimatedCreditsPercent(targetPercent))
    return () => window.cancelAnimationFrame(frameId)
  }, [creditsPercent, creditsReady])
  const isCreditsLow = hasCreditsValue && normalizedCreditsBalance / creditsLimit < 0.1
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
        <Toaster {...appToasterProps} />
        <div className="rounded-3xl border border-white/10 bg-white/5 px-6 py-5 text-center shadow-2xl backdrop-blur-sm">
          <p className="text-sm font-medium text-white">Session required</p>
          <p className="mt-1 text-sm text-slate-400">Redirecting to login…</p>
        </div>
      </div>
    )
  }
  return (
    <div className="app-root">
      <Toaster {...appToasterProps} />
      {globalBanner.active && globalBanner.message ? (
        <div className="fixed inset-x-0 top-0 z-[80] border-b border-amber-400/40 bg-amber-500/20 px-4 py-2 text-center text-sm font-semibold text-amber-100 backdrop-blur">
          <span className="inline-flex items-center gap-2">
            <AlertTriangle className="h-4 w-4" />
            {globalBanner.message}
          </span>
        </div>
      ) : null}

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
              billingLoading={!profileHydrated}
              cancelPending={cancelPending}
              cancelUntilLabel={cancelUntilLabel}
              creditsBalance={creditsBalance}
              monthlyLimit={creditsLimit}
              creditsBalanceLabel={creditsBalanceLabel}
              creditsLimitLabel={creditsLimitLabel}
              creditsPercent={creditsReady ? animatedCreditsPercent : 0}
              creditsLabelClass={creditsLabelClass}
              creditsLoading={!creditsReady}
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
                  onClick={() => handleMainNavigation(item.tab)}
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
                  <button key={item.tab} className={`topbar-nav ${activeTab === item.tab ? 'topbar-nav-active' : ''}`} type="button" onClick={() => handleMainNavigation(item.tab)}>
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
                <StatusDot label="Azure OpenAI" ok={configHealth.openai_ok} />
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
                  {creditsReady ? (
                    <span className={`text-[11px] font-semibold ${creditsLabelClass}`}>
                      {creditsBalanceLabel}
                    </span>
                  ) : (
                    <span className="h-[11px] w-10 rounded-md bg-slate-700/70 animate-pulse" />
                  )}
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
              <span>Queued mail</span>
              <strong>{stats.queued_mail_count}</strong>
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
              {!statsHydrated ? (
                <StatCardSkeletonList count={4} />
              ) : (
                <>
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
                </>
              )}
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
                  onClick={() => void refreshSignalLayer({ forceRefresh: true, silentNiche: false })}
                  disabled={nicheAdvice.loading || campaignLoading || refreshingDashboard}
                >
                  <RefreshCw className={`h-3.5 w-3.5 ${(nicheAdvice.loading || campaignLoading || refreshingDashboard) ? 'animate-spin' : ''}`} /> Refresh
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
                    {nicheAdvice.error.toLowerCase().includes('azure_openai') || nicheAdvice.error.toLowerCase().includes('openai_api_key')
                      ? 'Azure OpenAI is missing on Railway. Set AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, and AZURE_OPENAI_DEPLOYMENT_NAME, then refresh.'
                      : nicheAdvice.error}
                  </p>
                  <p className="mt-1 text-[11px] text-slate-500">Auto-retry will run in the background every few minutes.</p>
                  <button
                    className="mt-2 text-xs text-cyan-400 hover:text-cyan-300 underline"
                    type="button"
                    onClick={() => void refreshSignalLayer({ forceRefresh: true, silentNiche: false })}
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

        {isCreditsLoading ? (
          <section className="glass-card mb-6 rounded-[24px] border border-slate-700/40 bg-slate-900/50 p-5">
            <div className="animate-pulse space-y-3">
              <div className="h-3 w-32 rounded bg-slate-700/70" />
              <div className="h-6 w-64 rounded bg-slate-700/70" />
              <div className="h-4 w-full rounded bg-slate-800/80" />
            </div>
          </section>
        ) : null}

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

        {isLowOnCredits ? (
          <section className="glass-card mb-6 rounded-[24px] border border-amber-500/25 bg-amber-500/10 p-5 shadow-[0_12px_40px_rgba(245,158,11,0.12)]">
            <div className="flex flex-wrap items-center justify-between gap-4">
              <div>
                <p className="label-overline text-amber-300">Low credits warning</p>
                <h3 className="mt-1 text-lg font-semibold text-white">You are running low on credits.</h3>
                <p className="mt-1 text-sm text-slate-300">
                  Current balance: {creditsBalanceLabel} credits. Scrape uses 1/lead, AI enrichment uses 2/lead.
                </p>
              </div>
              <div className="flex flex-wrap gap-3">
                <button className="btn-primary" type="button" onClick={handleTopUpClick}>
                  <PlusCircle className="h-4 w-4" /> Buy Credits
                </button>
                <button className="btn-ghost" type="button" onClick={openPricingSection}>
                  <Zap className="h-4 w-4" /> Upgrade Plan
                </button>
              </div>
            </div>
          </section>
        ) : null}

        <OnboardingWizard
          open={onboardingWizardOpen}
          submitting={onboardingLaunching}
          onClose={() => closeOnboardingWizard(false)}
          onComplete={runOnboardingMagicSearch}
          completeCta="Run Magic Search"
          subtitle="Welcome to Sniped"
          title="Let’s launch your first lead stream"
        />

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
              status={scrapeCardStatusLabel}
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
                  <CheckboxField label="Speed Mode" checked={scrapeForm.speedMode} onChange={(v) => setScrapeForm({ ...scrapeForm, speedMode: v })} title="Prioritize quantity — skips slow social metrics collection" />
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
              <button className="workflow-btn" type="button" disabled={scrapeButtonLocked} onClick={onScrapeSubmit}>
                {scrapeSuccessLeadsFound ? (
                  <>
                    <CheckCircle2 className="h-4 w-4" /> Success! {scrapeSuccessLeadsFound} Leads Found
                  </>
                ) : scrapeIsActive || pendingRequest === 'scrape' ? (
                  <>
                    <RefreshCw className="h-4 w-4 animate-spin" /> Scraper Running...
                  </>
                ) : (
                  <>
                    <Database className="h-4 w-4" /> Launch Scrape
                  </>
                )}
              </button>
              {isOutOfCredits ? (
                <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-amber-300">
                  <span>Out of credits. You need at least 1 credit to scrape.</span>
                  <button type="button" className="btn-ghost px-2.5 py-1.5 text-xs" onClick={handleTopUpClick}>
                    <PlusCircle className="h-3.5 w-3.5" /> Buy Credits
                  </button>
                </div>
              ) : scrapeHasInsufficientCredits ? (
                <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-amber-300">
                  <span>Need {creditIntegerFormatter.format(requiredScrapeCredits)} credits for this scrape. You currently have {creditsBalanceLabel}.</span>
                  <button type="button" className="btn-ghost px-2.5 py-1.5 text-xs" onClick={() => setShowLowCreditsModal(true)}>
                    <Rocket className="h-3.5 w-3.5" /> View options
                  </button>
                </div>
              ) : null}

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
                  {scrapeProgress.status === 'queued' || scrapeProgress.status === 'pending' ? (
                    <p className="scrape-progress-copy">
                      ⏳ Scrape queued, waiting for worker slot...
                    </p>
                  ) : null}
                  {scrapeProgress.isLoading ? (
                    <p className="scrape-progress-copy">
                      {scrapeProgress.statusMessage || '🌐 Launching browser and opening Google Maps... (cold start can take up to ~30s)'}
                    </p>
                  ) : null}
                  {['running', 'processing', 'pending'].includes(scrapeProgress.status) && !scrapeProgress.isLoading ? (
                    <p className="scrape-progress-copy">
                      {scrapeProgress.statusMessage || (
                        <>
                          🔍 <span className="scrape-count-pulse">{scrapeProgress.progressCurrent}</span> / {scrapeProgress.totalToFind || Number(scrapeForm.results || 0)} leads found… (scanned {scrapeProgress.scannedCount})
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
                      {scrapeProgress.statusMessage
                        ? `Scrape failed: ${scrapeProgress.statusMessage}`
                        : `Scrape failed: Stopped at ${scrapeProgress.progressCurrent}. Check logs for details.`}
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
              status={String(enrichTaskView.status || '').toLowerCase() === 'queued' ? 'Queued' : (enrichRunRequested && enrichTaskView.running) ? 'Running' : 'Ready'}
              accent="teal"
            >
              <div className="grid gap-3 sm:grid-cols-2">
                <label className="field-label">
                  <span className="mb-1.5 block">Batch size</span>
                  <input className="glass-input" type="number" min="1" max="200" value={enrichForm.limit} onChange={(e) => setEnrichForm({ ...enrichForm, limit: e.target.value })} />
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
              <button className="workflow-btn" type="button" disabled={pendingRequest === 'enrich' || isAnalyzing || (enrichRunRequested && enrichTaskView.running) || enrichRetrySeconds > 0 || !canRunEnrich} onClick={onEnrichSubmit}>
                {pendingRequest === 'enrich' || isAnalyzing || (enrichRunRequested && enrichTaskView.running) ? (
                  <>
                    <RefreshCw className="h-4 w-4 animate-spin" /> AI is analyzing...
                  </>
                ) : enrichRetrySeconds > 0 ? (
                  <>
                    <RefreshCw className="h-4 w-4" /> Retry in {enrichRetrySeconds}s
                  </>
                ) : (
                  <>
                    <Sparkles className="h-4 w-4" /> {submitLabel('enrich', enrichTaskView.running, pendingRequest === 'enrich').replace('Start', 'Run')}
                  </>
                )}
              </button>
              {!canRunEnrich ? (
                <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-amber-300">
                  <span>
                    {enrichmentEligibleLeadIds.length <= 0
                      ? 'No eligible leads to enrich right now.'
                      : `Need ${requiredEnrichCreditsLabel} credits for enrichment. You have ${creditsBalanceLabel}.`}
                  </span>
                  {enrichmentEligibleLeadIds.length > 0 ? (
                    <button type="button" className="btn-ghost px-2.5 py-1.5 text-xs" onClick={handleTopUpClick}>
                      <PlusCircle className="h-3.5 w-3.5" /> Buy Credits
                    </button>
                  ) : null}
                </div>
              ) : null}
              {(pendingRequest === 'enrich' || isAnalyzing || enrichRunRequested || enrichTaskView.running || enrichRetrySeconds > 0) ? (
                <button className="workflow-btn" type="button" onClick={resetEnrichUiState} style={{ marginTop: '0.6rem', background: 'linear-gradient(135deg,#334155,#0f172a)' }}>
                  Reset Enrichment Status
                </button>
              ) : null}
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
                  {enrichProgress.status === 'queued' ? (
                    <p className="scrape-progress-copy">
                      ⏳ Enrichment task is queued. Progress will start as soon as a worker slot is available.
                    </p>
                  ) : null}
                  {enrichProgress.status === 'running' ? (
                    <p className="scrape-progress-copy">
                      ✨ Processing <span className="scrape-count-pulse">{enrichProgress.processed}</span> / {enrichProgress.total} leads, please wait...
                      {enrichProgress.currentLead ? ` Lead: ${enrichProgress.currentLead}` : ''}
                      {enrichProgress.statusMessage ? ` ${enrichProgress.statusMessage}` : ''}
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
            {isAdminUser ? (
              <button className={`tab-btn ${activeTab === 'admin' ? 'tab-active' : ''}`} type="button" onClick={() => handleMainNavigation('admin')}>
                <Shield className="inline h-3.5 w-3.5 mr-1" />
                ADMIN CENTER
              </button>
            ) : null}
            <div className="ml-auto rounded-full bg-white/5 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-slate-400">
              {activeTab === 'leads' ? `${filteredLeads.length} visible • ${Math.max(leadServerTotal, filteredLeads.length)} total leads` : activeTab === 'blacklist' ? `${blacklistedLeads.length} blacklisted` : activeTab === 'workers' ? `${workers.length} workers` : activeTab === 'tasks' || activeTab === 'history' ? `${deliverySummary.total} task manager items • ${taskHistory.length} history entries` : activeTab === 'mail' ? 'Mailer editor' : activeTab === 'qualify' ? `${qualifierData.data?.total ?? 0} gold mines` : activeTab === 'export' ? 'Reporting & exports' : activeTab === 'clients' ? `${clientFolders.length} client folders` : activeTab === 'admin' ? `${adminOverview.stats.total_users || 0} users • ${adminOverview.stats.total_leads || 0} leads` : activeTab === 'config' ? 'Platform settings' : null}
            </div>
          </div>

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

              <div className="rounded-2xl border border-cyan-500/20 bg-[linear-gradient(140deg,rgba(8,47,73,0.2),rgba(15,23,42,0.65))] px-3 py-3 shadow-[0_10px_34px_rgba(14,116,144,0.16)]">
                <div className="flex flex-wrap items-center gap-2">
                  <button
                    type="button"
                    className={`inline-flex items-center gap-2 rounded-xl border px-3 py-2 text-xs font-semibold transition ${aiFilterToolbarOpen ? 'border-cyan-400/50 bg-cyan-500/15 text-cyan-100' : 'border-white/10 bg-white/[0.03] text-slate-300 hover:text-white'}`}
                    onClick={() => setAiFilterToolbarOpen((prev) => !prev)}
                  >
                    <Sparkles className={`h-3.5 w-3.5 ${aiFilterLoading ? 'animate-spin' : ''}`} />
                    AI Pro Search
                  </button>
                  {isAiFilterActive ? (
                    <div className="flex min-w-[220px] flex-1 items-center gap-2 rounded-xl border border-cyan-500/30 bg-cyan-500/10 px-3 py-2 text-xs text-cyan-100">
                      <span className="truncate">{aiFilterSummary || `AI assistant narrowed this list to ${filteredLeads.length} lead(s).`}</span>
                      <button type="button" className="ml-auto text-xs font-semibold text-cyan-200 hover:text-white" onClick={clearAiFilter}>Clear</button>
                    </div>
                  ) : null}
                </div>

                {aiFilterToolbarOpen ? (
                  <>
                    <form
                      className="mt-2 flex flex-wrap items-center gap-2"
                      onSubmit={(e) => {
                        e.preventDefault()
                        void runAiFilter(aiFilterPrompt)
                      }}
                    >
                      <div className="relative min-w-[260px] flex-1">
                        <Sparkles className="absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-cyan-300" />
                        <input
                          className="glass-input h-10 w-full pl-8 text-sm"
                          type="text"
                          value={aiFilterPrompt}
                          placeholder="Find plumbers in London with slow websites"
                          onFocus={() => setAiFilterInputFocused(true)}
                          onBlur={() => setAiFilterInputFocused(false)}
                          onChange={(e) => setAiFilterPrompt(e.target.value)}
                        />
                      </div>
                      <button type="submit" className="btn-primary h-10 px-3 text-xs" disabled={aiFilterLoading}>
                        {aiFilterLoading ? 'Analyzing…' : 'Run'}
                      </button>
                      {isAiFilterActive ? (
                        <button type="button" className="btn-ghost h-10 px-3 text-xs" onClick={clearAiFilter}>
                          Reset
                        </button>
                      ) : null}
                    </form>

                    {aiFilterInputFocused || !aiFilterPrompt.trim() ? (
                      <div className="mt-2 flex flex-wrap gap-1.5">
                        {[
                          'Find plumbers in London with slow websites',
                          'Show local dentists in Berlin with no Instagram',
                          'Find gyms in Miami with weak SEO and low reviews',
                        ].map((sample) => (
                          <button
                            key={sample}
                            type="button"
                            className="rounded-lg border border-white/10 bg-white/[0.02] px-2.5 py-1 text-[11px] font-medium text-slate-300 transition hover:border-cyan-400/40 hover:text-cyan-100"
                            disabled={aiFilterLoading}
                            onMouseDown={(e) => e.preventDefault()}
                            onClick={() => {
                              setAiFilterPrompt(sample)
                              void runAiFilter(sample)
                            }}
                          >
                            {sample}
                          </button>
                        ))}
                      </div>
                    ) : null}
                  </>
                ) : null}
              </div>

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

              <div className="inline-flex items-center rounded-2xl border border-white/10 bg-white/[0.03] p-1">
                <button
                  type="button"
                  className={`rounded-xl px-3 py-1.5 text-xs font-semibold transition ${leadsViewMode === 'table' ? 'bg-cyan-500/20 text-cyan-100' : 'text-slate-400 hover:text-white'}`}
                  onClick={() => setLeadsViewMode('table')}
                >
                  Table View
                </button>
                <button
                  type="button"
                  className={`rounded-xl px-3 py-1.5 text-xs font-semibold transition ${leadsViewMode === 'pipeline' ? 'bg-cyan-500/20 text-cyan-100' : 'text-slate-400 hover:text-white'}`}
                  onClick={() => setLeadsViewMode('pipeline')}
                >
                  Pipeline View
                </button>
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
                  onClick={() => setLeadQuickFilter('qualified')}
                >
                  Qualified ({leadQuickCounts.qualified})
                </button>
                <button
                  type="button"
                  className={`btn-ghost px-3 py-1.5 text-xs ${leadQuickFilter === 'not_qualified' ? 'ring-1 ring-amber-400/70 text-amber-200' : ''}`}
                  onClick={() => setLeadQuickFilter('not_qualified')}
                >
                  Not Qualified ({leadQuickCounts.notQualified})
                </button>
                <button
                  type="button"
                  className={`btn-ghost px-3 py-1.5 text-xs ${leadQuickFilter === 'mailed' ? 'ring-1 ring-emerald-400/70 text-emerald-200' : ''}`}
                  onClick={() => setLeadQuickFilter('mailed')}
                >
                  Mailed ({leadQuickCounts.mailed})
                </button>
                <button
                  type="button"
                  className={`btn-ghost px-3 py-1.5 text-xs ${leadQuickFilter === 'opened' ? 'ring-1 ring-cyan-400/70 text-cyan-200' : ''}`}
                  onClick={() => setLeadQuickFilter('opened')}
                >
                  Opened ({leadQuickCounts.opened})
                </button>
                <button
                  type="button"
                  className={`btn-ghost px-3 py-1.5 text-xs ${leadQuickFilter === 'replied' ? 'ring-1 ring-emerald-400/70 text-emerald-200' : ''}`}
                  onClick={() => setLeadQuickFilter('replied')}
                >
                  Replied ({leadQuickCounts.replied})
                </button>
                {hasAnyLeadFiltersActive ? (
                  <button
                    type="button"
                    className="btn-ghost px-3 py-1.5 text-xs text-rose-200 ring-1 ring-rose-400/40"
                    onClick={clearAllLeadFilters}
                  >
                    Clear all filters
                  </button>
                ) : null}
              </div>

              {selectedLeadRows.length > 0 ? (
                <div className="sticky bottom-4 z-30 mt-2 flex flex-wrap items-center gap-2 rounded-2xl border border-cyan-500/30 bg-slate-950/95 px-3 py-2 shadow-[0_16px_48px_rgba(2,6,23,0.5)] backdrop-blur">
                  <span className="text-xs font-semibold text-cyan-200">
                    {selectedLeadRows.length} lead{selectedLeadRows.length === 1 ? '' : 's'} selected
                  </span>
                  <button
                    type="button"
                    className="btn-ghost px-2.5 py-1.5 text-xs"
                    onClick={() => void bulkAiFilterSelected()}
                    disabled={aiFilterLoading}
                  >
                    <Sparkles className="h-3.5 w-3.5" /> Bulk AI Filter
                  </button>
                  <button
                    type="button"
                    className="btn-ghost px-2.5 py-1.5 text-xs"
                    onClick={() => bulkExportSelectedCsv()}
                    disabled={!canBulkExport}
                  >
                    <Download className="h-3.5 w-3.5" /> Bulk Export (CSV)
                  </button>
                  <button
                    type="button"
                    className="btn-ghost px-2.5 py-1.5 text-xs text-rose-200 ring-1 ring-rose-400/35"
                    onClick={() => void bulkDeleteSelectedLeads()}
                  >
                    <Trash2 className="h-3.5 w-3.5" /> Bulk Delete
                  </button>
                  <button
                    type="button"
                    className="ml-auto text-xs font-semibold text-slate-400 transition hover:text-white"
                    onClick={clearSelectedLeads}
                  >
                    Clear selection
                  </button>
                </div>
              ) : null}

              {selectedLeadForEmailDraft ? (
                <div className="mt-3 rounded-[24px] border border-cyan-500/25 bg-slate-900/70 p-4 shadow-[0_10px_32px_rgba(6,182,212,0.16)]">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-[0.16em] text-cyan-300">Selected lead email draft</p>
                      <p className="text-sm text-slate-300">
                        {selectedLeadForEmailDraft.business_name || 'Selected business'} · template {leadEmailDraft.templateKey}
                      </p>
                    </div>
                    <button
                      type="button"
                      className="btn-ghost px-3 py-1.5 text-xs"
                      onClick={() => {
                        const leadNiche = deriveLeadIndustry(selectedLeadForEmailDraft) || selectedUserNiche
                        const templateKey = resolveLeadTemplateKey(selectedLeadForEmailDraft)
                        const template = resolveSnipedTemplateForSelection(leadNiche, templateKey)
                        setLeadEmailDraft({
                          leadId: Number(selectedLeadForEmailDraft.id || 0),
                          templateKey,
                          subject: String(template?.subject || ''),
                          body: String(template?.body || ''),
                        })
                      }}
                    >
                      Reset to template
                    </button>
                  </div>

                  <div className="mt-3 grid gap-3 xl:grid-cols-2">
                    <div className="rounded-2xl border border-white/10 bg-slate-950/60 p-3 space-y-3">
                      <label className="field-label">
                        <span className="mb-1.5 block">Subject (editable)</span>
                        <input
                          className="glass-input"
                          type="text"
                          value={leadEmailDraft.subject}
                          onChange={(e) => setLeadEmailDraft((prev) => ({ ...prev, subject: e.target.value }))}
                        />
                      </label>
                      <label className="field-label">
                        <span className="mb-1.5 block">Body (editable)</span>
                        <textarea
                          className="glass-input min-h-[210px]"
                          value={leadEmailDraft.body}
                          onChange={(e) => setLeadEmailDraft((prev) => ({ ...prev, body: e.target.value }))}
                        />
                      </label>
                    </div>

                    <div className="overflow-hidden rounded-2xl border border-white/10 bg-slate-950/70">
                      <div className="border-b border-white/10 bg-white/[0.03] px-3 py-2">
                        <p className="text-[11px] uppercase tracking-[0.14em] text-slate-400">Live preview for selected lead</p>
                      </div>
                      <div className="space-y-3 px-3 py-3">
                        <div className="rounded-xl border border-white/10 bg-white/[0.02] px-3 py-2">
                          <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">To</p>
                          <p className="mt-1 text-sm text-slate-200">
                            {selectedLeadForEmailDraft.contact_name || 'Business owner'}
                            {selectedLeadForEmailDraft.email ? ` <${selectedLeadForEmailDraft.email}>` : ''}
                          </p>
                        </div>
                        <div className="rounded-xl border border-white/10 bg-white/[0.02] px-3 py-2">
                          <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Subject</p>
                          <p className="mt-1 text-sm font-semibold text-white">{resolvedLeadDraftPreview.subject || 'No subject yet'}</p>
                        </div>
                        <div className="rounded-xl border border-white/10 bg-slate-950/80 p-3">
                          <pre className="whitespace-pre-wrap break-words font-sans text-[13px] leading-6 text-slate-200">{resolvedLeadDraftPreview.body || 'No body yet'}</pre>
                        </div>
                        <button
                          type="button"
                          className="btn-ghost w-full justify-center py-2 text-xs"
                          onClick={() => {
                            const text = `Subject: ${resolvedLeadDraftPreview.subject}\n\n${resolvedLeadDraftPreview.body}`
                            navigator.clipboard.writeText(text).then(() => toast.success('Lead draft copied'))
                          }}
                          disabled={!resolvedLeadDraftPreview.subject && !resolvedLeadDraftPreview.body}
                        >
                          <Clipboard className="h-3.5 w-3.5" /> Copy preview
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              ) : null}

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
                  {leadsViewMode === 'pipeline' ? (
                    <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleLeadPipelineDragEnd}>
                      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                        {leadPipelineOptions.map((stage) => {
                          const stageLeads = kanbanColumns[stage] || []
                          return (
                            <PipelineDropColumn key={`pipeline-column-${stage}`} stage={stage}>
                              <div className="mb-3 flex items-center justify-between gap-2">
                                <span className={`inline-flex items-center rounded-full border px-2 py-1 text-[10px] font-semibold ${pipelineStageBadgeClass(stage)}`}>{stage}</span>
                                <span className="text-xs text-slate-400">{stageLeads.length}</span>
                              </div>
                              <div className="space-y-2">
                                {stageLeads.length ? stageLeads.map((lead) => (
                                  <PipelineLeadCard
                                    key={`pipeline-card-${lead.id}`}
                                    lead={lead}
                                    onOpenDetails={openLeadDetailsModal}
                                    pendingStatusLeadId={pendingStatusLeadId}
                                  />
                                )) : (
                                  <div className="rounded-xl border border-dashed border-slate-700/70 px-3 py-6 text-center text-xs text-slate-500">
                                    Drop lead here
                                  </div>
                                )}
                              </div>
                            </PipelineDropColumn>
                          )
                        })}
                      </div>
                    </DndContext>
                  ) : null}

                  {/* Leads table */}
                  <div
                    id="leads-table"
                    key={`desktop-${leadFilterSignature}`}
                    className={`hidden overflow-hidden rounded-[24px] border border-slate-700/50 bg-slate-900/70 shadow-[0_10px_40px_rgba(2,6,23,0.28)] ${leadsViewMode === 'table' ? 'lg:block' : ''}`}
                  >
                <div className="max-h-[68vh] overflow-auto leads-fade-in" style={{overflowX: 'auto'}}>
                  <table className="apollo-table w-full table-fixed text-xs tracking-tight">
                    <colgroup>
                      <col style={{width: '4%'}} />
                      <col style={{width: '20%'}} />
                      <col style={{width: '12%'}} />
                      <col style={{width: '10%'}} />
                      <col style={{width: '4%'}} />
                      <col style={{width: '4%'}} />
                      <col style={{width: '4%'}} />
                      <col style={{width: '5%'}} />
                      <col style={{width: '8%'}} />
                      <col style={{width: '11%'}} />
                      <col style={{width: '9%'}} />
                      <col style={{minWidth: '160px', width: '13%'}} />
                    </colgroup>
                    <thead className="sticky top-0 bg-slate-900/95 backdrop-blur-xl">
                      <tr>
                        <th className="th-cell text-center">
                          <input
                            type="checkbox"
                            checked={areAllPageLeadsSelected}
                            onChange={toggleSelectAllPageLeads}
                            aria-label="Select all leads on this page"
                          />
                        </th>
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
                          <td colSpan={12} className="td-cell">
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
                        const trendMeta = resolveLeadTrendMeta(lead)
                        const trendPath = buildTrendSparklinePath(trendMeta.points)
                        const pipelineStage = resolvePipelineStage(lead)
                        const socialLinks = [
                          { key: 'linkedin', url: lead.linkedin_url, label: 'LinkedIn', Icon: Linkedin },
                          { key: 'instagram', url: lead.instagram_url, label: 'Instagram', Icon: Instagram },
                          { key: 'facebook', url: lead.facebook_url, label: 'Facebook', Icon: Facebook },
                          { key: 'twitter', url: lead.twitter_url, label: 'Twitter / X', Icon: Twitter },
                          { key: 'youtube', url: lead.youtube_url, label: 'YouTube', Icon: Youtube },
                        ].filter((item) => item.url)
                        const enrichmentState = String(lead.enrichment_status || lead.status || '').toLowerCase()
                        const shouldShowSearchingEmail = !lead.email && ['processing', 'pending', 'queued', 'scraped', 'new'].includes(enrichmentState)
                        const emailDisplay = lead.email || (shouldShowSearchingEmail ? 'Searching...' : '—')
                        const shareState = shareReportStateByLeadId[lead.id] || {}
                        const hasActiveShareLink = shareState.isActive !== undefined
                          ? Boolean(shareState.isActive)
                          : Boolean(lead.has_active_report_share)
                        const showRevokedFlash = Boolean(shareState.revokedFlash)
                        return (
                        <tr key={lead.id} className="td-row">
                          <td className="td-cell text-center">
                            <input
                              type="checkbox"
                              checked={selectedLeadIdSet.has(Number(lead.id))}
                              onChange={() => toggleLeadSelection(lead.id)}
                              aria-label={`Select lead ${lead.business_name || lead.id}`}
                            />
                          </td>
                          {/* Business + Niche + Contact merged */}
                          <td className="td-cell">
                            <div className="flex flex-col gap-0.5 min-w-0">
                              <span className="font-semibold text-white truncate block">{lead.business_name || '—'}</span>
                              <span className="text-[10px] text-slate-500 truncate block">{lead.search_keyword || 'manual'}</span>
                              {lead.contact_name && <span className="text-[10px] text-slate-600 truncate block">{lead.contact_name}</span>}
                              <div className="mt-1 flex flex-wrap gap-1">
                                {bestLeadScore > 0 && (
                                  <span className="inline-flex items-center gap-1 rounded-full border border-cyan-500/30 bg-cyan-500/10 px-1.5 py-0.5 text-[9px] font-semibold text-cyan-200">
                                    <Sparkles className="h-2.5 w-2.5" /> Niche {formatLeadScoreValue(bestLeadScore)}/10
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
                              {(lead.website_url || lead.maps_url) ? (
                                <div className="mt-1 inline-flex flex-wrap items-center gap-2">
                                  {lead.website_url ? (
                                    <a
                                      href={lead.website_url}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="inline-flex w-fit items-center gap-1 text-[10px] font-medium text-cyan-300 hover:text-cyan-100"
                                      title="Open website"
                                    >
                                      <Globe className="h-2.5 w-2.5" /> Website
                                    </a>
                                  ) : null}
                                  {lead.maps_url ? (
                                    <a
                                      href={lead.maps_url}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="inline-flex w-fit items-center gap-1 text-[10px] font-medium text-rose-300 hover:text-rose-100"
                                      title="Open Google Maps profile"
                                    >
                                      <MapPin className="h-2.5 w-2.5" /> Maps
                                    </a>
                                  ) : null}
                                </div>
                              ) : null}
                              <div className="mt-1 flex flex-wrap items-center gap-1.5">
                                <span className={`inline-flex items-center rounded-full border px-1.5 py-0.5 text-[9px] font-semibold ${pipelineStageBadgeClass(pipelineStage)}`}>
                                  {pipelineStage}
                                </span>
                                {hasReply(lead) ? (
                                  <span className="inline-flex items-center rounded-full border border-emerald-500/30 bg-emerald-500/10 px-1.5 py-0.5 text-[9px] font-semibold text-emerald-100">
                                    ↩ Replied
                                  </span>
                                ) : hasOpenedMail(lead) ? (
                                  <span className="inline-flex items-center rounded-full border border-cyan-500/30 bg-cyan-500/10 px-1.5 py-0.5 text-[9px] font-semibold text-cyan-100">
                                    👁 Opened x{Math.max(1, Number(lead.open_count || 0))}
                                  </span>
                                ) : null}
                                {Number(lead.qualification_score || 0) > 0 && (
                                  <span className="inline-flex items-center rounded-full border border-amber-500/30 bg-amber-500/10 px-1.5 py-0.5 text-[9px] font-semibold text-amber-100">
                                    Q {Math.round(Number(lead.qualification_score || 0))}/100
                                  </span>
                                )}
                                {socialLinks.length > 0 && (
                                  <div className="inline-flex items-center gap-1">
                                    {socialLinks.map((social) => (
                                      <a
                                        key={`${lead.id}-${social.key}`}
                                        href={social.url}
                                        target="_blank"
                                        rel="noreferrer"
                                        title={social.label}
                                        className="inline-flex h-5 w-5 items-center justify-center rounded-full border border-sky-500/30 bg-sky-500/10 text-sky-100 transition hover:border-sky-300/60 hover:text-sky-50"
                                      >
                                        <social.Icon className="h-3 w-3" />
                                      </a>
                                    ))}
                                  </div>
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
                              <span className={`truncate block min-w-0 text-[11px] ${lead.email ? 'text-slate-400' : shouldShowSearchingEmail ? 'text-cyan-300' : 'text-slate-500'}`}>{emailDisplay}</span>
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
                                  {Number(bestLeadScore || 0) >= 9 && (
                                    <span className="text-amber-400 text-xs leading-none">★</span>
                                  )}
                                  <span className={`score-orb ${scoreHeatTone(bestLeadScore)}`}>
                                    {bestLeadScore > 0 ? Number(bestLeadScore).toFixed(1) : '--'}
                                  </span>
                                </div>
                                {bestLeadScore > 0 && (
                                  <span className="text-[10px] font-semibold text-cyan-200">
                                    Niche {formatLeadScoreValue(bestLeadScore)}/10
                                  </span>
                                )}
                                {trendMeta.points.length >= 2 && (
                                  <span className={`inline-flex items-center gap-1 text-[10px] font-semibold ${trendMeta.direction === 'up' ? 'text-emerald-300' : trendMeta.direction === 'down' ? 'text-rose-300' : 'text-slate-400'}`}>
                                    <svg width="54" height="15" viewBox="0 0 54 15" fill="none" aria-hidden="true">
                                      <path d={trendPath} stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
                                    </svg>
                                    <TrendingUp className={`h-3 w-3 ${trendMeta.direction === 'down' ? 'rotate-180' : trendMeta.direction === 'flat' ? 'opacity-50' : ''}`} />
                                    {trendMeta.delta > 0 ? '+' : ''}{trendMeta.delta.toFixed(1)}
                                  </span>
                                )}
                                <span className={`text-[10px] font-semibold ${isQualifiedLead(lead) ? 'text-emerald-300' : 'text-amber-300'}`}>
                                  {isQualifiedLead(lead) ? 'Qualified' : 'Needs work'}
                                </span>
                                {Number(lead.qualification_score || 0) > 0 && (
                                  <span className="text-[10px] text-slate-400">
                                    Q: {Math.round(Number(lead.qualification_score || 0))}/100
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
                          <td className="td-cell" style={{overflow: 'visible'}}>
                            <div className="flex items-center justify-center gap-2" style={{flexShrink: 0, flexWrap: 'nowrap'}}>
                              {showRevokedFlash && (
                                <span className="inline-flex items-center rounded-full border border-emerald-500/35 bg-emerald-500/15 px-2 py-1 text-[10px] font-semibold text-emerald-100">
                                  Revoked
                                </span>
                              )}
                              <button type="button" className="icon-action-btn" onClick={() => openLeadDetailsModal(lead)} title="View lead details">
                                <Eye className="h-3.5 w-3.5" />
                              </button>
                              <button type="button" className="icon-action-btn" onClick={() => void moveLeadToMailer(lead)} title="Move to Mailer">
                                <Mail className="h-3.5 w-3.5" />
                              </button>
                              <button
                                type="button"
                                className="icon-action-btn"
                                onClick={() => void generateShareableLeadReport(lead)}
                                disabled={Boolean(shareState.generating || shareState.revoking)}
                                title={shareState.generating ? 'Generating share link…' : 'Generate shareable report link'}
                              >
                                <ExternalLink className="h-3.5 w-3.5" />
                              </button>
                              <button
                                type="button"
                                className="icon-action-btn"
                                onClick={() => openLeadGapReportPreview(lead)}
                                title="Open report preview"
                              >
                                <Globe className="h-3.5 w-3.5" />
                              </button>
                              <button
                                type="button"
                                className="icon-action-btn"
                                disabled={!hasActiveShareLink || Boolean(shareState.generating || shareState.revoking)}
                                onClick={() => void revokeShareableLeadReport(lead)}
                                title={shareState.revoking ? 'Revoking share link…' : hasActiveShareLink ? 'Revoke active share link' : 'No active share link'}
                              >
                                <EyeOff className="h-3.5 w-3.5" />
                              </button>
                              <button
                                type="button"
                                className="icon-action-btn"
                                disabled={pendingBlacklistLeadId === lead.id || isBlacklistedLeadStatus(lead.status)}
                                onClick={() => {
                                  if (window.confirm('Blacklist this lead? They will be hidden from active views.')) {
                                    void blacklistLead(lead.id)
                                  }
                                }}
                                title={pendingBlacklistLeadId === lead.id ? 'Blacklisting…' : 'Blacklist'}
                              >
                                <Ban className="h-3.5 w-3.5" />
                              </button>
                              <button
                                type="button"
                                className="icon-action-btn"
                                disabled={pendingBlacklistLeadId === lead.id}
                                onClick={() => void removeLeadFromActiveView(lead)}
                                title={pendingBlacklistLeadId === lead.id ? 'Removing…' : 'Remove from active view'}
                              >
                                <Trash2 className="h-3.5 w-3.5" />
                              </button>
                            </div>
                          </td>
                        </tr>
                        )}) : (
                        <tr>
                          <td colSpan={12} className="px-4 py-10 text-center text-sm text-slate-400">
                              {emptyLeadsMessage}
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

                  <div key={`mobile-${leadFilterSignature}`} className={`space-y-3 lg:hidden leads-fade-in ${leadsViewMode === 'table' ? '' : 'hidden'}`}>
                    {loadingLeads ? (
                      /* Fixed-height mobile skeletons — CLS = 0 */
                      <LeadCardSkeletonList count={4} />
                    ) : pagedLeads.length ? pagedLeads.map((lead) => {
                      const bestLeadScore = resolveBestLeadScore(lead)
                      const trendMeta = resolveLeadTrendMeta(lead)
                      const pipelineStage = resolvePipelineStage(lead)
                      const techStack = normalizeLeadInsightList(lead.tech_stack, 2)
                        const socialLinks = [
                          { key: 'linkedin', url: lead.linkedin_url, label: 'LinkedIn', Icon: Linkedin },
                          { key: 'instagram', url: lead.instagram_url, label: 'Instagram', Icon: Instagram },
                          { key: 'facebook', url: lead.facebook_url, label: 'Facebook', Icon: Facebook },
                          { key: 'twitter', url: lead.twitter_url, label: 'Twitter / X', Icon: Twitter },
                          { key: 'youtube', url: lead.youtube_url, label: 'YouTube', Icon: Youtube },
                        ].filter((item) => item.url)
                      const enrichmentState = String(lead.enrichment_status || lead.status || '').toLowerCase()
                      const shouldShowSearchingEmail = !lead.email && ['processing', 'pending', 'queued', 'scraped', 'new'].includes(enrichmentState)
                      const emailDisplay = lead.email || (shouldShowSearchingEmail ? 'Searching...' : '—')
                      const shareState = shareReportStateByLeadId[lead.id] || {}
                      const hasActiveShareLink = shareState.isActive !== undefined
                        ? Boolean(shareState.isActive)
                        : Boolean(lead.has_active_report_share)
                      const showRevokedFlash = Boolean(shareState.revokedFlash)
                      return (
                        <article key={`mobile-${lead.id}`} className="rounded-[22px] border border-slate-700/50 bg-slate-900/70 p-4 shadow-[0_8px_24px_rgba(2,6,23,0.2)]">
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0">
                              <label className="mb-1 inline-flex items-center gap-2 text-[11px] text-slate-400">
                                <input
                                  type="checkbox"
                                  checked={selectedLeadIdSet.has(Number(lead.id))}
                                  onChange={() => toggleLeadSelection(lead.id)}
                                />
                                Select
                              </label>
                              <p className="truncate text-base font-semibold text-white">{lead.business_name || '—'}</p>
                              <p className="truncate text-xs text-slate-400">{titleCaseLeadLabel(deriveLeadIndustry(lead))} • {deriveLeadRevenueBand(lead)}</p>
                            </div>
                            <span className={`inline-flex items-center rounded-full border px-2 py-1 text-[10px] font-semibold ${pipelineStageBadgeClass(pipelineStage)}`}>
                              {pipelineStage}
                            </span>
                          </div>

                          <div className="mt-3 space-y-2 text-sm text-slate-300">
                            <p className={`truncate ${lead.email ? 'text-slate-300' : shouldShowSearchingEmail ? 'text-cyan-300' : 'text-slate-500'}`}>{emailDisplay}</p>
                            {(lead.website_url || lead.maps_url) ? (
                              <div className="flex flex-wrap items-center gap-2">
                                {lead.website_url ? (
                                  <a href={lead.website_url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-xs text-cyan-300 hover:text-cyan-100">
                                    <Globe className="h-3.5 w-3.5" /> Website
                                  </a>
                                ) : null}
                                {lead.maps_url ? (
                                  <a href={lead.maps_url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-xs text-rose-300 hover:text-rose-100">
                                    <MapPin className="h-3.5 w-3.5" /> Maps
                                  </a>
                                ) : null}
                              </div>
                            ) : null}
                            <p>{lead.phone_formatted || lead.phone_number || 'No phone yet'}</p>
                            <div className="flex flex-wrap gap-2">
                              <span className="inline-flex items-center rounded-full border border-cyan-500/30 bg-cyan-500/10 px-2 py-1 text-[10px] font-semibold text-cyan-200">Niche {formatLeadScoreValue(bestLeadScore)}/10</span>
                              {trendMeta.points.length >= 2 && (
                                <span className={`inline-flex items-center gap-1 rounded-full border px-2 py-1 text-[10px] font-semibold ${trendMeta.direction === 'up' ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-100' : trendMeta.direction === 'down' ? 'border-rose-500/30 bg-rose-500/10 text-rose-100' : 'border-slate-600/40 bg-slate-700/30 text-slate-300'}`}>
                                  <TrendingUp className={`h-3 w-3 ${trendMeta.direction === 'down' ? 'rotate-180' : trendMeta.direction === 'flat' ? 'opacity-50' : ''}`} />
                                  {trendMeta.delta > 0 ? '+' : ''}{trendMeta.delta.toFixed(1)}
                                </span>
                              )}
                              <span className={`inline-flex items-center rounded-full border px-2 py-1 text-[10px] font-semibold ${isQualifiedLead(lead) ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-100' : 'border-amber-500/30 bg-amber-500/10 text-amber-100'}`}>{isQualifiedLead(lead) ? 'Qualified' : 'Needs work'}</span>
                              {hasReply(lead) ? (
                                <span className="inline-flex items-center rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-1 text-[10px] font-semibold text-emerald-100">↩ Replied</span>
                              ) : hasOpenedMail(lead) ? (
                                <span className="inline-flex items-center rounded-full border border-cyan-500/30 bg-cyan-500/10 px-2 py-1 text-[10px] font-semibold text-cyan-100">👁 Opened x{Math.max(1, Number(lead.open_count || 0))}</span>
                              ) : null}
                              {Number(lead.qualification_score || 0) > 0 && <span className="inline-flex items-center rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-1 text-[10px] font-semibold text-amber-100">Q {Math.round(Number(lead.qualification_score || 0))}/100</span>}
                              {socialLinks.map((social) => (
                                <a
                                  key={`mobile-${lead.id}-${social.key}`}
                                  href={social.url}
                                  target="_blank"
                                  rel="noreferrer"
                                  title={social.label}
                                  className="inline-flex h-7 w-7 items-center justify-center rounded-full border border-sky-500/30 bg-sky-500/10 text-sky-100 transition hover:border-sky-300/60 hover:text-sky-50"
                                >
                                  <social.Icon className="h-3.5 w-3.5" />
                                </a>
                              ))}
                              {techStack.map((stack) => (
                                <span key={`${lead.id}-mobile-${stack}`} className="inline-flex items-center rounded-full border border-violet-500/30 bg-violet-500/10 px-2 py-1 text-[10px] font-medium text-violet-200">{stack}</span>
                              ))}
                            </div>
                          </div>

                          <div className="mt-4 grid gap-2">
                            {showRevokedFlash && (
                              <div className="inline-flex items-center justify-center rounded-full border border-emerald-500/35 bg-emerald-500/15 px-3 py-1 text-[10px] font-semibold text-emerald-100">
                                Revoked
                              </div>
                            )}
                            <button type="button" className="btn-primary w-full justify-center py-3 text-sm" onClick={() => void moveLeadToMailer(lead)}>
                              <Send className="h-4 w-4" />
                              Move to Mailer
                            </button>
                            <div className="grid grid-cols-6 gap-2">
                              <button type="button" className="btn-ghost justify-center px-2 py-2 text-xs" onClick={() => openLeadDetailsModal(lead)}>
                                <Eye className="h-4 w-4" />
                              </button>
                              <button type="button" className="btn-ghost justify-center px-2 py-2 text-xs" onClick={() => openAiSummaryModal(lead)}>
                                <Sparkles className="h-4 w-4" />
                              </button>
                              <button
                                type="button"
                                className="btn-ghost justify-center px-2 py-2 text-xs"
                                disabled={Boolean(shareState.generating || shareState.revoking)}
                                onClick={() => void generateShareableLeadReport(lead)}
                                title={shareState.generating ? 'Generating share link…' : 'Share report'}
                              >
                                <ExternalLink className="h-4 w-4" />
                              </button>
                              <button
                                type="button"
                                className="btn-ghost justify-center px-2 py-2 text-xs"
                                onClick={() => openLeadGapReportPreview(lead)}
                                title="Open report preview"
                              >
                                <Globe className="h-4 w-4" />
                              </button>
                              <button
                                type="button"
                                className="btn-ghost justify-center px-2 py-2 text-xs"
                                disabled={!hasActiveShareLink || Boolean(shareState.generating || shareState.revoking)}
                                onClick={() => void revokeShareableLeadReport(lead)}
                                title={shareState.revoking ? 'Revoking share link…' : hasActiveShareLink ? 'Revoke share link' : 'No active share link'}
                              >
                                <EyeOff className="h-4 w-4" />
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
                            <button
                              type="button"
                              className="btn-ghost w-full justify-center px-2 py-2 text-xs"
                              disabled={pendingBlacklistLeadId === lead.id}
                              onClick={() => void removeLeadFromActiveView(lead)}
                            >
                              <Trash2 className="h-4 w-4" /> Remove
                            </button>
                          </div>
                        </article>
                      )
                    }) : (
                      <div className="rounded-[22px] border border-dashed border-slate-700 bg-slate-900/60 px-4 py-8 text-center text-sm text-slate-400">
                        {emptyLeadsMessage}
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
                    <button
                      type="button"
                      className="mt-3 inline-flex items-center gap-2 rounded-lg border border-cyan-400/30 bg-cyan-500/10 px-2.5 py-1.5 text-[11px] font-semibold text-cyan-100 transition hover:border-cyan-300/50 hover:bg-cyan-500/20 disabled:cursor-not-allowed disabled:opacity-60"
                      onClick={() => void seedTemplatePackToSavedTemplates({ silent: false })}
                      disabled={seedingTemplatePack}
                    >
                      <RefreshCw className={`h-3.5 w-3.5 ${seedingTemplatePack ? 'animate-spin' : ''}`} />
                      {seedingTemplatePack ? 'Seeding defaults...' : 'Seed defaults now'}
                    </button>
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
                              <div className="pt-2">
                                <button className="btn-primary" type="submit" disabled={savingConfig}>
                                  <Save className="h-4 w-4" />
                                  {savingConfig ? 'Saving…' : 'Save Template'}
                                </button>
                              </div>
                            </div>
                          </div>
                        )
                      })()}
                    </>
                  )}
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
                      This preview updates live as you edit the draft and shows how placeholders resolve for a sample lead.
                    </p>
                    <div className="mt-3 grid gap-3 sm:grid-cols-2">
                      <div className="rounded-xl border border-cyan-500/25 bg-cyan-500/[0.06] px-3 py-2">
                        <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Generated</p>
                        <p className="mt-1 text-sm font-semibold text-cyan-100">Live draft</p>
                        <div className="mt-2 rounded-lg border border-cyan-500/20 bg-slate-950/60 px-2.5 py-2">
                          <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Subject</p>
                          <p className="mt-1 text-[12px] leading-6 text-slate-200">
                            {mailPreviewRaw.subject ? renderTemplateWithPlaceholderHighlights(mailPreviewRaw.subject) : 'Select a template to preview the draft.'}
                          </p>
                          <p className="mt-2 text-[11px] uppercase tracking-[0.12em] text-slate-500">Body</p>
                          <pre className="mt-1 max-h-[84px] overflow-auto whitespace-pre-wrap break-words font-sans text-[12px] leading-6 text-slate-200">
                            {mailPreviewRaw.body ? renderTemplateWithPlaceholderHighlights(mailPreviewRaw.body) : 'Draft body will appear here.'}
                          </pre>
                        </div>
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
                              <p className="font-medium text-white">Business Owner</p>
                              <p className="text-xs text-slate-500">owner@business.com</p>
                            </div>
                          </div>
                          <div className="flex items-start gap-3">
                            <span className="w-12 shrink-0 text-[11px] uppercase tracking-[0.12em] text-slate-500">Subject</span>
                            <p className="font-medium text-white">{mailPreview.subject || 'Subject preview will appear here.'}</p>
                          </div>
                        </div>

                        <div className="rounded-2xl border border-white/10 bg-slate-950/70 p-4">
                          <pre className="min-h-[260px] whitespace-pre-wrap break-words font-sans text-[14px] leading-7 text-slate-200">{mailPreview.body || 'Preview body will appear here.'}</pre>
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
                  <p className="text-xs text-slate-500">Saved templates apply to both preview and real sends.</p>
                </div>
              </div>
            </form>
          ) : activeTab === 'admin' ? (
            <div className="space-y-5">
              <div className="grid gap-3 md:grid-cols-3">
                <div className="rounded-2xl border border-cyan-500/20 bg-cyan-500/10 p-4">
                  <p className="text-xs uppercase tracking-[0.12em] text-cyan-200/80">Total Users</p>
                  <p className="mt-2 text-3xl font-bold text-white">{Number(adminOverview.stats.total_users || 0).toLocaleString('en-US')}</p>
                </div>
                <div className="rounded-2xl border border-emerald-500/20 bg-emerald-500/10 p-4">
                  <p className="text-xs uppercase tracking-[0.12em] text-emerald-200/80">Total Revenue</p>
                  <p className="mt-2 text-3xl font-bold text-white">${formatUsd(adminOverview.stats.total_revenue || 0)}</p>
                </div>
                <div className="rounded-2xl border border-violet-500/20 bg-violet-500/10 p-4">
                  <p className="text-xs uppercase tracking-[0.12em] text-violet-200/80">Total Scraped Leads</p>
                  <p className="mt-2 text-3xl font-bold text-white">{Number(adminOverview.stats.total_leads || 0).toLocaleString('en-US')}</p>
                </div>
              </div>

              <div className="inline-flex flex-wrap items-center gap-2 rounded-2xl border border-white/10 bg-white/[0.03] p-1">
                <button type="button" className={`rounded-xl px-3 py-2 text-xs font-semibold ${adminSection === 'users' ? 'bg-cyan-500/20 text-cyan-100' : 'text-slate-300 hover:text-white'}`} onClick={() => setAdminSection('users')}>Users</button>
                <button type="button" className={`rounded-xl px-3 py-2 text-xs font-semibold ${adminSection === 'payments' ? 'bg-cyan-500/20 text-cyan-100' : 'text-slate-300 hover:text-white'}`} onClick={() => setAdminSection('payments')}>Payments</button>
                <button type="button" className={`rounded-xl px-3 py-2 text-xs font-semibold ${adminSection === 'system' ? 'bg-cyan-500/20 text-cyan-100' : 'text-slate-300 hover:text-white'}`} onClick={() => setAdminSection('system')}>System Health</button>
                <button type="button" className={`rounded-xl px-3 py-2 text-xs font-semibold ${adminSection === 'logs' ? 'bg-cyan-500/20 text-cyan-100' : 'text-slate-300 hover:text-white'}`} onClick={() => setAdminSection('logs')}>Logs</button>
              </div>

              {adminSection === 'users' ? (
                <>
                  <div className="rounded-2xl border border-white/10 bg-white/[0.02] p-4">
                    <div className="mb-3 flex items-center justify-between">
                      <p className="text-sm font-semibold text-white">Users</p>
                      <p className="text-xs text-slate-400">{adminOverview.users.length} rows</p>
                    </div>
                    <div className="max-h-[460px] overflow-auto rounded-xl border border-white/10">
                      <table className="min-w-full divide-y divide-white/10 text-sm">
                        <thead className="bg-white/[0.03] text-left text-xs uppercase tracking-[0.1em] text-slate-400">
                          <tr>
                            <th className="px-3 py-2">Email</th>
                            <th className="px-3 py-2">Last Login</th>
                            <th className="px-3 py-2">Plan</th>
                            <th className="px-3 py-2">Subscription</th>
                            <th className="px-3 py-2">Status</th>
                            <th className="px-3 py-2">Credits</th>
                            <th className="px-3 py-2">Actions</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-white/5">
                          {adminOverview.users.map((row) => (
                            <tr key={row.id || row.email} className="hover:bg-white/[0.03]">
                              <td className="px-3 py-2 text-slate-200">{row.email || '-'}</td>
                              <td className="px-3 py-2 text-slate-300">{row.last_login_at || '-'}</td>
                              <td className="px-3 py-2 text-slate-300">{row.plan_name || row.plan_key || 'free'}</td>
                              <td className="px-3 py-2">
                                <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-semibold ${row.subscription_active ? 'bg-emerald-500/20 text-emerald-200' : 'bg-slate-500/20 text-slate-300'}`}>
                                  {row.subscription_active ? 'active' : 'inactive'}
                                </span>
                              </td>
                              <td className="px-3 py-2">
                                <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-semibold ${row.is_blocked ? 'bg-rose-500/20 text-rose-200' : 'bg-emerald-500/20 text-emerald-200'}`}>
                                  {row.is_blocked ? 'blocked' : 'ok'}
                                </span>
                              </td>
                              <td className="px-3 py-2 text-slate-300">{Number(row.credits_balance || 0).toLocaleString('en-US')} / {Number(row.credits_limit || 0).toLocaleString('en-US')}</td>
                              <td className="px-3 py-2">
                                <div className="flex flex-wrap gap-1.5">
                                  <button type="button" className={`rounded-lg px-2 py-1 text-xs font-semibold ${row.is_blocked ? 'bg-emerald-500/20 text-emerald-100' : 'bg-rose-500/20 text-rose-100'}`} onClick={() => adminToggleBlock(row, !row.is_blocked)}>
                                    {row.is_blocked ? <UserCheck className="inline h-3.5 w-3.5 mr-1" /> : <UserX className="inline h-3.5 w-3.5 mr-1" />}
                                    {row.is_blocked ? 'Unblock' : 'BAN/BLOCK'}
                                  </button>
                                  <button type="button" className="rounded-lg bg-violet-500/20 px-2 py-1 text-xs font-semibold text-violet-100" onClick={() => adminImpersonate(row)}>
                                    <LogIn className="inline h-3.5 w-3.5 mr-1" />IMPERSONATE
                                  </button>
                                  <button type="button" className="rounded-lg bg-cyan-500/20 px-2 py-1 text-xs font-semibold text-cyan-100" onClick={() => adminResetPassword(row)}>
                                    <KeyRound className="inline h-3.5 w-3.5 mr-1" />RESET PASSWORD
                                  </button>
                                </div>
                              </td>
                            </tr>
                          ))}
                          {!adminOverview.users.length ? (
                            <tr>
                              <td className="px-3 py-6 text-center text-slate-500" colSpan={7}>{adminLoading ? 'Loading users…' : 'No users found.'}</td>
                            </tr>
                          ) : null}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </>
              ) : null}

              {adminSection === 'payments' ? (
                <>
                  <form className="rounded-2xl border border-white/10 bg-white/[0.03] p-4" onSubmit={adminUpdatePlan}>
                    <p className="text-xs uppercase tracking-[0.12em] text-slate-400">Manual Plan Override</p>
                    <div className="mt-3 grid gap-3 md:grid-cols-3">
                      <label className="field-label">
                        <span className="mb-1.5 block">User</span>
                        <select className="glass-input" value={adminPlanForm.userId} onChange={(e) => setAdminPlanForm((prev) => ({ ...prev, userId: e.target.value }))}>
                          <option value="">Select user...</option>
                          {adminOverview.users.map((row) => (
                            <option key={row.id} value={row.id}>{row.email}</option>
                          ))}
                        </select>
                      </label>
                      <label className="field-label">
                        <span className="mb-1.5 block">Plan</span>
                        <select className="glass-input" value={adminPlanForm.planKey} onChange={(e) => setAdminPlanForm((prev) => ({ ...prev, planKey: e.target.value }))}>
                          <option value="free">The Starter</option>
                          <option value="hustler">The Hustler</option>
                          <option value="growth">The Growth</option>
                          <option value="scale">The Scale</option>
                          <option value="empire">The Empire</option>
                        </select>
                      </label>
                      <div className="flex items-end">
                        <button className="btn-primary w-full" type="submit" disabled={adminLoading}>Apply Plan</button>
                      </div>
                    </div>
                  </form>

                  <div className="rounded-2xl border border-white/10 bg-white/[0.02] p-4">
                    <div className="mb-3 flex items-center justify-between">
                      <p className="text-sm font-semibold text-white">Latest Transactions</p>
                      <p className="text-xs text-slate-400">{adminOverview.transactions.length} entries</p>
                    </div>
                    <div className="max-h-[420px] overflow-auto rounded-xl border border-white/10">
                      <table className="min-w-full divide-y divide-white/10 text-sm">
                        <thead className="bg-white/[0.03] text-left text-xs uppercase tracking-[0.1em] text-slate-400">
                          <tr>
                            <th className="px-3 py-2">Date</th>
                            <th className="px-3 py-2">User</th>
                            <th className="px-3 py-2">Service</th>
                            <th className="px-3 py-2">Amount</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-white/5">
                          {adminOverview.transactions.map((row) => (
                            <tr key={`txn-${row.id}`} className="hover:bg-white/[0.03]">
                              <td className="px-3 py-2 text-slate-300">{row.date || '-'}</td>
                              <td className="px-3 py-2 text-slate-200">{row.email || row.user_id || '-'}</td>
                              <td className="px-3 py-2 text-slate-300">{row.service_type || '-'}</td>
                              <td className="px-3 py-2 text-emerald-200">${formatUsd(row.amount || 0)}</td>
                            </tr>
                          ))}
                          {!adminOverview.transactions.length ? (
                            <tr><td className="px-3 py-6 text-center text-slate-500" colSpan={4}>No transactions found.</td></tr>
                          ) : null}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </>
              ) : null}

              {adminSection === 'system' ? (
                <>
                  <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                    <div className="flex flex-wrap items-center justify-between gap-3">
                      <div>
                        <p className="text-xs uppercase tracking-[0.12em] text-slate-400">Scraper Health</p>
                        <p className="mt-1 text-sm text-slate-300">Latest task status: <span className="font-semibold text-white">{adminOverview.scraper.last_status || 'unknown'}</span></p>
                        <p className="mt-2 text-[11px] text-slate-500">Updated: {adminOverview.scraper.last_updated_at || 'n/a'}</p>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className={`inline-flex items-center gap-2 rounded-full px-3 py-1 text-xs font-semibold ${adminOverview.scraper.health === 'running' ? 'bg-amber-500/20 text-amber-200' : adminOverview.scraper.health === 'failing' ? 'bg-rose-500/20 text-rose-200' : 'bg-emerald-500/20 text-emerald-200'}`}>
                          <span className="h-2 w-2 rounded-full bg-current" />
                          {adminOverview.scraper.health || 'healthy'}
                        </span>
                        <button type="button" className="rounded-xl bg-rose-500/20 px-3 py-2 text-sm font-semibold text-rose-100" onClick={adminRestartScrapers}>
                          <RotateCcw className="inline h-4 w-4 mr-1" />RESTART SCRAPERS
                        </button>
                      </div>
                    </div>
                    {adminOverview.scraper.last_error ? <p className="mt-3 rounded-xl border border-rose-500/20 bg-rose-500/10 px-3 py-2 text-xs text-rose-100">{adminOverview.scraper.last_error}</p> : null}
                  </div>

                  <form className="rounded-2xl border border-white/10 bg-white/[0.03] p-4" onSubmit={adminSaveAiSignalsToggle}>
                    <p className="text-xs uppercase tracking-[0.12em] text-slate-400">AI Signals Global Switch</p>
                    <p className="mt-1 text-xs text-slate-500">Toggle market intelligence engine for all users.</p>
                    <label className="mt-3 inline-flex items-center gap-2 text-sm text-slate-300">
                      <input type="checkbox" checked={aiSignalsEnabledForm} onChange={(e) => setAiSignalsEnabledForm(e.target.checked)} /> Enable AI Signals platform-wide
                    </label>
                    <p className="mt-2 text-[11px] text-slate-500">
                      Last update: {adminOverview.ai_signals?.updated_at || 'n/a'}
                      {adminOverview.ai_signals?.updated_by ? ` by ${adminOverview.ai_signals.updated_by}` : ''}
                    </p>
                    <div className="mt-3 flex items-center gap-3">
                      <button className="btn-primary" type="submit" disabled={adminLoading}>Save AI Signal Toggle</button>
                    </div>
                  </form>

                  <form className="rounded-2xl border border-white/10 bg-white/[0.03] p-4" onSubmit={adminSaveGlobalNotification}>
                    <p className="text-xs uppercase tracking-[0.12em] text-slate-400">Global Notification</p>
                    <label className="field-label mt-3 block">
                      <span className="mb-1.5 block">Message</span>
                      <input className="glass-input" type="text" value={globalNoticeForm.message} onChange={(e) => setGlobalNoticeForm((prev) => ({ ...prev, message: e.target.value }))} placeholder="Maintenance in 10 min" />
                    </label>
                    <label className="mt-3 inline-flex items-center gap-2 text-sm text-slate-300">
                      <input type="checkbox" checked={globalNoticeForm.active} onChange={(e) => setGlobalNoticeForm((prev) => ({ ...prev, active: e.target.checked }))} /> Active banner
                    </label>
                    <div className="mt-3 flex items-center gap-3">
                      <button className="btn-primary" type="submit" disabled={adminLoading}><Bell className="inline h-4 w-4 mr-1" />Publish Banner</button>
                      <button className="rounded-xl border border-white/15 bg-white/[0.02] px-3 py-2 text-sm text-slate-300 hover:bg-white/[0.08]" type="button" onClick={() => void refreshAdminOverview()}>Sync</button>
                    </div>
                  </form>
                </>
              ) : null}

              {adminSection === 'logs' ? (
                <>
                  <div className="grid gap-3 md:grid-cols-2">
                    <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                      <p className="text-xs uppercase tracking-[0.12em] text-slate-400">Top Scrapers</p>
                      <div className="mt-3 space-y-2">
                        {adminOverview.top_scrapers.slice(0, 10).map((row) => (
                          <div key={`top-${row.user_id}`} className="flex items-center justify-between rounded-lg border border-white/10 bg-white/[0.02] px-3 py-2 text-sm">
                            <span className="text-slate-200">{row.email || row.user_id}</span>
                            <span className="font-semibold text-cyan-200">{Number(row.scraped_count || 0).toLocaleString('en-US')}</span>
                          </div>
                        ))}
                        {!adminOverview.top_scrapers.length ? <p className="text-sm text-slate-500">No scraper usage yet.</p> : null}
                      </div>
                    </div>
                    <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                      <p className="text-xs uppercase tracking-[0.12em] text-slate-400">Lead Quality</p>
                      <p className="mt-2 text-3xl font-bold text-white">{Number(adminOverview.lead_quality.success_rate || 0).toFixed(2)}%</p>
                      <p className="mt-1 text-sm text-slate-300">Success ratio from enrichment attempts.</p>
                      <p className="mt-3 text-xs text-slate-400">Successful: {Number(adminOverview.lead_quality.successful || 0).toLocaleString('en-US')}</p>
                      <p className="text-xs text-slate-400">Attempted: {Number(adminOverview.lead_quality.attempted || 0).toLocaleString('en-US')}</p>
                    </div>
                  </div>

                  <div className="rounded-2xl border border-white/10 bg-white/[0.02] p-4">
                    <div className="mb-3 flex items-center justify-between">
                      <p className="text-sm font-semibold text-white">Activity Logs</p>
                      <p className="text-xs text-slate-400">{adminOverview.logs.length} records</p>
                    </div>
                    <div className="max-h-[420px] overflow-auto rounded-xl border border-white/10">
                      <table className="min-w-full divide-y divide-white/10 text-sm">
                        <thead className="bg-white/[0.03] text-left text-xs uppercase tracking-[0.1em] text-slate-400">
                          <tr>
                            <th className="px-3 py-2">When</th>
                            <th className="px-3 py-2">Type</th>
                            <th className="px-3 py-2">User</th>
                            <th className="px-3 py-2">Status</th>
                            <th className="px-3 py-2">Message</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-white/5">
                          {adminOverview.logs.map((row) => (
                            <tr key={`${row.kind}-${row.id}-${row.created_at || ''}`} className="hover:bg-white/[0.03]">
                              <td className="px-3 py-2 text-slate-300">{row.created_at || '-'}</td>
                              <td className="px-3 py-2 text-slate-300">{row.kind || '-'}</td>
                              <td className="px-3 py-2 text-slate-200">{row.email || row.user_id || '-'}</td>
                              <td className="px-3 py-2 text-slate-300">{row.status || '-'}</td>
                              <td className="px-3 py-2 text-slate-300">{row.message || '-'}</td>
                            </tr>
                          ))}
                          {!adminOverview.logs.length ? <tr><td className="px-3 py-6 text-center text-slate-500" colSpan={5}>No logs found.</td></tr> : null}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </>
              ) : null}
            </div>
          ) : activeTab === 'config' ? (
            <form className="max-w-2xl space-y-6" onSubmit={saveConfig}>
              <div>
                <h3 className="text-base font-semibold text-white mb-4 flex items-center gap-2">
                  <Settings className="h-4 w-4 text-cyan-400" /> Azure OpenAI
                </h3>
                <p className="text-xs text-slate-500">API key is configured from environment-backed server settings.</p>
              </div>
              <div className="inline-flex items-center rounded-2xl border border-white/10 bg-white/[0.03] p-1">
                <button
                  type="button"
                  className={`rounded-xl px-3 py-1.5 text-xs font-semibold transition ${configSettingsTab === 'platform' ? 'bg-cyan-500/20 text-cyan-100' : 'text-slate-400 hover:text-white'}`}
                  onClick={() => setConfigSettingsTab('platform')}
                >
                  Platform
                </button>
                <button
                  type="button"
                  className={`rounded-xl px-3 py-1.5 text-xs font-semibold transition ${configSettingsTab === 'developer' ? 'bg-cyan-500/20 text-cyan-100' : 'text-slate-400 hover:text-white'}`}
                  onClick={() => setConfigSettingsTab('developer')}
                >
                  Developer
                </button>
              </div>
              {configSettingsTab === 'platform' ? (
              <>
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
              </>
              ) : (
                <div className="space-y-4 rounded-2xl border border-cyan-500/20 bg-cyan-500/5 p-4">
                  <h3 className="text-base font-semibold text-white flex items-center gap-2">
                    <TerminalSquare className="h-4 w-4 text-cyan-300" /> Developer Webhooks
                  </h3>
                  <p className="text-xs text-slate-400">POST events when lead state changes so your automations can react in real time.</p>
                  <label className="field-label block">
                    <span className="mb-1.5 block">Developer webhook URL</span>
                    <input
                      className="glass-input"
                      type="url"
                      placeholder="https://your-app.com/webhooks/leads"
                      value={configForm.developer_webhook_url || ''}
                      onChange={(e) => setConfigForm({ ...configForm, developer_webhook_url: e.target.value })}
                    />
                    <span className="mt-1 block text-[11px] text-slate-500">Events: <span className="text-slate-300">lead.moved_to_replied</span>, <span className="text-slate-300">lead.score_dropped_below_threshold</span></span>
                  </label>
                  <label className="field-label block max-w-sm">
                    <span className="mb-1.5 block">Score-drop threshold (0-10)</span>
                    <input
                      className="glass-input"
                      type="number"
                      min="0"
                      max="10"
                      step="0.1"
                      value={configForm.developer_score_drop_threshold ?? 6}
                      onChange={(e) => setConfigForm({ ...configForm, developer_score_drop_threshold: e.target.value })}
                    />
                  </label>
                </div>
              )}
              <div className="flex items-center gap-4">
                <button className="btn-primary" type="submit" disabled={savingConfig}>
                  <Save className="h-4 w-4" />
                  {savingConfig ? 'Saving…' : 'Save Config'}
                </button>
                <div className="flex gap-3 text-xs">
                  <span className={`flex items-center gap-1.5 ${configHealth.openai_ok ? 'text-emerald-400' : 'text-rose-400'}`}>
                    <span className={`h-1.5 w-1.5 rounded-full ${configHealth.openai_ok ? 'bg-emerald-400' : 'bg-rose-400'}`} />
                    Azure OpenAI {configHealth.openai_ok ? 'OK' : 'Not set'}
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
                <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-200">
                  {qualifierData.error}
                </div>
              ) : null}

              {!qualifierData.data && !qualifierData.loading ? (
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
                  <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                    <div className="glass-card rounded-2xl p-4 flex items-center gap-3">
                      <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-rose-500/15">
                        <Target className="h-5 w-5 text-rose-400" />
                      </div>
                      <div>
                        <p className="text-2xl font-bold text-white">{qualifierData.data?.counts?.no_website ?? qualifierData.data?.counts?.ghost ?? 0}</p>
                        <p className="text-xs text-slate-400 font-semibold uppercase tracking-wide">No Website</p>
                        <p className="text-[10px] text-rose-400 mt-0.5">Website-first pitch</p>
                      </div>
                    </div>
                    <div className="glass-card rounded-2xl p-4 flex items-center gap-3">
                      <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-amber-500/15">
                        <Search className="h-5 w-5 text-amber-400" />
                      </div>
                      <div>
                        <p className="text-2xl font-bold text-white">{qualifierData.data?.counts?.traffic_opportunity ?? qualifierData.data?.counts?.invisible_local ?? 0}</p>
                        <p className="text-xs text-slate-400 font-semibold uppercase tracking-wide">Traffic Opportunity</p>
                        <p className="text-[10px] text-amber-400 mt-0.5">Low SEO/perf, high upside</p>
                      </div>
                    </div>
                    <div className="glass-card rounded-2xl p-4 flex items-center gap-3">
                      <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-violet-500/15">
                        <Users className="h-5 w-5 text-violet-300" />
                      </div>
                      <div>
                        <p className="text-2xl font-bold text-white">{qualifierData.data?.counts?.competitor_gap ?? qualifierData.data?.counts?.invisible_giant ?? 0}</p>
                        <p className="text-xs text-slate-400 font-semibold uppercase tracking-wide">Competitor Gap</p>
                        <p className="text-[10px] text-violet-300 mt-0.5">Outranked by competitors</p>
                      </div>
                    </div>
                    <div className="glass-card rounded-2xl p-4 flex items-center gap-3">
                      <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-teal-500/15">
                        <TrendingUp className="h-5 w-5 text-teal-400" />
                      </div>
                      <div>
                        <p className="text-2xl font-bold text-white">{qualifierData.data?.counts?.site_speed ?? qualifierData.data?.counts?.tech_debt ?? 0}</p>
                        <p className="text-xs text-slate-400 font-semibold uppercase tracking-wide">Site Speed</p>
                        <p className="text-[10px] text-teal-400 mt-0.5">Performance below 50%</p>
                      </div>
                    </div>
                  </div>

                  {/* No Website bucket */}
                  {((qualifierData.data?.no_website?.length ?? qualifierData.data?.ghost?.length) ?? 0) > 0 && (
                    <div>
                      <div className="mb-3 flex items-center gap-2">
                        <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-rose-500/20 text-xs font-bold text-rose-400">1</span>
                        <h3 className="font-semibold text-white text-sm">No Website — Gold Mine Opportunity</h3>
                        <span className="rounded-full bg-rose-500/15 px-2 py-0.5 text-xs font-bold text-rose-400">
                          {(qualifierData.data?.no_website ?? qualifierData.data?.ghost ?? []).length} leads
                        </span>
                      </div>
                      <div className="space-y-3">
                        {(qualifierData.data?.no_website ?? qualifierData.data?.ghost ?? []).map((lead) => (
                          <QualifierLeadCard
                            key={lead.id}
                            lead={lead}
                            accentClass="border-rose-500/20 bg-rose-950/10"
                            badgeClass="bg-rose-500/15 text-rose-400"
                            onGenerateEmail={generateQualifierLeadEmail}
                            onAddToPipeline={addQualifierLeadToPipeline}
                            onSkip={skipQualifierLead}
                          />
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Traffic Opportunity bucket */}
                  {((qualifierData.data?.traffic_opportunity?.length ?? qualifierData.data?.invisible_local?.length) ?? 0) > 0 && (
                    <div>
                      <div className="mb-3 flex items-center gap-2">
                        <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-amber-500/20 text-xs font-bold text-amber-400">2</span>
                        <h3 className="font-semibold text-white text-sm">Traffic Opportunity</h3>
                        <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-xs font-bold text-amber-400">
                          {(qualifierData.data?.traffic_opportunity ?? qualifierData.data?.invisible_local ?? []).length} leads
                        </span>
                      </div>
                      <div className="space-y-3">
                        {(qualifierData.data?.traffic_opportunity ?? qualifierData.data?.invisible_local ?? []).map((lead) => (
                          <QualifierLeadCard
                            key={lead.id}
                            lead={lead}
                            accentClass="border-amber-500/20 bg-amber-950/10"
                            badgeClass="bg-amber-500/15 text-amber-400"
                            onGenerateEmail={generateQualifierLeadEmail}
                            onAddToPipeline={addQualifierLeadToPipeline}
                            onSkip={skipQualifierLead}
                          />
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Competitor Gap bucket */}
                  {((qualifierData.data?.competitor_gap?.length ?? qualifierData.data?.invisible_giant?.length) ?? 0) > 0 && (
                    <div>
                      <div className="mb-3 flex items-center gap-2">
                        <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-violet-500/20 text-xs font-bold text-violet-300">3</span>
                        <h3 className="font-semibold text-white text-sm">Competitor Gap</h3>
                        <span className="rounded-full bg-violet-500/15 px-2 py-0.5 text-xs font-bold text-violet-300">
                          {(qualifierData.data?.competitor_gap ?? qualifierData.data?.invisible_giant ?? []).length} leads
                        </span>
                      </div>
                      <div className="space-y-3">
                        {(qualifierData.data?.competitor_gap ?? qualifierData.data?.invisible_giant ?? []).map((lead) => (
                          <QualifierLeadCard
                            key={lead.id}
                            lead={lead}
                            accentClass="border-violet-500/20 bg-violet-950/10"
                            badgeClass="bg-violet-500/15 text-violet-300"
                            onGenerateEmail={generateQualifierLeadEmail}
                            onAddToPipeline={addQualifierLeadToPipeline}
                            onSkip={skipQualifierLead}
                          />
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Site Speed bucket */}
                  {((qualifierData.data?.site_speed?.length ?? qualifierData.data?.tech_debt?.length) ?? 0) > 0 && (
                    <div>
                      <div className="mb-3 flex items-center gap-2">
                        <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-teal-500/20 text-xs font-bold text-teal-400">4</span>
                        <h3 className="font-semibold text-white text-sm">Site Speed</h3>
                        <span className="rounded-full bg-teal-500/15 px-2 py-0.5 text-xs font-bold text-teal-400">
                          {(qualifierData.data?.site_speed ?? qualifierData.data?.tech_debt ?? []).length} leads
                        </span>
                      </div>
                      <div className="space-y-3">
                        {(qualifierData.data?.site_speed ?? qualifierData.data?.tech_debt ?? []).map((lead) => (
                          <QualifierLeadCard
                            key={lead.id}
                            lead={lead}
                            accentClass="border-teal-500/20 bg-teal-950/10"
                            badgeClass="bg-teal-500/15 text-teal-400"
                            onGenerateEmail={generateQualifierLeadEmail}
                            onAddToPipeline={addQualifierLeadToPipeline}
                            onSkip={skipQualifierLead}
                          />
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

              <div className="glass-card rounded-2xl p-5 flex flex-col gap-3">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <p className="font-semibold text-white text-sm flex items-center gap-2">AI Cost Dashboard {!canAdvancedReporting ? <PremiumBadge label="Business+" /> : null}</p>
                    <p className="text-xs text-slate-400 mt-1">Track total token usage and estimated LLM spend across your leads.</p>
                  </div>
                  <button className="btn-secondary" type="button" disabled={!canAdvancedReporting || loadingAiCostReport} onClick={() => void refreshAiCostReport()}>
                    <RefreshCw className={`h-4 w-4 ${loadingAiCostReport ? 'animate-spin' : ''}`} />
                    Refresh
                  </button>
                </div>

                <div className="grid gap-2 text-xs sm:grid-cols-3">
                  <div className="rounded-xl border border-slate-700/60 bg-slate-900/60 px-3 py-2">
                    <p className="text-slate-400">Total cost (USD)</p>
                    <p className="mt-1 text-lg font-semibold text-amber-200">${formatUsd(aiCostReport?.total_cost_usd || 0)}</p>
                  </div>
                  <div className="rounded-xl border border-slate-700/60 bg-slate-900/60 px-3 py-2">
                    <p className="text-slate-400">Total tokens</p>
                    <p className="mt-1 text-lg font-semibold text-white">{formatCreditAmount(aiCostReport?.total_tokens_used || 0)}</p>
                  </div>
                  <div className="rounded-xl border border-slate-700/60 bg-slate-900/60 px-3 py-2">
                    <p className="text-slate-400">Billed leads</p>
                    <p className="mt-1 text-lg font-semibold text-cyan-200">{Number(aiCostReport?.billed_leads_count || 0)}</p>
                  </div>
                </div>

                <div className="rounded-xl border border-slate-800 bg-slate-950/70 px-3 py-3">
                  <p className="text-[11px] uppercase tracking-[0.14em] text-slate-500">Top lead spend</p>
                  {!Array.isArray(aiCostReport?.top_leads) || aiCostReport.top_leads.length === 0 ? (
                    <p className="mt-2 text-xs text-slate-400">No billed leads yet.</p>
                  ) : (
                    <div className="mt-2 space-y-1.5">
                      {aiCostReport.top_leads.slice(0, 5).map((lead) => (
                        <div key={String(lead.id)} className="flex items-center justify-between gap-2 text-xs">
                          <span className="truncate text-slate-300">{lead.business_name || `Lead #${lead.id}`}</span>
                          <span className="whitespace-nowrap text-amber-200">${formatUsd(lead.cost_usd || 0)} • {formatCreditAmount(lead.tokens_used || 0)} tok</span>
                        </div>
                      ))}
                    </div>
                  )}
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

      <AnimatePresence>
        {showLowCreditsModal && (
          <Motion.div
            className="fixed inset-0 z-[70] flex items-center justify-center bg-[rgba(2,6,23,0.82)] p-3 backdrop-blur-sm sm:p-5"
            onClick={(e) => { if (e.target === e.currentTarget) setShowLowCreditsModal(false) }}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
          >
            <Motion.div
              className="glass-card w-full max-w-xl rounded-3xl p-6 shadow-2xl"
              initial={{ opacity: 0, y: 8, scale: 0.98 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0, y: 8, scale: 0.98 }}
              transition={{ duration: 0.2 }}
            >
              <div className="mb-6 flex items-start justify-between gap-4">
                <div>
                  <div className="mb-2 inline-flex h-10 w-10 items-center justify-center rounded-2xl bg-emerald-500/15 text-emerald-300 ring-1 ring-emerald-400/25">
                    <Rocket className="h-5 w-5" />
                  </div>
                  <p className="label-overline text-emerald-300">Credits Required</p>
                  <h3 className="mt-1.5 text-2xl font-semibold text-white">Need More Credits to Launch This Scrape</h3>
                  <p className="mt-2 max-w-xl text-sm leading-6 text-slate-300">
                    This scrape needs {creditIntegerFormatter.format(requiredScrapeCredits)} credits, but your account currently has only {creditsBalanceLabel}. Top up credits or upgrade your plan to keep searching without limits.
                  </p>
                </div>
                <button
                  type="button"
                  className="rounded-xl p-2 text-slate-400 transition-all duration-200 hover:bg-white/10 hover:text-white"
                  onClick={() => setShowLowCreditsModal(false)}
                  aria-label="Close"
                >
                  ✕
                </button>
              </div>

              <div className="grid gap-3 sm:grid-cols-3">
                <button
                  type="button"
                  className="btn-primary w-full justify-center"
                  onClick={() => {
                    setShowLowCreditsModal(false)
                    void handleTopUpClick()
                  }}
                >
                  <PlusCircle className="h-4 w-4" /> Buy Credits
                </button>
                <button
                  type="button"
                  className="workflow-btn w-full justify-center"
                  style={{ background: 'linear-gradient(135deg,#0ea5e9,#2563eb)' }}
                  onClick={() => {
                    setShowLowCreditsModal(false)
                    openPricingSection()
                  }}
                >
                  <ExternalLink className="h-4 w-4" /> View Plans
                </button>
                <button
                  type="button"
                  className="btn-ghost w-full justify-center"
                  onClick={() => setShowLowCreditsModal(false)}
                >
                  Cancel
                </button>
              </div>
            </Motion.div>
          </Motion.div>
        )}
      </AnimatePresence>

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

      {leadDetailsPreviewLead && (() => {
        const ld = leadDetailsPreviewLead
        const ldScore = resolveBestLeadScore(ld)
        const ldStage = resolvePipelineStage(ld)
        const ldStatus = normalizeLeadStatus(ld.status)
        const ldQualified = isQualifiedLead(ld)
        const ldTechStack = normalizeLeadInsightList(ld.tech_stack, 8)
        const ldIntentSignals = normalizeLeadInsightList(ld.intent_signals, 6)
        const ldRating = Number(ld.rating || 0)
        const ldReviews = Number(ld.review_count || 0)
        const ldGoogleClaimed = Boolean(ld.google_claimed)
        const ldPhone = ld.phone_formatted || ld.phone_number
        const ldEnrichmentPayload = parseLeadEnrichmentData(ld)
        const ldScoreBreakdown = resolveLeadScoreBreakdown(ld)
        const ldNicheName = String(ldEnrichmentPayload?.user_niche || user?.niche || getStoredValue('lf_niche') || '').trim() || 'Current Niche'
        const ldEmailHistoryItems = Array.isArray(leadEmailHistory?.items) ? leadEmailHistory.items : []
        return (
          <div
            className="fixed inset-0 z-[65] flex items-center justify-center bg-slate-950/85 px-4"
            style={{backdropFilter: 'blur(8px)'}}
            onClick={(e) => { if (e.target === e.currentTarget) closeLeadDetailsModal() }}
          >
            <div className="w-full max-w-2xl rounded-3xl border border-white/10 bg-[#0d1424] shadow-[0_28px_80px_rgba(2,6,23,0.6)] overflow-hidden">

              {/* Header */}
              <div className="flex items-start justify-between gap-3 border-b border-white/8 bg-gradient-to-r from-slate-900 to-slate-900/60 px-6 py-5">
                <div className="flex items-center gap-3 min-w-0">
                  <div className="flex h-11 w-11 flex-shrink-0 items-center justify-center rounded-2xl bg-cyan-500/15 border border-cyan-500/25">
                    <Building2 className="h-5 w-5 text-cyan-300" />
                  </div>
                  <div className="min-w-0">
                    <h3 className="text-lg font-semibold text-white truncate">{ld.business_name || 'Unknown business'}</h3>
                    <p className="text-xs text-slate-400 truncate">{ld.search_keyword || ld.address || ''}</p>
                  </div>
                </div>
                <div className="flex items-center gap-2 flex-shrink-0">
                  {ldScore > 0 && (
                    <span className={`inline-flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-bold ${ldQualified ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-200' : 'border-amber-500/30 bg-amber-500/10 text-amber-200'}`}>
                      <Sparkles className="h-3 w-3" /> Niche {formatLeadScoreValue(ldScore)}/10
                    </span>
                  )}
                  {ldScoreBreakdown.length > 0 && (
                    <button
                      type="button"
                      className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11px] font-semibold transition ${showLeadScoreBreakdown ? 'border-cyan-400/40 bg-cyan-500/15 text-cyan-200' : 'border-slate-600/60 bg-slate-800/70 text-slate-300 hover:border-cyan-500/40 hover:text-cyan-200'}`}
                      onClick={() => setShowLeadScoreBreakdown((prev) => !prev)}
                      title="Show Niche Suitability Breakdown"
                    >
                      <Info className="h-3.5 w-3.5" /> Breakdown
                    </button>
                  )}
                  <button
                    type="button"
                    className="flex h-8 w-8 items-center justify-center rounded-full border border-white/10 text-slate-400 transition hover:bg-white/10 hover:text-white"
                    onClick={closeLeadDetailsModal}
                    aria-label="Close"
                  >✕</button>
                </div>
              </div>

              <div className="max-h-[70vh] overflow-y-auto p-6 space-y-4">

                {showLeadScoreBreakdown && ldScoreBreakdown.length > 0 && (
                  <div className="rounded-2xl border border-cyan-500/20 bg-cyan-500/5 p-4">
                    <div className="mb-3 flex items-center justify-between gap-2">
                      <p className="text-[10px] font-bold uppercase tracking-widest text-cyan-300">Niche Suitability Breakdown</p>
                      <span className="text-[10px] text-slate-400">{ldNicheName}</span>
                    </div>
                    <div className="space-y-2">
                      {ldScoreBreakdown.map((row, index) => {
                        const positive = Number(row?.impact || 0) >= 0
                        const impactValue = Number(row?.impact || 0)
                        const impactLabel = `${positive ? '+' : ''}${impactValue.toFixed(1)}`
                        return (
                          <div key={`score-breakdown-${index}-${row.label}`} className="flex items-start gap-2 rounded-xl border border-white/10 bg-slate-900/60 px-3 py-2">
                            <span className={`min-w-[3.1rem] rounded-md border px-1.5 py-0.5 text-center text-xs font-bold ${positive ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-300' : 'border-rose-500/30 bg-rose-500/10 text-rose-300'}`}>
                              {impactLabel}
                            </span>
                            <div className="min-w-0">
                              <p className={`text-sm font-semibold ${positive ? 'text-emerald-200' : 'text-rose-200'}`}>{row.label}</p>
                              {row.detail ? <p className="text-xs text-slate-400">{row.detail}</p> : null}
                            </div>
                          </div>
                        )
                      })}
                    </div>
                  </div>
                )}

                {/* Contact + Business Info row */}
                <div className="grid gap-3 sm:grid-cols-2">

                  {/* Contact */}
                  <div className="rounded-2xl border border-white/8 bg-slate-900/60 p-4 space-y-2.5">
                    <p className="text-[10px] font-bold uppercase tracking-widest text-slate-500">Contact</p>
                    {ld.contact_name ? (
                      <div className="flex items-center gap-2">
                        <User className="h-3.5 w-3.5 flex-shrink-0 text-slate-500" />
                        <span className="text-sm text-slate-200">{ld.contact_name}</span>
                      </div>
                    ) : null}
                    {ld.email ? (
                      <div className="flex items-center gap-2">
                        <Mail className="h-3.5 w-3.5 flex-shrink-0 text-slate-500" />
                        <span className="text-sm text-slate-200 truncate flex-1">{ld.email}</span>
                        <button
                          type="button"
                          className="flex-shrink-0 rounded-md border border-slate-700/50 bg-slate-800/60 p-1 text-slate-400 transition hover:border-cyan-500/40 hover:text-cyan-300"
                          onClick={() => copyEmail(ld.email)}
                          title="Copy email"
                        ><Clipboard className="h-3 w-3" /></button>
                      </div>
                    ) : (
                      <div className="flex items-center gap-2">
                        <Mail className="h-3.5 w-3.5 flex-shrink-0 text-slate-600" />
                        <span className="text-sm text-slate-600">No email found</span>
                      </div>
                    )}
                    {ldPhone ? (
                      <div className="flex items-center gap-2">
                        <Phone className="h-3.5 w-3.5 flex-shrink-0 text-slate-500" />
                        <a href={`tel:${ldPhone.replace(/\s/g,'')}`} className="text-sm text-cyan-300 hover:text-cyan-100 transition font-mono">{ldPhone}</a>
                      </div>
                    ) : null}
                    {ld.address ? (
                      <div className="flex items-start gap-2">
                        <MapPin className="h-3.5 w-3.5 flex-shrink-0 text-slate-500 mt-0.5" />
                        <span className="text-sm text-slate-300 leading-tight">{ld.address}</span>
                      </div>
                    ) : null}
                  </div>

                  {/* Business insights */}
                  <div className="rounded-2xl border border-white/8 bg-slate-900/60 p-4 space-y-2.5">
                    <p className="text-[10px] font-bold uppercase tracking-widest text-slate-500">Business</p>
                    <div className="flex items-center gap-2">
                      <div className={`h-2 w-2 flex-shrink-0 rounded-full ${ldQualified ? 'bg-emerald-400' : 'bg-amber-400'}`} />
                      <span className={`text-sm font-semibold ${ldQualified ? 'text-emerald-300' : 'text-amber-300'}`}>{ldQualified ? 'Qualified lead' : 'Needs qualification'}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <Target className="h-3.5 w-3.5 flex-shrink-0 text-slate-500" />
                      <span className="text-sm text-slate-300">Status: <span className="text-slate-100 font-medium">{ldStatus}</span></span>
                    </div>
                    <div className="flex items-center gap-2">
                      <TrendingUp className="h-3.5 w-3.5 flex-shrink-0 text-slate-500" />
                      <span className="text-sm text-slate-300">Stage: <span className="text-slate-100 font-medium">{ldStage}</span></span>
                    </div>
                    {ldRating > 0 && (
                      <div className="flex items-center gap-2">
                        <Star className="h-3.5 w-3.5 flex-shrink-0 text-amber-400" />
                        <span className="text-sm text-slate-300">
                          {ldRating.toFixed(1)} rating
                          {ldReviews > 0 ? <span className="text-slate-500"> · {ldReviews} reviews</span> : null}
                        </span>
                      </div>
                    )}
                    {ldGoogleClaimed && (
                      <div className="flex items-center gap-2">
                        <CheckCircle2 className="h-3.5 w-3.5 flex-shrink-0 text-emerald-400" />
                        <span className="text-sm text-emerald-300">Google verified</span>
                      </div>
                    )}
                    {Number(ld.qualification_score || 0) > 0 && (
                      <div className="flex items-center gap-2">
                        <Sparkles className="h-3.5 w-3.5 flex-shrink-0 text-violet-400" />
                        <span className="text-sm text-slate-300">Qualification: <span className="text-violet-300 font-semibold">{Math.round(Number(ld.qualification_score))}/100</span></span>
                      </div>
                    )}
                  </div>
                </div>

                {/* Website + Maps CTA */}
                {(ld.website_url || ld.maps_url) ? (
                  <div className="grid gap-2 sm:grid-cols-2">
                    {ld.website_url ? (
                      <a
                        href={ld.website_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex w-full items-center justify-center gap-2 rounded-2xl border border-cyan-500/30 bg-cyan-500/10 py-2.5 text-sm font-semibold text-cyan-200 transition hover:bg-cyan-500/20 hover:text-cyan-100"
                      >
                        <ExternalLink className="h-4 w-4" /> Open Website
                      </a>
                    ) : (
                      <div className="flex w-full items-center justify-center gap-2 rounded-2xl border border-slate-700/40 bg-slate-800/30 py-2.5 text-sm text-slate-600">
                        <Globe className="h-4 w-4" /> No website on record
                      </div>
                    )}
                    {ld.maps_url ? (
                      <a
                        href={ld.maps_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex w-full items-center justify-center gap-2 rounded-2xl border border-rose-500/30 bg-rose-500/10 py-2.5 text-sm font-semibold text-rose-200 transition hover:bg-rose-500/20 hover:text-rose-100"
                      >
                        <MapPin className="h-4 w-4" /> Open Maps Profile
                      </a>
                    ) : (
                      <div className="flex w-full items-center justify-center gap-2 rounded-2xl border border-slate-700/40 bg-slate-800/30 py-2.5 text-sm text-slate-600">
                        <MapPin className="h-4 w-4" /> No maps link on record
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="flex w-full items-center justify-center gap-2 rounded-2xl border border-slate-700/40 bg-slate-800/30 py-2.5 text-sm text-slate-600">
                    <Globe className="h-4 w-4" /> No website or maps link on record
                  </div>
                )}

                {/* AI Insights */}
                {(ld.ai_description || ldTechStack.length > 0 || ldIntentSignals.length > 0) && (
                  <div className="rounded-2xl border border-violet-500/20 bg-violet-500/5 p-4 space-y-3">
                    <p className="text-[10px] font-bold uppercase tracking-widest text-violet-400">AI Insights</p>
                    {ld.ai_description && (
                      <p className="text-sm text-slate-200 leading-relaxed">{ld.ai_description}</p>
                    )}
                    {ldTechStack.length > 0 && (
                      <div>
                        <p className="mb-1.5 text-[10px] font-semibold uppercase tracking-wider text-slate-500">Tech stack</p>
                        <div className="flex flex-wrap gap-1.5">
                          {ldTechStack.map((t) => (
                            <span key={t} className="inline-flex items-center rounded-full border border-violet-500/30 bg-violet-500/10 px-2 py-0.5 text-[11px] font-medium text-violet-200">{t}</span>
                          ))}
                        </div>
                      </div>
                    )}
                    {ldIntentSignals.length > 0 && (
                      <div>
                        <p className="mb-1.5 text-[10px] font-semibold uppercase tracking-wider text-slate-500">Intent signals</p>
                        <div className="flex flex-wrap gap-1.5">
                          {ldIntentSignals.map((s) => (
                            <span key={s} className="inline-flex items-center rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-0.5 text-[11px] font-medium text-emerald-200">{s}</span>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}

                <div className="rounded-2xl border border-cyan-500/20 bg-cyan-500/5 p-4 space-y-3">
                  <div className="flex items-center justify-between gap-2">
                    <p className="text-[10px] font-bold uppercase tracking-widest text-cyan-300">Email History</p>
                    <button
                      type="button"
                      className="inline-flex items-center gap-1 rounded-lg border border-cyan-500/30 bg-cyan-500/10 px-2 py-1 text-[11px] font-semibold text-cyan-200 transition hover:bg-cyan-500/20"
                      onClick={() => void loadLeadEmailHistory(ld.id, { silent: true })}
                    >
                      <RefreshCw className="h-3 w-3" /> Refresh
                    </button>
                  </div>

                  {leadEmailHistory.loading && ldEmailHistoryItems.length === 0 ? (
                    <div className="space-y-2">
                      {[0, 1, 2].map((idx) => (
                        <div key={`history-skeleton-${idx}`} className="h-14 animate-pulse rounded-xl border border-white/8 bg-slate-900/50" />
                      ))}
                    </div>
                  ) : null}

                  {!leadEmailHistory.loading && leadEmailHistory.error ? (
                    <div className="rounded-xl border border-rose-500/25 bg-rose-500/10 px-3 py-2 text-xs text-rose-200">
                      {leadEmailHistory.error}
                    </div>
                  ) : null}

                  {!leadEmailHistory.loading && !leadEmailHistory.error && ldEmailHistoryItems.length === 0 ? (
                    <div className="rounded-xl border border-white/10 bg-slate-900/60 px-3 py-3 text-sm text-slate-400">
                      No communication history yet for this lead.
                    </div>
                  ) : null}

                  {ldEmailHistoryItems.length > 0 && (
                    <div className="space-y-2">
                      {ldEmailHistoryItems.map((entry) => {
                        const direction = String(entry?.direction || '').toLowerCase() === 'inbound' ? 'inbound' : 'outbound'
                        const isOutbound = direction === 'outbound'
                        const status = String(entry?.status || '').toLowerCase().trim()
                        const timeLabel = formatCommunicationTime(entry?.timestamp || entry?.created_at)
                        const subject = String(entry?.subject || '').trim()
                        const body = extractCommunicationBody(entry)
                        return (
                          <div
                            key={`email-history-${entry?.id || `${direction}-${timeLabel}`}`}
                            className={`flex ${isOutbound ? 'justify-end' : 'justify-start'}`}
                          >
                            <div className={`w-full max-w-[88%] rounded-2xl border px-3 py-2 ${isOutbound ? 'border-cyan-500/25 bg-cyan-500/10' : 'border-emerald-500/25 bg-emerald-500/10'}`}>
                              <div className="flex flex-wrap items-center justify-between gap-2">
                                <span className={`text-[10px] font-bold uppercase tracking-widest ${isOutbound ? 'text-cyan-200' : 'text-emerald-200'}`}>
                                  {isOutbound ? 'Sent' : 'Received'}
                                </span>
                                <span className="text-[11px] text-slate-400">{timeLabel}</span>
                              </div>

                              {subject ? (
                                <p className="mt-1 text-sm font-semibold text-white">{subject}</p>
                              ) : null}

                              {body ? (
                                <p className="mt-1 whitespace-pre-wrap break-words text-sm leading-relaxed text-slate-200">{body}</p>
                              ) : (
                                <p className="mt-1 text-sm text-slate-400">No body content stored.</p>
                              )}

                              <div className="mt-2 flex flex-wrap items-center gap-2">
                                {isOutbound && (status === 'opened' || status === 'replied') ? (
                                  <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-semibold text-emerald-200">
                                    <CheckCircle2 className="h-3 w-3" /> Opened
                                  </span>
                                ) : null}
                                {(status === 'replied' || (!isOutbound && status === 'received')) ? (
                                  <span className="inline-flex items-center gap-1 rounded-full border border-sky-500/30 bg-sky-500/10 px-2 py-0.5 text-[10px] font-semibold text-sky-200">
                                    <Reply className="h-3 w-3" /> Replied
                                  </span>
                                ) : null}
                              </div>
                            </div>
                          </div>
                        )
                      })}
                    </div>
                  )}
                </div>

                {/* Social links */}
                {(ld.linkedin_url || ld.instagram_url || ld.facebook_url || ld.twitter_url || ld.youtube_url) && (
                  <div className="flex items-center gap-2">
                    <p className="text-[10px] font-bold uppercase tracking-widest text-slate-500 mr-1">Social</p>
                    {ld.linkedin_url && (
                      <a href={ld.linkedin_url} target="_blank" rel="noreferrer" className="inline-flex h-8 w-8 items-center justify-center rounded-xl border border-sky-500/30 bg-sky-500/10 text-sky-300 transition hover:bg-sky-500/25"><Linkedin className="h-4 w-4" /></a>
                    )}
                    {ld.instagram_url && (
                      <a href={ld.instagram_url} target="_blank" rel="noreferrer" className="inline-flex h-8 w-8 items-center justify-center rounded-xl border border-pink-500/30 bg-pink-500/10 text-pink-300 transition hover:bg-pink-500/25"><Instagram className="h-4 w-4" /></a>
                    )}
                    {ld.facebook_url && (
                      <a href={ld.facebook_url} target="_blank" rel="noreferrer" className="inline-flex h-8 w-8 items-center justify-center rounded-xl border border-blue-500/30 bg-blue-500/10 text-blue-300 transition hover:bg-blue-500/25"><Facebook className="h-4 w-4" /></a>
                    )}
                    {ld.twitter_url && (
                      <a href={ld.twitter_url} target="_blank" rel="noreferrer" className="inline-flex h-8 w-8 items-center justify-center rounded-xl border border-slate-500/30 bg-slate-800/50 text-slate-300 transition hover:bg-slate-700/60"><Twitter className="h-4 w-4" /></a>
                    )}
                    {ld.youtube_url && (
                      <a href={ld.youtube_url} target="_blank" rel="noreferrer" className="inline-flex h-8 w-8 items-center justify-center rounded-xl border border-red-500/30 bg-red-500/10 text-red-300 transition hover:bg-red-500/25"><Youtube className="h-4 w-4" /></a>
                    )}
                  </div>
                )}

                {/* Footer actions */}
                <div className="flex gap-2 border-t border-white/8 pt-4">
                  <button
                    type="button"
                    className="flex flex-1 items-center justify-center gap-2 rounded-2xl border border-cyan-500/30 bg-cyan-500/10 py-2.5 text-sm font-semibold text-cyan-200 transition hover:bg-cyan-500/20"
                    onClick={() => { closeLeadDetailsModal(); void moveLeadToMailer(ld) }}
                  >
                    <Mail className="h-4 w-4" /> Move to Mailer
                  </button>
                  <button
                    type="button"
                    className="flex items-center justify-center gap-2 rounded-2xl border border-slate-700/50 bg-slate-800/50 px-4 py-2.5 text-sm font-semibold text-slate-300 transition hover:bg-slate-700/60"
                    onClick={closeLeadDetailsModal}
                  >
                    Close
                  </button>
                </div>

              </div>
            </div>
          </div>
        )
      })()}

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
