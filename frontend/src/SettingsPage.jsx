import {
  AlertTriangle,
  ArrowLeft,
  CheckCircle2,
  CreditCard,
  KeyRound,
  LogOut,
  Mail,
  Plus,
  Save,
  Shield,
  Trash2,
  UserCircle2,
  Zap,
} from 'lucide-react'
import { memo, useCallback, useEffect, useMemo, useState } from 'react'
import toast, { Toaster } from 'react-hot-toast'
import { useSearchParams } from 'react-router-dom'
import { clearUserSession, getRememberPreference, getStoredValue, setAuthSession } from './authStorage'
import { ALLOWED_NICHES, ACCOUNT_TYPE_OPTIONS } from './constants'
import Footer from './Footer'
import OnboardingWizard from './components/OnboardingWizard'
import { appToasterProps } from './toastTheme'

const API_BASE = String(import.meta.env.VITE_API_BASE_URL || '').trim().replace(/\/$/, '')
const NICHE_OPTIONS = ALLOWED_NICHES
const SETTINGS_TABS = [
  { id: 'profile', label: 'Profile', icon: UserCircle2 },
  { id: 'smtp', label: 'Emails/SMTP', icon: Mail },
  { id: 'security', label: 'Security', icon: Shield },
  { id: 'billing', label: 'Billing', icon: CreditCard },
]

const inputClass =
  'w-full rounded-xl border border-slate-700 bg-[#111827] px-3 py-2.5 text-sm text-slate-100 outline-none transition-all duration-200 placeholder:text-slate-500 focus:border-[#FFC107] focus:ring-2 focus:ring-[#FFC107]/25'

const DEFAULT_FREE_CREDIT_LIMIT = 50
const DEFAULT_AVERAGE_DEAL_VALUE = 1000
const ONBOARDING_COMPLETED_KEY = 'lf_onboarding_completed_v1'
const ONBOARDING_DISMISSED_KEY = 'lf_onboarding_dismissed_v1'
const selectClass = `${inputClass} saas-select`

const createDefaultSmtp = () => ({
  host: 'smtp.gmail.com',
  port: 587,
  email: '',
  password: '',
  from_name: '',
  password_set: false,
})

async function fetchJson(path, options) {
  const token = getStoredValue('lf_token')
  const headers = {
    ...(options?.headers || {}),
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  }

  const response = await fetch(`${API_BASE}${path}`, { ...(options || {}), headers })
  const text = await response.text()
  let data = null

  if (text) {
    try {
      data = JSON.parse(text)
    } catch {
      data = { detail: text }
    }
  }

  if (!response.ok) {
    const message = data?.detail || `Request failed (${response.status})`
    throw new Error(message)
  }

  return data || {}
}

const SettingsSidebar = memo(function SettingsSidebar({ activeTab, onTabChange }) {
  return (
    <aside className="rounded-xl border border-slate-800 bg-[#0D1117] p-3">
      <div className="mb-3 px-1">
        <p className="text-[11px] uppercase tracking-[0.14em] text-slate-500">Settings</p>
      </div>
      <nav className="space-y-2">
        {SETTINGS_TABS.map((tab) => {
          const Icon = tab.icon
          const active = activeTab === tab.id
          return (
            <button
              key={tab.id}
              type="button"
              className={`topbar-nav w-full justify-start ${active ? 'topbar-nav-active' : ''}`}
              onClick={() => onTabChange(tab.id)}
            >
              <Icon className="h-4 w-4" />
              {tab.label}
            </button>
          )
        })}
      </nav>
    </aside>
  )
})

function StickyActions({ saving, loading, label }) {
  return (
    <div className="sticky bottom-0 mt-6 border-t border-slate-800 bg-[#0D1117]/95 pt-4 backdrop-blur">
      <button
        type="submit"
        disabled={saving || loading}
        className="inline-flex items-center gap-2 rounded-xl border border-[#FFC107]/80 bg-gradient-to-r from-[#d9a406] to-[#FFC107] px-4 py-2.5 text-sm font-bold text-[#0D1117] transition-all duration-200 hover:brightness-105 disabled:opacity-60"
      >
        <Save className="h-4 w-4" />
        {saving ? 'Saving...' : label}
      </button>
    </div>
  )
}

function ProfileTab({ profileForm, onProfileChange, onSave, saving, loading, onReopenOnboarding }) {
  return (
    <form onSubmit={onSave} className="rounded-xl border border-slate-800 bg-[#0D1117] p-5">
      <div className="mb-5">
        <h2 className="text-lg font-semibold text-white">Profile</h2>
        <p className="mt-1 text-sm text-slate-400">Personal account details only.</p>
        <div className="mt-3">
          <button
            type="button"
            onClick={onReopenOnboarding}
            className="inline-flex items-center gap-2 rounded-xl border border-cyan-500/50 bg-cyan-500/10 px-3 py-2 text-sm font-semibold text-cyan-200 transition-all duration-200 hover:border-cyan-400 hover:bg-cyan-500/20"
          >
            <Zap className="h-4 w-4" /> Re-open Onboarding
          </button>
        </div>
      </div>

      <div className="grid gap-4 sm:grid-cols-2">
        <label className="block text-sm text-slate-300">
          <span className="mb-1.5 block">Display Name</span>
          <input
            className={inputClass}
            type="text"
            value={profileForm.display_name}
            onChange={(event) => onProfileChange('display_name', event.target.value)}
            placeholder="Your name"
          />
        </label>

        <label className="block text-sm text-slate-300">
          <span className="mb-1.5 block">Email</span>
          <input className={`${inputClass} opacity-80`} type="email" value={profileForm.email} readOnly />
        </label>

        <label className="block text-sm text-slate-300">
          <span className="mb-1.5 block">Account Type</span>
          <select
            className={selectClass}
            value={profileForm.account_type}
            onChange={(event) => onProfileChange('account_type', event.target.value)}
          >
            {ACCOUNT_TYPE_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>{option.label}</option>
            ))}
          </select>
        </label>

        <label className="block text-sm text-slate-300">
          <span className="mb-1.5 block">Niche</span>
          <select
            className={selectClass}
            value={profileForm.niche}
            onChange={(event) => onProfileChange('niche', event.target.value)}
          >
            {NICHE_OPTIONS.map((niche) => (
              <option key={niche} value={niche}>{niche}</option>
            ))}
          </select>
        </label>

        <label className="block text-sm text-slate-300">
          <span className="mb-1.5 block">Average Deal Value (€)</span>
          <input
            className={inputClass}
            type="number"
            min="0"
            value={profileForm.average_deal_value}
            onChange={(event) => onProfileChange('average_deal_value', event.target.value)}
            placeholder="1000"
          />
          <span className="mt-1.5 block text-xs text-slate-500">
            Used for the Revenue Opportunity widget inside your dashboard.
          </span>
        </label>
      </div>

      <StickyActions saving={saving} loading={loading} label="Save Profile" />
    </form>
  )
}

function SmtpSetupModal({ onClose }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm" onClick={onClose}>
      <div
        className="relative w-full max-w-lg max-h-[90vh] overflow-y-auto rounded-2xl border border-white/10 bg-[#0D1117] p-6 shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between mb-5">
          <h2 className="text-lg font-bold text-white">How to setup SMTP?</h2>
          <button onClick={onClose} className="text-slate-400 hover:text-white text-xl leading-none">✕</button>
        </div>

        {/* Why */}
        <div className="mb-5 rounded-xl bg-yellow-500/10 border border-yellow-500/20 p-3 text-sm text-yellow-300">
          SMTP lets Sniped send emails from <strong>your</strong> inbox so they land in primary — not spam.
        </div>

        {/* Supported providers */}
        <h3 className="text-sm font-semibold text-white mb-2">Supported Providers</h3>
        <div className="grid grid-cols-2 gap-2 mb-5">
          {['Gmail', 'Outlook', 'Zoho Mail', 'Custom SMTP'].map((p) => (
            <div key={p} className="rounded-lg border border-slate-700 bg-slate-800/50 px-3 py-2 text-sm text-slate-300">{p}</div>
          ))}
        </div>

        {/* Host/Port table */}
        <h3 className="text-sm font-semibold text-white mb-2">Host &amp; Port Reference</h3>
        <div className="rounded-xl border border-slate-700 overflow-hidden mb-5 text-sm">
          <table className="w-full">
            <thead className="bg-slate-800">
              <tr>
                <th className="px-3 py-2 text-left text-slate-400 font-medium">Provider</th>
                <th className="px-3 py-2 text-left text-slate-400 font-medium">Host</th>
                <th className="px-3 py-2 text-left text-slate-400 font-medium">Port</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800">
              {[
                ['Gmail', 'smtp.gmail.com', '587'],
                ['Outlook', 'smtp-mail.outlook.com', '587'],
                ['Zoho', 'smtp.zoho.com', '587'],
                ['Yahoo', 'smtp.mail.yahoo.com', '587'],
              ].map(([name, host, port]) => (
                <tr key={name} className="bg-[#111827]">
                  <td className="px-3 py-2 text-slate-300">{name}</td>
                  <td className="px-3 py-2 text-yellow-400 font-mono text-xs">{host}</td>
                  <td className="px-3 py-2 text-slate-300">{port}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Gmail guide */}
        <h3 className="text-sm font-semibold text-white mb-3">Gmail Step-by-Step</h3>
        <ol className="space-y-3 mb-5">
          {[
            { step: '1', title: 'Enable 2-Factor Authentication', desc: 'Go to myaccount.google.com → Security → 2-Step Verification and turn it on.' },
            { step: '2', title: 'Create an App Password', desc: 'Go to myaccount.google.com → Security → App Passwords. Select "Mail" + "Other" and generate.' },
            { step: '3', title: 'Use the App Password here', desc: 'Paste the 16-character app password in the Password field above — NOT your regular Gmail password.' },
          ].map(({ step, title, desc }) => (
            <li key={step} className="flex gap-3">
              <span className="flex-shrink-0 w-6 h-6 rounded-full bg-yellow-500 text-slate-900 text-xs font-bold flex items-center justify-center">{step}</span>
              <div>
                <p className="text-sm font-medium text-white">{title}</p>
                <p className="text-xs text-slate-400 mt-0.5">{desc}</p>
              </div>
            </li>
          ))}
        </ol>

        <div className="flex gap-3">
          <a
            href="https://myaccount.google.com/apppasswords"
            target="_blank"
            rel="noopener noreferrer"
            className="flex-1 text-center rounded-xl bg-yellow-500 text-slate-900 text-sm font-bold py-2.5 hover:bg-yellow-400 transition-colors"
          >
            Open Google App Passwords ↗
          </a>
          <button
            onClick={onClose}
            className="flex-1 rounded-xl border border-slate-700 text-slate-300 text-sm py-2.5 hover:bg-slate-800 transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  )
}

function SmtpTab({ smtpAccounts, onAdd, onRemove, onUpdate, onSave, saving, loading, customSmtpAllowed }) {
  const [showSetupModal, setShowSetupModal] = useState(false)
  const locked = !customSmtpAllowed
  return (
    <form onSubmit={onSave} className="rounded-xl border border-slate-800 bg-[#0D1117] p-5">
      {showSetupModal && <SmtpSetupModal onClose={() => setShowSetupModal(false)} />}
      <div className="mb-5 flex items-start justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Emails / SMTP</h2>
          <p className="mt-1 text-sm text-slate-400">Connected sending accounts for outreach delivery.</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setShowSetupModal(true)}
            className="inline-flex items-center gap-1.5 rounded-xl border border-slate-600 bg-slate-800 px-3 py-2 text-xs font-semibold text-slate-300 hover:text-white hover:border-slate-500 transition-colors"
          >
            How to setup?
          </button>
          <button
            type="button"
            onClick={onAdd}
            disabled={locked}
            className="inline-flex items-center gap-2 rounded-xl border border-[#FFC107]/80 bg-gradient-to-r from-[#d9a406] to-[#FFC107] px-3 py-2 text-xs font-bold text-[#0D1117] transition-all duration-200 hover:brightness-105"
          >
            <Plus className="h-3.5 w-3.5" /> Add SMTP Account
          </button>
        </div>
      </div>

      {locked ? (
        <div className="mb-4 rounded-xl border border-cyan-500/30 bg-cyan-500/10 px-3 py-2 text-xs text-cyan-100">
          Free plan uses Sniped system SMTP for initial outreach (up to 50 sends). Custom SMTP/Google OAuth is available on paid plans.
        </div>
      ) : null}

      <div className="space-y-4">
        {smtpAccounts.map((account, index) => {
          const isActive = Boolean(account.host && account.email && (account.password_set || account.password))
          return (
            <article key={`smtp-${index}`} className="rounded-xl border border-slate-800 bg-[#111827] p-4">
              <div className="mb-3 flex items-center justify-between gap-2">
                <div className="flex items-center gap-2">
                  <p className="text-sm font-semibold text-white">SMTP #{index + 1}</p>
                  <span
                    className={`rounded-xl px-2 py-0.5 text-[11px] font-semibold ${isActive
                      ? 'border border-emerald-400/40 bg-emerald-500/15 text-emerald-300'
                      : 'border border-amber-400/40 bg-amber-500/15 text-amber-300'}`}
                  >
                    Status: {isActive ? 'Active' : 'Inactive'}
                  </span>
                </div>
                <button
                  type="button"
                  className="text-xs text-rose-300 transition-all duration-200 hover:text-rose-200 disabled:opacity-50"
                  onClick={() => onRemove(index)}
                  disabled={locked || smtpAccounts.length <= 1}
                >
                  Remove
                </button>
              </div>

              <div className="grid gap-4 sm:grid-cols-2">
                <label className="block text-sm text-slate-300">
                  <span className="mb-1.5 block">Host</span>
                  <input className={inputClass} type="text" value={account.host} disabled={locked} onChange={(event) => onUpdate(index, 'host', event.target.value)} />
                </label>
                <label className="block text-sm text-slate-300">
                  <span className="mb-1.5 block">Port</span>
                  <input className={inputClass} type="number" value={account.port} disabled={locked} onChange={(event) => onUpdate(index, 'port', event.target.value)} />
                </label>
                <label className="block text-sm text-slate-300">
                  <span className="mb-1.5 block">Email</span>
                  <input className={inputClass} type="email" value={account.email} disabled={locked} onChange={(event) => onUpdate(index, 'email', event.target.value)} />
                </label>
                <label className="block text-sm text-slate-300">
                  <span className="mb-1.5 block">From Name</span>
                  <input className={inputClass} type="text" value={account.from_name} disabled={locked} onChange={(event) => onUpdate(index, 'from_name', event.target.value)} />
                </label>
              </div>

              <label className="mt-4 block text-sm text-slate-300">
                <span className="mb-1.5 block">Password {account.password_set ? '(stored)' : ''}</span>
                <input
                  className={inputClass}
                  type="password"
                  value={account.password}
                  disabled={locked}
                  placeholder={account.password_set ? 'Leave blank to keep existing password' : 'SMTP password'}
                  onChange={(event) => onUpdate(index, 'password', event.target.value)}
                />
              </label>
            </article>
          )
        })}
      </div>

      <StickyActions saving={saving} loading={loading || locked} label="Save SMTP Settings" />
    </form>
  )
}

function SecurityTab({
  profileForm,
  onProfileChange,
  onSavePassword,
  saving,
  loading,
  onSignOut,
  onOpenDelete,
}) {
  return (
    <form onSubmit={onSavePassword} className="rounded-xl border border-slate-800 bg-[#0D1117] p-5">
      <div className="mb-5">
        <h2 className="text-lg font-semibold text-white">Security</h2>
        <p className="mt-1 text-sm text-slate-400">Password updates and account protection controls.</p>
      </div>

      <div className="rounded-xl border border-slate-800 bg-[#111827] p-4">
        <div className="mb-4 flex items-center gap-2 text-sm font-semibold text-slate-200">
          <KeyRound className="h-4 w-4 text-[#FFC107]" /> Change Password
        </div>

        <div className="grid gap-4 sm:grid-cols-3">
          <label className="block text-sm text-slate-300">
            <span className="mb-1.5 block">Current Password</span>
            <input
              className={inputClass}
              type="password"
              value={profileForm.current_password}
              onChange={(event) => onProfileChange('current_password', event.target.value)}
            />
          </label>
          <label className="block text-sm text-slate-300">
            <span className="mb-1.5 block">New Password</span>
            <input
              className={inputClass}
              type="password"
              value={profileForm.new_password}
              onChange={(event) => onProfileChange('new_password', event.target.value)}
            />
          </label>
          <label className="block text-sm text-slate-300">
            <span className="mb-1.5 block">Confirm Password</span>
            <input
              className={inputClass}
              type="password"
              value={profileForm.confirm_password}
              onChange={(event) => onProfileChange('confirm_password', event.target.value)}
            />
          </label>
        </div>
      </div>

      <div className="mt-4 rounded-xl border border-rose-500/25 bg-rose-500/10 p-4">
        <div className="flex items-center gap-2 text-sm font-semibold text-rose-200">
          <AlertTriangle className="h-4 w-4" /> Danger Zone
        </div>
        <p className="mt-2 text-xs text-rose-100/70">
          Delete account permanently. This action cannot be undone.
        </p>
        <button
          type="button"
          onClick={onOpenDelete}
          className="mt-3 inline-flex items-center gap-2 rounded-xl border border-rose-400/40 bg-rose-500/15 px-3 py-2 text-xs font-semibold text-rose-200 transition-all duration-200 hover:bg-rose-500/25"
        >
          <Trash2 className="h-3.5 w-3.5" /> Delete Account
        </button>
      </div>

      <div className="sticky bottom-0 mt-6 flex flex-wrap items-center gap-3 border-t border-slate-800 bg-[#0D1117]/95 pt-4 backdrop-blur">
        <button
          type="submit"
          disabled={saving || loading}
          className="inline-flex items-center gap-2 rounded-xl border border-[#FFC107]/80 bg-gradient-to-r from-[#d9a406] to-[#FFC107] px-4 py-2.5 text-sm font-bold text-[#0D1117] transition-all duration-200 hover:brightness-105 disabled:opacity-60"
        >
          <Save className="h-4 w-4" />
          {saving ? 'Saving...' : 'Update Password'}
        </button>
        <button
          type="button"
          onClick={onSignOut}
          className="inline-flex items-center gap-2 rounded-xl border border-slate-700 bg-[#111827] px-4 py-2.5 text-sm font-semibold text-slate-200 transition-all duration-200 hover:border-slate-600 hover:text-white"
        >
          <LogOut className="h-4 w-4" /> Sign Out
        </button>
      </div>
    </form>
  )
}

function BillingTab({
  accountType,
  planName,
  isSubscribed,
  subscriptionActive,
  credits,
  creditsLimit,
  subscriptionStatus,
  subscriptionCancelAt,
  subscriptionCancelAtPeriodEnd,
  onManagePlans,
  onOpenCancelModal,
  onReactivateSubscription,
  actionLoading,
}) {
  const usagePct = Math.max(0, Math.min(100, Math.round((Number(credits || 0) / Math.max(1, Number(creditsLimit || DEFAULT_FREE_CREDIT_LIMIT))) * 100)))
  const normalizedStatus = String(subscriptionStatus || '').toLowerCase().trim()
  const cancelDate = subscriptionCancelAt ? new Date(subscriptionCancelAt) : null
  const cancelDateValid = Boolean(cancelDate && !Number.isNaN(cancelDate.getTime()))
  const cancelDateFuture = Boolean(cancelDateValid && cancelDate > new Date())
  const cancelDateLabel = cancelDateValid ? cancelDate.toLocaleDateString() : String(subscriptionCancelAt || '').trim()
  const hasPaidSubscription = Boolean(subscriptionActive)
    || Boolean(isSubscribed)
    || ['active', 'paid', 'trialing'].includes(normalizedStatus)
  const cancellationScheduled = Boolean(subscriptionCancelAtPeriodEnd) && (cancelDateFuture || !cancelDateValid)
  const showCancelButton = hasPaidSubscription && !cancellationScheduled
  const showReactivateButton = hasPaidSubscription && cancellationScheduled
  const showSubscribeButton = !hasPaidSubscription
  const statusLabel = showSubscribeButton
    ? 'Free tier'
    : cancellationScheduled
      ? `Active${cancelDateLabel ? ` (Ends on ${cancelDateLabel})` : ''}`
      : ['trialing', 'paid'].includes(normalizedStatus)
        ? normalizedStatus.charAt(0).toUpperCase() + normalizedStatus.slice(1)
        : 'Active'

  return (
    <section className="rounded-xl border border-slate-800 bg-[#0D1117] p-5">
      <div className="mb-5">
        <h2 className="text-lg font-semibold text-white">Billing</h2>
        <p className="mt-1 text-sm text-slate-400">Subscription overview and billing management.</p>
      </div>

      <div className="grid gap-4 sm:grid-cols-2">
        <div className="rounded-xl border border-slate-800 bg-[#111827] p-4">
          <p className="text-[11px] uppercase tracking-[0.14em] text-slate-500">Current Plan</p>
          <p className="mt-2 text-lg font-semibold text-white">{planName}</p>
          <p className="mt-1 text-sm text-slate-400">Status: {statusLabel}</p>
        </div>

        <div className="rounded-xl border border-slate-800 bg-[#111827] p-4">
          <p className="text-[11px] uppercase tracking-[0.14em] text-slate-500">Account Type</p>
          <p className="mt-2 text-lg font-semibold text-white">{accountType}</p>
          <p className="mt-1 text-sm text-slate-400">Managed from your profile preferences.</p>
        </div>
      </div>

      <div className="mt-4 rounded-xl border border-slate-800 bg-[#111827] p-4">
        <div className="mb-2 flex items-center justify-between">
          <p className="text-sm font-semibold text-white">Credits</p>
          <p className="text-sm font-semibold text-[#FFC107]">{Number(credits || 0).toLocaleString('en-US')} / {Number(creditsLimit || DEFAULT_FREE_CREDIT_LIMIT).toLocaleString('en-US')}</p>
        </div>
        <div className="h-2 w-full overflow-hidden rounded-xl bg-slate-700/70">
          <div className="h-full rounded-xl bg-gradient-to-r from-[#d9a406] to-[#FFC107] transition-[width] duration-200" style={{ width: `${usagePct}%` }} />
        </div>
      </div>

      <div className="mt-6 flex flex-wrap items-center gap-3">
        {showSubscribeButton ? (
          <button
            type="button"
            onClick={onManagePlans}
            className="inline-flex items-center gap-2 rounded-xl border border-[#FFC107]/80 bg-gradient-to-r from-[#d9a406] to-[#FFC107] px-4 py-2.5 text-sm font-bold text-[#0D1117] transition-all duration-200 hover:brightness-105"
          >
            <CreditCard className="h-4 w-4" />
            Subscribe
          </button>
        ) : (
          <button
            type="button"
            onClick={onManagePlans}
            className="inline-flex items-center gap-2 rounded-xl border border-[#FFC107]/80 bg-gradient-to-r from-[#d9a406] to-[#FFC107] px-4 py-2.5 text-sm font-bold text-[#0D1117] transition-all duration-200 hover:brightness-105"
          >
            <CreditCard className="h-4 w-4" />
            Change Plans
          </button>
        )}
        {showCancelButton ? (
          <button
            type="button"
            onClick={onOpenCancelModal}
            disabled={actionLoading}
            className="inline-flex items-center gap-2 rounded-xl border border-rose-500/70 bg-rose-500/10 px-4 py-2.5 text-sm font-semibold text-rose-200 transition-all duration-200 hover:bg-rose-500/15 disabled:opacity-60"
          >
            <AlertTriangle className="h-4 w-4" />
            {actionLoading ? 'Canceling...' : 'Cancel Subscription'}
          </button>
        ) : null}
        {showReactivateButton ? (
          <button
            type="button"
            onClick={onReactivateSubscription}
            disabled={actionLoading}
            className="inline-flex items-center gap-2 rounded-xl border border-emerald-500/70 bg-emerald-500/10 px-4 py-2.5 text-sm font-semibold text-emerald-200 transition-all duration-200 hover:bg-emerald-500/15 disabled:opacity-60"
          >
            <CheckCircle2 className="h-4 w-4" />
            {actionLoading ? 'Reactivating...' : 'Reactivate'}
          </button>
        ) : null}
        <button
          type="button"
          onClick={() => window.location.assign('/app')}
          className="inline-flex items-center gap-2 rounded-xl border border-slate-700 bg-[#111827] px-4 py-2.5 text-sm font-semibold text-slate-200 transition-all duration-200 hover:border-slate-600 hover:text-white"
        >
          <Zap className="h-4 w-4" /> Go to Dashboard Top Up
        </button>
      </div>
      {showCancelButton ? (
        <p className="mt-3 text-xs text-slate-400">
          Cancel keeps your subscription active until the end of this billing period.
        </p>
      ) : null}
      {showReactivateButton && cancelDateLabel ? (
        <p className="mt-3 text-xs text-slate-400">
          Current subscription remains active until {cancelDateLabel}.
        </p>
      ) : null}
    </section>
  )
}

function CancelSubscriptionModal({ open, loading, onClose, onConfirm }) {
  if (!open) return null

  return (
    <div className="fixed inset-0 z-[130] flex items-center justify-center bg-slate-950/80 p-4 backdrop-blur-sm" onClick={onClose}>
      <div
        className="w-full max-w-md rounded-xl border border-rose-500/30 bg-[#0D1117] p-6 shadow-[0_32px_90px_rgba(0,0,0,0.5)]"
        onClick={(event) => event.stopPropagation()}
      >
        <h3 className="text-lg font-bold text-white">Cancel Subscription</h3>
        <p className="mt-2 text-sm text-slate-300">
          Are you sure you want to cancel? You will keep your remaining credits until the end of the period.
        </p>

        <div className="mt-6 flex flex-wrap gap-3">
          <button
            type="button"
            disabled={loading}
            onClick={() => void onConfirm()}
            className="rounded-xl border border-rose-400/40 bg-rose-500/15 px-4 py-2.5 text-sm font-semibold text-rose-200 transition-all duration-200 hover:bg-rose-500/25 disabled:opacity-45"
          >
            {loading ? 'Canceling...' : 'Yes, Cancel'}
          </button>
          <button
            type="button"
            onClick={onClose}
            disabled={loading}
            className="rounded-xl border border-slate-700 bg-[#111827] px-4 py-2.5 text-sm font-semibold text-slate-200 transition-all duration-200 hover:border-slate-600 hover:text-white"
          >
            Keep Subscription
          </button>
        </div>
      </div>
    </div>
  )
}

function DeleteAccountModal({ open, deleting, confirmText, password, onConfirmTextChange, onPasswordChange, onClose, onDelete }) {
  if (!open) return null

  return (
    <div className="fixed inset-0 z-[130] flex items-center justify-center bg-slate-950/80 p-4 backdrop-blur-sm" onClick={onClose}>
      <div
        className="w-full max-w-md rounded-xl border border-rose-500/30 bg-[#0D1117] p-6 shadow-[0_32px_90px_rgba(0,0,0,0.5)]"
        onClick={(event) => event.stopPropagation()}
      >
        <h3 className="text-lg font-bold text-white">Delete Account</h3>
        <p className="mt-2 text-sm text-slate-300">
          Enter current password and type DELETE to confirm permanent account removal.
        </p>

        <div className="mt-4 space-y-4">
          <label className="block text-sm text-slate-300">
            <span className="mb-1.5 block">Current Password</span>
            <input className={inputClass} type="password" value={password} onChange={(event) => onPasswordChange(event.target.value)} autoFocus />
          </label>
          <label className="block text-sm text-slate-300">
            <span className="mb-1.5 block">Type DELETE</span>
            <input className={inputClass} type="text" value={confirmText} onChange={(event) => onConfirmTextChange(event.target.value)} autoComplete="off" />
          </label>
        </div>

        <div className="mt-6 flex flex-wrap gap-3">
          <button
            type="button"
            disabled={deleting || confirmText !== 'DELETE' || !password}
            onClick={() => void onDelete()}
            className="rounded-xl border border-rose-400/40 bg-rose-500/15 px-4 py-2.5 text-sm font-semibold text-rose-200 transition-all duration-200 hover:bg-rose-500/25 disabled:opacity-45"
          >
            {deleting ? 'Deleting...' : 'Confirm Delete'}
          </button>
          <button
            type="button"
            onClick={onClose}
            disabled={deleting}
            className="rounded-xl border border-slate-700 bg-[#111827] px-4 py-2.5 text-sm font-semibold text-slate-200 transition-all duration-200 hover:border-slate-600 hover:text-white"
          >
            Cancel
          </button>
        </div>
      </div>
    </div>
  )
}

export default function SettingsPage() {
  const [activeTab, setActiveTab] = useState('profile')
  const [searchParams, setSearchParams] = useSearchParams()
  const [configLoading, setConfigLoading] = useState(true)
  const [profileSaving, setProfileSaving] = useState(false)
  const [smtpSaving, setSmtpSaving] = useState(false)
  const [billingActionLoading, setBillingActionLoading] = useState(false)
  const [deleteLoading, setDeleteLoading] = useState(false)
  const [showCancelSubscriptionModal, setShowCancelSubscriptionModal] = useState(false)
  const [showDeleteModal, setShowDeleteModal] = useState(false)
  const [deleteConfirmText, setDeleteConfirmText] = useState('')
  const [onboardingWizardOpen, setOnboardingWizardOpen] = useState(false)
  const [onboardingCompleting, setOnboardingCompleting] = useState(false)

  const [profileForm, setProfileForm] = useState({
    email: '',
    display_name: '',
    niche: 'B2B Service Provider',
    account_type: 'entrepreneur',
    average_deal_value: String(getStoredValue('lf_average_deal_value') || DEFAULT_AVERAGE_DEAL_VALUE),
    current_password: '',
    new_password: '',
    confirm_password: '',
    delete_password: '',
  })
  const [smtpAccounts, setSmtpAccounts] = useState([createDefaultSmtp()])
  const [billingSnapshot, setBillingSnapshot] = useState({
    isSubscribed: String(getStoredValue('lf_is_subscribed') || '').trim().toLowerCase() === 'true',
    subscriptionActive: String(getStoredValue('lf_is_subscribed') || '').trim().toLowerCase() === 'true',
    planName: String(getStoredValue('lf_plan_name') || 'Free Plan').trim() || 'Free Plan',
    credits: Number(getStoredValue('lf_credits_balance') || getStoredValue('lf_credits') || DEFAULT_FREE_CREDIT_LIMIT),
    creditsLimit: Number(getStoredValue('lf_credits_limit') || DEFAULT_FREE_CREDIT_LIMIT),
    subscriptionStatus: '',
    subscriptionCancelAt: null,
    subscriptionCancelAtPeriodEnd: false,
  })

  const hasToken = useMemo(() => Boolean(getStoredValue('lf_token')), [])
  const sessionToken = useMemo(() => getStoredValue('lf_token'), [])

  const fetchUser = useCallback(async () => {
    if (!sessionToken) return null

    const profileData = await fetchJson('/api/auth/profile', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token: sessionToken }),
    })

    setProfileForm((prev) => ({
      ...prev,
      email: profileData.email || '',
      display_name: profileData.display_name || '',
      niche: profileData.niche || 'B2B Service Provider',
      account_type: profileData.account_type || 'entrepreneur',
      average_deal_value: String(profileData.average_deal_value ?? prev.average_deal_value ?? DEFAULT_AVERAGE_DEAL_VALUE),
      current_password: '',
      new_password: '',
      confirm_password: '',
      delete_password: '',
    }))

    const normalizedSubscriptionStatus = String(profileData.subscription_status || profileData.subscriptionStatus || '').toLowerCase().trim()
    const storedIsSubscribed = String(getStoredValue('lf_is_subscribed') || '').trim().toLowerCase() === 'true'
    const storedPlanName = String(getStoredValue('lf_plan_name') || '').trim()
    const storedCredits = Number(getStoredValue('lf_credits_balance') || getStoredValue('lf_credits') || 0)
    const storedCreditsLimit = Number(getStoredValue('lf_credits_limit') || DEFAULT_FREE_CREDIT_LIMIT)
    const hasApiSubscriptionFields = (
      Object.prototype.hasOwnProperty.call(profileData, 'isSubscribed')
      || Object.prototype.hasOwnProperty.call(profileData, 'subscription_active')
      || Object.prototype.hasOwnProperty.call(profileData, 'subscription_status')
      || Object.prototype.hasOwnProperty.call(profileData, 'subscription_cancel_at_period_end')
      || Object.prototype.hasOwnProperty.call(profileData, 'subscription_cancel_at')
    )
    const subscriptionActive = Boolean(profileData.subscription_active ?? profileData.isSubscribed)
    const isSubscribed = hasApiSubscriptionFields
      ? (
        subscriptionActive
        || ['active', 'paid', 'trialing'].includes(normalizedSubscriptionStatus)
        || (Boolean(profileData.subscription_cancel_at_period_end) && Boolean(profileData.subscription_cancel_at))
      )
      : storedIsSubscribed

    setBillingSnapshot({
      isSubscribed,
      subscriptionActive,
      planName: String(profileData.currentPlanName || storedPlanName || (isSubscribed ? 'Pro Plan' : 'Free Plan')).trim(),
      credits: Number(profileData.credits ?? profileData.credits_balance ?? storedCredits ?? 0),
      creditsLimit: Number(profileData.creditLimit ?? profileData.monthly_quota ?? profileData.monthly_limit ?? profileData.credits_limit ?? storedCreditsLimit ?? DEFAULT_FREE_CREDIT_LIMIT),
      subscriptionStatus: String(profileData.subscription_status || profileData.subscriptionStatus || '').trim().toLowerCase() || (storedIsSubscribed ? 'active' : ''),
      subscriptionCancelAt: profileData.subscription_cancel_at || null,
      subscriptionCancelAtPeriodEnd: Boolean(profileData.subscription_cancel_at_period_end),
    })

    return profileData
  }, [sessionToken])

  useEffect(() => {
    if (!hasToken) {
      window.location.assign('/login')
      return
    }

    let ignore = false

    async function loadPageData() {
      try {
        const [config, profileData] = await Promise.all([
          fetchJson('/api/config'),
          fetchUser(),
        ])

        if (ignore) return

        setSmtpAccounts(
          Array.isArray(config.smtp_accounts) && config.smtp_accounts.length > 0
            ? config.smtp_accounts.map((account) => ({
                host: account.host || 'smtp.gmail.com',
                port: Number(account.port) || 587,
                email: account.email || '',
                password: '',
                from_name: account.from_name || '',
                password_set: Boolean(account.password_set),
              }))
            : [createDefaultSmtp()],
        )

        void profileData
      } catch (error) {
        if (!ignore) {
          toast.error(error.message || 'Failed to load settings')
        }
      } finally {
        if (!ignore) setConfigLoading(false)
      }
    }

    void loadPageData()

    return () => {
      ignore = true
    }
  }, [fetchUser, hasToken, sessionToken])

  useEffect(() => {
    if (!sessionToken) return
    if (String(searchParams.get('payment') || '').trim().toLowerCase() !== 'success') return

    let cancelled = false

    const refreshAfterPayment = async () => {
      try {
        await fetchUser()
        if (cancelled) return
        toast.success('Payment successful. Billing updated.')
        const nextParams = new URLSearchParams(searchParams)
        nextParams.delete('payment')
        setSearchParams(nextParams, { replace: true })
      } catch (error) {
        if (!cancelled) {
          toast.error(error.message || 'Failed to refresh billing data')
        }
      }
    }

    void refreshAfterPayment()

    return () => {
      cancelled = true
    }
  }, [fetchUser, searchParams, sessionToken, setSearchParams])

  const updateProfileField = useCallback((field, value) => {
    setProfileForm((prev) => ({ ...prev, [field]: value }))
  }, [])

  const updateSmtp = useCallback((index, field, value) => {
    setSmtpAccounts((prev) => prev.map((account, accountIndex) => (accountIndex === index ? { ...account, [field]: value } : account)))
  }, [])

  const addSmtp = useCallback(() => {
    setSmtpAccounts((prev) => [...prev, createDefaultSmtp()])
  }, [])

  const removeSmtp = useCallback((index) => {
    setSmtpAccounts((prev) => {
      if (prev.length <= 1) return prev
      return prev.filter((_, accountIndex) => accountIndex !== index)
    })
  }, [])

  const signOut = useCallback(() => {
    clearUserSession()
    localStorage.removeItem('lf_pending_signup')
    window.location.assign('/login')
  }, [])

  const saveProfile = useCallback(async (event) => {
    event.preventDefault()
    if (!sessionToken) {
      toast.error('Session expired. Please login again.')
      window.location.assign('/login')
      return
    }

    setProfileSaving(true)
    try {
      const updated = await fetchJson('/api/auth/profile', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          token: sessionToken,
          display_name: profileForm.display_name,
          niche: profileForm.niche,
          account_type: profileForm.account_type,
          average_deal_value: Number(profileForm.average_deal_value || DEFAULT_AVERAGE_DEAL_VALUE),
        }),
      })

      setProfileForm((prev) => ({
        ...prev,
        email: updated.email || prev.email,
        display_name: updated.display_name || prev.display_name,
        niche: updated.niche || prev.niche,
        account_type: updated.account_type || prev.account_type,
        average_deal_value: String(updated.average_deal_value ?? prev.average_deal_value ?? DEFAULT_AVERAGE_DEAL_VALUE),
      }))

      setAuthSession(
        {
          lf_token: sessionToken,
          lf_email: updated.email || profileForm.email || '',
          lf_niche: updated.niche || profileForm.niche || '',
          lf_display_name: updated.display_name || profileForm.display_name || '',
          lf_account_type: updated.account_type || profileForm.account_type || '',
          lf_average_deal_value: String(updated.average_deal_value ?? profileForm.average_deal_value ?? DEFAULT_AVERAGE_DEAL_VALUE),
        },
        getRememberPreference(),
      )
      toast.success('Profile saved')
    } catch (error) {
      toast.error(error.message || 'Failed to save profile')
    } finally {
      setProfileSaving(false)
    }
  }, [profileForm.account_type, profileForm.average_deal_value, profileForm.display_name, profileForm.email, profileForm.niche, sessionToken])

  const saveSecurity = useCallback(async (event) => {
    event.preventDefault()
    if (!sessionToken) {
      toast.error('Session expired. Please login again.')
      window.location.assign('/login')
      return
    }
    if (!profileForm.current_password || !profileForm.new_password || !profileForm.confirm_password) {
      toast.error('Fill all password fields before saving.')
      return
    }
    if (profileForm.new_password !== profileForm.confirm_password) {
      toast.error('New password and confirmation do not match.')
      return
    }
    if (profileForm.new_password.length < 8) {
      toast.error('New password must be at least 8 characters.')
      return
    }

    setProfileSaving(true)
    try {
      await fetchJson('/api/auth/profile', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          token: sessionToken,
          current_password: profileForm.current_password,
          new_password: profileForm.new_password,
          display_name: profileForm.display_name,
          niche: profileForm.niche,
          account_type: profileForm.account_type,
        }),
      })

      setProfileForm((prev) => ({
        ...prev,
        current_password: '',
        new_password: '',
        confirm_password: '',
      }))
      toast.success('Password updated')
    } catch (error) {
      toast.error(error.message || 'Failed to update password')
    } finally {
      setProfileSaving(false)
    }
  }, [profileForm.account_type, profileForm.confirm_password, profileForm.current_password, profileForm.display_name, profileForm.new_password, profileForm.niche, sessionToken])

  const saveSmtp = useCallback(async (event) => {
    event.preventDefault()
    setSmtpSaving(true)

    try {
      await fetchJson('/api/config', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          smtp_accounts: smtpAccounts.map((account) => ({
            host: account.host,
            port: Number(account.port) || 587,
            email: account.email,
            password: account.password || '',
            from_name: account.from_name || '',
          })),
        }),
      })

      setSmtpAccounts((prev) => prev.map((account) => ({
        ...account,
        password: '',
        password_set: Boolean(account.password_set || account.password),
      })))
      toast.success('SMTP settings saved')
    } catch (error) {
      toast.error(error.message || 'Failed to save SMTP settings')
    } finally {
      setSmtpSaving(false)
    }
  }, [smtpAccounts])

  const goToPricing = useCallback(() => {
    window.location.assign('/pricing')
  }, [])

  const applyBillingApiSnapshot = useCallback((data) => {
    if (!data || typeof data !== 'object') return
    const nextStatus = String(data.subscription_status || '').trim().toLowerCase()
    const nextPlanName = String(data.currentPlanName || '').trim()
    const nextSubscriptionActive = Boolean(data.subscription_active ?? data.isSubscribed)
    const nextIsSubscribed = nextSubscriptionActive || ['active', 'paid', 'trialing'].includes(nextStatus)
      || (Boolean(data.subscription_cancel_at_period_end) && Boolean(data.subscription_cancel_at))
    setBillingSnapshot((prev) => ({
      ...prev,
      isSubscribed: nextIsSubscribed,
      subscriptionActive: nextSubscriptionActive,
      planName: nextPlanName || prev.planName,
      subscriptionStatus: nextStatus || prev.subscriptionStatus,
      subscriptionCancelAt: data.subscription_cancel_at || null,
      subscriptionCancelAtPeriodEnd: Boolean(data.subscription_cancel_at_period_end),
    }))
  }, [])

  const closeCancelSubscriptionModal = useCallback(() => {
    if (billingActionLoading) return
    setShowCancelSubscriptionModal(false)
  }, [billingActionLoading])

  const cancelSubscription = useCallback(async () => {
    setBillingActionLoading(true)
    try {
      const data = await fetchJson('/api/stripe/cancel-subscription', {
        method: 'POST',
      })
      applyBillingApiSnapshot(data)
      setShowCancelSubscriptionModal(false)
      toast.success('Subscription canceled. Access remains until period end.')
    } catch (error) {
      toast.error(error.message || 'Could not cancel subscription.')
    } finally {
      setBillingActionLoading(false)
    }
  }, [applyBillingApiSnapshot])

  const reactivateSubscription = useCallback(async () => {
    setBillingActionLoading(true)
    try {
      const data = await fetchJson('/api/stripe/reactivate-subscription', {
        method: 'POST',
      })
      applyBillingApiSnapshot(data)
      toast.success('Subscription reactivated.')
    } catch (error) {
      toast.error(error.message || 'Could not reactivate subscription.')
    } finally {
      setBillingActionLoading(false)
    }
  }, [applyBillingApiSnapshot])

  const closeDeleteModal = useCallback(() => {
    if (deleteLoading) return
    setShowDeleteModal(false)
    setDeleteConfirmText('')
    setProfileForm((prev) => ({ ...prev, delete_password: '' }))
  }, [deleteLoading])

  const deleteAccount = useCallback(async () => {
    if (!sessionToken) {
      toast.error('Session expired. Please login again.')
      window.location.assign('/login')
      return
    }
    if (!profileForm.delete_password) {
      toast.error('Enter your current password to delete account.')
      return
    }
    if (deleteConfirmText !== 'DELETE') {
      toast.error('Type DELETE to continue.')
      return
    }

    setDeleteLoading(true)
    try {
      await fetchJson('/api/auth/delete-account', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          token: sessionToken,
          current_password: profileForm.delete_password,
        }),
      })

      clearUserSession()
      localStorage.removeItem('lf_pending_signup')
      toast.success('Account deleted successfully.')
      window.setTimeout(() => {
        window.location.assign('/login')
      }, 900)
    } catch (error) {
      toast.error(error.message || 'Failed to delete account.')
    } finally {
      setDeleteLoading(false)
    }
  }, [deleteConfirmText, profileForm.delete_password, sessionToken])

  const profileAccountTypeLabel = useMemo(() => {
    const selected = ACCOUNT_TYPE_OPTIONS.find((item) => item.value === profileForm.account_type)
    return selected ? selected.label : 'Account'
  }, [profileForm.account_type])

  const closeOnboardingWizard = useCallback(() => {
    if (onboardingCompleting) return
    setOnboardingWizardOpen(false)
  }, [onboardingCompleting])

  const completeOnboardingFromSettings = useCallback(async () => {
    setOnboardingCompleting(true)
    try {
      localStorage.setItem(ONBOARDING_COMPLETED_KEY, '1')
      localStorage.removeItem(ONBOARDING_DISMISSED_KEY)
      toast.success('Onboarding walkthrough finished.')
      setOnboardingWizardOpen(false)
    } finally {
      setOnboardingCompleting(false)
    }
  }, [])

  return (
    <div className="app-root min-h-screen text-slate-100">
      <Toaster {...appToasterProps} />

      <div className="flex w-full flex-col gap-3 px-4 pb-8 pt-1 sm:px-6 xl:px-8">
        <div className="mx-auto w-full max-w-6xl space-y-5">
          <div className="glass-card rounded-3xl p-5 sm:p-6">
            <div className="mb-3">
              <p className="label-overline">Sniped Workspace</p>
              <h1 className="mt-2 text-2xl font-bold tracking-tight text-white sm:text-3xl">Settings</h1>
            </div>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="flex flex-wrap items-center gap-2">
              <button
                type="button"
                onClick={() => window.location.assign('/app')}
                className="inline-flex items-center gap-2 rounded-xl border border-[#FFC107]/70 bg-[#FFC107]/10 px-3 py-2 text-sm font-semibold text-[#FFC107] transition-all duration-200 hover:border-[#FFC107] hover:bg-[#FFC107]/15"
              >
                <ArrowLeft className="h-4 w-4" /> Back to Dashboard
              </button>

              <button
                type="button"
                onClick={() => setOnboardingWizardOpen(true)}
                className="inline-flex items-center gap-2 rounded-xl border border-cyan-500/50 bg-cyan-500/10 px-3 py-2 text-sm font-semibold text-cyan-200 transition-all duration-200 hover:border-cyan-400 hover:bg-cyan-500/20"
              >
                <Zap className="h-4 w-4" /> Re-open Onboarding
              </button>
            </div>

            <div className="inline-flex items-center gap-2 rounded-xl border border-slate-800 bg-[#111827] px-3 py-2 text-xs text-slate-300">
              <CheckCircle2 className="h-3.5 w-3.5 text-emerald-400" />
              {configLoading ? 'Loading settings...' : `${profileAccountTypeLabel} workspace`}
            </div>
          </div>
          </div>

          <div className="glass-card rounded-3xl p-4 sm:p-5">
            <div className="grid gap-5 lg:grid-cols-[240px_1fr]">
              <SettingsSidebar activeTab={activeTab} onTabChange={setActiveTab} />

              <main className="min-w-0">
                {activeTab === 'profile' ? (
                  <ProfileTab
                    profileForm={profileForm}
                    onProfileChange={updateProfileField}
                    onSave={saveProfile}
                    saving={profileSaving}
                    loading={configLoading}
                    onReopenOnboarding={() => setOnboardingWizardOpen(true)}
                  />
                ) : null}

                {activeTab === 'smtp' ? (
                  <SmtpTab
                    smtpAccounts={smtpAccounts}
                    onAdd={addSmtp}
                    onRemove={removeSmtp}
                    onUpdate={updateSmtp}
                    onSave={saveSmtp}
                    saving={smtpSaving}
                    loading={configLoading}
                    customSmtpAllowed={Boolean(billingSnapshot.isSubscribed)}
                  />
                ) : null}

                {activeTab === 'security' ? (
                  <SecurityTab
                    profileForm={profileForm}
                    onProfileChange={updateProfileField}
                    onSavePassword={saveSecurity}
                    saving={profileSaving}
                    loading={configLoading}
                    onSignOut={signOut}
                    onOpenDelete={() => setShowDeleteModal(true)}
                  />
                ) : null}

                {activeTab === 'billing' ? (
                  <BillingTab
                    accountType={profileAccountTypeLabel}
                    planName={billingSnapshot.planName}
                    isSubscribed={billingSnapshot.isSubscribed}
                    subscriptionActive={billingSnapshot.subscriptionActive}
                    credits={billingSnapshot.credits}
                    creditsLimit={billingSnapshot.creditsLimit}
                    subscriptionStatus={billingSnapshot.subscriptionStatus}
                    subscriptionCancelAt={billingSnapshot.subscriptionCancelAt}
                    subscriptionCancelAtPeriodEnd={billingSnapshot.subscriptionCancelAtPeriodEnd}
                    onManagePlans={goToPricing}
                    onOpenCancelModal={() => setShowCancelSubscriptionModal(true)}
                    onReactivateSubscription={reactivateSubscription}
                    actionLoading={billingActionLoading}
                  />
                ) : null}
              </main>
            </div>
          </div>
        </div>
      </div>

      <div className="mt-6">
        <Footer />
      </div>

      <DeleteAccountModal
        open={showDeleteModal}
        deleting={deleteLoading}
        confirmText={deleteConfirmText}
        password={profileForm.delete_password}
        onConfirmTextChange={setDeleteConfirmText}
        onPasswordChange={(value) => updateProfileField('delete_password', value)}
        onClose={closeDeleteModal}
        onDelete={deleteAccount}
      />

      <CancelSubscriptionModal
        open={showCancelSubscriptionModal}
        loading={billingActionLoading}
        onClose={closeCancelSubscriptionModal}
        onConfirm={cancelSubscription}
      />

      <OnboardingWizard
        open={onboardingWizardOpen}
        submitting={onboardingCompleting}
        onClose={closeOnboardingWizard}
        onComplete={completeOnboardingFromSettings}
        completeCta="Finish Onboarding"
        subtitle="Settings walkthrough"
        title="Replay your setup wizard"
      />
    </div>
  )
}
