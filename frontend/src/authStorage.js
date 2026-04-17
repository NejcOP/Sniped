const AUTH_KEYS = ['lf_token', 'lf_niche', 'lf_email', 'lf_display_name', 'lf_contact_name', 'lf_account_type', 'lf_average_deal_value']
const BILLING_KEYS = [
  'lf_credits',
  'lf_credits_balance',
  'lf_topup_credits_balance',
  'lf_credits_limit',
  'lf_is_subscribed',
  'lf_plan_name',
  'lf_plan_key',
  'lf_pending_checkout_plan',
]
const REMEMBER_KEY = 'lf_remember_me'
const REMEMBERED_EMAIL_KEY = 'lf_remembered_email'

function hasWindow() {
  return typeof window !== 'undefined'
}

function getStorage(type) {
  if (!hasWindow()) return null
  return type === 'session' ? window.sessionStorage : window.localStorage
}

export function getStoredValue(key) {
  const localValue = getStorage('local')?.getItem(key)
  if (localValue) return localValue
  return getStorage('session')?.getItem(key) || ''
}

export function setAuthSession(data, remember = true) {
  const target = getStorage(remember ? 'local' : 'session')
  const other = getStorage(remember ? 'session' : 'local')
  if (!target || !other) return

  AUTH_KEYS.forEach((key) => {
    target.removeItem(key)
    other.removeItem(key)
  })

  Object.entries(data).forEach(([key, value]) => {
    if (!AUTH_KEYS.includes(key)) return
    if (value === undefined || value === null || value === '') return
    target.setItem(key, String(value))
  })

  getStorage('local')?.setItem(REMEMBER_KEY, remember ? '1' : '0')
}

export function clearAuthSession() {
  AUTH_KEYS.forEach((key) => {
    getStorage('local')?.removeItem(key)
    getStorage('session')?.removeItem(key)
  })
}

export function clearBillingCache() {
  BILLING_KEYS.forEach((key) => {
    getStorage('local')?.removeItem(key)
    getStorage('session')?.removeItem(key)
  })
}

export function clearUserSession() {
  clearAuthSession()
  clearBillingCache()
}

export function getRememberPreference() {
  return getStorage('local')?.getItem(REMEMBER_KEY) !== '0'
}

export function setRememberedEmail(email, remember) {
  const storage = getStorage('local')
  if (!storage) return
  if (remember && email) {
    storage.setItem(REMEMBERED_EMAIL_KEY, email)
  } else {
    storage.removeItem(REMEMBERED_EMAIL_KEY)
  }
}

export function getRememberedEmail() {
  return getStorage('local')?.getItem(REMEMBERED_EMAIL_KEY) || ''
}
