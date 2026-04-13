import { StrictMode, useEffect } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter as Router, Routes, Route, Navigate, useLocation } from 'react-router-dom'
import './index.css'
import App from './App.jsx'
import LandingPage from './LandingPage.jsx'
import FeaturesPage from './FeaturesPage.jsx'
import FeatureDetailPage from './FeatureDetailPage.jsx'
import FAQPage from './FAQPage.jsx'
import TermsPage from './TermsPage.jsx'
import PrivacyPolicyPage from './PrivacyPolicyPage.jsx'
import CookiePolicyPage from './CookiePolicyPage.jsx'
import GDPRCompliancePage from './GDPRCompliancePage.jsx'
import LoginPage from './LoginPage.jsx'
import SignupPage from './SignupPage.jsx'
import AccountTypePage from './AccountTypePage.jsx'
import ColdEmailOpenerPage from './ColdEmailOpenerPage.jsx'
import SettingsPage from './SettingsPage.jsx'
import ForgotPasswordPage from './ForgotPasswordPage.jsx'
import ResetPasswordPage from './ResetPasswordPage.jsx'
import BlogPage from './BlogPage.jsx'
import HelpCenterPage from './HelpCenterPage.jsx'
import ApiDocsPage from './ApiDocsPage.jsx'
import AppSumoRedemptionPage from './AppSumoRedemptionPage.jsx'
import SystemStatusPage from './SystemStatusPage.jsx'

function ScrollToTop() {
  const { pathname } = useLocation()

  useEffect(() => {
    window.scrollTo({ top: 0, left: 0, behavior: 'auto' })
  }, [pathname])

  return null
}

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <Router>
      <ScrollToTop />
      <Routes>
        {/* Main app */}
        <Route path="/app/*" element={<App />} />
        <Route path="/tasks" element={<App initialTab="tasks" />} />
        <Route path="/delivery" element={<Navigate to="/tasks" replace />} />
        <Route path="/settings" element={<SettingsPage />} />

        {/* Auth */}
        <Route path="/login" element={<LoginPage />} />
        <Route path="/forgot-password" element={<ForgotPasswordPage />} />
        <Route path="/reset-password" element={<ResetPasswordPage />} />
        <Route path="/get-started" element={<AccountTypePage />} />
        <Route path="/signup" element={<SignupPage />} />

        {/* Cold Email Opener tool */}
        <Route path="/cold-email-opener" element={<ColdEmailOpenerPage />} />

        {/* Product feature pages */}
        <Route path="/features" element={<FeaturesPage />} />
        <Route path="/pricing" element={<LandingPage />} />
        <Route path="/features/:slug" element={<FeatureDetailPage />} />
        <Route path="/faq" element={<FAQPage />} />
        <Route path="/legal/terms" element={<TermsPage />} />
        <Route path="/legal/privacy" element={<PrivacyPolicyPage />} />
        <Route path="/legal/cookies" element={<CookiePolicyPage />} />
        <Route path="/legal/gdpr" element={<GDPRCompliancePage />} />
        <Route path="/blog" element={<BlogPage />} />
        <Route path="/help" element={<HelpCenterPage />} />
        <Route path="/docs" element={<ApiDocsPage />} />
        <Route path="/redeem" element={<AppSumoRedemptionPage />} />
        <Route path="/status" element={<SystemStatusPage />} />

        {/* Landing page (default) */}
        <Route path="/" element={<LandingPage />} />
        <Route path="*" element={<LandingPage />} />
      </Routes>
    </Router>
  </StrictMode>,
)
