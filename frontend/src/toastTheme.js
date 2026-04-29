export const appToasterProps = {
  position: 'top-right',
  gutter: 10,
  containerStyle: {
    top: 16,
    right: 16,
  },
  toastOptions: {
    duration: 3200,
    style: {
      background: 'linear-gradient(180deg, rgba(15, 23, 42, 0.94) 0%, rgba(7, 17, 31, 0.98) 100%)',
      color: '#e2e8f0',
      border: '1px solid rgba(148, 163, 184, 0.18)',
      borderRadius: '16px',
      boxShadow: '0 24px 60px rgba(2, 6, 23, 0.42)',
      backdropFilter: 'blur(18px)',
      padding: '12px 14px',
      fontSize: '13px',
      lineHeight: '1.45',
      maxWidth: '380px',
    },
    success: {
      duration: 2600,
      iconTheme: {
        primary: '#10b981',
        secondary: '#ecfdf5',
      },
      style: {
        background: 'linear-gradient(180deg, rgba(6, 78, 59, 0.34) 0%, rgba(7, 17, 31, 0.98) 100%)',
        border: '1px solid rgba(16, 185, 129, 0.32)',
        color: '#ecfdf5',
      },
    },
    error: {
      duration: 4200,
      iconTheme: {
        primary: '#f59e0b',
        secondary: '#fff7ed',
      },
      style: {
        background: 'linear-gradient(180deg, rgba(120, 53, 15, 0.26) 0%, rgba(7, 17, 31, 0.98) 100%)',
        border: '1px solid rgba(245, 158, 11, 0.28)',
        color: '#fff7ed',
      },
    },
  },
}