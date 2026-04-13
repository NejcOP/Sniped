/**
 * useDebounce — performance hook
 *
 * Delays updating the returned value until the user stops typing for `delay` ms.
 * Prevents hammering the API on every keystroke (avoids CPU overload on the client).
 *
 * Usage:
 *   const debouncedSearch = useDebounce(rawSearch, 300)
 *   useEffect(() => fetchLeads(debouncedSearch), [debouncedSearch])
 */
import { useEffect, useRef, useState } from 'react'

export function useDebounce(value, delay = 300) {
  const [debouncedValue, setDebouncedValue] = useState(value)
  const timerRef = useRef(null)

  useEffect(() => {
    // Clear any pending timer on each new value
    if (timerRef.current !== null) {
      window.clearTimeout(timerRef.current)
    }
    timerRef.current = window.setTimeout(() => {
      setDebouncedValue(value)
    }, delay)

    return () => {
      if (timerRef.current !== null) {
        window.clearTimeout(timerRef.current)
      }
    }
  }, [value, delay])

  return debouncedValue
}
