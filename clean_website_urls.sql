-- Clean and fix website URLs in the database
-- This script handles Google redirects and removes social media URLs as primary website
-- Safe to run multiple times (idempotent operations)

-- 1. Update leads where website_url contains Google redirect
-- Extract the actual URL from the 'q' parameter
UPDATE public.leads
SET website_url = (
    -- Extract the 'q' parameter value from the redirect URL
    substring(
        website_url 
        from 'q=([^&]+)'
    ) 
    -- URL decode the extracted parameter (basic handling of common cases)
    |> replace('%3A', ':') 
    |> replace('%2F', '/') 
    |> replace('%3F', '?') 
    |> replace('%3D', '=')
    |> replace('%26', '&')
)
WHERE website_url LIKE '%google.%/url?%q=%'
  AND website_url NOT LIKE 'https://%' 
  AND website_url NOT LIKE 'http://%';

-- 2. Set website_url to NULL for Yelp URLs (redirect to Yelp instead of actual site)
UPDATE public.leads
SET website_url = NULL
WHERE website_url LIKE '%yelp.com%';

-- 3. Set website_url to NULL for Facebook pages (redirect to Facebook instead of actual site)
UPDATE public.leads
SET website_url = NULL
WHERE website_url LIKE '%facebook.com%';

-- 4. Set website_url to NULL for Instagram profiles (redirect to Instagram instead of actual site)
UPDATE public.leads
SET website_url = NULL
WHERE website_url LIKE '%instagram.com%';

-- 5. Verify results - show updated URLs
SELECT 
    id,
    business_name,
    website_url,
    created_at
FROM public.leads
WHERE website_url IS NOT NULL 
  AND (
    website_url LIKE '%google.%'
    OR website_url LIKE '%yelp.com%'
    OR website_url LIKE '%facebook.com%'
    OR website_url LIKE '%instagram.com%'
  )
ORDER BY created_at DESC
LIMIT 20;

-- Summary statistics
SELECT 
    COUNT(*) as total_leads,
    COUNT(CASE WHEN website_url IS NULL THEN 1 END) as leads_without_website,
    COUNT(CASE WHEN website_url IS NOT NULL THEN 1 END) as leads_with_website,
    COUNT(CASE WHEN website_url LIKE '%google.%' THEN 1 END) as google_redirect_urls,
    COUNT(CASE WHEN website_url LIKE '%yelp.com%' THEN 1 END) as yelp_urls,
    COUNT(CASE WHEN website_url LIKE '%facebook.com%' THEN 1 END) as facebook_urls,
    COUNT(CASE WHEN website_url LIKE '%instagram.com%' THEN 1 END) as instagram_urls
FROM public.leads;
