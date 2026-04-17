-- Run once in Supabase SQL Editor.
-- Resets billing/subscription state and clears cached Stripe linkage for a clean test baseline.

do $$
declare
    free_quota integer := 50;
begin
    if to_regclass('public.users') is not null then
        execute 'alter table public.users alter column plan_key set default ''free''';
        execute format('alter table public.users alter column credits_balance set default %s', free_quota);
        execute format('alter table public.users alter column monthly_quota set default %s', free_quota);
        execute format('alter table public.users alter column monthly_limit set default %s', free_quota);
        execute format('alter table public.users alter column credits_limit set default %s', free_quota);

        update public.users
        set
            plan_key = 'free',
            credits_balance = free_quota,
            monthly_quota = free_quota,
            monthly_limit = free_quota,
            credits_limit = free_quota,
            topup_credits_balance = 0,
            subscription_active = false,
            subscription_status = null,
            subscription_cancel_at = null,
            subscription_cancel_at_period_end = false,
            stripe_customer_id = null,
            subscription_start_date = (now() at time zone 'utc')::text,
            updated_at = (now() at time zone 'utc')::text;
    end if;

    if to_regclass('public.subscriptions') is not null then
        delete from public.subscriptions;
    end if;

    if to_regclass('public.system_runtime') is not null then
        delete from public.system_runtime
        where key like 'runtime_billing:%'
           or key like 'stripe_topup_applied:%';
    end if;
end $$;