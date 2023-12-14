with attribution as (
	--сюда можно вставить запрос с любой атрибуции с task 1
	with sessions_with_paid_mark as (
	select *,
		case when medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg') -- необходимо выделить все платные метки из данных и здесь дополнить / убрать ненужное
			then 1 else 0 end as is_paid
	from sessions
	), 
	visitors_with_leads as (
		select s.visitor_id,
			visit_date as datetime,
			source as utm_source,
			medium as utm_medium, 
			campaign as utm_campaign,
			created_at,
			amount, 
			closing_reason,
			status_id,
			row_number() over(partition by s.visitor_id order by is_paid desc, visit_date desc) as rn
		from sessions_with_paid_mark s
		left join leads l 
			on l.visitor_id = s.visitor_id 
		   and l.created_at  >= s.visit_date
	) 
	select *
	from visitors_with_leads
	where rn = 1
),
aggregated_data as (
	select date(datetime) as date,
		case when utm_medium = 'referral' then 'web-site' else lower(utm_source) end as utm_source,
		utm_medium, 
		utm_campaign,
		count(visitor_id) as visitors_count,
		count(case when created_at is not null then visitor_id end) leads_count,
		count(case when status_id =142 then visitor_id end) purchases_count,
		sum(case when status_id =142 then amount end) revenue
	from attribution 
	group by 1,2,3,4
),
marketing_data as (
	select date(campaign_date) as date,
		utm_source,
		utm_medium,
		utm_campaign,
		sum(daily_spent) as cost
	from ya_ads ya 
	group by 1,2,3,4
	union all 
	select date(campaign_date) as date,
		utm_source,
		utm_medium,
		utm_campaign,
		sum(daily_spent) as cost
	from vk_ads va 
	group by 1,2,3,4
)
select a.date  visit_date,
	a.utm_source,
	a.utm_medium,
	a.utm_campaign,
	sum(visitors_count) visitors_count,
	sum(cost) total_cost ,
	sum(leads_count) leads_count,
	sum(purchases_count) purchases_count,
	sum(revenue) revenue
from aggregated_data a 
left join marketing_data m 
	on a.date = m.date
   and lower(a.utm_source) = m.utm_source
   and lower(a.utm_medium) = m.utm_medium
   and lower(a.utm_campaign) = m.utm_campaign
where 1=1 
-- a.utm_source in ('yandex', 'vk')
and revenue is not null
group by 1,2,3,4
order by revenue desc, visit_date,  visitors_count , utm_source, utm_medium, utm_campaign
