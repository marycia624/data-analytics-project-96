with sessions_with_paid_mark as (
	select *,
		case when medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social') -- необходимо выделить все платные метки из данных и здесь дополнить / убрать ненужное
			then 1 else 0 end as is_paid
	from sessions
	), 
	visitors_with_leads as (
		select s.visitor_id,
			visit_date as visit_date,
			source as utm_source,
			medium as utm_medium, 
			campaign as utm_campaign,
			l.lead_id ,
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
	select visitor_id,
		visit_date,
		utm_source,
		utm_medium, 
		utm_campaign,
		lead_id ,
		created_at,
		amount, 
		closing_reason,
		status_id
	from visitors_with_leads
	where rn = 1
	and amount is not null
	order by amount desc, visit_date , utm_source, utm_medium, utm_campaign
