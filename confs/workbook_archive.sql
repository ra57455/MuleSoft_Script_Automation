select
site_id,
site_name,
site_luid,
project_name,
project_luid,
document_name,
document_luid,
size,
type,
repo_url,
user_name,
user_email,
inactive_days
from
(
  select
  usage.actual_workbook_id as actual_workbook_id,
  usage.wb_name as document_name,
  usage.wb_size as size,
  usage.wb_luid as document_luid,
  usage.type as type,
  usage.repo_url as repo_url,
  usage.hist_workbook_id,
  usage.site_id as site_id,
  usage.site_name as site_name,
  usage.site_luid as site_luid,
  usage.proj_name as project_name,
  usage.proj_luid as project_luid,
  usage.user_name as user_name,
  usage.user_email as user_email,
  usage.days_inactive as inactive_days,
  size.id as work_rev_id,
  size.size as actual_workbook_size,
  size.workbook_id as actual_workbook_id_fr_size
  from
  (
    select
    w.id as actual_workbook_id,
    w.name as wb_name,
    w.size as wb_size,
    w.luid as wb_luid,
    w.data_engine_extracts as type,
    w.repository_url as repo_url,
    summ.hist_workbook_id,
    s.url_namespace as site_id,
    s.name as site_name,
    s.luid as site_luid,
    p.name as proj_name,
    p.luid as proj_luid,
    su.name as user_name,
    su.email as user_email,
    (
            case when max(summ.workbook_last_accessed_at) IS NULL then ((now()::date) - w.created_at::date)
            else (now()::date - max(summ.workbook_last_accessed_at)::date)
            end
    ) as days_inactive,
    max(summ.historical_event_type_id) as event_id,
    max(summ.workbook_last_accessed_at) as workbook_last_accessed_at
    from workbooks w
    left join
    (
    	select
    	hw.id as hist_event_workbook_id,
    	hw.workbook_id as hist_workbook_id,
    	hw.size as hist_workbook_size,
    	hw.name as hist_workbook_name,
    	he.hist_workbook_id as historical_event_workbook_id,
    	he.historical_event_type_id as historical_event_type_id,
    	he.created_at as workbook_last_accessed_at
    	from
    	hist_workbooks hw
    	join
    	(
        	select
        	hist_summ.hist_workbook_id as hist_workbook_id,
        	hist_summ.historical_event_type_id as historical_event_type_id,
        	max(hist_summ.created_at) as created_at
        	from historical_events hist_summ
        	join
        	(
        		select max(created_at) as m_created_at, hist_workbook_id  from historical_events
       			where historical_event_type_id in (84,140,141) --and  hist_summ.hist_workbook_id = 1836
        		group by 2
        	) 	hist_events
        	on (
        		hist_summ.created_at = hist_events.m_created_at
        		and
        		hist_summ.hist_workbook_id=hist_events.hist_workbook_id
        	)
        	group by 1,2
    	) he on hw.id=he.hist_workbook_id
    ) summ on w.id=summ.hist_workbook_id
    join sites s on w.site_id=s.id
    join projects p on w.project_id=p.id
    join users u on u.id=w.owner_id
    join system_users su on u.system_user_id=su.id
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
    order by days_inactive
  ) as usage
  join
  (
    select a.id, a.workbook_id, a.size from hist_workbooks a
    join
    (
        select max(id) as max_rev_id, workbook_id from hist_workbooks group by 2
    ) b
    on a.id=b.max_rev_id
  ) as size
  on usage.actual_workbook_id=size.workbook_id
) as data
where
--(
    inactive_days > 120
--	size > 10000000000 or
--	(size < 1000000000 and inactive_days > 365) or
--	((size >= 1000000000 and size <=2000000000) and inactive_days > 365) or
--	((size >= 2000000000 and size <=10000000000) and inactive_days > 30)
--)
--and
--project_name not like 'External_%'
order by site_name
