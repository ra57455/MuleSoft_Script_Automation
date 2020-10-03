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
    a.datasource_name as document_name,
    a.ds_repo_url as repo_url,
    a.site_id,
    a.site_name,
    a.site_luid,
    b.last_accessed,
    a.project_name,
    a.project_luid,
    a.ds_created_date,
    a.datasource_luid as document_luid,
    a.size,
    a.type,
    a.user_name,
    a.user_email,
    (
      case when (b.last_accessed) is null then ((now()::date) - a.ds_created_date::date)
      else (now()::date - b.last_accessed::date)
      end
    ) as inactive_days
    from
    (
      select
      ds.id as ds_id,
      ds.name as datasource_name,
      ds.repository_url as ds_repo_url,
      ds.created_at as ds_created_date,
      ds.size as size,
      ds.luid as datasource_luid,
      p.name as project_name,
      dc.dbname as dc_dbname,
      s.url_namespace as site_id,
      s.name as site_name,
      s.luid as site_luid,
      p.luid as project_luid,
      ds.data_engine_extracts as type,
      su.name as user_name,
      su.email as user_email
      from datasources as ds left join
      (
        select dbname, site_id
        from data_connections
        where owner_type = 'Workbook' and dbclass = 'sqlproxy'
      )
      as dc
      on ds.repository_url=dc.dbname
      and ds.site_id = dc.site_id
      join sites s on ds.site_id=s.id
      join projects p on ds.project_id=p.id
      join users u on u.id=ds.owner_id
      join system_users su on u.system_user_id=su.id
      where
     --ds.parent_workbook_id is null and
      ds.data_engine_extracts is true
      --dc.dbname is null
    ) as a left join
    (
      select max(he_event.created_at) as last_accessed, hist_event.datasource_id from historical_events as he_event join
      (
          select max(id) as rev_id, datasource_id from
          (
             select id, datasource_id from hist_datasources as ds join
              (
                  select max(created_at) as la,
                  hist_datasource_id,
                  historical_event_type_id
                  from historical_events
                  where historical_event_type_id=112
                  group by 2, 3
              ) he on ds.id=he.hist_datasource_id
              order by id desc
          ) as hist_event_data
          group by datasource_id
      ) as hist_event
      on he_event.hist_datasource_id=hist_event.rev_id
      where he_event.historical_event_type_id=112
      group by 2
    ) as b on
    a.ds_id=b.datasource_id
  ) as data
where
--project_name not like 'External_%'
--and
inactive_days > 120
--(
  --      size > 10000000000 --or
        --(size < 1000000000 and inactive_days > 365) or
        --((size >= 1000000000 and size <=2000000000) and inactive_days > 285) or
        --((size >= 2000000000 and size <=10000000000) and inactive_days > 180)
--)