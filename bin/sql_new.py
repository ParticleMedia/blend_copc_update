

def dump_score_sql_group(os, prev_pdate, curr_pdate, user_condition, doc_condition, exp_name):
    sql = '''
select cjv.os, cast(split_part(split_part(cjv.nr_note_str,  'sc#rankq#', 2) , "|", 1) as float) as score
    from warehouse.online_cjv_parquet_hourly cjv, cjv.nr_buckets
    where cjv.channel_name = "foryou" and
        cjv.joined = 1 and
        cjv.pdate = "{}" and
        (cjv.clicked = 1 or cjv.checked = 1) and
        cjv.os = "{}" and
        cjv.nr_note_str like "%ug:{}%" and
        {} and {}
having score > 0
order by rand()
limit 100000
    '''
    bucket_condition = "true"
    if len(exp_name) != 0:
        bucket_condition = 'nr_buckets.item in ("{}")'.format(exp_name)
    return sql.format(curr_pdate, os, user_condition, doc_condition, bucket_condition)

def calc_copc_sql_group(os, prev_pdate, curr_pdate, user_condition, doc_condition, x0, k, exp_name):
    sql = '''
with cjvt as (
    select
        cjv.os, cjv.clicked, cjv.checked,
        1 / (1 + dpow(e(), -1 * {} * (cast(split_part(split_part(cjv.nr_note_str,  'sc#rankq#', 2) , "|", 1) as float) - {}))) as bfm_monica
    from warehouse.online_cjv_parquet_hourly cjv, cjv.nr_buckets 
    where cjv.channel_name = "foryou" and
        cjv.joined = 1 and
        (cjv.checked = 1 or cjv.clicked = 1) and
        pdate = "{}"  and
        cjv.os = "{}" and
        cjv.nr_note_str like "%ug:{}%" and
        {} and {}
    having bfm_monica > 0
)

select
sum(cjvt.clicked) / sum(cjvt.bfm_monica) as copc2, 1 as auc2
from cjvt where bfm_monica > 0
group by cjvt.os order by sum(cjvt.checked) desc
    '''
    bucket_condition = "true"
    if len(exp_name) != 0:
        bucket_condition = 'nr_buckets.item in ("{}")'.format(exp_name)
    return sql.format(k, x0, curr_pdate, os, user_condition, doc_condition, bucket_condition)
