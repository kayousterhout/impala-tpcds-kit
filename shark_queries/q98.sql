use tpcds_15tb_rcfile;
set mapred.fairscheduler.pool=default9;
set mapred.reduce.tasks=300;
select i_item_desc, i_category, i_class, i_current_price, ss_ext_sales_price
from  store_sales_cached join item on (store_sales_cached.ss_item_sk = item.i_item_sk)
where ss_item_sk = i_item_sk and i_category = 'Jewelry'
limit 100;
