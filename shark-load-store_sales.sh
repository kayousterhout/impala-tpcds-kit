#!/bin/bash

source /root/impala-tpcds-kit/tpcds-env.sh

echo "Creating output in $FLATFILE_HDFS_ROOT"

pushd /root

/root/shark/bin/shark -skipRddReload -h localhost -p 4444 -e '
drop table if exists store_sales;
create table store_sales (
  ss_sold_time_sk           int,
  ss_item_sk                int,
  ss_customer_sk            int,
  ss_cdemo_sk               int,
  ss_hdemo_sk               int,
  ss_addr_sk                int,
  ss_store_sk               int,
  ss_promo_sk               int,
  ss_ticket_number          int,
  ss_quantity               int,
  ss_wholesale_cost         double,
  ss_list_price             double,
  ss_sales_price            double,
  ss_ext_discount_amt       double,
  ss_ext_sales_price        double,
  ss_ext_wholesale_cost     double,
  ss_ext_list_price         double,
  ss_ext_tax                double,
  ss_coupon_amt             double,
  ss_net_paid               double,
  ss_net_paid_inc_tax       double,
  ss_net_profit             double
)
partitioned by (ss_sold_date_sk int)
stored as orc tblproperties("orc.compress"="SNAPPY")
;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 2000;

insert overwrite table store_sales
partition(ss_sold_date_sk)
select
ss_sold_time_sk,
ss_item_sk,
ss_customer_sk,
ss_cdemo_sk,
ss_hdemo_sk,
ss_addr_sk,
ss_store_sk,
ss_promo_sk,
ss_ticket_number,
ss_quantity,
ss_wholesale_cost,
ss_list_price,
ss_sales_price,
ss_ext_discount_amt,
ss_ext_sales_price,
ss_ext_wholesale_cost,
ss_ext_list_price,
ss_ext_tax,
ss_coupon_amt,
ss_net_paid,
ss_net_paid_inc_tax,
ss_net_profit,
ss_sold_date_sk
from et_store_sales
where ss_sold_date_sk is not null
distribute by ss_sold_date_sk;

show tables;'

popd
