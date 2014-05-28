#!/bin/bash

source /root/impala-tpcds-kit/tpcds-env.sh

echo "Creating output in $FLATFILE_HDFS_ROOT"

pushd /root

/root/shark/bin/shark -skipRddReload -h localhost -p 4444 -e '
drop table if exists customer_demographics;
create table customer_demographics
(
  cd_demo_sk                int,
  cd_gender                 string,
  cd_marital_status         string,
  cd_education_status       string,
  cd_purchase_estimate      int,
  cd_credit_rating          string,
  cd_dep_count              int,
  cd_dep_employed_count     int,
  cd_dep_college_count      int 
)
stored as orc tblproperties("orc.compress"="SNAPPY");

insert into table customer_demographics select * from et_customer_demographics;

drop table if exists date_dim;
create table date_dim
(
  d_date_sk                 int,
  d_date_id                 string,
  d_date                    string, -- YYYY-MM-DD format
  d_month_seq               int,
  d_week_seq                int,
  d_quarter_seq             int,
  d_year                    int,
  d_dow                     int,
  d_moy                     int,
  d_dom                     int,
  d_qoy                     int,
  d_fy_year                 int,
  d_fy_quarter_seq          int,
  d_fy_week_seq             int,
  d_day_name                string,
  d_quarter_name            string,
  d_holiday                 string,
  d_weekend                 string,
  d_following_holiday       string,
  d_first_dom               int,
  d_last_dom                int,
  d_same_day_ly             int,
  d_same_day_lq             int,
  d_current_day             string,
  d_current_week            string,
  d_current_month           string,
  d_current_quarter         string,
  d_current_year            string 
)
stored as orc tblproperties("orc.compress"="SNAPPY");

insert into table date_dim select * from et_date_dim;

drop table if exists time_dim;
create table time_dim
(
  t_time_sk                 int,
  t_time_id                 string,
  t_time                    int,
  t_hour                    int,
  t_minute                  int,
  t_second                  int,
  t_am_pm                   string,
  t_shift                   string,
  t_sub_shift               string,
  t_meal_time               string
)
stored as orc tblproperties("orc.compress"="SNAPPY");

insert into table time_dim select * from et_time_dim;

drop table if exists item;
create table item
(
  i_item_sk                 int,
  i_item_id                 string,
  i_rec_start_date          string,
  i_rec_end_date            string,
  i_item_desc               string,
  i_current_price           double,
  i_wholesale_cost          double,
  i_brand_id                int,
  i_brand                   string,
  i_class_id                int,
  i_class                   string,
  i_category_id             int,
  i_category                string,
  i_manufact_id             int,
  i_manufact                string,
  i_size                    string,
  i_formulation             string,
  i_color                   string,
  i_units                   string,
  i_container               string,
  i_manager_id              int,
  i_product_name            string
)
stored as orc tblproperties("orc.compress"="SNAPPY");

insert into table item select * from et_item;

drop table if exists store;
create table store
(
  s_store_sk                int,
  s_store_id                string,
  s_rec_start_date          string,
  s_rec_end_date            string,
  s_closed_date_sk          int,
  s_store_name              string,
  s_number_employees        int,
  s_floor_space             int,
  s_hours                   string,
  s_manager                 string,
  s_market_id               int,
  s_geography_class         string,
  s_market_desc             string,
  s_market_manager          string,
  s_division_id             int,
  s_division_name           string,
  s_company_id              int,
  s_company_name            string,
  s_street_number           string,
  s_street_name             string,
  s_street_type             string,
  s_suite_number            string,
  s_city                    string,
  s_county                  string,
  s_state                   string,
  s_zip                     string,
  s_country                 string,
  s_gmt_offset              double,
  s_tax_precentage          double
)
stored as orc tblproperties("orc.compress"="SNAPPY");

insert into table store select * from et_store;

drop table if exists customer;
create table customer
(
  c_customer_sk             int,
  c_customer_id             string,
  c_current_cdemo_sk        int,
  c_current_hdemo_sk        int,
  c_current_addr_sk         int,
  c_first_shipto_date_sk    int,
  c_first_sales_date_sk     int,
  c_salutation              string,
  c_first_name              string,
  c_last_name               string,
  c_preferred_cust_flag     string,
  c_birth_day               int,
  c_birth_month             int,
  c_birth_year              int,
  c_birth_country           string,
  c_login                   string,
  c_email_address           string,
  c_last_review_date        string
)
stored as orc tblproperties("orc.compress"="SNAPPY");

insert into table customer select * from et_customer;

drop table if exists promotion;
create table promotion
(
  p_promo_sk                int,
  p_promo_id                string,
  p_start_date_sk           int,
  p_end_date_sk             int,
  p_item_sk                 int,
  p_cost                    double,
  p_response_target         int,
  p_promo_name              string,
  p_channel_dmail           string,
  p_channel_email           string,
  p_channel_catalog         string,
  p_channel_tv              string,
  p_channel_radio           string,
  p_channel_press           string,
  p_channel_event           string,
  p_channel_demo            string,
  p_channel_details         string,
  p_purpose                 string,
  p_discount_active         string 
)
stored as orc tblproperties("orc.compress"="SNAPPY");

insert into table promotion select * from et_promotion;

drop table if exists household_demographics;
create table household_demographics
(
  hd_demo_sk                int,
  hd_income_band_sk         int,
  hd_buy_potential          string,
  hd_dep_count              int,
  hd_vehicle_count          int
)
stored as orc tblproperties("orc.compress"="SNAPPY");

insert into table household_demographics select * from et_household_demographics;

drop table if exists customer_address;
create table customer_address
(
  ca_address_sk             int,
  ca_address_id             string,
  ca_street_number          string,
  ca_street_name            string,
  ca_street_type            string,
  ca_suite_number           string,
  ca_city                   string,
  ca_county                 string,
  ca_state                  string,
  ca_zip                    string,
  ca_country                string,
  ca_gmt_offset             int,
  ca_location_type          string
)
stored as orc tblproperties("orc.compress"="SNAPPY");

insert into table customer_address select * from et_customer_address;

drop table if exists inventory;
create table inventory
(
  inv_date_sk               int,
  inv_item_sk               int,
  inv_warehouse_sk          int,
  inv_quantity_on_hand      int
)
stored as orc tblproperties("orc.compress"="SNAPPY");

insert into table inventory select * from et_inventory;

show tables;'

popd
