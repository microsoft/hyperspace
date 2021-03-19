/*
 * Copyright (2020) The Hyperspace Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.hyperspace.goldstandard

import java.io.File

import org.apache.hadoop.fs.Path

import com.microsoft.hyperspace._
import com.microsoft.hyperspace.goldstandard.IndexLogEntryCreator.createIndex
import com.microsoft.hyperspace.index.IndexConstants.INDEX_SYSTEM_PATH
import com.microsoft.hyperspace.util.FileUtils

class TPCDS_Hyperspace extends PlanStabilitySuite {

  override val goldenFilePath: String =
    new File(baseResourcePath, "hyperspace/approved-plans-v1_4").getAbsolutePath

  val indexSystemPath = new File(baseResourcePath, "hyperspace/indexes").toString

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(INDEX_SYSTEM_PATH, indexSystemPath)
    spark.enableHyperspace()
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(indexSystemPath))
    super.afterAll()
  }

  tpcdsQueries.foreach { q =>
    test(s"check simplified (tpcds-v1.4/$q)") {

      // scalastyle:off filelinelengthchecker
      // TODO: Read indexes from a file.
      val indexes = Seq(
        "dtindex;date_dim;d_date_sk;d_year",
        "ssIndex;store_sales;ss_sold_date_sk;ss_customer_sk",
        "JoinIndex00-index-33-inv_item_sk-3;inventory;inv_item_sk;inv_date_sk,inv_warehouse_sk,inv_quantity_on_hand",
        "JoinIndex01-index-34-inv_date_sk-3;inventory;inv_date_sk;inv_warehouse_sk,inv_quantity_on_hand,inv_item_sk",
        "JoinIndex02-index-42-inv_warehouse_sk-3;inventory;inv_warehouse_sk;inv_date_sk,inv_quantity_on_hand,inv_item_sk",
        "JoinIndex03-index-0-d_date_sk-10;date_dim;d_date_sk;d_moy,d_year,d_date,d_month_seq,d_qoy,d_dom,d_week_seq,d_dow,d_day_name,d_quarter_name",
        "FilterIndex04-index-11-d_year-5;date_dim;d_year;d_date_sk,d_date,d_week_seq,d_moy,d_day_name",
        "FilterIndex05-index-13-d_moy_d_year-2;date_dim;d_moy,d_year;d_date_sk,d_date",
        "FilterIndex06-index-14-d_date-2;date_dim;d_date;d_date_sk,d_week_seq",
        "FilterIndex07-index-17-d_month_seq-7;date_dim;d_month_seq;d_date_sk,d_date,d_day_name,d_week_seq,d_moy,d_qoy,d_year",
        "JoinIndex08-index-2-i_item_sk-17;item;i_item_sk;i_item_id,i_item_desc,i_manager_id,i_brand,i_brand_id,i_category,i_class,i_current_price,i_class_id,i_category_id,i_product_name,i_size,i_color,i_units,i_manufact_id,i_manufact,i_wholesale_cost",
        "FilterIndex09-index-32-i_item_sk-1;item;i_item_sk;i_item_id",
        "FilterIndex10-index-39-i_category-9;item;i_category;i_item_id,i_item_desc,i_class,i_item_sk,i_current_price,i_manufact_id,i_class_id,i_brand_id,i_category_id",
        "FilterIndex11-index-48-i_manager_id-7;item;i_manager_id;i_brand,i_brand_id,i_item_sk,i_manufact,i_manufact_id,i_category,i_category_id",
        "JoinIndex12-index-1-ss_sold_date_sk-21;store_sales;ss_sold_date_sk;ss_ext_sales_price,ss_item_sk,ss_sales_price,ss_store_sk,ss_addr_sk,ss_hdemo_sk,ss_ticket_number,ss_customer_sk,ss_quantity,ss_list_price,ss_net_profit,ss_coupon_amt,ss_ext_list_price,ss_ext_discount_amt,ss_wholesale_cost,ss_cdemo_sk,ss_promo_sk,ss_ext_wholesale_cost,ss_ext_tax,ss_net_paid,ss_sold_time_sk",
        "JoinIndex13-index-3-ss_item_sk-16;store_sales;ss_item_sk;ss_ext_sales_price,ss_sold_date_sk,ss_sales_price,ss_store_sk,ss_addr_sk,ss_quantity,ss_list_price,ss_net_paid,ss_ticket_number,ss_customer_sk,ss_net_profit,ss_promo_sk,ss_wholesale_cost,ss_coupon_amt,ss_cdemo_sk,ss_hdemo_sk",
        "JoinIndex14-index-5-ss_store_sk-20;store_sales;ss_store_sk;ss_sales_price,ss_item_sk,ss_sold_date_sk,ss_net_profit,ss_hdemo_sk,ss_ticket_number,ss_customer_sk,ss_coupon_amt,ss_addr_sk,ss_net_paid,ss_sold_time_sk,ss_quantity,ss_list_price,ss_cdemo_sk,ss_ext_sales_price,ss_promo_sk,ss_ext_wholesale_cost,ss_wholesale_cost,ss_ext_list_price,ss_ext_tax",
        "JoinIndex15-index-12-ss_customer_sk-20;store_sales;ss_customer_sk;ss_hdemo_sk,ss_ticket_number,ss_sold_date_sk,ss_store_sk,ss_quantity,ss_item_sk,ss_addr_sk,ss_net_profit,ss_coupon_amt,ss_net_paid,ss_promo_sk,ss_list_price,ss_cdemo_sk,ss_wholesale_cost,ss_ext_list_price,ss_ext_sales_price,ss_ext_wholesale_cost,ss_ext_discount_amt,ss_ext_tax,ss_sales_price",
        "JoinIndex16-index-22-ss_ticket_number-16;store_sales;ss_ticket_number;ss_quantity,ss_item_sk,ss_sold_date_sk,ss_customer_sk,ss_store_sk,ss_net_paid,ss_sales_price,ss_ext_sales_price,ss_promo_sk,ss_net_profit,ss_wholesale_cost,ss_addr_sk,ss_hdemo_sk,ss_list_price,ss_coupon_amt,ss_cdemo_sk",
        "JoinIndex17-index-27-ss_hdemo_sk-19;store_sales;ss_hdemo_sk;ss_addr_sk,ss_net_profit,ss_coupon_amt,ss_ticket_number,ss_sold_date_sk,ss_customer_sk,ss_store_sk,ss_sold_time_sk,ss_promo_sk,ss_item_sk,ss_list_price,ss_cdemo_sk,ss_wholesale_cost,ss_ext_list_price,ss_ext_tax,ss_ext_sales_price,ss_sales_price,ss_quantity,ss_ext_wholesale_cost",
        "JoinIndex18-index-28-ss_addr_sk-18;store_sales;ss_addr_sk;ss_ext_sales_price,ss_item_sk,ss_sold_date_sk,ss_sales_price,ss_quantity,ss_net_profit,ss_cdemo_sk,ss_store_sk,ss_ext_wholesale_cost,ss_hdemo_sk,ss_coupon_amt,ss_ticket_number,ss_customer_sk,ss_promo_sk,ss_list_price,ss_wholesale_cost,ss_ext_list_price,ss_ext_tax",
        "JoinIndex19-index-47-ss_cdemo_sk-16;store_sales;ss_cdemo_sk;ss_store_sk,ss_addr_sk,ss_promo_sk,ss_item_sk,ss_hdemo_sk,ss_list_price,ss_coupon_amt,ss_ticket_number,ss_sold_date_sk,ss_customer_sk,ss_wholesale_cost,ss_sales_price,ss_quantity,ss_ext_sales_price,ss_ext_wholesale_cost,ss_net_profit",
        "JoinIndex20-index-43-t_time_sk-4;time_dim;t_time_sk;t_hour,t_minute,t_time,t_meal_time",
        "JoinIndex21-index-20-sr_item_sk-7;store_returns;sr_item_sk;sr_ticket_number,sr_return_amt,sr_return_quantity,sr_returned_date_sk,sr_customer_sk,sr_reason_sk,sr_net_loss",
        "JoinIndex22-index-21-sr_ticket_number-7;store_returns;sr_ticket_number;sr_item_sk,sr_return_amt,sr_return_quantity,sr_returned_date_sk,sr_customer_sk,sr_net_loss,sr_reason_sk",
        "JoinIndex23-index-37-sr_returned_date_sk-7;store_returns;sr_returned_date_sk;sr_item_sk,sr_return_quantity,sr_customer_sk,sr_ticket_number,sr_return_amt,sr_store_sk,sr_net_loss",
        "JoinIndex24-index-4-s_store_sk-15;store;s_store_sk;s_state,s_store_name,s_store_id,s_market_id,s_zip,s_city,s_company_name,s_county,s_company_id,s_street_name,s_street_number,s_suite_number,s_street_type,s_gmt_offset,s_number_employees",
        "JoinIndex25-index-18-cd_demo_sk-8;customer_demographics;cd_demo_sk;cd_education_status,cd_marital_status,cd_gender,cd_purchase_estimate,cd_credit_rating,cd_dep_count,cd_dep_college_count,cd_dep_employed_count",
        "JoinIndex26-index-6-cs_sold_date_sk-26;catalog_sales;cs_sold_date_sk;cs_bill_addr_sk,cs_ext_sales_price,cs_item_sk,cs_bill_customer_sk,cs_quantity,cs_list_price,cs_order_number,cs_bill_hdemo_sk,cs_promo_sk,cs_ship_date_sk,cs_bill_cdemo_sk,cs_net_profit,cs_catalog_page_sk,cs_call_center_sk,cs_sales_price,cs_coupon_amt,cs_ext_wholesale_cost,cs_ext_discount_amt,cs_ext_list_price,cs_warehouse_sk,cs_ship_addr_sk,cs_wholesale_cost,cs_sold_time_sk,cs_net_paid,cs_ship_mode_sk,cs_net_paid_inc_tax",
        "JoinIndex27-index-10-cs_item_sk-22;catalog_sales;cs_item_sk;cs_sold_date_sk,cs_bill_addr_sk,cs_ext_sales_price,cs_quantity,cs_bill_customer_sk,cs_list_price,cs_order_number,cs_bill_hdemo_sk,cs_promo_sk,cs_ship_date_sk,cs_bill_cdemo_sk,cs_ship_addr_sk,cs_net_profit,cs_net_paid,cs_ext_discount_amt,cs_catalog_page_sk,cs_coupon_amt,cs_sales_price,cs_ext_list_price,cs_warehouse_sk,cs_wholesale_cost,cs_call_center_sk",
        "JoinIndex28-index-23-cs_bill_customer_sk-12;catalog_sales;cs_bill_customer_sk;cs_sold_date_sk,cs_quantity,cs_item_sk,cs_list_price,cs_sales_price,cs_ext_wholesale_cost,cs_ext_discount_amt,cs_ext_list_price,cs_ext_sales_price,cs_net_profit,cs_coupon_amt,cs_bill_cdemo_sk",
        "JoinIndex29-index-25-cs_order_number-19;catalog_sales;cs_order_number;cs_sold_date_sk,cs_quantity,cs_bill_hdemo_sk,cs_promo_sk,cs_item_sk,cs_ship_date_sk,cs_bill_cdemo_sk,cs_ext_sales_price,cs_sales_price,cs_warehouse_sk,cs_ext_list_price,cs_net_profit,cs_net_paid,cs_catalog_page_sk,cs_bill_customer_sk,cs_wholesale_cost,cs_call_center_sk,cs_ext_ship_cost,cs_ship_addr_sk",
        "JoinIndex30-index-49-cs_promo_sk-13;catalog_sales;cs_promo_sk;cs_order_number,cs_sold_date_sk,cs_quantity,cs_bill_hdemo_sk,cs_item_sk,cs_ship_date_sk,cs_bill_cdemo_sk,cs_net_profit,cs_ext_sales_price,cs_catalog_page_sk,cs_coupon_amt,cs_sales_price,cs_list_price",
        "JoinIndex31-index-7-ca_address_sk-11;customer_address;ca_address_sk;ca_state,ca_gmt_offset,ca_country,ca_city,ca_county,ca_zip,ca_street_name,ca_street_number,ca_street_type,ca_suite_number,ca_location_type",
        "FilterIndex32-index-35-ca_state-11;customer_address;ca_state;ca_address_sk,ca_street_name,ca_country,ca_zip,ca_gmt_offset,ca_county,ca_street_number,ca_street_type,ca_city,ca_suite_number,ca_location_type",
        "JoinIndex33-index-44-ca_state-4;customer_address;ca_state;ca_country,ca_address_sk,ca_county,ca_zip",
        "FilterIndex34-index-45-ca_gmt_offset-1;customer_address;ca_gmt_offset;ca_address_sk",
        "JoinIndex35-index-24-w_warehouse_sk-6;warehouse;w_warehouse_sk;w_warehouse_name,w_state,w_country,w_county,w_city,w_warehouse_sq_ft",
        "JoinIndex36-index-46-web_site_sk-3;web_site;web_site_sk;web_company_name,web_site_id,web_name",
        "JoinIndex37-index-9-ws_sold_date_sk-21;web_sales;ws_sold_date_sk;ws_item_sk,ws_bill_addr_sk,ws_ext_sales_price,ws_quantity,ws_bill_customer_sk,ws_list_price,ws_net_paid,ws_order_number,ws_ship_mode_sk,ws_warehouse_sk,ws_sold_time_sk,ws_ext_discount_amt,ws_net_profit,ws_web_page_sk,ws_sales_price,ws_ext_list_price,ws_ext_wholesale_cost,ws_promo_sk,ws_web_site_sk,ws_wholesale_cost,ws_ship_customer_sk",
        "JoinIndex38-index-15-ws_item_sk-16;web_sales;ws_item_sk;ws_bill_addr_sk,ws_ext_sales_price,ws_sold_date_sk,ws_quantity,ws_bill_customer_sk,ws_list_price,ws_ship_customer_sk,ws_order_number,ws_sales_price,ws_net_profit,ws_web_page_sk,ws_web_site_sk,ws_net_paid,ws_ext_discount_amt,ws_wholesale_cost,ws_promo_sk",
        "JoinIndex39-index-30-ws_bill_customer_sk-10;web_sales;ws_bill_customer_sk;ws_item_sk,ws_quantity,ws_sold_date_sk,ws_list_price,ws_ext_list_price,ws_ext_discount_amt,ws_net_paid,ws_ext_sales_price,ws_ext_wholesale_cost,ws_sales_price",
        "JoinIndex40-index-31-ws_order_number-16;web_sales;ws_order_number;ws_warehouse_sk,ws_ship_addr_sk,ws_net_profit,ws_ship_date_sk,ws_ext_ship_cost,ws_web_site_sk,ws_item_sk,ws_quantity,ws_net_paid,ws_sold_date_sk,ws_web_page_sk,ws_sales_price,ws_ext_sales_price,ws_promo_sk,ws_wholesale_cost,ws_bill_customer_sk",
        "JoinIndex41-index-36-wr_item_sk-11;web_returns;wr_item_sk;wr_return_quantity,wr_order_number,wr_return_amt,wr_returned_date_sk,wr_net_loss,wr_refunded_cdemo_sk,wr_reason_sk,wr_refunded_cash,wr_fee,wr_refunded_addr_sk,wr_returning_cdemo_sk",
        "JoinIndex42-index-41-wr_order_number-11;web_returns;wr_order_number;wr_return_quantity,wr_item_sk,wr_return_amt,wr_net_loss,wr_returned_date_sk,wr_refunded_cdemo_sk,wr_reason_sk,wr_refunded_cash,wr_fee,wr_refunded_addr_sk,wr_returning_cdemo_sk",
        "JoinIndex43-index-8-c_customer_sk-17;customer;c_customer_sk;c_current_addr_sk,c_first_name,c_last_name,c_current_cdemo_sk,c_preferred_cust_flag,c_salutation,c_birth_country,c_login,c_customer_id,c_email_address,c_birth_year,c_birth_month,c_current_hdemo_sk,c_first_sales_date_sk,c_first_shipto_date_sk,c_last_review_date,c_birth_day",
        "JoinIndex44-index-16-c_current_addr_sk-17;customer;c_current_addr_sk;c_customer_sk,c_current_cdemo_sk,c_first_name,c_last_name,c_customer_id,c_last_review_date,c_email_address,c_salutation,c_login,c_birth_year,c_birth_month,c_birth_country,c_birth_day,c_preferred_cust_flag,c_current_hdemo_sk,c_first_sales_date_sk,c_first_shipto_date_sk",
        "JoinIndex45-index-38-c_current_cdemo_sk-10;customer;c_current_cdemo_sk;c_customer_sk,c_current_addr_sk,c_birth_year,c_birth_month,c_current_hdemo_sk,c_first_sales_date_sk,c_first_shipto_date_sk,c_customer_id,c_first_name,c_last_name",
        "JoinIndex46-index-19-hd_demo_sk-4;household_demographics;hd_demo_sk;hd_vehicle_count,hd_dep_count,hd_buy_potential,hd_income_band_sk",
        "JoinIndex47-index-26-cr_item_sk-8;catalog_returns;cr_item_sk;cr_order_number,cr_return_amount,cr_return_quantity,cr_returned_date_sk,cr_refunded_cash,cr_reversed_charge,cr_store_credit,cr_net_loss",
        "JoinIndex48-index-29-cr_order_number-7;catalog_returns;cr_order_number;cr_item_sk,cr_return_amount,cr_return_quantity,cr_refunded_cash,cr_reversed_charge,cr_store_credit,cr_net_loss",
        "JoinIndex49-index-40-p_promo_sk-4;promotion;p_promo_sk;p_channel_event,p_channel_email,p_channel_dmail,p_channel_tv"
      )
      // scalastyle:on filelinelengthchecker
      indexes.foreach(createIndex(_, spark))

      // Enable cross join because some queries fail during query optimization phase.
      withSQLConf(
        ("spark.sql.crossJoin.enabled" -> "true"),
        ("spark.sql.autoBroadcastJoinThreshold" -> "-1")) {
        testQuery("tpcds/queries", q)
      }
    }
  }
}
