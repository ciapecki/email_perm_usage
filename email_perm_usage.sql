select a."Opty Status", count(*), count(distinct a."Contact Id") from oppt3110 a
where a."Opty Status" not in ('No Opportunity','Lead:Declined','Lost')
group by a."Opty Status"

select a."Contact Id" from oppt3110 a 
group by a."Contact Id"
having count(*) > 1

select a.*, to_date(a."Opened Date",'YYYY-MM-DD HH24:MI:SS') dat from oppt3110 a
where a."Contact Id" in ('3-BNK-670','3-7EA-1742','3-5FV-1708')

select * from oppts3
/*
drop table oppt3110_distinct;
create table oppt3110_distinct as
select a."Contact Id" contact_rowid, max(to_date(a."Opened Date",'YYYY-MM-DD HH24:MI:SS')) op_date from oppt3110 a
group by a."Contact Id"
*/

select * from sumant_oppts1 a
where a."Contact or Prospect ID" in ('1000330697','1000397541','1005257847','1004242242')

select * from emea_optins_log

--- REQUIREMENTS:
/*

create table email_optins_log 
(id number,
 proc_name varchar2(4000),
 execution_date date,
 execution_comment varchar2(4000)
);
create sequence email_optins_log_seq;

1. opportunities pulled from GCM (joined Custom Opportunity Contacts Segmentation and hidden Opportunity Contacts Segmentation (emails) joined by opportunity_rowid)
2. tars pulled from GCM (prepared by Sree in lm_emea.emea_gcm_tar_summary)
3. emea_optins_flags
4. function is_table_populated()
5. proc drop_table2()
6. tables: gcd_dw.gcd_individuals, gcd_dw.gcd_countries, gcd_dw.gcd_regions, dm_metrics.email_suppression, gcd_dw.gcd_individual_services
7. gcd_dw.gcd_correspondence_details, gcd_dw.lb_organizations_eu_vw, gcd_dw.gcd_gcm_activities, dm_metrics.LIST_MGMT_CONTACT_HISTORY
*/

grant select on sumant_oppts1 to public;
grant select on oppts3 to public;

drop table oppt3110_distinct;
create table oppt3110_distinct as
select a."Contact or Prospect ID" contact_prospect_rowid,
max(upper(trim(a."Email Address"))) email_address,
max(to_date(b."Opened Date",'YYYY-MM-DD HH24:MI:SS')) op_date
from sumant_oppts1 a, oppts3 b
where a.row_id = b.row_id
and b."Opty Status" not in ('Lead:Declined','No Opportunity','Lost')
group by a."Contact or Prospect ID";
create unique index bt_oppts3110 on oppt3110_distinct (contact_prospect_rowid);
--create index bt_oppts3110_email on oppt3110_distinct (email_address);

create table oppt3110_email_distinct as
select upper(trim(a."Email Address")) email_address,
max(to_date(b."Opened Date",'YYYY-MM-DD HH24:MI:SS')) op_date
from sumant_oppts1 a, oppts3 b
where a.row_id = b.row_id
and b."Opty Status" not in ('Lead:Declined','No Opportunity','Lost')
group by upper(trim(a."Email Address"));
drop index bt_oppts3110_email;
create index bt_oppts3110_email on oppt3110_distinct (email_address);

select * from oppt3110_distinct a where a.contact_prospect_rowid = '3-627G-10436'
select * from oppt3110_distinct a where a.email_address = 'PERNOEL@GET2NET.DK'

drop table emea_optins_flags2;
create table emea_optins_flags2 nologging as
select a.*, b.op_date op_date1, c.op_date op_date2, replace(greatest(nvl(b.op_date,to_date('01011900','DDMMYYYY')),nvl(c.op_date,to_date('01011900','DDMMYYYY'))),to_date('01011900','DDMMYYYY'),null) op_date 
from emea_optins_flags a, oppt3110_distinct b, oppt3110_email_distinct c
where b.contact_prospect_rowid (+) = coalesce(a.contact_rowid, a.prospect_rowid)
and a.email_address = c.email_address (+);

/*
select * from emea_optins_flags a, oppt3110_distinct b
where coalesce(a.contact_rowid, a.prospect_rowid) = b.contact_prospect_rowid
*/

select count(*) from emea_optins_flags2 a

select count(*) from lm_emea.emea_gcm_tar_summary a

---
create table tar1 nologging as
select a.gsi_party_id, a.duns_number, max(to_date(a.tar_date,'MM/DD/YYYY HH24:MI')) tar_date 
from lm_emea.emea_gcm_tar_summary a
group by a.gsi_party_id, a.duns_number;

create index bt_tar1_part on tar1 (gsi_party_id);
create index bt_tar1_duns on tar1 (duns_number);


drop table emea_optins_flags3;
create table emea_optins_flags3 as
select a.*, b.org_id, b.org_party_id, b.last_email_contacted_date from emea_optins_flags2 a, gcd_dw.gcd_individuals b
where a.individual_id = b.individual_id;

--create index bt_emea_opt_fl3_part on emea_optins_flags3 (gsi_party_id);
create index bt_emea_opt_fl3_org on emea_optins_flags3 (org_id);

drop table emea_optins_flags4;
create table emea_optins_flags4 nologging as
select a.*, b.gsi_party_id, b.duns_number from emea_optins_flags3 a, gcd_dw.lb_organizations_eu_vw b
where a.org_id = b.org_id (+);

select count(*) from emea_optins_flags3 -- 3354678                               
select count(*) from emea_optins_flags4 -- 3354678                               

create index bt_emea_optins_fl4_part on emea_optins_flags4 (gsi_party_id);
create index bt_emea_optins_fl4_duns on emea_optins_flags4 (duns_number);

drop table tar1_part;
create table tar1_part nologging as
select a.individual_id, max(b.tar_date) tar_date_party_id from emea_optins_flags4 a, tar1 b
where a.gsi_party_id = b.gsi_party_id
group by a.individual_id, a.email_address;
drop table tar1_duns;
create table tar1_duns nologging as
select a.individual_id, max(b.tar_date) tar_date_duns from emea_optins_flags4 a, tar1 b
where a.duns_number = b.duns_number
group by a.individual_id, a.email_address;

drop index bt_tar1_part_ind;
drop index bt_tar1_duns_ind;
create unique index bt_tar1_part_ind on tar1_part (individual_id) compute statistics;
create unique index bt_tar1_duns_ind on tar1_duns (individual_id) compute statistics;

/*
drop table emea_optins_flags5;

create table emea_optins_flags5 nologging as
select a.*, b.tar_date_party_id tar_date_partyid, c.tar_date_duns tar_date_duns, 
(case when d.org_id is not null then greatest(d.db_inst,d.applications) end) customer
from emea_optins_flags3 a, tar1_part b, tar1_duns c, prods_emea_flags d
where  a.individual_id = b.individual_id (+)
and a.individual_id = c.individual_id (+)
and a.org_id = d.org_id (+)
and coalesce(d.db_inst(+),d.applications(+) ) is not null;

select count(*) from emea_optins_flags5 -- 3354371
*/
drop table acts;
create table acts as
select a.classification, count(*) cnt_all, count(distinct a.individual_id) cnt_distinct_inds 
from gcd_dw.gcd_gcm_activities a
where a.activity_date > add_months(sysdate,-6)
group by a.classification
order by cnt_all desc;

create table activities6 nologging as
select a.individual_id, max(a.activity_date) act_date from gcd_dw.gcd_gcm_activities a
where a.activity_date >= add_months(sysdate,-6)
and a.classification in ('SDS','ERS','Software Downloaded','EBN',
						'iSeminar Webshow Attended','Event - Walk-in')
group by a.individual_id;
create unique index bt_acts6_ind on activities6 (individual_id);

create table acts18_2 nologging as
select a.classification, a.status, count(*) cnt_all, count(distinct individual_id) disti_inds
from gcd_dw.gcd_gcm_activities a
where a.activity_date >= add_months(sysdate,-18)
and a.classification in ('EVENT','ERS')
group by classification, status
order by cnt_all desc;

select * from acts18 a where a.cnt_all > 10;
select * from acts18_2;
select * from acts18_3

rename activities18 to activities18_bak;

--drop table activities18;
create table activities18 nologging as
select a.individual_id, max(a.activity_date) act_date from gcd_dw.gcd_gcm_activities a
where a.activity_date >= add_months(sysdate,-18)
and (a.classification in ('SDS','Software Downloaded','OTN SOFTWARE DOWNLOAD',
						'Event - Walk-in','Event - Pre-Reg Attendee')
	 or a.classification = 'EVENT' and a.status in ('Attended','ATTENDED','Walkin')
	 or a.classification = 'ERS' and a.response_type in ('Event - Pre-Reg Attendee','Event - Walk-in'))
group by a.individual_id;
create unique index bt_acts18_ind on activities18 (individual_id);

select count(*) from activities6 -- 755149
select * from activities6
select count(*) from activities18 -- old 1.171.576
-- new 1.120.014
select * from activities18

create table email_sent_hist1 as
select a.individual_id, max(a.list_sent_date) last_email_date 
from dm_metrics.LIST_MGMT_CONTACT_HISTORY a
where upper(a.contact_channel) = 'EMAIL'
and a.individual_id is not null
and a.list_sent_date is not null
group by a.individual_id;
create unique index bt_email_hist1_ind on email_sent_hist1 (individual_id);

create table email_sent_hist2 as
select upper(a.email_address) email_address, max(a.list_sent_date) last_email_date 
from dm_metrics.LIST_MGMT_CONTACT_HISTORY a
where upper(a.contact_channel) = 'EMAIL'
and a.email_address is not null
and a.list_sent_date is not null
group by upper(a.email_address);

drop index bt_email_hist2_email;
create unique index bt_email_hist2_email on email_sent_hist2 (email_address) compute statistics;


drop index bt_emea_opt_fl4_ind;
create unique index bt_emea_opt_fl4_ind on emea_optins_flags4 (individual_id) compute statistics;

rename emea_optins_flags5 to emea_optins_flags5_bak;

drop table emea_optins_flags5;
create table emea_optins_flags5 nologging as
select /*+ index(b bt_tar1_part_ind) 
		   index(c bt_tar1_duns_ind) index(d BT_PRODS_EMEA_FLAGS_ORG_ID) 
		   index(e BT_ACTS6_IND) index(f BT_ACTS18_IND) */ 
a.*, b.tar_date_party_id tar_date_partyid, c.tar_date_duns tar_date_duns, 
(case when d.org_id is not null then greatest(nvl(d.db_inst,add_months(sysdate,-70)),
											  nvl(d.applications,add_months(sysdate,-70))
											 ) end) customer,
e.act_date activity_date, f.act_date activity_date18
/*
(case when e1.last_email_date >= nvl(e2.last_email_date,add_months(sysdate,-50)) then e1.last_email_date
		else e2.last_email_date end) email_hist_sent_date
*/
from emea_optins_flags4 a, tar1_part b, tar1_duns c, prods_emea_flags d,

activities6 e, activities18 f
--email_sent_hist1 e1, email_sent_hist2 e2
where  a.individual_id = b.individual_id (+)
and a.individual_id = c.individual_id (+)
and a.org_id = d.org_id (+)
and coalesce(d.db_inst(+),d.applications(+) ) is not null
and a.individual_id = e.individual_id (+)
and a.individual_id = f.individual_id (+);

create unique index bt_emea_optins_fl5_ind on emea_optins_flags5 (individual_id);
create index bt_emea_optins_fl5_em on emea_optins_flags5 (email_address);

create table emea_optins_flags6 nologging as
select /*+ index(e1 BT_EMAIL_HIST1_IND) index(e2 BT_EMAIL_HIST2_EMAIL) */ a.*, 
(case when e1.last_email_date >= nvl(e2.last_email_date,add_months(sysdate,-50)) then e1.last_email_date
		else e2.last_email_date end) email_hist_sent_date
from emea_optins_flags5 a, email_sent_hist1 e1, email_sent_hist2 e2
where a.individual_id = e1.individual_id (+)
		and a.email_address = e2.email_address (+);
-- index(e1 BT_EMAIL_HIST1_IND) index(e2 BT_EMAIL_HIST2_EMAIL) 
-- ordered use_hash 

select count(*) from emea_optins_flags6 a
where a.email_hist_sent_date is not null

select count(*) from emea_optins_flags5 -- 3.354.678
select count(*) from emea_optins_flags4 -- 3.354.678
select count(*) from emea_optins_flags6 -- 3.354.678

create table new_optin_rules as
select a.sub_region_name, count(*) cnt,
sum(case when a.vani_prfl = 'Y' or a.vani_prfl_email = 'Y' then 1 else 0 end) vani_prfl,
sum(case when (a.gcd_services = 'Y' or a.correspondence1 = 'Y') and a.suppression is null and a.vani_prfl <> 'N' 
												and a.vani_prfl_email <> 'N' then 1 else 0 end) gcd,
sum(case when a.op_date >= add_months(sysdate,-6) then 1 else 0 end) oppt6_recs,
sum(case when a.op_date >= add_months(sysdate,-6) and (a.vani_prfl is null and a.vani_prfl_email is null) then 1 else 0 end) oppt6_no_prfl_recs,
sum(case when a.tar_date_partyid >= add_months(sysdate,-6) or
			  a.tar_date_duns >= add_months(sysdate,-6) then 1 else 0 end) tar6_recs,
sum(case when a.tar_date_partyid >= add_months(sysdate,-6) or
			  a.tar_date_duns >= add_months(sysdate,-6) 
			  and (a.vani_prfl is null and a.vani_prfl_email is null) then 1 else 0 end) tar6_no_prfl_recs,
sum(case when a.customer is not null then 1 else 0 end) customer,
sum(case when a.customer is not null 
		and (a.vani_prfl is null and a.vani_prfl_email is null) then 1 else 0 end) customer_no_prfl,
sum(case when a.activity_date >= add_months(sysdate,-6) then 1 else 0 end) act6_recs,
sum(case when a.activity_date >= add_months(sysdate,-6)
		and (a.vani_prfl is null and a.vani_prfl_email is null)  then 1 else 0 end) act6_recs,
sum(case when a.activity_date18 >= add_months(sysdate,-18) then 1 else 0 end) act18_recs,
sum(case when a.activity_date18 >= add_months(sysdate,-18)
		and (a.vani_prfl is null and a.vani_prfl_email is null)  then 1 else 0 end) act18_recs,

sum(case when (a.vani_prfl = 'Y' or a.vani_prfl_email = 'Y'
		or a.op_date >= add_months(sysdate,-6)
		or a.tar_date_partyid >= add_months(sysdate,-6) or
			  a.tar_date_duns >= add_months(sysdate,-6)
		or a.customer is not null
		or a.activity_date >= add_months(sysdate,-6)
			) 
		and nvl(a.vani_prfl,'A') <> 'N' and nvl(a.vani_prfl_email,'A') <> 'N'
		and a.suppression is null or a.gcd_services = 'Y'
		then 1 else 0 end) future_optins,
sum(case when (a.vani_prfl = 'Y' or a.vani_prfl_email = 'Y'
		or a.op_date >= add_months(sysdate,-6)
		or a.tar_date_partyid >= add_months(sysdate,-6) or
			  a.tar_date_duns >= add_months(sysdate,-6)
		or a.customer is not null
		or a.activity_date18 >= add_months(sysdate,-18)
			) 
		and nvl(a.vani_prfl,'A') <> 'N' and nvl(a.vani_prfl_email,'A') <> 'N'
		and a.suppression is null or a.gcd_services = 'Y'
		then 1 else 0 end) future_optins18,

sum(a.kcierpisz_optin) curr1,
sum(case when (a.vani_prfl = 'Y' or a.vani_prfl_email = 'Y' or a.gcd_services = 'Y' or a.correspondence1 = 'Y')
				and nvl(a.vani_prfl,'A') <> 'N' and nvl(a.vani_prfl_email,'A') <> 'N' and a.suppression is null then 1 else 0 end) curr2
from emea_optins_flags6 a
group by rollup (a.sub_region_name)

rename new_optin_rules2 to new_optin_rules2_bak;

drop table new_optin_rules2;
create table new_optin_rules2 as
select a.sub_region_name, count(*) cnt,
sum(case when nvl(a.email_hist_sent_date,add_months(sysdate,-50)) >= add_months(sysdate,-6) then 1 else 0 end) email_6_mnths,
sum(case when nvl(a.activity_date18,add_months(sysdate,-50)) >= add_months(sysdate,-6) then 1 else 0 end) mkt_act_6_mnths,
sum(case when a.customer is not null 
			or nvl(a.tar_date_partyid,add_months(sysdate,-50)) >= add_months(sysdate,-6)
			or nvl(a.tar_date_duns,add_months(sysdate,-50)) >= add_months(sysdate,-6)
			or nvl(a.op_date,add_months(sysdate,-50)) >= add_months(sysdate,-6)
	then 1 else 0 end) ins_bas_tar_oport_6_mnths,
sum(case when 
			(a.gcd_services = 'Y' or a.correspondence1 = 'Y') and a.suppression is null   -- GCD Y
			and a.vani_prfl is null and a.vani_prfl_email is null						  -- null PROFILE
			and nvl(a.email_hist_sent_date,add_months(sysdate,-50)) >= add_months(sysdate,-6) 
			and nvl(a.activity_date18,add_months(sysdate,-50)) < add_months(sysdate,-6)
			and a.customer is null
		then 1 else 0 end) count_1,
sum(case when (a.gcd_services = 'Y' or a.correspondence1 = 'Y') and a.suppression is null   -- GCD Y
			and a.vani_prfl is null and a.vani_prfl_email is null						  -- null PROFILE
			and nvl(a.email_hist_sent_date,add_months(sysdate,-50)) >= add_months(sysdate,-6)
			and nvl(a.activity_date18,add_months(sysdate,-50)) >= add_months(sysdate,-6)
			and a.customer is null
		 then 1 else 0 end) count_2,
sum(case when (a.gcd_services = 'Y' or a.correspondence1 = 'Y') and a.suppression is null   -- GCD Y
			and a.vani_prfl is null and a.vani_prfl_email is null						  -- null PROFILE
			and nvl(a.email_hist_sent_date,add_months(sysdate,-50)) >= add_months(sysdate,-6)
			and nvl(a.activity_date18,add_months(sysdate,-50)) < add_months(sysdate,-6)
			and a.customer is not null
		 then 1 else 0 end) count_3,
sum(case when (a.gcd_services = 'Y' or a.correspondence1 = 'Y') and a.suppression is null   -- GCD Y
			and a.vani_prfl is null and a.vani_prfl_email is null						  -- null PROFILE
			and nvl(a.email_hist_sent_date,add_months(sysdate,-50)) >= add_months(sysdate,-6)
			and nvl(a.activity_date18,add_months(sysdate,-50)) >= add_months(sysdate,-6)
			and a.customer is not null
		 then 1 else 0 end) count_4,
sum(case when (a.gcd_services = 'Y' or a.correspondence1 = 'Y') and a.suppression is null   -- GCD Y
			and a.vani_prfl is null and a.vani_prfl_email is null						  -- null PROFILE
			and nvl(a.email_hist_sent_date,add_months(sysdate,-50)) < add_months(sysdate,-6)
			and nvl(a.activity_date18,add_months(sysdate,-50)) >= add_months(sysdate,-6)
			and a.customer is not null
		 then 1 else 0 end) count_5,
sum(case when (a.gcd_services = 'Y' or a.correspondence1 = 'Y') and a.suppression is null   -- GCD Y
			and a.vani_prfl is null and a.vani_prfl_email is null						  -- null PROFILE
			and nvl(a.email_hist_sent_date,add_months(sysdate,-50)) < add_months(sysdate,-6)
			and nvl(a.activity_date18,add_months(sysdate,-50)) < add_months(sysdate,-6)
			and a.customer is not null
		 then 1 else 0 end) count_6,		 
sum(case when (a.gcd_services = 'Y' or a.correspondence1 = 'Y') and a.suppression is null   -- GCD Y
			and a.vani_prfl is null and a.vani_prfl_email is null						  -- null PROFILE
			and nvl(a.email_hist_sent_date,add_months(sysdate,-50)) < add_months(sysdate,-6)
			and nvl(a.activity_date18,add_months(sysdate,-50)) >= add_months(sysdate,-6)
			and a.customer is null
		 then 1 else 0 end) count_7,		 
sum(case when (a.gcd_services = 'Y' or a.correspondence1 = 'Y') and a.suppression is null   -- GCD Y
			and a.vani_prfl is null and a.vani_prfl_email is null						  -- null PROFILE
			and nvl(a.email_hist_sent_date,add_months(sysdate,-50)) < add_months(sysdate,-6)
			and nvl(a.activity_date18,add_months(sysdate,-50)) < add_months(sysdate,-6)
			and a.customer is null
		 then 1 else 0 end) count_8
 from emea_optins_flags6 a
group by rollup (a.sub_region_name)

select * from new_optin_rules2


select * from emea_optins_flags6 a
where a.vani_prfl = 'N' and a.correspondence1 = 'Y'
and a.suppression is null

create bitmap index emea_optins_fl6_optin on emea_optins_flags6 (optin);
create unique index bt_emea_optins_fl6_ind on emea_optins_flags6 (individual_id);

select * from emea_optins_flags6 a, gcd_dw.List_build_Individuals_specs1 b
where a.optin <> b.contact_email
and a.individual_id = b.individual_id
and rownum < 10


select * from prods_emea_flags a
where coalesce(a.db_inst,a.applications) is not null


select * from gcd_dw.lb_organizations_eu_vw a
where a.gsi_party_id = 21418330

select * from gcd_dw.lb_organizations_eu_vw a
where a.duns_number = '487839789' and rownum < 2

select * from gcd_dw.lb_individuals_eu_vw a
where a.org_id = 3002117

select * from gcd_dw.lb_individuals_eu_vw a
where a.org_id = 56971967

select * from gcd_dw.gcd_gcm_activities a
where a.classification = 'ERS'
and a.activity_date > add_months(sysdate,-9)
and a.activity_date < add_months(sysdate,-8)
and rownum < 10


