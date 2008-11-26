PACKAGE BODY EMAIL_PKG
IS

/*
    git revision: 4dc55942c235798601107d2b742bd3159ed96185
*/

function is_table_populated
    (tableName in varchar2)
    return boolean is

    result boolean := false;
    rows_no number := 0;

    type cur_ref is ref cursor;
    tab cur_ref;

begin

        open tab for 'select count(*) from ' || tableName || ' where rownum < 2';
        loop
            exit when tab%notfound;
            fetch tab into rows_no;
            if rows_no > 0 then
                result := true;
            end if;
        end loop;
    close tab;

    return result;
exception when others then
    result := false;
    return result;
end;

procedure drop_table2(table_name in varchar2, log_table in varchar2)
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   sqlstmt varchar2(32766) := '';
   log_stmt varchar2(4000) := '';
begin
    sqlstmt := 'drop table ' || table_name || ' purge';
    dbms_output.put_line(sqlstmt);
    execute immediate sqlstmt;
    insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || ' DROP',sysdate,'DROPPED - ');
    commit;
exception when others then
    err_msg := SUBSTR(SQLERRM, 1, 100);
    insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || ' DROP',sysdate,'NOT DROPPED - ' || err_msg);
    commit;
end;

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
   currently tables: KCIERPISZ.SUMANT_OPPTS1, KCIERPISZ.OPPTS3
2. tars pulled from GCM (prepared by Sree in lm_emea.emea_gcm_tar_summary)
    table: lm_emea.emea_gcm_tar_summary
3. table containing customer information: kcierpisz.prods_emea_flags
4. function is_table_populated() -> for checking if base table exists and if it contains data in it
5. proc drop_table2()
6. tables: gcd_dw.gcd_individuals, gcd_dw.gcd_countries, gcd_dw.gcd_regions,
    dm_metrics.email_suppression, gcd_dw.gcd_individual_services
    gcd_dw.gcd_correspondence_details, gcd_dw.lb_organizations_eu_vw, gcd_dw.gcd_gcm_activities,
    dm_metrics.LIST_MGMT_CONTACT_HISTORY,
    dm_metrics.vg_prfl_email_subscriptions
*/

procedure PROC_EMAIL_OPTINS

/*
creates a table email_optins and a simplified view email_optins_vw
view fields:
SUB_REGION_NAME,COUNTRY_ID,INDIVIDUAL_ID,EMAIL_ADDRESS,
CONTACT_ROWID,PROSPECT_ROWID,EMAIL_PERMISSION

table fields:
SUB_REGION_NAME,COUNTRY_ID,INDIVIDUAL_ID,EMAIL_ADDRESS,
CONTACT_ROWID,PROSPECT_ROWID,CONTACT_EMAIL_PRFL,
CONTACT_EMAIL_PRFL2,GCD_SERVICES,SUPPRESSION,CORRESPONDENCE1,
OPTIN,OP_DATE1,OP_DATE2,OP_DATE,ORG_ID,ORG_PARTY_ID,
LAST_EMAIL_CONTACTED_DATE,GSI_PARTY_ID,DUNS_NUMBER,
TAR_DATE_PARTYID,TAR_DATE_DUNS,CUSTOMER,ACTIVITY_DATE18,
EMAIL_HIST_SENT_DATE,EMAIL_PERMISSION3

*/

is

    omit boolean := false; --- just for testing to omit long running table creations
    
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_name varchar2(30) := 'EMAIL_OPTINS';
   email_optins_log    varchar2(30) := 'email_optins_log';
   sqlstmt varchar2(32000) := '';
   view_stmt varchar2(4000) := '';

   all_opportunities varchar2(61) := 'KCIERPISZ.SUMANT_OPPTS1';
/*
    table containing all_opportunities -> prepared in GCM "Custom Opportunity Segmentation" SA:
NAME                            Null?     Type
------------------------------- --------- -----
Contact or Prospect ID                    VARCHAR2(4000)
Email Address                             VARCHAR2(4000)
ROW_ID                                    VARCHAR2(4000)
Opportunity Name                          VARCHAR2(4000)

*/
   opts_details      varchar2(61) := 'KCIERPISZ.OPPTS3';
/*
    table containing all_opportunities -> prepared in GCM "Custom Contacts Opportunity Segmentation" SA:
NAME                            Null?     Type
------------------------------- --------- -----
Opty Status                               VARCHAR2(4000)
Opened Date                               VARCHAR2(4000)
ROW_ID                                    VARCHAR2(4000)
*/
   opt_contacts      varchar2(61) := 'oppt3110_distinct';
/* table that will contain grouped by contact_prospect_rowid, email_address, op_date */

   opt_emails        varchar2(61) := 'oppt3110_email_distinct';
/* table that will contain grouped by email_address, op_date */

   tars              varchar2(61) := 'lm_emea.emea_gcm_tar_summary';
/* table prepared by Sree that contains 2yrs TAR information from GCM "Custom Service Request Segmantation" SA */

   tar1              varchar2(61) := 'tar_tmp1';
/* table that will contain grouped by (gsi_party_id, duns_number) max(tar_date) */

   activities        varchar2(61) := 'activities18'; -- 18months activities
/* table that will contain grouped by individual_id act_date based on information coming from
        gcd_dw.gcd_gcm_activities
        act_date is the max date for the following condition:
        
        activity_date >= add_months(sysdate,-18)
        and (a.classification in (''SDS'',''Software Downloaded'',''OTN SOFTWARE DOWNLOAD'',
                            	  ''Event - Walk-in'',''Event - Pre-Reg Attendee'')
      	     or a.classification = ''EVENT'' and a.status in (''Attended'',''ATTENDED'',''Walkin'')
          	 or a.classification = ''ERS'' and a.response_type in (''Event - Pre-Reg Attendee'',''Event - Walk-in'')
            )               	
*/

   email_sent_hist1  varchar2(61) := 'email_sent_h1'; -- based on individual_id
/* table that will contain grouped by individual_id last_email_date
    based on information coming from dm_metrics.LIST_MGMT_CONTACT_HISTORY
    with condition:
          where upper(a.contact_channel) = ''EMAIL''
                and a.individual_id is not null
                and a.list_sent_date is not null
*/

   email_sent_hist2  varchar2(61) := 'email_sent_h2'; -- based on email_address
/* table that will contain grouped by email_address last_email_date
    based on information coming from dm_metrics.LIST_MGMT_CONTACT_HISTORY
          where upper(a.contact_channel) = ''EMAIL''
                and a.email_address is not null
                and a.list_sent_date is not null
*/

   prods_emea_flags  varchar2(61) := 'kcierpisz.prods_emea_flags';
/* table that contains customers -> EMEA only */

begin
    insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'PROC_' || table_name, sysdate,'START');
    commit;
    if is_table_populated('gcd_dw.gcd_individuals') then
        insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'POPULATED');
        commit;

            drop_table2(table_name || '_TMP1', email_optins_log);
            drop_table2(table_name || '_TMP2', email_optins_log);
            drop_table2(table_name || '_TMP3', email_optins_log);
            drop_table2(table_name || '_TMP4', email_optins_log);
            drop_table2(table_name || '_TMP5', email_optins_log);

            begin
            sqlstmt := 'create table ' || table_name || '_tmp1 nologging as
                        select c.name as sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid, a.prospect_rowid,
                        a.org_id, a.org_party_id, a.last_email_contacted_date
                        from gcd_dw.gcd_individuals a, gcd_dw.gcd_countries b, gcd_dw.gcd_regions c
                        where
                        a.country_id in (2,3,5,6,11,14,15,17,20,21,23,27,28,33,34,35,37,39,41,42,48,49,50,53,54,56,57,58,59,64,66,67,68,69,70,71,73,74,75,76,79,80,81,82,83,84,85,86,88,91,92,96,99,100,103,104,105,106,107,110,111,112,116,117,119,120,121,122,123,124,125,126,128,129,130,133,134,136,137,138,139,142,143,146,147,149,152,153,154,157,158,162,163,172,173,175,176,177,178,179,184,185,186,187,188,189,191,192,194,195,196,197,199,200,201,203,204,205,206,207,209,210,212,216,217,218,221,222,223,224,228,235,236,238,239,242,243,244,246,247)
                        --a.country_id in (2,3,5,6) -- TEST ONLY
                        and a.country_id = b.country_id
                        and b.region_id = c.region_id';
                        --and a.email_address like ''_%@_%.__%''';

            dbms_output.put_line(sqlstmt);
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp1 CREATE', sysdate,'CREATING...');
            commit;
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp1 CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp1 CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            -- prepare vg_prfl table
            if is_table_populated('dm_metrics.vg_prfl_email_subscriptions')
                and omit <> true
            then
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'vg_prfl CREATE', sysdate,'CREATING...');
                commit;

            begin
                sqlstmt := 'drop table vg_prfl';
                execute immediate sqlstmt;
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'vg_prfl DROP', sysdate,'DROPEED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'vg_prfl DROP', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
            sqlstmt := 'create table vg_prfl nologging as
            select a.new_individual_id, min(a.email_opt_in_flag_aftr_sup) contact_email,
		      min(a.email) email_address from dm_metrics.vg_prfl_email_subscriptions a
		        where a.case <> ''OTHERS'' and a.use_this_email = ''Y''
                group by a.new_individual_id';

                   dbms_output.put_line(sqlstmt);
                execute immediate sqlstmt;
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'vg_prfl CREATE', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'vg_prfl CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


            begin

            sqlstmt := 'create index bt_vg_prfl_new_ind on vg_prfl (new_individual_id)';
            execute immediate sqlstmt;
            sqlstmt := 'create index bt_vg_prfl_contact_email on vg_prfl (contact_email)';
            execute immediate sqlstmt;
            sqlstmt := 'create index bt_vg_prfl_email on vg_prfl (email_address)';
            execute immediate sqlstmt;

                dbms_output.put_line(sqlstmt);
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'vg_prfl indexes CREATE', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'vg_prfl indexes CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            else
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'dm_metrics.vg_prfl_email_subscriptions not populated or OMIT true', sysdate,'NOT POPULATED - ');
                commit;
            end if;
            --------------------------

         if is_table_populated('vg_prfl') then
            begin
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp2 CREATE', sysdate,'CREATING...');
            commit;
            sqlstmt := 'create table ' || table_name || '_tmp2 nologging as
                select a.sub_region_name, a.country_id, a.individual_id, a.contact_rowid, a.prospect_rowid,
                a.org_id, a.org_party_id, a.last_email_contacted_date,
                (case when b.email_address is not null then b.email_address else a.email_address end) email_address,
                b.contact_email contact_email_prfl
                from ' || table_name || '_tmp1 a, vg_prfl b
                where a.individual_id  = b.new_individual_id (+)';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp2 CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp2 CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            begin
            sqlstmt := 'alter table ' || table_name || '_tmp2 add (contact_email_prfl2 varchar2(1))';
            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp2 ALTER add contact_email_prfl2', sysdate,'ALTERED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp2 ALTER add contact_email_prfl2', sysdate,'NOT ALTERED - ' || err_msg);
                commit;
            end;

            begin
            sqlstmt := 'create index bt_' || table_name || '_tmp2_em on ' || table_name || '_tmp2 (email_address)';
            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp2 INDEX on email_address', sysdate,'INDEX CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp2 INDEX on email_address', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


            begin
            sqlstmt := 'update ' || table_name || '_tmp2 a set
                        a.contact_email_prfl2 = (select min(contact_email) from vg_prfl b
                                                                where b.email_address = a.email_address)';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp2 UPDATE based on email from profile', sysdate,'UPDATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp2 UPDATE based on email from profile', sysdate,'NOT UPDATED - ' || err_msg);
                commit;
            end;

          end if;

         if is_table_populated('dm_metrics.email_suppression') then
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp3 CREATE and dm_metics.email_suppression POPULATED', sysdate,'CREATING...');
            commit;
            begin
            sqlstmt := 'create table ' || table_name || '_tmp3 nologging as
                select a.sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid, a.prospect_rowid,
                a.org_id, a.org_party_id, a.last_email_contacted_date,
                a.contact_email_prfl,a.contact_email_prfl2, max(case when b.email_address is not null then 1 end) suppression
                from ' || table_name || '_tmp2 a, dm_metrics.email_suppression b
                where a.email_address = b.email_address (+)
                group by a.sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid,
                a.org_id, a.org_party_id, a.last_email_contacted_date,
                a.prospect_rowid, a.contact_email_prfl, a.contact_email_prfl2';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp3 CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp3 CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;
          end if;

         if is_table_populated('gcd_dw.gcd_individual_services') then
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp4 CREATE and gcd_individual_services POPULATED', sysdate,'CREATING...');
            commit;
            begin
            sqlstmt := 'create table ' || table_name || '_tmp4 nologging as
                select a.*, (case when b.individual_id is not null and b.news_letter_flg in (''Y'',''1'') then ''Y''
				                  when b.individual_id is not null and b.news_letter_flg in (''N'') then ''N'' end) gcd_services
                from ' || table_name || '_tmp3 a, gcd_dw.gcd_individual_services b
                where a.individual_id = b.individual_id (+)
                and b.service_type_id (+) = 39';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp4 CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp4 CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;
          end if;

         if is_table_populated('gcd_dw.gcd_correspondence_details') then
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp5 CREATE and gcd_correspondence_details POPULATED', sysdate,'CREATING...');
            commit;

            begin
            sqlstmt := 'create table ' || table_name || '_tmp5 nologging as
                select a.*,  b.permission_given_flg correspondence1
                from ' || table_name || '_tmp4 a, gcd_dw.gcd_correspondence_details b
                where a.individual_id = b.individual_id (+)
                and b.correspondence_type_id (+) = 1';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp5 CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp5 CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;
          end if;


        if is_table_populated(table_name || '_tmp5') then
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp5', sysdate,'POPULATED');
            commit;

            drop_table2(table_name || '_FLAGS_BAK', email_optins_log);

            begin
                execute immediate 'alter table ' || table_name || '_FLAGS rename to ' || table_name || '_FLAGS_bak';
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS rename -> ' || table_name || '_FLAGS_bak', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'create table ' || table_name || '_FLAGS nologging as
                select a.sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid, a.prospect_rowid,
                a.org_id, a.org_party_id, a.last_email_contacted_date,
                a.contact_email_prfl, a.contact_email_prfl2, a.gcd_services, a.suppression,
                a.correspondence1,
                (case when a.contact_email_prfl = ''Y'' or a.contact_email_prfl2 = ''Y'' then ''Y''
                	  when a.contact_email_prfl = ''N'' or a.contact_email_prfl2 = ''N'' then ''N''
                	  when a.suppression = 1 or a.gcd_services = ''N'' then ''N''
                	  when a.gcd_services = ''Y'' or a.correspondence1 = ''Y'' then ''Y''
                	  end) optin
                    from ' || table_name || '_tmp5 a
                    where email_address like ''_%@_%.__%''';
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS created from ' || table_name || '_tmp5', sysdate,'CREATED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS not created from ' || table_name || '_tmp5', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'DROP INDEX BT_' || table_name || '_FLAGS_ind_id';
                execute immediate 'DROP INDEX BT_' || table_name || '_FLAGS_email';
                execute immediate 'DROP INDEX FB_' || table_name || '_FLAGS_rowid';
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS indexes', sysdate,'DROPPED');
                commit;
           EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate '
                    CREATE Unique INDEX BT_' || table_name || '_FLAGS_ind_id ON ' || table_name || '_FLAGS (  individual_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_' || table_name || '_FLAGS_email ON ' || table_name || '_FLAGS (  email_address  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX FB_' || table_name || '_FLAGS_rowid ON ' || table_name || '_FLAGS (  nvl(contact_rowid,prospect_rowid)  )
                    COMPUTE STATISTICS';

                    insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS indexes', sysdate,'CREATED');
                    commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;
           else
             insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp5', sysdate,'NOT POPULATED');
           end if;


           ---------preparing opportunities tables ---------------

            --   all_opportunities = 'KCIERPISZ.SUMANT_OPPTS1';
            --   opts_details      = 'KCIERPISZ.OPPTS3';

         if is_table_populated(all_opportunities) and
            is_table_populated(opts_details)
         then
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,all_opportunities || ' and ' || opts_details || ' POPULATED', sysdate,'POPULATED');
            commit;

                drop_table2(opt_contacts, email_optins_log);
                drop_table2(opt_emails,email_optins_log);

            begin
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,opt_contacts || ' CREATE', sysdate,'CREATING...');
            commit;
            sqlstmt := 'create table ' || opt_contacts || ' as
                        select a."Contact or Prospect ID" contact_prospect_rowid,
                        max(upper(trim(a."Email Address"))) email_address,
                        max(to_date(b."Opened Date",''YYYY-MM-DD HH24:MI:SS'')) op_date
                        from ' || all_opportunities || ' a, ' || opts_details || ' b
                        where a.row_id = b.row_id
                        and b."Opty Status" not in (''Lead:Declined'',''No Opportunity'',''Lost'')
                        group by a."Contact or Prospect ID"
                        ';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,opt_contacts || ' CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,opt_contacts || ' CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            begin
            sqlstmt := 'create unique index bt_' || opt_contacts || '_cnt_id on ' || opt_contacts || ' (contact_prospect_rowid)';
            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,opt_contacts || ' INDEX CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,opt_contacts || ' INDEX CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


            begin
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,opt_emails || ' CREATE', sysdate,'CREATING...');
            commit;
            sqlstmt := 'create table ' || opt_emails || ' as
                        select upper(trim(a."Email Address")) email_address,
                        max(to_date(b."Opened Date",''YYYY-MM-DD HH24:MI:SS'')) op_date
                        from ' || all_opportunities || ' a, ' || opts_details || ' b
                        where a.row_id = b.row_id
                        and b."Opty Status" not in (''Lead:Declined'',''No Opportunity'',''Lost'')
                        and a."Email Address" is not null
                        group by upper(trim(a."Email Address"))
                        ';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,opt_emails || ' CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,opt_emails || ' CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            begin
            sqlstmt := 'create unique index bt_opt_em_email on ' || opt_emails || ' (email_address)';
            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,opt_emails || ' INDEX CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,opt_emails || ' INDEX CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

          end if;

           ------------------------


           ---- preparing TARs -------

            -- tars = 'lm_emea.emea_gcm_tar_summary'

         if is_table_populated(tars)
            and omit <> true
         then
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tars || ' POPULATED', sysdate,'POPULATED');
            commit;

                drop_table2(tar1, email_optins_log);

            begin
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tar1 || ' CREATE', sysdate,'CREATING...');
            commit;
            sqlstmt := 'create table ' || tar1 || ' nologging as
                        select a.gsi_party_id, a.duns_number, max(to_date(a.tar_date,''MM/DD/YYYY HH24:MI'')) tar_date
                        from ' || tars || ' a
                        group by a.gsi_party_id, a.duns_number
                        ';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tar1 || ' CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tar1 || ' CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            begin
            sqlstmt := 'create index bt_' || tar1 || '_email on ' || tar1 || ' (gsi_party_id)';
            execute immediate sqlstmt;
            sqlstmt := 'create index bt_' || tar1 || '_duns on ' || tar1 || ' (duns_number)';
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tar1 || ' INDEXes gsi_party_id and duns_number', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tar1 || ' INDEXes gsi_party_id and duns_number', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

          else
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tars, sysdate,'NOT POPULATED');
          end if;

           ----------------------------

            ----------------- based on LORI's chart rules ----------------
                       if is_table_populated(table_name || '_FLAGS')
                          and is_table_populated(opt_contacts)
                          and is_table_populated(opt_emails)
                        then

                            drop_table2(table_name || '_FLAGS2', email_optins_log);
                          begin
                            sqlstmt := 'create table ' || table_name || '_FLAGS2 nologging as
                                        select a.*, b.op_date op_date1, c.op_date op_date2,
                                            (case when nvl(c.op_date,to_date(''01011900'',''DDMMYYYY'')) >
                                                            nvl(b.op_date,to_date(''01011900'',''DDMMYYYY''))
                                                    then c.op_date
                                                    else b.op_date end) op_date
                                        from ' || table_name || '_FLAGS a, ' || opt_contacts || ' b, ' || opt_emails || ' c
                                        where b.contact_prospect_rowid (+) = coalesce(a.contact_rowid, a.prospect_rowid)
                                        and a.email_address = c.email_address (+)
                                        ';
                            execute immediate sqlstmt;
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS2 created from ' || table_name || '_FLAGS', sysdate,'CREATED');
                            commit;
                         exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS2 created from ' || table_name || '_FLAGS', sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                         end;
                         
                         begin
                            sqlstmt := 'create index bt_opt_fl2_org on ' || table_name || '_FLAGS2' || ' (org_id)';
                            execute immediate sqlstmt;
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS2 INDEX org_id', sysdate,'CREATED');
                            commit;
                        exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS2 INDEX org_id', sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                        end;

                       else
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS', sysdate,'NOT POPULATED');

                       end if;

                       if is_table_populated(table_name || '_FLAGS2') then
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS2', sysdate,'POPULATED');
                            commit;

                        drop_table2(table_name || '_FLAGS4', email_optins_log);
                        begin
                            sqlstmt := 'create table ' || table_name || '_FLAGS4 nologging as
                            select a.*, b.gsi_party_id, b.duns_number
                            from ' || table_name || '_flags2 a, gcd_dw.lb_organizations_eu_vw b
                            where a.org_id = b.org_id (+)
                            ';
                            execute immediate sqlstmt;
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS4', sysdate,'CREATED');
                            commit;
                         exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS4',sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                         end;

                        begin
                            sqlstmt := 'create index bt_opt_fl4_part on ' || table_name || '_FLAGS4' || ' (gsi_party_id)';
                            execute immediate sqlstmt;
                            sqlstmt := 'create index bt_opt_fl4_duns on ' || table_name || '_FLAGS4' || ' (duns_number)';
                            execute immediate sqlstmt;
                            sqlstmt := 'create unique index bt_opt_fl4_ind on ' || table_name || '_FLAGS4' || ' (individual_id)';
                            execute immediate sqlstmt;
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS4 INDEXes on gsi_party_id and org_id and ind_id', sysdate,'CREATED');
                            commit;
                        exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS4 INDEXes on gsi_party_id and org_id and ind_id', sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                        end;

                       else
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS2', sysdate,'NOT POPULATED');
                       end if;


                       if is_table_populated(table_name || '_FLAGS4')
                            and is_table_populated(tar1) then
                        drop_table2(tar1 || '_part', email_optins_log);
                        drop_table2(tar1 || '_duns', email_optins_log);



                        begin
                            sqlstmt := 'create table ' || tar1 || '_part nologging as
                            select a.individual_id, max(b.tar_date) tar_date_party_id
                            from ' || table_name || '_FLAGS4 a, ' || tar1 || ' b
                            where a.gsi_party_id = b.gsi_party_id
                            group by a.individual_id, a.email_address
                            ';
                            execute immediate sqlstmt;
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tar1 || '_part', sysdate,'CREATED');
                            commit;
                         exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tar1 || '_part',sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                         end;

                        begin
                            sqlstmt := 'create table ' || tar1 || '_duns nologging as
                            select a.individual_id, max(b.tar_date) tar_date_duns
                            from ' || table_name || '_flags4 a, ' || tar1 || ' b
                            where a.duns_number = b.duns_number
                            group by a.individual_id, a.email_address
                            ';
                            execute immediate sqlstmt;
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tar1 || '_duns', sysdate,'CREATED');
                            commit;
                         exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tar1 || '_duns',sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                         end;

                         begin
                            sqlstmt := 'create index bt_tar1_part_ind on ' || tar1 || '_part' || ' (individual_id)';
                            execute immediate sqlstmt;
                            sqlstmt := 'create index bt_tar1_duns_ind on ' || tar1 || '_duns' || ' (individual_id)';
                            execute immediate sqlstmt;

                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tar1 || '_part and _duns INDEXes on individual_id', sysdate,'CREATED');
                            commit;
                        exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tar1 || '_part and _duns INDEXes on individual_id', sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                        end;


                       else
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS4', sysdate,'NOT POPULATED');
                       end if;

            ---------------------------------------------

                ---- prepare activities

                        if is_table_populated('gcd_dw.gcd_gcm_activities')
                            and omit <> true
                        then

                        drop_table2(activities, email_optins_log);

                        begin
                            sqlstmt := 'create table ' || activities || ' nologging as
                            select a.individual_id, max(a.activity_date) act_date from gcd_dw.gcd_gcm_activities a
                            where a.activity_date >= add_months(sysdate,-18)
                            and (a.classification in (''SDS'',''Software Downloaded'',''OTN SOFTWARE DOWNLOAD'',
                            						  ''Event - Walk-in'',''Event - Pre-Reg Attendee'')
                            	 or a.classification = ''EVENT'' and a.status in (''Attended'',''ATTENDED'',''Walkin'')
                            	 or a.classification = ''ERS'' and a.response_type in (''Event - Pre-Reg Attendee'',''Event - Walk-in''))
                            group by a.individual_id
                            ';
                            execute immediate sqlstmt;
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,activities, sysdate,'CREATED');
                            commit;
                         exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,activities,sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                         end;

                        begin
                            sqlstmt := 'create unique index bt_acts_ind on ' || activities || ' (individual_id)';
                            execute immediate sqlstmt;
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,activities || ' INDEX on individual_id', sysdate,'CREATED');
                            commit;
                        exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,activities || ' INDEX on individual_id', sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                        end;


                       else
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'gcd_dw.gcd_gcm_activities', sysdate,'NOT POPULATED');
                       end if;


                    --------- prepare email sent history ---------------

/* we don't need it, I guess :)
                        if is_table_populated('dm_metrics.LIST_MGMT_CONTACT_HISTORY')
                            and omit <> true
                        then

                        drop_table2(email_sent_hist1, email_optins_log);
                        drop_table2(email_sent_hist2, email_optins_log);

                        begin
                            sqlstmt := 'create table ' || email_sent_hist1 || ' nologging as
                                    select a.individual_id, max(a.list_sent_date) last_email_date
                                    from dm_metrics.LIST_MGMT_CONTACT_HISTORY a
                                    where upper(a.contact_channel) = ''EMAIL''
                                    and a.individual_id is not null
                                    and a.list_sent_date is not null
                                    group by a.individual_id
                                    ';
                            execute immediate sqlstmt;
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,email_sent_hist1, sysdate,'CREATED');
                            commit;
                         exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,email_sent_hist1,sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                         end;

                        begin
                            sqlstmt := 'create table ' || email_sent_hist2 || ' nologging as
                                    select upper(a.email_address) email_address, max(a.list_sent_date) last_email_date
                                    from dm_metrics.LIST_MGMT_CONTACT_HISTORY a
                                    where upper(a.contact_channel) = ''EMAIL''
                                    and a.email_address is not null
                                    and a.list_sent_date is not null
                                    group by upper(a.email_address)
                                    ';
                            execute immediate sqlstmt;
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,email_sent_hist2, sysdate,'CREATED');
                            commit;
                         exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,email_sent_hist2,sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                         end;

                         begin
                            execute immediate 'drop index bt_email_hist1_ind';
                            execute immediate 'drop index bt_email_hist2_email';
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,email_sent_hist1 || ' and ' || email_sent_hist2 || ' indexes', sysdate,'DROPPED');
                            commit;
                         exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,email_sent_hist1 || ' and ' || email_sent_hist2 || ' indexes', sysdate,'NOT DROPPED - ' || err_msg);
                            commit;
                         end;

                         begin
                            sqlstmt := 'create index bt_email_hist1_ind on ' || email_sent_hist1 || ' (individual_id)';
                            execute immediate sqlstmt;
                            sqlstmt := 'create index bt_email_hist2_email on ' || email_sent_hist2 || ' (email_address)';
                            execute immediate sqlstmt;

                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,email_sent_hist1 || ' and ' || email_sent_hist2 || ' indexes', sysdate,'CREATED');
                            commit;
                        exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,email_sent_hist1 || ' and ' || email_sent_hist2 || ' indexes', sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                        end;


                       else
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'dm_metrics.LIST_MGMT_CONTACT_HISTORY', sysdate,'NOT POPULATED');
                       end if;


                -----------------------------------------------------
*/
                        if is_table_populated(table_name || '_FLAGS4') then

                        drop_table2(table_name || '_FLAGS5', email_optins_log);

                        begin
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS5', sysdate,'CREATING...');
                            commit;

                            sqlstmt := 'create table ' || table_name || '_flags5 nologging as
                                        select /*+ index(b bt_tar1_part_ind)
                            		    index(c bt_tar1_duns_ind) index(d BT_PRODS_EMEA_FLAGS_ORG_ID)
                                		index(f bt_acts_ind) */
                                    a.*, b.tar_date_party_id tar_date_partyid, c.tar_date_duns tar_date_duns,
                                    (case when d.org_id is not null then greatest(nvl(d.db_inst,add_months(sysdate,-70)),
											  nvl(d.applications,add_months(sysdate,-70))
											 ) end) customer,
                                    f.act_date activity_date18
                                    from ' || table_name || '_flags4 a, ' || tar1 || '_part b, ' ||
                                            tar1 || '_duns c, ' || prods_emea_flags || ' d, ' ||
                                            activities || ' f
                                    where  a.individual_id = b.individual_id (+)
                                    and a.individual_id = c.individual_id (+)
                                    and a.org_id = d.org_id (+)
                                    and coalesce(d.db_inst(+),d.applications(+) ) is not null
                                    and a.individual_id = f.individual_id (+)
                                    ';
                                    
                            execute immediate sqlstmt;
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS5', sysdate,'CREATED');
                            commit;
                         exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS5',sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                         end;

                         begin
                            sqlstmt := 'create unique index bt_em_fl5_ind on ' || table_name || '_FLAGS5 (individual_id)';
                            execute immediate sqlstmt;
                            sqlstmt := 'create index bt_em_fl5_em on ' || table_name || '_FLAGS5 (email_address)';
                            execute immediate sqlstmt;

                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS5 indexes on ind_id and email_address', sysdate,'CREATED');
                            commit;
                        exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS5 indexes on ind_id and email_address', sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                        end;


                       else
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS4', sysdate,'NOT POPULATED');
                       end if;


/*
                        if is_table_populated(table_name || '_FLAGS5') then

                        drop_table2(table_name || '_FLAGS6', email_optins_log);

                        begin
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS6', sysdate,'CREATING...');
                            commit;

                            sqlstmt := 'create table ' || table_name || '_flags6 nologging as
                                select
*/
                                /*+ index(e1 bt_email_hist1_ind) index(e2 BT_EMAIL_HIST2_EMAIL) */
/*                                a.*,
                                (case when e1.last_email_date >= nvl(e2.last_email_date,add_months(sysdate,-50)) then e1.last_email_date
		                          else e2.last_email_date end) email_hist_sent_date
                                from ' || table_name || '_flags5 a, ' ||
                                    email_sent_hist1 || ' e1, ' ||
                                    email_sent_hist2 || ' e2
                                where a.individual_id = e1.individual_id (+)
                        		and a.email_address = e2.email_address (+)
                            ';

                            execute immediate sqlstmt;
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS6', sysdate,'CREATED');
                            commit;
                         exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS6',sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                         end;

                       else
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS5', sysdate,'NOT POPULATED');
                       end if;

*/ -- since we don't need email_sent_history
---------- FINAL

--               if is_table_populated(table_name || '_FLAGS6') then
               if is_table_populated(table_name || '_FLAGS5') then

                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS5', sysdate,'POPULATED');
                            commit;

                        drop_table2(table_name || '_BAK', email_optins_log);

--                        email_optins -> email_optins_bak
                        begin
                            execute immediate 'alter table ' || table_name || ' rename to ' || table_name || '_bak';
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || ' rename -> ' || table_name || '_bak', sysdate,'RENAMED');
                            commit;
                        EXCEPTION WHEN OTHERS THEN
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || ' rename', sysdate,'NOT RENAMED - ' || err_msg);
                            commit;
                        end;

                        begin
                            sqlstmt := 'create table ' || table_name || ' nologging as
                                        select a.*,
                                        (case
                                         when a.contact_email_prfl = ''N'' or a.contact_email_prfl2 = ''N''
                                                    or a.suppression is not null
                                                    or coalesce(a.contact_email_prfl,a.contact_email_prfl2) is null
                                                        and a.suppression is null
                                                        and a.gcd_services = ''N''
                                          then ''N''
                                        when
                                            (a.contact_email_prfl = ''Y'' or a.contact_email_prfl2 = ''Y''
		                                      or nvl(a.op_date,add_months(sysdate,-60)) >= add_months(sysdate,-18)
		                                      or nvl(a.tar_date_partyid,add_months(sysdate,-60)) >= add_months(sysdate,-18) or
			                                 nvl(a.tar_date_duns,add_months(sysdate,-60)) >= add_months(sysdate,-18)
		                                      or a.customer is not null
		                                      or nvl(a.activity_date18,add_months(sysdate,-60)) >= add_months(sysdate,-18)
			                                )
                                                and nvl(a.contact_email_prfl,''A'') <> ''N'' and nvl(a.contact_email_prfl2,''A'') <> ''N''
                                                and a.suppression is null
                                           or
                                                (a.gcd_services = ''Y'' or a.correspondence1 = ''Y'')
                                                and (a.contact_email_prfl is null and a.contact_email_prfl2 is null)
                                                and a.suppression is null
		                                  then ''Y''

                                         

                                          when
                                                a.contact_email_prfl is null and a.contact_email_prfl2 is null
                                                and nvl(a.op_date,add_months(sysdate,-60)) < add_months(sysdate,-18)
                                                and nvl(a.tar_date_partyid,add_months(sysdate,-60)) < add_months(sysdate,-18)
                                                and nvl(a.tar_date_duns,add_months(sysdate,-60)) < add_months(sysdate,-18)
                                                and a.customer is null
                                                and nvl(a.activity_date18,add_months(sysdate,-60)) < add_months(sysdate,-18)
                                                and a.gcd_services is null
                                                and nvl(a.sub_region_name,''A'') not in (''MIDDLE EAST'',''AFRICA'')
                                                then null
                                          when
                                                a.contact_email_prfl is null and a.contact_email_prfl2 is null
                                                and nvl(a.op_date,add_months(sysdate,-60)) < add_months(sysdate,-18)
                                                and nvl(a.tar_date_partyid,add_months(sysdate,-60)) < add_months(sysdate,-18)
                                                and nvl(a.tar_date_duns,add_months(sysdate,-60)) < add_months(sysdate,-18)
                                                and a.customer is null
                                                and nvl(a.activity_date18,add_months(sysdate,-60)) < add_months(sysdate,-18)
                                                and nvl(a.gcd_services,''Y'') = ''Y''
                                                and nvl(a.sub_region_name,''A'') in (''MIDDLE EAST'',''AFRICA'')
                                                then ''Y''
                                          end) email_permission,

     (case
        when a.contact_email_prfl = ''N'' or a.contact_email_prfl2 = ''N''
            or a.suppression is not null
         then ''N''

        when coalesce(a.contact_email_prfl,a.contact_email_prfl2) = ''Y''
             and a.suppression is null then ''Y''

        when (nvl(a.op_date,add_months(sysdate,-60)) >= add_months(sysdate,-18)
		      or nvl(a.tar_date_partyid,add_months(sysdate,-60)) >= add_months(sysdate,-18)
              or nvl(a.tar_date_duns,add_months(sysdate,-60)) >= add_months(sysdate,-18)
		      or a.customer is not null
		      or nvl(a.activity_date18,add_months(sysdate,-60)) >= add_months(sysdate,-18)
			  )
              and coalesce(a.contact_email_prfl,a.contact_email_prfl2) is null
              and a.suppression is null
              then ''Y''

        when
            nvl(a.sub_region_name,''A'') in (''MIDDLE EAST'',''AFRICA'')
            AND coalesce(a.contact_email_prfl, a.contact_email_prfl2) is null
            and a.suppression is null
          then ''Y''

        when
            nvl(a.sub_region_name,''A'') not in (''MIDDLE EAST'',''AFRICA'')
            and coalesce(a.contact_email_prfl,a.contact_email_prfl2) is null
            and a.suppression is null
          then null
    end) email_permission2,
                                          
         (case
        when a.contact_email_prfl = ''N'' or a.contact_email_prfl2 = ''N''
            or a.suppression is not null
         then ''N''

        when (a.contact_email_prfl = ''Y'' or a.contact_email_prfl2 = ''Y'')
             and a.suppression is null then ''Y''

        when (nvl(a.op_date,add_months(sysdate,-60)) >= add_months(sysdate,-18)
		      or nvl(a.tar_date_partyid,add_months(sysdate,-60)) >= add_months(sysdate,-18)
              or nvl(a.tar_date_duns,add_months(sysdate,-60)) >= add_months(sysdate,-18)
		      or a.customer is not null
		      or nvl(a.activity_date18,add_months(sysdate,-60)) >= add_months(sysdate,-18)
			  )
              and coalesce(a.contact_email_prfl,a.contact_email_prfl2) is null
              and a.suppression is null
              then ''Y''

        when nvl(a.gcd_services,''A'') = ''Y'' or nvl(a.correspondence1,''A'') = ''Y'' then ''Y''

        when nvl(a.gcd_services,''A'') = ''N'' or nvl(a.correspondence1,''A'') = ''N'' then ''N''

        when
            nvl(a.sub_region_name,''A'') in (''MIDDLE EAST'',''AFRICA'')
            and (nvl(a.gcd_services,''Y'') = ''Y''  or nvl(a.correspondence1,''Y'') = ''Y'')
          then ''Y''

        when
            nvl(a.sub_region_name,''A'') not in (''MIDDLE EAST'',''AFRICA'')
            and coalesce(a.gcd_services, a.correspondence1) is null
          then null
    end) email_permission3,

                                          
    (case
        when a.contact_email_prfl = ''N'' or a.contact_email_prfl2 = ''N''
            or a.suppression is not null
         then ''N''

        when (a.contact_email_prfl = ''Y'' or a.contact_email_prfl2 = ''Y'')
             and a.suppression is null then ''Y''

        when (nvl(a.op_date,add_months(sysdate,-60)) >= add_months(sysdate,-18)
		      or nvl(a.tar_date_partyid,add_months(sysdate,-60)) >= add_months(sysdate,-18)
              or nvl(a.tar_date_duns,add_months(sysdate,-60)) >= add_months(sysdate,-18)
		      or a.customer is not null
		      or nvl(a.activity_date18,add_months(sysdate,-60)) >= add_months(sysdate,-18)
			  )
              and coalesce(a.contact_email_prfl,a.contact_email_prfl2) is null
              and a.suppression is null
              then ''Y''

        when nvl(a.gcd_services,''A'') = ''N'' or nvl(a.correspondence1,''A'') = ''N'' then ''N''
        when nvl(a.gcd_services,''A'') = ''Y'' or nvl(a.correspondence1,''A'') = ''Y'' then ''Y''

        when
            nvl(a.sub_region_name,''A'') in (''MIDDLE EAST'',''AFRICA'')
            and (nvl(a.gcd_services,''Y'') = ''Y''  or nvl(a.correspondence1,''Y'') = ''Y'')
          then ''Y''

        when
            nvl(a.sub_region_name,''A'') not in (''MIDDLE EAST'',''AFRICA'')
            and coalesce(a.gcd_services, a.correspondence1) is null
          then null
    end) email_permission4

                                        from ' || table_name || '_FLAGS5 a';
                            execute immediate sqlstmt;
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || ' created from ' || table_name || '_FLAGS5', sysdate,'CREATED');
                            commit;
                        EXCEPTION WHEN OTHERS THEN
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || ' created from ' || table_name || '_FLAGS5', sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                        end;

                        begin
                            execute immediate 'drop index bt_em_op_ind1';
                            execute immediate 'drop index bt_em_op_email1';
                            execute immediate 'drop index bm_em_op_country1';
                            execute immediate 'drop index bm_em_op_region1';
                            execute immediate 'drop index FB_em_op_rowid1';
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || ' indexes', sysdate,'DROPED');
                            commit;
                         exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || ' indexes',sysdate,'NOT DROPPED - ' || err_msg);
                            commit;
                         end;

                        begin
                            sqlstmt := 'create unique index bt_em_op_ind1 on ' || table_name || ' (individual_id)';
                            execute immediate sqlstmt;
                            sqlstmt := 'create bitmap index bm_em_op_country1 on ' || table_name || ' (country_id)';
                            execute immediate sqlstmt;
                            sqlstmt := 'create bitmap index bm_em_op_region1 on ' || table_name || ' (sub_region_name)';
                            execute immediate sqlstmt;
                            sqlstmt := 'create index bt_em_op_email1 on ' || table_name || ' (email_address)';
                            execute immediate sqlstmt;
                            sqlstmt := 'CREATE INDEX FB_em_op_rowid1 ON ' || table_name || ' (  nvl(contact_rowid,prospect_rowid)  )';
                            execute immediate sqlstmt;
                            sqlstmt := 'create bitmap index bm_em_op_perm4 on ' || table_name || ' (email_permission4)';
                            execute immediate sqlstmt;

                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || ' indexes', sysdate,'CREATED');
                            commit;
                         exception when others then
                            err_msg := SUBSTR(SQLERRM, 1, 100);
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || ' indexes',sysdate,'NOT CREATED - ' || err_msg);
                            commit;
                         end;


                       else
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS5', sysdate,'NOT POPULATED');
                       end if;

                -----------------------


                view_stmt := 'create or replace view ' || table_name || '_vw as
                    select a.sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid, a.prospect_rowid,
                    a.email_permission4 email_permission
                    from ' || table_name || ' a';

                begin
                    execute immediate view_stmt;
                    insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'email_optins_vw view', sysdate,'CREATED');
                    commit;
                exception when others then
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'email_optins_vw view', sysdate,'NOT CREATED - ' || err_msg);
                    commit;
                end;


    else
        insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'NOT POPULATED ending');
        commit;
    end if;


    begin
      execute immediate 'GRANT SELECT ON ' ||  table_name || ' TO public';
      execute immediate 'GRANT SELECT ON ' ||  table_name || '_vw TO public';
      insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'GRANT SELECT ON ' || table_name || ' TO public', sysdate,'GRANTED');
      commit;

    end;

    insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'PROC_' || table_name, sysdate,'END');
    commit;

end;

END;

