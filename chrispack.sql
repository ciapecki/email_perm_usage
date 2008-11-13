PACKAGE BODY CHRISPACK AS


FUNCTION get_value_from_query(q_string IN varchar2)
return varchar2 IS
    type quer is ref cursor;
    cur quer;
    result     varchar2(4000);
BEGIN
    result := '';
    open cur for q_string;
    fetch cur into result;
    close cur;

    return result;
END;

function email_suppression(email_address in varchar2)
return varchar2
is
    return_value varchar2(10) := '------';
    sql_query    varchar2(4000) := 'SELECT type from dm_metrics.email_suppression where rownum = 1 and email_address = ''' || email_address || '''';
begin
--    dbms_output.put_line(sql_query);
    return_value := get_value_from_query(sql_query);
    if return_value is not null then
        return_value := 'suppressed';
    end if;
    return return_value;
end;

function email_optout(individual_id in varchar2)
return varchar2
is
    return_value varchar2(10) := '------';
    sql_query    varchar2(4000) := 'SELECT individual_id from dm_metrics.email_optout where rownum = 1 and individual_id = ' || individual_id;
begin
 --   dbms_output.put_line(sql_query);
    return_value := get_value_from_query(sql_query);
    if return_value is not null then
        return_value := 'opt-out';
    end if;
    return return_value;
end;

function get_correspondence_details(individual_id in varchar2)
return varchar2
is
    return_value varchar2(3000) := '';
    sql_query    varchar2(4000) := 'SELECT permission_given_flg from gcd_dw.gcd_correspondence_details where correspondence_type_id = 1 and individual_id = ' || individual_id;

    type cur_ref is ref cursor;
    curs cur_ref;
    
    permission_given_flg varchar2(1) := '';
begin
--    dbms_output.put_line(sql_query);
    
    open curs for sql_query;
    fetch curs into permission_given_flg;
    loop
        exit when curs%notfound;
        return_value := return_value || permission_given_flg || ',';
        fetch curs into permission_given_flg;
    end loop;
    close curs;
    
    if return_value is not null then
        return_value := substr(return_value,1,length(return_value)-1);
    end if;
    
    return return_value;
end;

function get_individual_services(individual_id in varchar2)
return varchar2
is
    return_value varchar2(3000) := '';
    sql_query    varchar2(4000) := 'SELECT news_letter_flg from gcd_dw.gcd_individual_services where service_type_id = 39 and individual_id = ' || individual_id;

    type cur_ref is ref cursor;
    curs cur_ref;

    flg varchar2(1) := '';
begin
--    dbms_output.put_line(sql_query);

    open curs for sql_query;
    fetch curs into flg;
    loop
        exit when curs%notfound;
        return_value := return_value || flg || ',';
        fetch curs into flg;
    end loop;
    close curs;

    if return_value is not null then
        return_value := substr(return_value,1,length(return_value)-1);
    end if;

    return return_value;
end;

function get_emea_optin(individual_id in varchar2)
return varchar2
is
    return_value varchar2(10) := '';
    sql_query    varchar2(4000) := 'SELECT individual_id from kcierpisz.emea_optins_me where rownum = 1 and individual_id = ' || individual_id;
begin
    return_value := get_value_from_query(sql_query);
    if return_value is not null then
        return_value := 'opt-in';
    end if;
    return return_value;
end;


PROCEDURE SPAM_CHECK(individual_id in integer,
                     email_address in varchar2,
                     first_name in varchar2 default NULL,
                     last_name in varchar2 default NULL,
                     country_id in integer default NULL,
                     table_name in varchar2 default spam_results_table)
IS

    type cur_ref is ref cursor;
    curs cur_ref;

    ind_id          integer := 0;
    email           varchar2(1000) := '';
    first_n         varchar2(1000) := '';
    last_n          varchar2(1000) := '';
    country         integer := 0;
    
    email_suppr     varchar2(30) := '';
    email_optoutt   varchar2(30) := '';
    gcd_corresp     varchar2(30) := '';
    indiv_serv      varchar2(30) := '';
    emea_optin      varchar2(30) := '';

    create_sql      varchar2(1000) := '';
    insert_sql      varchar2(1000) := '';
    
    sql_query varchar2(3000) := 'select individual_id, email_address, first_name, last_name, country_id from gcd_dw.gcd_individuals';
BEGIN


    create_sql := 'create table ' || table_name ||
        '(individual_id integer,
          email_address varchar2(300),
          first_name    varchar2(100),
          last_name     varchar2(100),
          country       integer,
          email_suppression varchar2(30),
          email_optout      varchar2(30),
          correspondence_det varchar2(30),
          individual_services varchar2(30),
          emea_optins       varchar2(30))';

    begin
        execute immediate create_sql;
    exception when others then
        dbms_output.put_line(table_name || ' cannot be created');
        raise;
    end;

    if individual_id is not NULL then
        sql_query := sql_query || ' where individual_id = ' || individual_id; -- || ';';
    else
        if email_address is not null then
            sql_query := sql_query || ' where email_address = ''' || upper(trim(replace(email_address,'''',''''''))) || '''';
            if first_name is not null then
                sql_query := sql_query || ' and upper(first_name) = ''' || upper(trim(replace(first_name,'''',''''''))) || '''';
            end if;
            if last_name is not null then
                sql_query := sql_query || ' and upper(last_name) = ''' || upper(trim(replace(last_name,'''',''''''))) || '''';
            end if;
            if country_id is not null then
                sql_query := sql_query || ' and country_id = ' || country_id;
            end if;
  --          sql_query := sql_query || ';';
        else
            if first_name is not null and last_name is not null then
                sql_query := sql_query || ' where upper(first_name) = ''' || upper(replace(first_name,'''','''''')) || '''' || chr(10) || CHR(13);
                sql_query := sql_query || ' and upper(last_name) = ''' || upper(replace(last_name,'''','''''')) || '''' || chr(10) || CHR(13);
                if country_id is not null then
                    sql_query := sql_query || ' and country_id = ' || country_id || chr(10) || CHR(13);
                end if;
            else
                sql_query := '';
            end if;
    --        sql_query := sql_query || ';';
        end if;
    end if;

    dbms_output.put_line('sql_query: ' || sql_query);

    dbms_output.put_line(' individual_id |        email_address        |       first_n       |       last_n        |country|email_suppr|email_optout|correspondence|newsletter|emea_optin|' || chr(10));

  begin
    open curs for sql_query;
    fetch curs into ind_id,email,first_n,last_n,country;
    loop
        exit when curs%notfound;
        
        email_suppr := email_suppression(email);
        email_optoutt := email_optout(ind_id);
        gcd_corresp := get_correspondence_details(ind_id);
        indiv_serv  := get_individual_services(ind_id);
        emea_optin  := get_emea_optin(ind_id);

        dbms_output.put_line(
        rpad(nvl(to_char(ind_id),' '),   length('               '),' ') || '|' ||
        rpad(nvl(to_char(email),' '),    length('       email_address         '),' ') || '|' ||
        rpad(nvl(to_char(first_n),' '),  length('                     '),' ') || '|' ||
        rpad(nvl(to_char(last_n),' '),   length('                     '),' ') || '|' ||
        rpad(nvl(to_char(country),' '),  length('       '),' ') || '|' ||
        rpad(nvl(to_char(email_suppr),' '),  length('           '),' ')|| '|'  ||
        rpad(nvl(to_char(email_optoutt),' '),length('            '),' ') || '|' ||
        rpad(nvl(to_char(gcd_corresp),' '),  length('              '),' ') || '|' ||
        rpad(nvl(to_char(indiv_serv),' '),   length('          '),' ') || '|' ||
        rpad(nvl(to_char(emea_optin),' '),   length('          '),' ') || '|'
        );


        insert_sql := 'insert into ' || table_name || ' values (' ||
            ind_id || 
            ',''' || replace(email,'''','''''')  || '''' ||
            ',''' || replace(first_n,'''','''''')  || '''' ||
            ',''' || replace(last_n,'''','''''')  || '''' ||
            ',' || country  ||
            ',''' || email_suppr  || '''' ||
            ',''' || email_optoutt  || '''' ||
            ',''' || gcd_corresp  || '''' ||
            ',''' || indiv_serv  || '''' ||
            ',''' || emea_optin  || ''')';

        begin
            execute immediate insert_sql;
          exception when others then
            dbms_output.put_line('error with: ' || insert_sql);
            raise;
        end;
        
        --gcd_corresp || ' ' || indiv_serv || ' ' || emea_optin || chr(10));
        fetch curs into ind_id,email,first_n,last_n,country;
    end loop;
    close curs;

   exception when others then
        dbms_output.put_line('your sql does not return values: ' || sql_query);
   end;
    
        execute immediate 'commit';

END;

function colorify_info(
    info varchar2)
    return varchar2 is
    result varchar2(4000);
begin
    if instr(info,'EXCEPTION') > 0 then
        result := '<FONT color="red">' || replace(info,'EXCEPTION','<i><b>EXCEPTION</b></i>') || '</FONT>';
    else
        if instr(info,' 0 rows') > 0 then
            result := '<FONT color="blue">' || replace(info, ' 0','<i><b> 0</b></i>') || '</FONT>';
        else
            result := info;
        end if;
    end if;
    return result;
end;


PROCEDURE SEND_EMAIL_ABOUT_JUPITER IS

    conn utl_smtp.connection;
    mailFROM    VARCHAR2(64);
    mailTO      VARCHAR2(64);

    mailDATE    VARCHAR2(20);
    table_info  varchar2(2000);
    email_body1  varchar2(4000);
    email_body2  varchar2(4000);
    subj        varchar2(1000);

BEGIN
    mailFROM := 'krzysztof.cierpisz@oracle.com';
    mailTO   := 'krzysztof.cierpisz@oracle.com';

    SELECT TO_CHAR(SYSDATE + 9/24,'MM/DD/YYYY HH24:MI:SS') INTO mailDATE FROM dual;

    email_body1 := 'Tables info created on: <b>' || mailDATE || ' CET</b>' || '<BR>';
    email_body2 := '';

    select get_details('gcd_dw_data.list_build_individuals1') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw_data.list_build_organizations1') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';

    select get_details('dm_metrics.email_suppression') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';
    select get_details('dm_metrics.email_optout') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw_data.gcd_individual_services') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw_data.gcd_correspondence_details') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';

    select get_details('gcd_dw_data.gcd_products') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw_data.gcd_orgs_products_vw') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw_data.gcd_tar_summary') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw_data.gcd_order_entry') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';

    select get_details('gcd_dw_data.GCD_IND_POSTAL_ADDRESSES') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw.GCD_ORG_POSTAL_ADDRESSES') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw_data.GCD_activities') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw_data.GCD_activity_types') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('gsrt.gsrt_prod_hierarchy_staging') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('gsrt.gsrt_ref') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('prods_emea') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('prods_emea_gsrt') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('prods_emea_flags') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('emea_optins') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';

    --- REMOTE
    --select get_details('chross_de.emea_optins@dwprd.us.oracle.com') into table_info from dual;
    --email_body2 := chrispack.colorify_info(table_info) || '<BR>';
    --select get_details('chross_de.emea_inds@dwprd.us.oracle.com') into table_info from dual;
    --email_body2 := email_body2 || chrispack.colorify_info(table_info) || '<BR>';


    select get_details('gcd_dw.GCD_ORGS_PRODUCTS_VW@dwprd.us.oracle.com') into table_info from dual;
    email_body2 := email_body2 || chrispack.colorify_info(table_info) || '<BR>';

    -- DETAILED
    select get_info('prods_emea') into table_info from dual;
    email_body2 := email_body2 || chrispack.colorify_info(table_info) || '<BR>';
    select get_info('prods_emea_gsrt') into table_info from dual;
    email_body2 := email_body2 || chrispack.colorify_info(table_info) || '<BR>';
    select get_info('prods_emea_flags') into table_info from dual;
    email_body2 := email_body2 || chrispack.colorify_info(table_info) || '<BR>';
    select get_info('emea_optins') into table_info from dual;
    email_body2 := email_body2 || chrispack.colorify_info(table_info) || '<BR>';


    subj := 'JUPITER INFO about existing tables local and remote';
    if instr(email_body1,'red') > 0 then
        subj := replace(subj, 'local','local (EXCEPTIONS) ');
    end if;
    if instr(email_body1,'blue') > 0 then
        subj := replace(subj, 'local','local (0 rows) ');
    end if;
        if instr(email_body2,'red') > 0 then
        subj := replace(subj, 'remote','remote (EXCEPTIONS) ');
    end if;
    if instr(email_body2,'blue') > 0 then
        subj := replace(subj, 'remote','remote (0 rows) ');
    end if;


    conn := demo_mail.begin_mail(
        sender     => mailFROM,
        recipients => mailTO,
        subject    => subj,
        mime_type  => 'text/html');

    demo_mail.write_text(
        conn    => conn,
        message => email_body1 || chr(13) || '<BR>REMOTE:<BR>' || CHR(13) || email_body2);

    demo_mail.end_mail( conn => conn );
END; -- Procedure

PROCEDURE SEND_EMAIL_ABOUT_MKTGCD IS

    conn utl_smtp.connection;
    mailFROM    VARCHAR2(64);
    mailTO      VARCHAR2(64);

    mailDATE    VARCHAR2(20);
    table_info  varchar2(2000);
    email_body1  varchar2(4000);
    email_body2  varchar2(4000);
    subj        varchar2(1000);

BEGIN
    mailFROM := 'krzysztof.cierpisz@oracle.com';
    mailTO   := 'krzysztof.cierpisz@oracle.com';

    SELECT TO_CHAR(SYSDATE + 9/24,'MM/DD/YYYY HH24:MI:SS') INTO mailDATE FROM dual;


    email_body1 := 'Tables info created on: <b>' || mailDATE || ' CET</b>' || '<BR>';
    email_body2 := '';

    select get_details('gcd_dw.list_build_individuals2') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw.list_build_organizations1') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';

    select get_details('dm_metrics.email_suppression') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';
    select get_details('dm_metrics.email_optout') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw.gcd_individual_services') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw.gcd_correspondence_details') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';

    select get_details('gcd_dw.gcd_products') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw.gcd_orgs_products_vw') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw.gcd_tar_summary') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw.gcd_order_entry') into table_info from dual;
    email_body1 := email_body1 || colorify_info(table_info) || '<BR>';

    select get_details('gcd_dw.GCD_IND_POSTAL_ADDRESSES') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw.GCD_ORG_POSTAL_ADDRESSES') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw.GCD_activities') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('gcd_dw.GCD_activity_types') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('prods_emea_flags') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('emea_optins_me') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';
    select get_details('emea_optins_prfl') into table_info from dual;
    email_body1 := email_body1 || chrispack.colorify_info(table_info) || '<BR>';

    --- REMOTE
    --select get_details('chross_de.emea_optins@dwprd.us.oracle.com') into table_info from dual;
    --email_body2 := chrispack.colorify_info(table_info) || '<BR>';
    --select get_details('chross_de.emea_inds@dwprd.us.oracle.com') into table_info from dual;
    --email_body2 := email_body2 || chrispack.colorify_info(table_info) || '<BR>';


    select get_details('gcd_dw.GCD_ORGS_PRODUCTS_VW@dwprd.us.oracle.com') into table_info from dual;
    email_body2 := email_body2 || chrispack.colorify_info(table_info) || '<BR>';

    -- DETAILED
    select get_info('prods_emea_flags') into table_info from dual;
    email_body2 := email_body2 || chrispack.colorify_info(table_info) || '<BR>';
    select get_info('emea_optins_me') into table_info from dual;
    email_body2 := email_body2 || chrispack.colorify_info(table_info) || '<BR>';
    select get_info('emea_optins_prfl') into table_info from dual;
    email_body2 := email_body2 || chrispack.colorify_info(table_info) || '<BR>';



    subj := 'MKTGCD INFO about existing tables local and remote';
    conn := demo_mail.begin_mail(
        sender     => mailFROM,
        recipients => mailTO,
        subject    => subj,
        mime_type  => 'text/html');

    demo_mail.write_text(
        conn    => conn,
        message => email_body1 || chr(13) || '<BR>REMOTE:<BR>' || CHR(13) || email_body2);

    demo_mail.end_mail( conn => conn );
exception when others then
    raise;
END; -- Procedure


FUNCTION test_fun
  ( c IN varchar2)
  RETURN  varchar2 IS

    country varchar2(200) := '';

BEGIN
    country := 'some output';
    RETURN country;
END;

procedure test_proc
is
    sqlstmt varchar2(100) := '';
begin
    sqlstmt := 'create TABLE test_a (id number)';
    execute immediate sqlstmt;
end;

FUNCTION COL2ROW
  ( param IN varchar2)
  RETURN  varchar2 IS

    type cur_ref is ref cursor;
    cur cur_ref;
    lv_stmt     varchar2(4000) := param;

--     result col1.id%type;
     result varchar2(4000);
--     result2 number;

     return_val varchar2(4000);
BEGIN
    return_val := '';
    open cur for lv_stmt;
    fetch cur into result;
    loop
        exit when cur%notfound or length(return_val || result) >= 3998;
        return_val := return_val || result || ',';
        fetch cur into result;
    end loop;
    close cur;


    RETURN trim(substr(return_val,1,length(return_val)-1));
--return param;
--EXCEPTION
--   WHEN exception_name THEN
--       statements ;
END;

FUNCTION utreplace (string in varchar, tokenList in varchar) RETURN varchar
AS LANGUAGE JAVA
NAME 'Utils.remove (java.lang.String, java.lang.String) return java.lang.String';

FUNCTION eraseAlph (string in varchar) RETURN varchar
AS LANGUAGE JAVA
NAME 'Erase.eraseAlph (java.lang.String) return java.lang.String';

FUNCTION eraseNo (string in varchar) RETURN varchar
AS LANGUAGE JAVA
NAME 'Erase.eraseNo (java.lang.String) return java.lang.String';

FUNCTION TABLE_EXISTS
  ( tableName IN varchar2)
  RETURN  varchar2 IS

   exists_con varchar2(5) := 'FALSE';
   tableNameTmp varchar2(30) := '';

   cursor tables_search(tableName varchar2) is
    select table_name from user_tables
    where table_name = upper(tableName);

BEGIN

    exists_con := 'FALSE';

    open tables_search(tableName);
    loop
       exit when tables_search%notfound;
       fetch tables_search into tableNameTmp;
       if tableNameTmp = upper(tableName) then
            exists_con := 'TRUE';
       end if;
    end loop;
    close tables_search;

return exists_con;

END;

FUNCTION GETCOUNTRY
  ( countryID IN varchar2)
  RETURN  varchar2 IS

    country varchar2(200) := '';
CURSOR countries(countryID varchar2) IS
    select name from gcd_dw.gcd_countries
    where country_id = countryID;

BEGIN
    open countries(countryID);
    --loop
        fetch countries into country;
    --    exit when countries%NOTFOUND;
    --end loop;
    close countries;
    RETURN trim(country);
END;

FUNCTION country_lov_2
  ( countryID IN varchar2)
  RETURN  varchar2 IS

    country varchar2(200) := '';
CURSOR countries(countryName varchar2) IS
    select iso_2_char_code from gcd_DW.gcd_countries
--    where upper(name) = upper(countryName);
    where country_id = countryID;

BEGIN
    open countries(countryID);
    --loop
        fetch countries into country;
    --    exit when countries%NOTFOUND;
    --end loop;
    close countries;
    RETURN trim(country);
END;

FUNCTION urlCorrect(url varchar2)
return varchar2
is
  tmp varchar2(500) := url;
  tmp1 integer default 0;
BEGIN
  tmp1 := instr(tmp,'#');
  if tmp1 > 0 then
    tmp := substr(tmp,1,instr(tmp,'#')-1);
  end if;
  return tmp;
END urlcorrect;

FUNCTION number_of_english_chars(txt in varchar2, min_reach in number default 4000)
return number
is
    txt_upper           varchar2(4000) := upper(trim(txt));
    txt_upper_length    number := length(txt_upper);
    c                   varchar2(10);
    counter             number := 1;
    result              number := 0;
begin
    --dbms_output.put_line('txt_upper;');
    --dbms_output.put_line(txt_upper);
    --dbms_output.put_line(txt_upper_length);
    if txt_upper is null or txt_upper_length < 1 then
        return 0;
    end if;
    for counter in 0..txt_upper_length-1 loop
        c := substr(txt_upper,counter+1,1);
        if c between 'A' and 'Z' then
            result := result + 1;
        end if;
        if result >= min_reach then
            return result;
        end if;
    end loop;
    
    return result;
end;

FUNCTION only_consonants(txt in varchar2)
return number
is
    txt_upper       varchar2(4000) := upper(txt);
    int_length      number := length(txt);
    c               varchar2(10);
    counter         number := 1;
begin

    if txt_upper is null then
     return 0;
    end if;

    for counter in 1..int_length loop
        c := substr(txt_upper,counter,1);
        if c = 'A' or c = 'E' or c = 'O' or c = 'I' or c = 'U' or c = 'Y' then
            return 0;
        end if;
    end loop;

    return 1;
end;

FUNCTION get_only_consonants(txt in varchar2)
return varchar2
is
    txt_upper       varchar2(4000) := upper(txt);
    int_length      number := length(txt);
    c               varchar2(10);
    counter         number := 1;
    result          varchar2(4000) := '';
begin

    if txt_upper is null then
     return 0;
    end if;

    for counter in 1..int_length loop
        c := substr(txt_upper,counter,1);
        if c = 'A' or c = 'E' or c = 'O' or c = 'I' or c = 'U' or c = 'Y' then
            goto NEXTT;
        else
            result := result || c;
        end if;
        <<NEXTT>> null;
    end loop;

    return result;
end;

FUNCTION ONLY_VOCALS(txt in varchar2)
return number

is
    txt_v varchar2(4000) := upper(txt);
    pos_int number := 1;
    length_int number := length(txt);
    letter_c varchar2(4000);
    switch_n number := 1;
begin
    if txt_v is null then
     return 0;
    end if;
    WHILE (pos_int <= length_int)
    LOOP
        letter_c := substr(txt_v,pos_int,1);
        if (ascii(letter_c) not between 66 and 68
            and ascii(letter_c) not between 70 and 72
            and ascii(letter_c) not between 74 and 78
            and ascii(letter_c) not between 80 and 84
            and ascii(letter_c) not between 86 and 90) then
            switch_n := 0;
            dbms_output.put_line(letter_c||' '||TO_CHAR(ascii(letter_c)));
        end if;

        --dbms_output.put_line(letter_c);
        pos_int := pos_int+1;
    END LOOP;
    return switch_n;
end;

 FUNCTION ROWTOCOL( p_slct IN VARCHAR2,
                    p_dlmtr IN VARCHAR2 DEFAULT ',' ) RETURN VARCHAR2
 is
   err_num NUMBER;
   err_msg VARCHAR2(100);

 TYPE c_refcur IS REF CURSOR;
     lc_str    VARCHAR2(4000);
     lc_colval VARCHAR2(4000);
     c_dummy   c_refcur;
     l         number;

 BEGIN
    OPEN c_dummy FOR p_slct;
    LOOP
     FETCH c_dummy INTO lc_colval;
     EXIT WHEN c_dummy%NOTFOUND or (lengthb(lc_colval) + lengthb(lc_str)) >= 3998;
         lc_str := lc_str || p_dlmtr || lc_colval;
    END LOOP;
    CLOSE c_dummy;    RETURN SUBSTR(lc_str,2);
 exception when others then
                 err_msg := SUBSTR(SQLERRM, 1, 100);
    dbms_output.put_line('err: ' || err_msg);
    dbms_output.put_line('lc_colval: ' || lc_colval);
    dbms_output.put_line('lc_str: ' || lc_str);
    dbms_output.put_line('lc_str: ' || length(lc_str));
    raise;
 END;

/*
FUNCTION repl
	(string_in varchar2, pattern1 varchar2, pattern2 varchar2,
	  pattern3 varchar2, pattern4 varchar2, pattern5 varchar2,
      pattern6 varchar2)
RETURN varchar2
IS
tmp char(100);
--declare
--  variable result number;
BEGIN
  tmp := replace(upper(string_in),pattern1);
  tmp := replace(tmp,pattern2);
  tmp := replace(tmp,pattern3);
  tmp := replace(tmp,pattern4);
  tmp := replace(tmp,pattern5);
  tmp := replace(tmp,pattern6);
 RETURN trim(tmp);
END repl;

FUNCTION match_amb(name1 in varchar2, name2 in varchar2)
RETURN varchar2
is
*/
/*
	Polish ....
	checks if the name1 is a part of the name2
	additionaly in the first name all the '¿'
	are replaced with the '_'
	e.g.
    match_amb('ab¿d','cabcdef') returns 'TRUE'
*/
/*

p_name1   varchar2(500) := upper(name1);
p_name2   varchar2(500) := upper(name2);
tmp1	  varchar2(500) DEFAULT NULL;
tmp2	  varchar2(500) DEFAULT NULL;
tmp1len	  integer default 0;
tmp2len	  integer default 0;
small	  varchar2(500);
big	      varchar2(500);


BEGIN
    p_name1:=replace(upper(p_name1),'SP. Z O.O.');
	p_name1:=replace(upper(p_name1),'S.A.');
	p_name2:=replace(upper(p_name2),'SP. Z O.O.');
	p_name2:=replace(upper(p_name2),'S.A.');
	tmp1:=replace(trim(p_name1),chr(191),'_');
	tmp2:=replace(trim(p_name2),chr(191),'_');
	tmp1len:=length(tmp1);
	tmp2len:=length(tmp2);
		
	-- which one is shorter
	--in elsif check if shorter is in the longer one

    IF tmp1len <= tmp2len THEN
		small := '%' || tmp1 || '%';
		big   := '%' || tmp2 || '%';
	ELSE
		small := '%' || tmp2 || '%';
		big   := '%' || tmp1 || '%';
	END IF;
*/
    /* PARTY_NAME must be at least 4 chars long */
/*    IF (length(big) < 6) OR (length(small) < 6) THEN RETURN 'FALSE';
	END IF;

	IF (big LIKE small) OR (small LIKE big) THEN RETURN 'TRUE';
	ELSE RETURN small|| ' '||big||' FALSE';
	END IF;
	
END match_amb;

FUNCTION match_amb_it(name1 in varchar2, name2 in varchar2)
RETURN varchar2
is
*/
/*
	Polish ....
	checks if the name1 is a part of the name2
	additionaly in the first name all the '¿'
	are replaced with the '_'
	e.g.
    match_amb('ab¿d','cabcdef') returns 'TRUE'
*/

/*
p_name1   varchar2(500) := upper(name1);
p_name2   varchar2(500) := upper(name2);
tmp1	  varchar2(500) DEFAULT NULL;
tmp2	  varchar2(500) DEFAULT NULL;
tmp1len	  integer default 0;
tmp2len	  integer default 0;
small	  varchar2(500);
big	      varchar2(500);


BEGIN
    p_name1:=replace(upper(p_name1),'SPA');
	p_name1:=replace(upper(p_name1),'S.P.A.');
	p_name1:=replace(upper(p_name1),'ARL');
	p_name1:=replace(upper(p_name1),'A.R.L.');
    p_name1:=replace(upper(p_name1),'A. R. L.');
	p_name1:=replace(upper(p_name1),'A R.L.');
	p_name1:=replace(upper(p_name1),'SRL');
    p_name2:=replace(upper(p_name2),'SPA');
	p_name2:=replace(upper(p_name2),'S.P.A.');
	p_name2:=replace(upper(p_name2),'ARL');
	p_name2:=replace(upper(p_name2),'A.R.L.');
    p_name2:=replace(upper(p_name2),'A. R. L.');
	p_name2:=replace(upper(p_name2),'A R.L.');
	p_name2:=replace(upper(p_name2),'SRL');

	tmp1:=p_name1;
	tmp2:=p_name2;
	tmp1len:=length(tmp1);
	tmp2len:=length(tmp2);

    IF tmp1len <= tmp2len THEN
		small := tmp1 || '%';
		big   := tmp2 || '%';
	ELSE
		small := tmp2 || '%';
		big   := tmp1 || '%';
	END IF;
*/
    /* PARTY_NAME must be at least 4 chars long */
/*    IF (length(big) < 6) OR (length(small) < 6) THEN RETURN 'FALSE';
	END IF;

	IF (big LIKE small) OR (small LIKE big) THEN RETURN 'TRUE';
	ELSE RETURN small|| ' '||big||' FALSE';
	END IF;
	
END match_amb_it;
*/

PROCEDURE PROC_PRODS_EMEA
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   sqlstmt varchar2(32767) := '';
   sqlstmt2 varchar2(32767) := '';
   sqlstmt3 varchar2(32767) := '';
BEGIN
    insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'PROC_PRODS_EMEA', sysdate,'START');
    commit;

    sqlstmt := '
create table prods_emea2 as
select b.country_id, b.org_id, max(b.duns_number) as duns_number,
max(case when a.prod_tier2 = ''0YF0'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_DB_INST,
max(case when a.prod_tier2 = ''0YF0'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DB_INST,
max(case when a.prod_tier2 = ''0YF0'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as DB_INST,
max(case when a.prod_tier6 = ''0Z10'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_DB_EE,
max(case when a.prod_tier6 = ''0Z10'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DB_EE,
max(case when a.prod_tier6 = ''0Z10'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as DB_EE,
max(case when a.prod_tier6 = ''0Z58'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_DB_SE,
max(case when a.prod_tier6 = ''0Z58'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DB_SE,
max(case when a.prod_tier6 = ''0Z58'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as DB_SE,
max(case when a.prod_tier6 = ''0ZW3'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_DB_SEO,
max(case when a.prod_tier6 = ''0ZW3'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DB_SEO,
max(case when a.prod_tier6 = ''0ZW3'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as DB_SEO,
max(case when a.prod_tier5 = ''0YM9'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_DB_ENTERPRISE_MGMT,
max(case when a.prod_tier5 = ''0YM9'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DB_ENTERPRISE_MGMT,
max(case when a.prod_tier5 = ''0YM9'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as DB_ENTERPRISE_MGMT,
max(case when a.prod_tier2 = ''0YY1'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_APPLICATIONS,
max(case when a.prod_tier2 = ''0YY1'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as APPLICATIONS, -- added
max(case when a.prod_tier2 = ''0YY1'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_APPLICATIONS,

max(case when a.prod_tier5 = ''0Z1A'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_timesten,
max(case when a.prod_tier5 = ''0Z1A'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_timesten,
max(case when a.prod_tier5 = ''0Z1A'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as timesten,


max(case when a.prod_tier5 = ''0DM1'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_demantra,
max(case when a.prod_tier5 = ''0DM1'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_demantra,
max(case when a.prod_tier5 = ''0DM1'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as demantra,

max(case when a.prod_tier4 = ''0DM2'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_ORACLE_CRM_APPL,
max(case when a.prod_tier4 = ''0DM2'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ORACLE_CRM_APPL,
max(case when a.prod_tier4 = ''0DM2'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as ORACLE_CRM_APPL,
max(case when a.prod_tier4 = ''0DM3'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_PSFT_CRM_APPL,
max(case when a.prod_tier4 = ''0DM3'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_PSFT_CRM_APPL,
max(case when a.prod_tier4 = ''0DM3'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as PSFT_CRM_APPL,
max(case when a.prod_tier4 = ''0GW1'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_SIEBEL_CRM_APPL,
max(case when a.prod_tier4 = ''0GW1'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_SIEBEL_CRM_APPL,
max(case when a.prod_tier4 = ''0GW1'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as SIEBEL_CRM_APPL,
max(case when a.prod_tier5 = ''0Y26'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_APPLIC_SERVERS,
max(case when a.prod_tier5 = ''0Y26'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_APPLIC_SERVERS,
max(case when a.prod_tier5 = ''0Y26'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as APPLIC_SERVERS,
max(case when a.prod_tier6 = ''0ZB8'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_IAS_EE,
max(case when a.prod_tier6 = ''0ZB8'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_IAS_EE,
max(case when a.prod_tier6 = ''0ZB8'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as IAS_EE,
max(case when a.prod_tier6 = ''0ZC4'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_IAS_SE,
max(case when a.prod_tier6 = ''0ZC4'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_IAS_SE,
max(case when a.prod_tier6 = ''0ZC4'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as IAS_SE,
max(case when a.prod_tier6 = ''0ZJ8'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_IAS_Jave_Edit,
max(case when a.prod_tier6 = ''0ZJ8'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_IAS_Java_Edit,
max(case when a.prod_tier6 = ''0ZJ8'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as IAS_Java_Edit,
max(case when a.prod_tier6 = ''0ZX2'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_BPEL_PROCESS_MGR,
max(case when a.prod_tier6 = ''0ZX2'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_BPEL_PROCESS_MGR,
max(case when a.prod_tier6 = ''0ZX2'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as BPEL_PROCESS_MGR,
max(case when a.prod_tier6 = ''0YO6'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_AS_OTHER,
max(case when a.prod_tier6 = ''0YO6'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_AS_OTHER,
max(case when a.prod_tier6 = ''0YO6'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as AS_OTHER,
max(case when a.prod_tier6 = ''0ZX5'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_PORTAL,
max(case when a.prod_tier6 = ''0ZX5'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_PORTAL,
max(case when a.prod_tier6 = ''0ZX5'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as PORTAL,
max(case when a.prod_tier6 = ''0Z3B'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_IAS_SEO,
max(case when a.prod_tier6 = ''0Z3B'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_IAS_SEO,
max(case when a.prod_tier6 = ''0Z3B'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as IAS_SEO,
max(case when a.prod_tier6 = ''0ZUH'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_PROV_PACK,
max(case when a.prod_tier6 = ''0ZUH'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_PROV_PACK,
max(case when a.prod_tier6 = ''0ZUH'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as PROV_PACK,
max(case when a.prod_tier6 = ''0ZS5'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_CHA_MGMT_PACK,
max(case when a.prod_tier6 = ''0ZS5'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_CHA_MGMT_PACK,
max(case when a.prod_tier6 = ''0ZS5'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as CHA_MGMT_PACK,
max(case when a.prod_tier6 = ''0ZA6'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_ADV_SECURITY,
max(case when a.prod_tier6 = ''0ZA6'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ADV_SECURITY,
max(case when a.prod_tier6 = ''0ZA6'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as ADV_SECURITY,
max(case when a.prod_tier6 = ''0ZS1'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_LABEL_SECURITY,
max(case when a.prod_tier6 = ''0ZS1'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_LABEL_SECURITY,
max(case when a.prod_tier6 = ''0ZS1'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as LABEL_SECURITY,
max(case when a.prod_tier6 = ''0FH9'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_ORA_SECUR,
max(case when a.prod_tier6 = ''0FH9'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ORA_SECUR,
max(case when a.prod_tier6 = ''0FH9'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as ORA_SECUR,
max(case when a.prod_tier6 = ''0EC2'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_ORA_SEC_BK,
max(case when a.prod_tier6 = ''0EC2'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ORA_SEC_BK,
max(case when a.prod_tier6 = ''0EC2'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as ORA_SEC_BK,
max(case when a.prod_tier6 = ''0ZG3'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_PARTITIONING,
max(case when a.prod_tier6 = ''0ZG3'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_PARTITIONING,
max(case when a.prod_tier6 = ''0ZG3'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as PARTITIONING,
max(case when a.prod_tier6 = ''0ZS2'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_SPATIAL,
max(case when a.prod_tier6 = ''0ZS2'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_SPATIAL,
max(case when a.prod_tier6 = ''0ZS2'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as SPATIAL,
max(case when a.prod_tier6 = ''0ZH6'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_DATA_MINING,
max(case when a.prod_tier6 = ''0ZH6'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DATA_MINING,
max(case when a.prod_tier6 = ''0ZH6'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as DATA_MINING,
max(case when a.prod_tier5 = ''0YH1'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_EBS,
max(case when a.prod_tier5 = ''0YH1'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_EBS,
max(case when a.prod_tier5 = ''0YH1'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as EBS,
max(case when a.prod_tier5 = ''0YO1'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_EBS_SPECIAL_EDIT,
max(case when a.prod_tier5 = ''0YO1'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_EBS_SPECIAL_EDIT,
max(case when a.prod_tier5 = ''0YO1'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as EBS_SPECIAL_EDIT,
max(case when a.prod_tier5 = ''0MA4'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_TOOLS_INST,
max(case when a.prod_tier5 = ''0MA4'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_TOOLS_INST,
max(case when a.prod_tier5 = ''0MA4'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as TOOLS_INST,
max(case when a.prod_tier6 = ''0Z07'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_IDS,
max(case when a.prod_tier6 = ''0Z07'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_IDS,
max(case when a.prod_tier6 = ''0Z07'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as IDS,';

sqlstmt2 := '
max(case when a.prod_tier5 = ''0YC5'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_HUMAN_RES,
max(case when a.prod_tier5 = ''0YC5'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_HUMAN_RES,
max(case when a.prod_tier5 = ''0YC5'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as HUMAN_RES,
max(case when a.prod_tier4 = ''0KR7'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_BI_TIER4,
max(case when a.prod_tier4 = ''0KR7'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_BI_TIER4,
max(case when a.prod_tier4 = ''0KR7'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as BI_TIER4,
max(case when a.prod_tier6 = ''0ZY7'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_BI_SE,
max(case when a.prod_tier6 = ''0ZY7'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_BI_SE,
max(case when a.prod_tier6 = ''0ZY7'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as BI_SE,
max(case when a.prod_tier5 = ''0EB7'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_BI_TECH,
max(case when a.prod_tier5 = ''0EB7'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_BI_TECH,
max(case when a.prod_tier5 = ''0EB7'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as BI_TECH,
max(case when a.prod_tier6 = ''0ZK3'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_E_BI,
max(case when a.prod_tier6 = ''0ZK3'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_E_BI,
max(case when a.prod_tier6 = ''0ZK3'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as E_BI,
max(case when a.prod_tier4 = ''0YI6'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_COLLABORATION,
max(case when a.prod_tier4 = ''0YI6'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_COLLABORATION,
max(case when a.prod_tier4 = ''0YI6'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as COLLABORATION,
max(case when a.prod_tier6 = ''0ZI8'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_COLLABORATION_SUITE,
max(case when a.prod_tier6 = ''0ZI8'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_COLLABORATION_SUITE,
max(case when a.prod_tier6 = ''0ZI8'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as COLLABORATION_SUITE,
max(case when a.prod_tier6 = ''0Z1D'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_RECORDS_MGMT,
max(case when a.prod_tier6 = ''0Z1D'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_RECORDS_MGMT,
max(case when a.prod_tier6 = ''0Z1D'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as RECORDS_MGMT,
max(case when a.prod_tier6 = ''0ZJ1'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_CONTENT_SERIVCES,
max(case when a.prod_tier6 = ''0ZJ1'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_CONTENT_SERIVCES,
max(case when a.prod_tier6 = ''0ZJ1'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as CONTENT_SERIVCES,
max(case when a.prod_brand = ''JD-EDWARDS'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_JDE,
max(case when a.prod_tier5 = ''0YC1'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_MANUFACTURING,
max(case when a.prod_tier5 = ''0YC1'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_MANUFACTURING,
max(case when a.prod_tier5 = ''0YC1'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as MANUFACTURING,
max(case when a.prod_tier5 = ''0YB8'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_ORDER_MGMT,
max(case when a.prod_tier5 = ''0YB8'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ORDER_MGMT,
max(case when a.prod_tier5 = ''0YB8'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as ORDER_MGMT,
max(case when a.prod_tier5 = ''0YC2'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_FINANCIALS,
max(case when a.prod_tier5 = ''0YC2'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_FINANCIALS,
max(case when a.prod_tier5 = ''0YC2'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as FINANCIALS,
max(case when a.prod_tier5 = ''0YC4'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_PROCUREMENT,
max(case when a.prod_tier5 = ''0YC4'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_PROCUREMENT,
max(case when a.prod_tier5 = ''0YC4'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as PROCUREMENT,
max(case when a.prod_tier5 = ''0YV9'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_SCM, -- SUPPLY_CHAIN_MANAGEMENT
max(case when a.prod_tier5 = ''0YV9'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_SCM,
max(case when a.prod_tier5 = ''0YV9'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as SCM,
max(case when a.prod_tier5 = ''0WA5'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_PSFT_ENTERPRISE_ERP,
max(case when a.prod_tier5 = ''0WA5'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_PSFT_ENTERPRISE_ERP,
max(case when a.prod_tier5 = ''0WA5'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as PSFT_ENTERPRISE_ERP,

max(case when a.prod_tier5 = ''0WA9'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_JDE_WORLD_ERP,
max(case when a.prod_tier5 = ''0WA9'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_JDE_WORLD_ERP,
max(case when a.prod_tier5 = ''0WA9'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as JDE_WORLD_ERP,

max(case when a.prod_tier5 = ''0WA8'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_JDE_ENTERPRISEONE_ERP,
max(case when a.prod_tier5 = ''0WA8'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_JDE_ENTERPRISEONE_ERP,
max(case when a.prod_tier5 = ''0WA8'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as JDE_ENTERPRISEONE_ERP,
max(case when a.prod_tier5 = ''0SY3'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_SIEBEL_ANALYTICS,
max(case when a.prod_tier5 = ''0SY3'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_SIEBEL_ANALYTICS,
max(case when a.prod_tier5 = ''0SY3'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as SIEBEL_ANALYTICS,
max(case when a.prod_tier4 = ''0YV1'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_ACQUIR_RETAIL_APPL,
max(case when a.prod_tier4 = ''0YV1'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ACQUIR_RETAIL_APPL,
max(case when a.prod_tier4 = ''0YV1'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as ACQUIR_RETAIL_APPL,
max(case when a.prod_tier4 = ''0WD3'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_ORACLE_RETAIL_APPL,
max(case when a.prod_tier4 = ''0WD3'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ORACLE_RETAIL_APPL,
max(case when a.prod_tier4 = ''0WD3'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as ORACLE_RETAIL_APPL,
max(case when a.prod_tier4 = ''0YO2'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_ORACLE_VERTICAL_APPL,
max(case when a.prod_tier4 = ''0YO2'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ORACLE_VERTICAL_APPL,
max(case when a.prod_tier4 = ''0YO2'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as ORACLE_VERTICAL_APPL,

max(case when a.prod_tier3 = ''0YF3'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_MIDDLEWARE,
max(case when a.prod_tier3 = ''0YF3'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_MIDDLEWARE,
max(case when a.prod_tier3 = ''0YF3'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as MIDDLEWARE,
max(case when a.prod_tier4 = ''0ZV5'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_IDENTITY_MGMT,
max(case when a.prod_tier4 = ''0ZV5'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_IDENTITY_MGMT,
max(case when a.prod_tier4 = ''0ZV5'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as IDENTITY_MGMT,
max(case when a.prod_tier6 = ''0ZX8'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_FORMS_AND_REPORTS,
max(case when a.prod_tier6 = ''0ZX8'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_FORMS_AND_REPORTS,
max(case when a.prod_tier6 = ''0ZX8'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as FORMS_AND_REPORTS,
max(case when a.prod_tier6 = ''0Z80'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_OLAP,
max(case when a.prod_tier6 = ''0Z80'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_OLAP,
max(case when a.prod_tier6 = ''0Z80'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as OLAP,
max(case when a.prod_tier6 = ''0ZC3'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_DATA_WAREHOUSE,
max(case when a.prod_tier6 = ''0ZC3'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DATA_WAREHOUSE,
max(case when a.prod_tier6 = ''0ZC3'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as DATA_WAREHOUSE,
max(case when a.prod_tier6 = ''0ZG2'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_RAC,
max(case when a.prod_tier6 = ''0ZG2'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_RAC,
max(case when a.prod_tier6 = ''0ZG2'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as RAC,';

sqlstmt3 := '
max(case when a.prod_tier6 = ''0ZS3'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_TUNING_PACK,
max(case when a.prod_tier6 = ''0ZS3'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_TUNING_PACK,
max(case when a.prod_tier6 = ''0ZS3'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as TUNING_PACK,
max(case when a.prod_tier6 = ''0ZS4'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_DIAGNOSTICS_PACK,
max(case when a.prod_tier6 = ''0ZS4'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DIAGNOSTICS_PACK,
max(case when a.prod_tier6 = ''0ZS4'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as DIAGNOSTICS_PACK,
max(case when a.prod_tier5 = ''0EC1'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_SECURE_BACKUP,
max(case when a.prod_tier5 = ''0EC1'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_SECURE_BACKUP,
max(case when a.prod_tier5 = ''0EC1'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as SECURE_BACKUP,
max(case when a.prod_tier5 = ''0XY1'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_SECURE_ENTERP_SEARCH,
max(case when a.prod_tier5 = ''0XY1'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_SECURE_ENTERP_SEARCH,
max(case when a.prod_tier5 = ''0XY1'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as SECURE_ENTERP_SEARCH,
max(case when a.prod_tier5 = ''0YO4'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_LEARNING_MGMT,
max(case when a.prod_tier5 = ''0YO4'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_LEARNING_MGMT,
max(case when a.prod_tier5 = ''0YO4'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as LEARNING_MGMT,

max(case when a.prod_tier6 = ''0ZY6'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_EPB,
max(case when a.prod_tier6 = ''0ZY6'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_EPB,
max(case when a.prod_tier6 = ''0ZY6'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as EPB,

max(case when a.prod_tier6 = ''0ZL6'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_OFA,
max(case when a.prod_tier6 = ''0ZL6'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_OFA,
max(case when a.prod_tier6 = ''0ZL6'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as OFA,

max(case when a.prod_tier6 = ''0YS2'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_PSFT_FSM,
max(case when a.prod_tier6 = ''0YS2'' and to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60) then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_PSFT_FSM,
max(case when a.prod_tier6 = ''0YS2'' and nvl(o.order_date,sysdate - 100) > add_months(sysdate,-60) then o.order_date end) as PSFT_FSM,

max(case when a.prod_brand = ''IBM'' and a.prod_name in (''DB2'',''DBMS'') and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_DB2,
max(case when a.prod_brand = ''IBM'' and a.prod_name like ''%AS%400%'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_AS400,
max(case when a.prod_brand = ''MICROSOFT'' and a.prod_name = ''SQL-SERVER'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_MSSQL,
max(case when a.prod_BRAND = ''SYBASE'' AND a.PROD_NAME = ''SYBASE'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_SYBASE,
max(case when a.prod_BRAND = ''MYSQL-AB'' and a.PROD_NAME = ''MYSQL'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_MYSQL,
max(case when a.PROD_NAME = ''INGRES'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_INGRES,
max(case when a.prod_brand = ''SAP'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_SAP,
max(case when a.prod_brand = ''QAD'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_QAD,
max(case when a.prod_brand = ''MICROSOFT'' and a.created_date > add_months(sysdate,-60) then a.created_date end) as PROD_MICROSOFT,
max(case when a.prod_brand = ''BAAN'' and a.created_date > add_months(sysdate,-60) then a.created_date end)as PROD_BAAN
from gcd_dw.gcd_orgs_products_vw a, gcd_dw.list_build_organizations_eu b,
gcd_dw.gcd_tar_summary t, gcd_dw_data.gcd_order_entry o
where a.org_id = b.org_id
and a.prod_id = t.prod_id (+)
and a.org_id = t.ultimate_org_id (+)
and a.prod_id = o.prod_id (+)
and a.org_id = o.org_id (+)
and ( a.created_date > add_months(sysdate,-60)
or nvl(o.order_date, add_months(sysdate, -100)) > add_months(sysdate, -60)
or to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60)
     )
and (a.prod_tier2 = ''0YF0'' -- DB_INST
or a.prod_tier6 = ''0Z10'' -- DB_EE
or a.prod_tier6 = ''0Z58'' -- DB_SE
or a.prod_tier6 = ''0ZW3'' -- DB_SEO
or a.prod_tier5 = ''0YM9'' -- DB_ENTERPRISE_MGMT
or a.prod_tier5 = ''0DM1'' -- demantra
or a.prod_tier5 = ''0Z1A'' -- timesten
or a.prod_tier2 = ''0YY1'' -- APPLICATIONS
or a.prod_tier4 = ''0DM2'' -- ORACLE CRM
or a.prod_tier4 = ''0DM3'' -- PSFT_CRM
or a.prod_tier4 = ''0GW1'' -- SIEBEL_CRM
or a.prod_tier5 = ''0Y26'' -- AS
or a.prod_tier6 = ''0ZX8'' -- FORMS
or a.prod_tier6 = ''0Z3B'' -- IAS_SEO
or a.prod_tier6 = ''0ZB8'' -- IAS_EE
or a.prod_tier6 = ''0ZC4'' -- IAS_SE
or a.prod_tier6 = ''0ZJ8'' -- IAS_Java_edit
or a.prod_tier6 = ''0ZX2'' -- BPEL_PROCESS_MGR
or a.prod_tier6 = ''0YO6'' -- AS_OTHER
or a.prod_tier6 = ''0ZX5'' -- PORTAL
or a.prod_tier6 = ''0ZUH'' -- PROV_PACK
or a.prod_tier6 = ''0ZS5'' -- CHA_MGMT_PACK
or a.prod_tier6 = ''0ZA6'' -- ADVANCED_SECURITY
or a.prod_tier6 = ''0ZS1'' -- LABLE_SECURITY
or a.prod_tier6 = ''0FH9'' -- ORA_SECUR
or a.prod_tier6 = ''0EC2'' -- ORA_SEC_BK
or a.prod_tier6 = ''0ZG3'' -- ORA_PAR
or a.prod_tier6 = ''0ZS2'' -- SPATIAL
or a.prod_tier6 = ''0ZH6'' -- DATA MINING
or a.prod_tier5 = ''0YH1'' -- EBS
or a.prod_tier5 = ''0YO1'' -- EBS_SPECIAL_EDITION
or a.prod_tier5 = ''0MA4'' -- TOOLS_INST
or a.prod_tier6 = ''0Z07'' -- INTERNET DEVELOPER SUITE
or a.prod_tier5 = ''0YC5'' -- HUMAN RESSOURCES
or a.prod_tier5 = ''0YC1'' -- MANUFACTURING
or a.prod_tier5 = ''0YB8'' -- ORDER MANAGEMENT
or a.prod_tier4 = ''0KR7'' -- BI TIER4
or a.prod_tier6 = ''0ZY7'' -- BUSINESS INTELLIGENCE
or a.prod_tier5 = ''0EB7'' -- BI_TECH
or a.prod_tier6 = ''0ZK3'' -- E-BUSINESS INTELLIGENCE
or a.prod_tier4 = ''0YI6'' -- COLLABORATION
or a.prod_tier6 = ''0ZI8'' -- COLLABORATION SUITE
or a.prod_tier6 = ''0Z1D'' -- RECORDS MGMT
or a.prod_tier6 = ''0ZJ1'' -- CONTENT SERVICES
or a.prod_brand = ''JD-EDWARDS'' -- JDE
or a.prod_tier5 = ''0YC1'' -- MANUFACTURING
or a.prod_tier5 = ''0YC2'' -- FINANCIALS
or a.prod_tier5 = ''0YC4'' -- PROCUREMENT
or a.prod_tier5 = ''0YV9'' -- SCM -- SUPPLY_CHAIN_MANAGEMENT
or a.prod_tier5 = ''0WA5'' -- PSFT_ENTERPRISE_ERP
or a.prod_tier5 = ''0WA9'' -- JDE_WORLD_ERP
or a.prod_tier5 = ''0WA8'' -- JDE_ENTERPRISEONE_ERP
or a.prod_tier5 = ''0SY3'' -- SIEBEL ANALYTICS
or a.prod_tier4 = ''0YV1'' -- ACQUIRED_RETAIL_APPLIC
or a.prod_tier4 = ''0WD3'' -- ORACLE_RETAIL_APPLIC
or a.prod_tier4 = ''0YO2'' -- ORACLE_VERTICAL_APPLIC

or a.prod_tier3 = ''0YF3'' -- MIDDLEWARE
or a.prod_tier4 = ''0ZV5'' -- IDENTITY MANAGEMENT

or a.prod_tier6 = ''0ZX8'' -- FORMS_AND_REPORT
or a.prod_tier6 = ''0Z80'' -- OLAP
or a.prod_tier6 = ''0ZC3'' -- DATA_WAREHOUSE
or a.prod_tier6 = ''0ZG2'' -- RAC
or a.prod_tier6 = ''0ZS3'' -- TUNING_PACK
or a.prod_tier6 = ''0ZS4'' -- DIAGNOSTICS_PACK
or a.prod_tier5 = ''0EC1'' -- SECURE_BACKUP
or a.prod_tier5 = ''0XY1'' -- SECURE_ENTERP_SEARCH
or a.prod_tier5 = ''0YO4'' -- LEARNING MGMT
or a.prod_tier6 = ''0ZY6'' -- EPB
or a.prod_tier6 = ''0ZL6'' -- OFA
or a.prod_tier6 = ''0YS2'' -- PSFT_FMS

--or a.prod_brand = ''IBM'' -- DB2, AS400
or a.prod_brand = ''IBM'' and a.prod_name in (''DB2'',''DBMS'')
or a.prod_brand = ''IBM'' and a.prod_name like ''%AS%400%''
or a.prod_BRAND = ''SYBASE'' AND a.PROD_NAME = ''SYBASE'' -- SYBASE
or a.prod_BRAND = ''MYSQL-AB'' and a.PROD_NAME = ''MYSQL'' -- MYSQL
or a.PROD_NAME  = ''INGRES'' -- INGRES
or a.prod_brand = ''SAP''
or a.prod_brand = ''QAD''
or a.prod_brand = ''MICROSOFT''
or a.prod_brand = ''BAAN''
)
            and b.marketing_status not in (''BAD DATA'',''DELETED'')
        group by b.country_id, b.org_id';

            begin
                execute immediate 'drop table prods_emea2';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

        begin
            if is_table_populated('gcd_dw.gcd_orgs_products_vw') then
                execute immediate sqlstmt || sqlstmt2 || sqlstmt3;
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea2 create', sysdate,'CREATED');
            end if;
        exception when others then
            err_msg := SUBSTR(SQLERRM, 1, 100);
            insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea2 create', sysdate,'!! '  || err_msg);
        end;

        begin
            if is_table_populated('prods_emea2') then

                -- drop prods_emea_bak
              begin
                execute immediate 'drop table prods_emea_bak';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                    commit;
              end;

                -- prods_emea -> prods_emea_bak
              begin
                execute immediate 'alter table prods_emea rename to prods_emea_bak'; ------------!!!!!!!!!!!!!!!!!!! rec
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea rename -> proc_emea_bak', sysdate,'RENAMED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
              end;

                -- prods_emea2 -> prods_emea
              begin
                execute immediate 'alter table prods_emea2 rename to prods_emea'; ------ !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea2 rename -> prods_emea', sysdate,'RENAMED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea2 rename -> emea_inds', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate 'DROP INDEX BT_prods_emea_org_id';
                execute immediate 'DROP INDEX BMap_prods_emea_country_id';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea indexes', sysdate,'DROPPED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate '
                    CREATE Unique INDEX BT_prods_emea_org_id ON prods_emea (  org_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE Bitmap INDEX BMap_prods_emea_country_id ON prods_emea (  country_id  )
                    COMPUTE STATISTICS';
                    insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea indexes', sysdate,'CREATED');
                    commit;
               EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate 'GRANT SELECT ON prods_emea TO public';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea GRANT to public', sysdate, 'GRANTED');
                commit;
              exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea GRANT to public', sysdate,'NOT GRANTED - ' || err_msg);
                commit;
              end;

            end if;
        end;

END;

PROCEDURE PROC_PRODS_EMEA_A
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   sqlstmt varchar2(32767) := '';
   sqlstmt2 varchar2(32767) := '';
BEGIN
    insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PROC_PRODS_EMEA_A', sysdate,'START');
    commit;

    sqlstmt := '
create table PRODS_EMEA_A2 as
select b.country_id, b.org_id, max(b.duns_number) as duns_number,
max(case when a.prod_tier2 = ''0YF0''  then a.created_date end) as PROD_DB_INST,
max(case when a.prod_tier2 = ''0YF0'' and t.tar_month is not null and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DB_INST,
max(case when a.prod_tier2 = ''0YF0'' then o.order_date end) as DB_INST,
max(case when a.prod_tier6 = ''0Z10''  then a.created_date end) as PROD_DB_EE,
max(case when a.prod_tier6 = ''0Z10'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DB_EE,
max(case when a.prod_tier6 = ''0Z10'' then o.order_date end) as DB_EE,
max(case when a.prod_tier6 = ''0Z58''  then a.created_date end) as PROD_DB_SE,
max(case when a.prod_tier6 = ''0Z58'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DB_SE,
max(case when a.prod_tier6 = ''0Z58'' then o.order_date end) as DB_SE,
max(case when a.prod_tier6 = ''0ZW3''  then a.created_date end) as PROD_DB_SEO,
max(case when a.prod_tier6 = ''0ZW3'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DB_SEO,
max(case when a.prod_tier6 = ''0ZW3'' then o.order_date end) as DB_SEO,
max(case when a.prod_tier5 = ''0YM9''  then a.created_date end) as PROD_DB_ENTERPRISE_MGMT,
max(case when a.prod_tier5 = ''0YM9'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DB_ENTERPRISE_MGMT,
max(case when a.prod_tier5 = ''0YM9'' then o.order_date end) as DB_ENTERPRISE_MGMT,
max(case when a.prod_tier2 = ''0YY1''  then a.created_date end) as PROD_APPLICATIONS,
max(case when a.prod_tier2 = ''0YY1'' then o.order_date end) as APPLICATIONS, -- added
max(case when a.prod_tier2 = ''0YY1'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_APPLICATIONS,
max(case when a.prod_tier4 = ''0DM2''  then a.created_date end) as PROD_ORACLE_CRM_APPL,
max(case when a.prod_tier4 = ''0DM2'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ORACLE_CRM_APPL,
max(case when a.prod_tier4 = ''0DM2'' then o.order_date end) as ORACLE_CRM_APPL,
max(case when a.prod_tier4 = ''0DM3''  then a.created_date end) as PROD_PSFT_CRM_APPL,
max(case when a.prod_tier4 = ''0DM3'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_PSFT_CRM_APPL,
max(case when a.prod_tier4 = ''0DM3'' then o.order_date end) as PSFT_CRM_APPL,
max(case when a.prod_tier4 = ''0GW1''  then a.created_date end) as PROD_SIEBEL_CRM_APPL,
max(case when a.prod_tier4 = ''0GW1'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_SIEBEL_CRM_APPL,
max(case when a.prod_tier4 = ''0GW1'' then o.order_date end) as SIEBEL_CRM_APPL,
max(case when a.prod_tier5 = ''0Y26''  then a.created_date end) as PROD_APPLIC_SERVERS,
max(case when a.prod_tier5 = ''0Y26'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_APPLIC_SERVERS,
max(case when a.prod_tier5 = ''0Y26'' then o.order_date end) as APPLIC_SERVERS,
max(case when a.prod_tier6 = ''0ZB8''  then a.created_date end) as PROD_IAS_EE,
max(case when a.prod_tier6 = ''0ZB8'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_IAS_EE,
max(case when a.prod_tier6 = ''0ZB8'' then o.order_date end) as IAS_EE,
max(case when a.prod_tier6 = ''0ZC4''  then a.created_date end) as PROD_IAS_SE,
max(case when a.prod_tier6 = ''0ZC4'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_IAS_SE,
max(case when a.prod_tier6 = ''0ZC4'' then o.order_date end) as IAS_SE,
max(case when a.prod_tier6 = ''0ZJ8''  then a.created_date end) as PROD_IAS_Jave_Edit,
max(case when a.prod_tier6 = ''0ZJ8'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_IAS_Java_Edit,
max(case when a.prod_tier6 = ''0ZJ8'' then o.order_date end) as IAS_Java_Edit,
max(case when a.prod_tier6 = ''0ZX2''  then a.created_date end) as PROD_BPEL_PROCESS_MGR,
max(case when a.prod_tier6 = ''0ZX2'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_BPEL_PROCESS_MGR,
max(case when a.prod_tier6 = ''0ZX2'' then o.order_date end) as BPEL_PROCESS_MGR,
max(case when a.prod_tier6 = ''0YO6''  then a.created_date end) as PROD_AS_OTHER,
max(case when a.prod_tier6 = ''0YO6'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_AS_OTHER,
max(case when a.prod_tier6 = ''0YO6'' then o.order_date end) as AS_OTHER,
max(case when a.prod_tier6 = ''0ZX5''  then a.created_date end) as PROD_PORTAL,
max(case when a.prod_tier6 = ''0ZX5'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_PORTAL,
max(case when a.prod_tier6 = ''0ZX5'' then o.order_date end) as PORTAL,
max(case when a.prod_tier6 = ''0Z3B''  then a.created_date end) as PROD_IAS_SEO,
max(case when a.prod_tier6 = ''0Z3B'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_IAS_SEO,
max(case when a.prod_tier6 = ''0Z3B'' then o.order_date end) as IAS_SEO,
max(case when a.prod_tier6 = ''0ZUH''  then a.created_date end) as PROD_PROV_PACK,
max(case when a.prod_tier6 = ''0ZUH'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_PROV_PACK,
max(case when a.prod_tier6 = ''0ZUH'' then o.order_date end) as PROV_PACK,
max(case when a.prod_tier6 = ''0ZS5''  then a.created_date end) as PROD_CHA_MGMT_PACK,
max(case when a.prod_tier6 = ''0ZS5'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_CHA_MGMT_PACK,
max(case when a.prod_tier6 = ''0ZS5'' then o.order_date end) as CHA_MGMT_PACK,
max(case when a.prod_tier6 = ''0ZA6''  then a.created_date end) as PROD_ADV_SECURITY,
max(case when a.prod_tier6 = ''0ZA6'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ADV_SECURITY,
max(case when a.prod_tier6 = ''0ZA6'' then o.order_date end) as ADV_SECURITY,
max(case when a.prod_tier6 = ''0ZS1''  then a.created_date end) as PROD_LABEL_SECURITY,
max(case when a.prod_tier6 = ''0ZS1'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_LABEL_SECURITY,
max(case when a.prod_tier6 = ''0ZS1'' then o.order_date end) as LABEL_SECURITY,
max(case when a.prod_tier6 = ''0FH9''  then a.created_date end) as PROD_ORA_SECUR,
max(case when a.prod_tier6 = ''0FH9'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ORA_SECUR,
max(case when a.prod_tier6 = ''0FH9'' then o.order_date end) as ORA_SECUR,
max(case when a.prod_tier6 = ''0EC2''  then a.created_date end) as PROD_ORA_SEC_BK,
max(case when a.prod_tier6 = ''0EC2'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ORA_SEC_BK,
max(case when a.prod_tier6 = ''0EC2'' then o.order_date end) as ORA_SEC_BK,
max(case when a.prod_tier6 = ''0ZG3''  then a.created_date end) as PROD_PARTITIONING,
max(case when a.prod_tier6 = ''0ZG3'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_PARTITIONING,
max(case when a.prod_tier6 = ''0ZG3'' then o.order_date end) as PARTITIONING,
max(case when a.prod_tier6 = ''0ZS2''  then a.created_date end) as PROD_SPATIAL,
max(case when a.prod_tier6 = ''0ZS2'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_SPATIAL,
max(case when a.prod_tier6 = ''0ZS2'' then o.order_date end) as SPATIAL,
max(case when a.prod_tier6 = ''0ZH6''  then a.created_date end) as PROD_DATA_MINING,
max(case when a.prod_tier6 = ''0ZH6'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DATA_MINING,
max(case when a.prod_tier6 = ''0ZH6'' then o.order_date end) as DATA_MINING,
max(case when a.prod_tier5 = ''0YH1''  then a.created_date end) as PROD_EBS,
max(case when a.prod_tier5 = ''0YH1'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_EBS,
max(case when a.prod_tier5 = ''0YH1'' then o.order_date end) as EBS,
max(case when a.prod_tier5 = ''0YO1''  then a.created_date end) as PROD_EBS_SPECIAL_EDIT,
max(case when a.prod_tier5 = ''0YO1'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_EBS_SPECIAL_EDIT,
max(case when a.prod_tier5 = ''0YO1'' then o.order_date end) as EBS_SPECIAL_EDIT,
max(case when a.prod_tier5 = ''0MA4''  then a.created_date end) as PROD_TOOLS_INST,
max(case when a.prod_tier5 = ''0MA4'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_TOOLS_INST,
max(case when a.prod_tier5 = ''0MA4'' then o.order_date end) as TOOLS_INST,
max(case when a.prod_tier6 = ''0Z07''  then a.created_date end) as PROD_IDS,
max(case when a.prod_tier6 = ''0Z07'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_IDS,
max(case when a.prod_tier6 = ''0Z07'' then o.order_date end) as IDS,
max(case when a.prod_tier5 = ''0YC5''  then a.created_date end) as PROD_HUMAN_RES,
max(case when a.prod_tier5 = ''0YC5'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_HUMAN_RES,
max(case when a.prod_tier5 = ''0YC5'' then o.order_date end) as HUMAN_RES,
max(case when a.prod_tier4 = ''0KR7''  then a.created_date end) as PROD_BI_TIER4,
max(case when a.prod_tier4 = ''0KR7'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_BI_TIER4,
max(case when a.prod_tier4 = ''0KR7'' then o.order_date end) as BI_TIER4,
max(case when a.prod_tier6 = ''0ZY7''  then a.created_date end) as PROD_BI_SE,
max(case when a.prod_tier6 = ''0ZY7'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_BI_SE,
max(case when a.prod_tier6 = ''0ZY7'' then o.order_date end) as BI_SE,
max(case when a.prod_tier5 = ''0EB7''  then a.created_date end) as PROD_BI_TECH,
max(case when a.prod_tier5 = ''0EB7'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_BI_TECH,
max(case when a.prod_tier5 = ''0EB7'' then o.order_date end) as BI_TECH,
max(case when a.prod_tier6 = ''0ZK3''  then a.created_date end) as PROD_E_BI,
max(case when a.prod_tier6 = ''0ZK3'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_E_BI,
max(case when a.prod_tier6 = ''0ZK3'' then o.order_date end) as E_BI,
max(case when a.prod_tier4 = ''0YI6''  then a.created_date end) as PROD_COLLABORATION,
max(case when a.prod_tier4 = ''0YI6'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_COLLABORATION,
max(case when a.prod_tier4 = ''0YI6'' then o.order_date end) as COLLABORATION,
max(case when a.prod_tier6 = ''0ZI8''  then a.created_date end) as PROD_COLLABORATION_SUITE,
max(case when a.prod_tier6 = ''0ZI8'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_COLLABORATION_SUITE,
max(case when a.prod_tier6 = ''0ZI8'' then o.order_date end) as COLLABORATION_SUITE,
max(case when a.prod_tier6 = ''0Z1D''  then a.created_date end) as PROD_RECORDS_MGMT,
max(case when a.prod_tier6 = ''0Z1D'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_RECORDS_MGMT,
max(case when a.prod_tier6 = ''0Z1D'' then o.order_date end) as RECORDS_MGMT,
max(case when a.prod_tier6 = ''0ZJ1''  then a.created_date end) as PROD_CONTENT_SERIVCES,
max(case when a.prod_tier6 = ''0ZJ1'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_CONTENT_SERIVCES,
max(case when a.prod_tier6 = ''0ZJ1'' then o.order_date end) as CONTENT_SERIVCES,
max(case when a.prod_brand = ''JD-EDWARDS''  then a.created_date end) as PROD_JDE,
max(case when a.prod_tier5 = ''0YC1''  then a.created_date end) as PROD_MANUFACTURING,
max(case when a.prod_tier5 = ''0YC1'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_MANUFACTURING,
max(case when a.prod_tier5 = ''0YC1'' then o.order_date end) as MANUFACTURING,
max(case when a.prod_tier5 = ''0YB8''  then a.created_date end) as PROD_ORDER_MGMT,
max(case when a.prod_tier5 = ''0YB8'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ORDER_MGMT,
max(case when a.prod_tier5 = ''0YB8'' then o.order_date end) as ORDER_MGMT,
max(case when a.prod_tier5 = ''0YC2''  then a.created_date end) as PROD_FINANCIALS,
max(case when a.prod_tier5 = ''0YC2'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_FINANCIALS,
max(case when a.prod_tier5 = ''0YC2'' then o.order_date end) as FINANCIALS,
max(case when a.prod_tier5 = ''0YC4''  then a.created_date end) as PROD_PROCUREMENT,
max(case when a.prod_tier5 = ''0YC4'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_PROCUREMENT,
max(case when a.prod_tier5 = ''0YC4'' then o.order_date end) as PROCUREMENT,
max(case when a.prod_tier5 = ''0YV9''  then a.created_date end) as PROD_SCM, -- SUPPLY_CHAIN_MANAGEMENT
max(case when a.prod_tier5 = ''0YV9'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_SCM,
max(case when a.prod_tier5 = ''0YV9'' then o.order_date end) as SCM,
max(case when a.prod_tier5 = ''0WA5''  then a.created_date end) as PROD_PSFT_ENTERPRISE_ERP,
max(case when a.prod_tier5 = ''0WA5'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_PSFT_ENTERPRISE_ERP,
max(case when a.prod_tier5 = ''0WA5'' then o.order_date end) as PSFT_ENTERPRISE_ERP,

max(case when a.prod_tier5 = ''0WA9''  then a.created_date end) as PROD_JDE_WORLD_ERP,
max(case when a.prod_tier5 = ''0WA9'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_JDE_WORLD_ERP,
max(case when a.prod_tier5 = ''0WA9'' then o.order_date end) as JDE_WORLD_ERP,

max(case when a.prod_tier5 = ''0WA8''  then a.created_date end) as PROD_JDE_ENTERPRISEONE_ERP,
max(case when a.prod_tier5 = ''0WA8'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_JDE_ENTERPRISEONE_ERP,
max(case when a.prod_tier5 = ''0WA8'' then o.order_date end) as JDE_ENTERPRISEONE_ERP,
max(case when a.prod_tier5 = ''0SY3''  then a.created_date end) as PROD_SIEBEL_ANALYTICS,
max(case when a.prod_tier5 = ''0SY3'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_SIEBEL_ANALYTICS,
max(case when a.prod_tier5 = ''0SY3'' then o.order_date end) as SIEBEL_ANALYTICS,
max(case when a.prod_tier4 = ''0YV1''  then a.created_date end) as PROD_ACQUIR_RETAIL_APPL,
max(case when a.prod_tier4 = ''0YV1'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ACQUIR_RETAIL_APPL,
max(case when a.prod_tier4 = ''0YV1'' then o.order_date end) as ACQUIR_RETAIL_APPL,
max(case when a.prod_tier4 = ''0WD3''  then a.created_date end) as PROD_ORACLE_RETAIL_APPL,
max(case when a.prod_tier4 = ''0WD3'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ORACLE_RETAIL_APPL,
max(case when a.prod_tier4 = ''0WD3'' then o.order_date end) as ORACLE_RETAIL_APPL,
max(case when a.prod_tier4 = ''0YO2''  then a.created_date end) as PROD_ORACLE_VERTICAL_APPL,
max(case when a.prod_tier4 = ''0YO2'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_ORACLE_VERTICAL_APPL,
max(case when a.prod_tier4 = ''0YO2'' then o.order_date end) as ORACLE_VERTICAL_APPL,

max(case when a.prod_tier3 = ''0YF3''  then a.created_date end) as PROD_MIDDLEWARE,
max(case when a.prod_tier3 = ''0YF3'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_MIDDLEWARE,
max(case when a.prod_tier3 = ''0YF3'' then o.order_date end) as MIDDLEWARE,
max(case when a.prod_tier4 = ''0ZV5''  then a.created_date end) as PROD_IDENTITY_MGMT,
max(case when a.prod_tier4 = ''0ZV5'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_IDENTITY_MGMT,
max(case when a.prod_tier4 = ''0ZV5'' then o.order_date end) as IDENTITY_MGMT,
max(case when a.prod_tier6 = ''0ZX8''  then a.created_date end) as PROD_FORMS_AND_REPORTS,
max(case when a.prod_tier6 = ''0ZX8'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_FORMS_AND_REPORTS,
max(case when a.prod_tier6 = ''0ZX8'' then o.order_date end) as FORMS_AND_REPORTS,
max(case when a.prod_tier6 = ''0Z80''  then a.created_date end) as PROD_OLAP,
max(case when a.prod_tier6 = ''0Z80'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_OLAP,
max(case when a.prod_tier6 = ''0Z80'' then o.order_date end) as OLAP,
max(case when a.prod_tier6 = ''0ZC3''  then a.created_date end) as PROD_DATA_WAREHOUSE,
max(case when a.prod_tier6 = ''0ZC3'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DATA_WAREHOUSE,
max(case when a.prod_tier6 = ''0ZC3'' then o.order_date end) as DATA_WAREHOUSE,
max(case when a.prod_tier6 = ''0ZG2''  then a.created_date end) as PROD_RAC,
max(case when a.prod_tier6 = ''0ZG2'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_RAC,
max(case when a.prod_tier6 = ''0ZG2'' then o.order_date end) as RAC,';

sqlstmt2 := '
max(case when a.prod_tier6 = ''0ZS3''  then a.created_date end) as PROD_TUNING_PACK,
max(case when a.prod_tier6 = ''0ZS3'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_TUNING_PACK,
max(case when a.prod_tier6 = ''0ZS3'' then o.order_date end) as TUNING_PACK,
max(case when a.prod_tier6 = ''0ZS4''  then a.created_date end) as PROD_DIAGNOSTICS_PACK,
max(case when a.prod_tier6 = ''0ZS4'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_DIAGNOSTICS_PACK,
max(case when a.prod_tier6 = ''0ZS4'' then o.order_date end) as DIAGNOSTICS_PACK,
max(case when a.prod_tier5 = ''0EC1''  then a.created_date end) as PROD_SECURE_BACKUP,
max(case when a.prod_tier5 = ''0EC1'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_SECURE_BACKUP,
max(case when a.prod_tier5 = ''0EC1'' then o.order_date end) as SECURE_BACKUP,
max(case when a.prod_tier5 = ''0XY1''  then a.created_date end) as PROD_SECURE_ENTERP_SEARCH,
max(case when a.prod_tier5 = ''0XY1'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_SECURE_ENTERP_SEARCH,
max(case when a.prod_tier5 = ''0XY1'' then o.order_date end) as SECURE_ENTERP_SEARCH,
max(case when a.prod_tier5 = ''0YO4''  then a.created_date end) as PROD_LEARNING_MGMT,
max(case when a.prod_tier5 = ''0YO4'' and t.tar_month is not null then to_date(t.tar_month || ''/'' || t.tar_year, ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') end) as TAR_LEARNING_MGMT,
max(case when a.prod_tier5 = ''0YO4'' then o.order_date end) as LEARNING_MGMT,
max(case when a.prod_brand = ''IBM'' and a.prod_name in (''DB2'',''DBMS'')  then a.created_date end) as PROD_DB2,
max(case when a.prod_brand = ''IBM'' and a.prod_name like ''%AS%400%''  then a.created_date end) as PROD_AS400,
max(case when a.prod_brand = ''MICROSOFT'' and a.prod_name = ''SQL-SERVER''  then a.created_date end) as PROD_MSSQL,
max(case when a.prod_BRAND = ''SYBASE'' AND a.PROD_NAME = ''SYBASE''  then a.created_date end) as PROD_SYBASE,
max(case when a.prod_BRAND = ''MYSQL-AB'' and a.PROD_NAME = ''MYSQL''  then a.created_date end) as PROD_MYSQL,
max(case when a.PROD_NAME = ''INGRES''  then a.created_date end) as PROD_INGRES,
max(case when a.prod_brand = ''SAP''  then a.created_date end) as PROD_SAP,
max(case when a.prod_brand = ''QAD''  then a.created_date end) as PROD_QAD,
max(case when a.prod_brand = ''MICROSOFT''  then a.created_date end) as PROD_MICROSOFT,
max(case when a.prod_brand = ''BAAN''  then a.created_date end)as PROD_BAAN
from gcd_dw.gcd_orgs_products_vw a, gcd_dw.list_build_organizations_eu b,
gcd_dw.gcd_tar_summary t, gcd_dw_data.gcd_order_entry o
where a.org_id = b.org_id
and a.prod_id = t.prod_id (+)
and a.org_id = t.ultimate_org_id (+)
and a.prod_id = o.prod_id (+)
and a.org_id = o.org_id (+)
/*and ( a.created_date > add_months(sysdate,-60)
or nvl(o.order_date, add_months(sysdate, -100)) > add_months(sysdate, -60)
or to_date(nvl(t.tar_month,''JAN'') || ''/'' || nvl(t.tar_year,1990), ''MON/YYYY'',''NLS_DATE_LANGUAGE = AMERICAN'') > add_months(sysdate, -60)
     )*/
and (a.prod_tier2 = ''0YF0'' -- DB_INST
or a.prod_tier6 = ''0Z10'' -- DB_EE
or a.prod_tier6 = ''0Z58'' -- DB_SE
or a.prod_tier6 = ''0ZW3'' -- DB_SEO
or a.prod_tier5 = ''0YM9'' -- DB_ENTERPRISE_MGMT
or a.prod_tier2 = ''0YY1'' -- APPLICATIONS
or a.prod_tier4 = ''0DM2'' -- ORACLE CRM
or a.prod_tier4 = ''0DM3'' -- PSFT_CRM
or a.prod_tier4 = ''0GW1'' -- SIEBEL_CRM
or a.prod_tier5 = ''0Y26'' -- AS
or a.prod_tier6 = ''0ZX8'' -- FORMS
or a.prod_tier6 = ''0Z3B'' -- IAS_SEO
or a.prod_tier6 = ''0ZB8'' -- IAS_EE
or a.prod_tier6 = ''0ZC4'' -- IAS_SE
or a.prod_tier6 = ''0ZJ8'' -- IAS_Java_edit
or a.prod_tier6 = ''0ZX2'' -- BPEL_PROCESS_MGR
or a.prod_tier6 = ''0YO6'' -- AS_OTHER
or a.prod_tier6 = ''0ZX5'' -- PORTAL
or a.prod_tier6 = ''0ZUH'' -- PROV_PACK
or a.prod_tier6 = ''0ZS5'' -- CHA_MGMT_PACK
or a.prod_tier6 = ''0ZA6'' -- ADVANCED_SECURITY
or a.prod_tier6 = ''0ZS1'' -- LABLE_SECURITY
or a.prod_tier6 = ''0FH9'' -- ORA_SECUR
or a.prod_tier6 = ''0EC2'' -- ORA_SEC_BK
or a.prod_tier6 = ''0ZG3'' -- ORA_PAR
or a.prod_tier6 = ''0ZS2'' -- SPATIAL
or a.prod_tier6 = ''0ZH6'' -- DATA MINING
or a.prod_tier5 = ''0YH1'' -- EBS
or a.prod_tier5 = ''0YO1'' -- EBS_SPECIAL_EDITION
or a.prod_tier5 = ''0MA4'' -- TOOLS_INST
or a.prod_tier6 = ''0Z07'' -- INTERNET DEVELOPER SUITE
or a.prod_tier5 = ''0YC5'' -- HUMAN RESSOURCES
or a.prod_tier5 = ''0YC1'' -- MANUFACTURING
or a.prod_tier5 = ''0YB8'' -- ORDER MANAGEMENT
or a.prod_tier4 = ''0KR7'' -- BI TIER4
or a.prod_tier6 = ''0ZY7'' -- BUSINESS INTELLIGENCE
or a.prod_tier5 = ''0EB7'' -- BI_TECH
or a.prod_tier6 = ''0ZK3'' -- E-BUSINESS INTELLIGENCE
or a.prod_tier4 = ''0YI6'' -- COLLABORATION
or a.prod_tier6 = ''0ZI8'' -- COLLABORATION SUITE
or a.prod_tier6 = ''0Z1D'' -- RECORDS MGMT
or a.prod_tier6 = ''0ZJ1'' -- CONTENT SERVICES
or a.prod_brand = ''JD-EDWARDS'' -- JDE
or a.prod_tier5 = ''0YC1'' -- MANUFACTURING
or a.prod_tier5 = ''0YC2'' -- FINANCIALS
or a.prod_tier5 = ''0YC4'' -- PROCUREMENT
or a.prod_tier5 = ''0YV9'' -- SCM -- SUPPLY_CHAIN_MANAGEMENT
or a.prod_tier5 = ''0WA5'' -- PSFT_ENTERPRISE_ERP
or a.prod_tier5 = ''0WA9'' -- JDE_WORLD_ERP
or a.prod_tier5 = ''0WA8'' -- JDE_ENTERPRISEONE_ERP
or a.prod_tier5 = ''0SY3'' -- SIEBEL ANALYTICS
or a.prod_tier4 = ''0YV1'' -- ACQUIRED_RETAIL_APPLIC
or a.prod_tier4 = ''0WD3'' -- ORACLE_RETAIL_APPLIC
or a.prod_tier4 = ''0YO2'' -- ORACLE_VERTICAL_APPLIC

or a.prod_tier3 = ''0YF3'' -- MIDDLEWARE
or a.prod_tier4 = ''0ZV5'' -- IDENTITY MANAGEMENT

or a.prod_tier6 = ''0ZX8'' -- FORMS_AND_REPORT
or a.prod_tier6 = ''0Z80'' -- OLAP
or a.prod_tier6 = ''0ZC3'' -- DATA_WAREHOUSE
or a.prod_tier6 = ''0ZG2'' -- RAC
or a.prod_tier6 = ''0ZS3'' -- TUNING_PACK
or a.prod_tier6 = ''0ZS4'' -- DIAGNOSTICS_PACK
or a.prod_tier5 = ''0EC1'' -- SECURE_BACKUP
or a.prod_tier5 = ''0XY1'' -- SECURE_ENTERP_SEARCH
or a.prod_tier5 = ''0YO4'' -- LEARNING MGMT
--or a.prod_brand = ''IBM'' -- DB2, AS400
or a.prod_brand = ''IBM'' and a.prod_name in (''DB2'',''DBMS'')
or a.prod_brand = ''IBM'' and a.prod_name like ''%AS%400%''
or a.prod_BRAND = ''SYBASE'' AND a.PROD_NAME = ''SYBASE'' -- SYBASE
or a.prod_BRAND = ''MYSQL-AB'' and a.PROD_NAME = ''MYSQL'' -- MYSQL
or a.PROD_NAME  = ''INGRES'' -- INGRES
or a.prod_brand = ''SAP''
or a.prod_brand = ''QAD''
or a.prod_brand = ''MICROSOFT''
or a.prod_brand = ''BAAN''
)
            and b.marketing_status not in (''BAD DATA'',''DELETED'')
        group by b.country_id, b.org_id';

            begin
                execute immediate 'drop table PRODS_EMEA_A2';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

        begin
            if is_table_populated('gcd_dw.gcd_orgs_products_vw') then
                execute immediate sqlstmt || sqlstmt2;
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A2 create', sysdate,'CREATED');
            end if;
        exception when others then
            err_msg := SUBSTR(SQLERRM, 1, 100);
            insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A2 create', sysdate,'!! '  || err_msg);
        end;

        begin
            if is_table_populated('PRODS_EMEA_A2') then

                -- drop PRODS_EMEA_A_bak
              begin
                execute immediate 'drop table PRODS_EMEA_A_bak';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                    commit;
              end;

                -- PRODS_EMEA_A -> PRODS_EMEA_A_bak
              begin
                execute immediate 'alter table PRODS_EMEA_A rename to PRODS_EMEA_A_bak'; ------------!!!!!!!!!!!!!!!!!!! rec
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A rename -> proc_emea_bak', sysdate,'RENAMED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
              end;

                -- PRODS_EMEA_A2 -> PRODS_EMEA_A
              begin
                execute immediate 'alter table PRODS_EMEA_A2 rename to PRODS_EMEA_A'; ------ !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A2 rename -> PRODS_EMEA_A', sysdate,'RENAMED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A2 rename -> emea_inds', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate 'DROP INDEX BT_PRODS_EMEA_A_org_id';
                execute immediate 'DROP INDEX BMap_PRODS_EMEA_A_country_id';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A indexes', sysdate,'DROPPED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate '
                    CREATE Unique INDEX BT_PRODS_EMEA_A_org_id ON PRODS_EMEA_A (  org_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE Bitmap INDEX BMap_PRODS_EMEA_A_country_id ON PRODS_EMEA_A (  country_id  )
                    COMPUTE STATISTICS';
                    insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A indexes', sysdate,'CREATED');
                    commit;
               EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate 'GRANT SELECT ON PRODS_EMEA_A TO public';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A GRANT to public', sysdate, 'GRANTED');
                commit;
              exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'PRODS_EMEA_A GRANT to public', sysdate,'NOT GRANTED - ' || err_msg);
                commit;
              end;

            end if;
        end;

END;


PROCEDURE PROC_PRODS_EMEA_GSRT
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   sqlstmt varchar2(32000) := '';
BEGIN
    insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT PROC_PRODS_EMEA_GSRT', sysdate,'START');
    commit;

   /* sqlstmt := '
        create table prods_emea_gsrt2 as
        select b.country_id, b.duns_number, max(b.org_id) as org_id,
        max(case when a.prod_tier2 = ''YF0'' then c.revenue_recognition_date end) as DB_INST,
        max(case when a.prod_tier6 = ''Z10'' then c.revenue_recognition_date end) as DB_EE,
        max(case when a.prod_tier6 = ''Z58'' then c.revenue_recognition_date end) as DB_SE,
        max(case when a.prod_tier6 = ''ZW3'' then c.revenue_recognition_date end) as DB_SEO,
        max(case when a.prod_tier5 = ''YM9'' then c.revenue_recognition_date end) as DB_ENTERPRISE_MGMT,
        max(case when a.prod_tier2 = ''YY1'' then c.revenue_recognition_date end) as APPLICATIONS,
        max(case when a.prod_tier5 = ''Z1A'' then c.revenue_recognition_date end) as timesten,
        max(case when a.prod_tier5 = ''DM1'' then c.revenue_recognition_date end) as demantra,
        max(case when a.prod_tier4 = ''DM2'' then c.revenue_recognition_date end) as ORACLE_CRM_APPL,
        max(case when a.prod_tier4 = ''DM3'' then c.revenue_recognition_date end) as PSFT_CRM_APPL,
        max(case when a.prod_tier4 = ''GW1'' then c.revenue_recognition_date end) as SIEBEL_CRM_APPL,
        max(case when a.prod_tier5 = ''Y26'' then c.revenue_recognition_date end) as APPLIC_SERVERS,
        max(case when a.prod_tier6 = ''ZB8'' then c.revenue_recognition_date end) as IAS_EE,
        max(case when a.prod_tier6 = ''ZC4'' then c.revenue_recognition_date end) as IAS_SE,
        max(case when a.prod_tier6 = ''ZJ8'' then c.revenue_recognition_date end) as IAS_Java_Edit,
        max(case when a.prod_tier6 = ''ZX2'' then c.revenue_recognition_date end) as BPEL_PROCESS_MGR,
        max(case when a.prod_tier6 = ''YO6'' then c.revenue_recognition_date end) as AS_OTHER,
        max(case when a.prod_tier6 = ''ZX5'' then c.revenue_recognition_date end) as PORTAL,
        max(case when a.prod_tier6 = ''Z3B'' then c.revenue_recognition_date end) as IAS_SEO,
        max(case when a.prod_tier6 = ''ZUH'' then c.revenue_recognition_date end) as PROV_PACK,
        max(case when a.prod_tier6 = ''ZS5'' then c.revenue_recognition_date end) as CHA_MGMT_PACK,
        max(case when a.prod_tier6 = ''ZA6'' then c.revenue_recognition_date end) as ADV_SECURITY,
        max(case when a.prod_tier6 = ''ZS1'' then c.revenue_recognition_date end) as LABEL_SECURITY,
        max(case when a.prod_tier6 = ''FH9'' then c.revenue_recognition_date end) as ORA_SECUR,
        max(case when a.prod_tier6 = ''EC2'' then c.revenue_recognition_date end) as ORA_SEC_BK,
        max(case when a.prod_tier6 = ''ZG3'' then c.revenue_recognition_date end) as PARTITIONING,
        max(case when a.prod_tier6 = ''ZS2'' then c.revenue_recognition_date end) as SPATIAL,
        max(case when a.prod_tier6 = ''ZH6'' then c.revenue_recognition_date end) as DATA_MINING,
        max(case when a.prod_tier5 = ''YH1'' then c.revenue_recognition_date end) as EBS,
        max(case when a.prod_tier5 = ''YO1'' then c.revenue_recognition_date end) as EBS_SPECIAL_EDIT,
        max(case when a.prod_tier5 = ''MA4'' then c.revenue_recognition_date end) as TOOLS_INST,
        max(case when a.prod_tier6 = ''Z07'' then c.revenue_recognition_date end) as IDS,
        max(case when a.prod_tier5 = ''YC5'' then c.revenue_recognition_date end) as HUMAN_RES,
        max(case when a.prod_tier4 = ''KR7'' then c.revenue_recognition_date end) as BI_TIER4,
        max(case when a.prod_tier6 = ''ZY7'' then c.revenue_recognition_date end) as BI_SE,
        max(case when a.prod_tier5 = ''EB7'' then c.revenue_recognition_date end) as BI_TECH,
        max(case when a.prod_tier6 = ''ZK3'' then c.revenue_recognition_date end) as E_BI,
        max(case when a.prod_tier4 = ''YI6'' then c.revenue_recognition_date end) as COLLABORATION,
        max(case when a.prod_tier6 = ''ZI8'' then c.revenue_recognition_date end) as COLLABORATION_SUITE,
        max(case when a.prod_tier6 = ''Z1D'' then c.revenue_recognition_date end) as RECORDS_MGMT,
        max(case when a.prod_tier6 = ''ZJ1'' then c.revenue_recognition_date end) as CONTENT_SERIVCES,

        max(case when a.prod_tier5 = ''YC1'' then c.revenue_recognition_date end) as MANUFACTURING,
        max(case when a.prod_tier5 = ''YB8'' then c.revenue_recognition_date end) as ORDER_MGMT,
        max(case when a.prod_tier5 = ''YC2'' then c.revenue_recognition_date end) as FINANCIALS,
        max(case when a.prod_tier5 = ''YC4'' then c.revenue_recognition_date end) as PROCUREMENT,
        max(case when a.prod_tier5 = ''YV9'' then c.revenue_recognition_date end) as SCM, -- SUPPLY_CHAIN_MANAGEMENT
        max(case when a.prod_tier5 = ''WA5'' then c.revenue_recognition_date end) as PSFT_ENTERPRISE_ERP,
        max(case when a.prod_tier5 = ''WA9'' then c.revenue_recognition_date end) as JDE_WORLD_ERP,
        max(case when a.prod_tier5 = ''WA8'' then c.revenue_recognition_date end) as JDE_ENTERPRISEONE_ERP,
        max(case when a.prod_tier5 = ''SY3'' then c.revenue_recognition_date end) as SIEBEL_ANALYTICS,
        max(case when a.prod_tier4 = ''YV1'' then c.revenue_recognition_date end) as ACQUIR_RETAIL_APPL,
        max(case when a.prod_tier4 = ''WD3'' then c.revenue_recognition_date end) as ORACLE_RETAIL_APPL,
        max(case when a.prod_tier4 = ''YO2'' then c.revenue_recognition_date end) as ORACLE_VERTICAL_APPL,
        max(case when a.prod_tier3 = ''YF3'' then c.revenue_recognition_date end) as MIDDLEWARE,
        max(case when a.prod_tier4 = ''ZV5'' then c.revenue_recognition_date end) as IDENTITY_MGMT,
        max(case when a.prod_tier6 = ''ZX8'' then c.revenue_recognition_date end) as FORMS_AND_REPORTS,
        max(case when a.prod_tier6 = ''Z80'' then c.revenue_recognition_date end) as OLAP,
        max(case when a.prod_tier6 = ''ZC3'' then c.revenue_recognition_date end) as DATA_WAREHOUSE,
        max(case when a.prod_tier6 = ''ZG2'' then c.revenue_recognition_date end) as RAC,
        max(case when a.prod_tier6 = ''ZS3'' then c.revenue_recognition_date end) as TUNING_PACK,
        max(case when a.prod_tier6 = ''ZS4'' then c.revenue_recognition_date end) as DIAGNOSTICS_PACK,
        max(case when a.prod_tier5 = ''EC1'' then c.revenue_recognition_date end) as SECURE_BACKUP,
        max(case when a.prod_tier5 = ''XY1'' then c.revenue_recognition_date end) as SECURE_ENTERP_SEARCH,
        max(case when a.prod_tier5 = ''YO4'' then c.revenue_recognition_date end) as LEARNING_MGMT,
        max(case when a.prod_tier4 = ''YQ8'' then c.revenue_recognition_date end) as PSFT_VERTICAL_APPS,
        max(case when a.prod_tier6 = ''ZY6'' then c.revenue_recognition_date end) as EPB,
        max(case when a.prod_tier6 = ''ZL6'' then c.revenue_recognition_date end) as OFA,
        max(case when a.prod_tier6 = ''YS2'' then c.revenue_recognition_date end) as PSFT_FMS
        from gsrt.gsrt_ref c, gcd_dw.list_build_organizations_eu b,
             gsrt.gsrt_prod_hierarchy_staging a
        where c.duns_number = b.duns_number
              and c.prod_code = a.prod_code
              and c.revenue_recognition_date > add_months(sysdate,-60) -- 5 yrs back
            and (a.prod_tier2 = ''YF0'' -- DB_INST
                 or a.prod_tier6 = ''Z10'' -- DB_EE
    	         or a.prod_tier6 = ''Z58'' -- DB_SE
            	 or a.prod_tier6 = ''ZW3'' -- DB_SEO
            	 or a.prod_tier5 = ''YM9'' -- DB_ENTERPRISE_MGMT

                 or a.prod_tier5 = ''DM1'' -- demantra
                 or a.prod_tier5 = ''Z1A'' -- timesten

            	 or a.prod_tier4 = ''DM2'' -- ORACLE CRM
            	 or a.prod_tier4 = ''DM3'' -- PSFT_CRM
            	 or a.prod_tier4 = ''GW1'' -- SIEBEL_CRM
                 or a.prod_tier5 = ''Y26'' -- AS

                 or a.prod_tier6 = ''Z3B'' -- IAS_SEO
                 or a.prod_tier6 = ''ZB8'' -- IAS_EE
                 or a.prod_tier6 = ''ZC4'' -- IAS_SE
                 or a.prod_tier6 = ''ZJ8'' -- IAS_Java_edit
                 or a.prod_tier6 = ''ZX2'' -- BPEL_PROCESS_MGR
                 or a.prod_tier6 = ''YO6'' -- AS_OTHER
                 or a.prod_tier6 = ''ZX5'' -- PORTAL
                 or a.prod_tier6 = ''ZUH'' -- PROV_PACK
                 or a.prod_tier6 = ''ZS5'' -- CHA_MGMT_PACK
                 or a.prod_tier6 = ''ZA6'' -- ADVANCED_SECURITY
                 or a.prod_tier6 = ''ZS1'' -- LABLE_SECURITY
                 or a.prod_tier6 = ''FH9'' -- ORA_SECUR
                 or a.prod_tier6 = ''EC2'' -- ORA_SEC_BK
                 or a.prod_tier6 = ''ZG3'' -- ORA_PAR
                 or a.prod_tier6 = ''ZS2'' -- SPATIAL
                 or a.prod_tier6 = ''ZH6'' -- DATA MINING
                 or a.prod_tier5 = ''YH1'' -- EBS
                 or a.prod_tier5 = ''YO1'' -- EBS_SPECIAL_EDITION
                 or a.prod_tier5 = ''MA4'' -- TOOLS_INST
                 or a.prod_tier6 = ''Z07'' -- INTERNET DEVELOPER SUITE
                 or a.prod_tier5 = ''YC5'' -- HUMAN RESSOURCES
                 or a.prod_tier5 = ''YC1'' -- MANUFACTURING
                 or a.prod_tier5 = ''YB8'' -- ORDER MANAGEMENT
                 or a.prod_tier4 = ''KR7'' -- BI_TIER4
                 or a.prod_tier6 = ''ZY7'' -- BUSINESS INTELLIGENCE
                 or a.prod_tier5 = ''EB7'' -- BI_TECH
                 or a.prod_tier6 = ''ZK3'' -- E-BUSINESS INTELLIGENCE
                 or a.prod_tier4 = ''YI6'' -- COLLABORATION
                 or a.prod_tier6 = ''ZI8'' -- COLLABORATION SUITE
                 or a.prod_tier6 = ''Z1D'' -- RECORDS MGMT
                 or a.prod_tier6 = ''ZJ1'' -- CONTENT SERVICES
                 or a.prod_tier5 = ''YC1'' -- MANUFACTURING
                 or a.prod_tier5 = ''YC2'' -- FINANCIALS
                 or a.prod_tier5 = ''YC4'' -- PROCUREMENT
                 or a.prod_tier5 = ''YV9'' -- SCM -- SUPPLY_CHAIN_MANAGEMENT
                 or a.prod_tier5 = ''WA5'' -- PSFT_ENTERPRISE_ERP
                 or a.prod_tier5 = ''WA9'' -- JDE_WORLD_ERP
                 or a.prod_tier5 = ''WA8'' -- JDE_ENTERPRISEONE_ERP
                 or a.prod_tier5 = ''SY3'' -- SIEBEL ANALYTICS
                 or a.prod_tier4 = ''YV1'' -- ACQUIRED_RETAIL_APPLIC
                 or a.prod_tier4 = ''WD3'' -- ORACLE_RETAIL_APPLIC
                 or a.prod_tier4 = ''YO2'' -- ORACLE_VERTICAL_APPLIC

                 or a.prod_tier3 = ''YF3'' -- MIDDLEWARE
                 or a.prod_tier4 = ''ZV5'' -- IDENTITY MANAGEMENT

                 or a.prod_tier6 = ''ZX8'' -- FORMS_AND_REPORT
                 or a.prod_tier6 = ''Z80'' -- OLAP
                 or a.prod_tier6 = ''ZC3'' -- DATA_WAREHOUSE
                 or a.prod_tier6 = ''ZG2'' -- RAC
                 or a.prod_tier6 = ''ZS3'' -- TUNING_PACK
                 or a.prod_tier6 = ''ZS4'' -- DIAGNOSTICS_PACK

                 or a.prod_tier5 = ''EC1'' -- SECURE_BACKUP
                 or a.prod_tier5 = ''XY1'' -- SECURE_ENTERP_SEARCH

                 or a.prod_tier2 = ''YY1'' -- APPLICATIONS tier2
                 or a.prod_tier5 = ''YO4'' -- LEARNING MGMT

                 or a.prod_tier4 = ''YQ8'' -- PSFT_VERTICAL_APPS
                 
                 or a.prod_tier6 = ''ZY6'' -- EPB
                 or a.prod_tier6 = ''ZL6'' -- OFA
                 or a.prod_tier6 = ''YS2'' -- PSFT_FMS

            )
            and b.marketing_status not in (''BAD DATA'',''DELETED'')
        group by b.country_id, b.duns_number';

            begin
                execute immediate 'drop table prods_emea_gsrt2';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

        begin
            if is_table_populated('gsrt.gsrt_ref') then
                execute immediate sqlstmt;
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt2 create', sysdate,'CREATED');
            end if;
        exception when others then
            err_msg := SUBSTR(SQLERRM, 1, 100);
            insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt2 create', sysdate,'!! '  || err_msg);
        end;

        begin
            if is_table_populated('prods_emea_gsrt2') then

                -- drop prods_emea_bak
              begin
                execute immediate 'drop table prods_emea_gsrt_bak';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                    commit;
              end;

                -- prods_emea -> prods_emea_bak
              begin
                execute immediate 'alter table prods_emea_gsrt rename to prods_emea_gsrt_bak';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt rename -> prods_emea_gsrt_bak', sysdate,'RENAMED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
              end;

                -- prods_emea2 -> prods_emea
              begin
                execute immediate 'alter table prods_emea_gsrt2 rename to prods_emea_gsrt';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt2 rename -> prods_emea_gsrt', sysdate,'RENAMED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt2 rename -> prods_emea_gsrt', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate 'DROP INDEX BT_prods_emea_gsrt_org_id';
                execute immediate 'DROP INDEX BM_prods_emea_gsrt_country_id';
                execute immediate 'DROP INDEX BT_prods_emea_gsrt_duns_no';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt indexes', sysdate,'DROPPED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate '
                    CREATE Unique INDEX BT_prods_emea_gsrt_duns_no ON prods_emea_gsrt (  duns_number  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_prods_emea_gsrt_org_id ON prods_emea_gsrt (  org_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE Bitmap INDEX BM_prods_emea_gsrt_country_id ON prods_emea_gsrt (  country_id  )
                    COMPUTE STATISTICS';
                    insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt indexes', sysdate,'CREATED');
                    commit;
               EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate 'GRANT SELECT ON prods_emea_gsrt TO public';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt GRANT to public', sysdate, 'GRANTED');
                commit;
              exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt GRANT to public', sysdate,'NOT GRANTED - ' || err_msg);
                commit;
              end;

            end if;
        end;

*/
END;


PROCEDURE PROC_PRODS_EMEA_A_GSRT
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   sqlstmt varchar2(32000) := '';
BEGIN
    insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PROC_PRODS_EMEA_A_GSRT', sysdate,'START');
    commit;

    sqlstmt := '
        create table PRODS_EMEA_A_gsrt2 as
        select b.country_id, b.duns_number, max(b.org_id) as org_id,
        max(case when a.prod_tier2 = ''YF0'' then c.revenue_recognition_date end) as DB_INST,
        max(case when a.prod_tier6 = ''Z10'' then c.revenue_recognition_date end) as DB_EE,
        max(case when a.prod_tier6 = ''Z58'' then c.revenue_recognition_date end) as DB_SE,
        max(case when a.prod_tier6 = ''ZW3'' then c.revenue_recognition_date end) as DB_SEO,
        max(case when a.prod_tier5 = ''YM9'' then c.revenue_recognition_date end) as DB_ENTERPRISE_MGMT,
        max(case when a.prod_tier2 = ''YY1'' then c.revenue_recognition_date end) as APPLICATIONS,
        max(case when a.prod_tier4 = ''DM2'' then c.revenue_recognition_date end) as ORACLE_CRM_APPL,
        max(case when a.prod_tier4 = ''DM3'' then c.revenue_recognition_date end) as PSFT_CRM_APPL,
        max(case when a.prod_tier4 = ''GW1'' then c.revenue_recognition_date end) as SIEBEL_CRM_APPL,
        max(case when a.prod_tier5 = ''Y26'' then c.revenue_recognition_date end) as APPLIC_SERVERS,
        max(case when a.prod_tier6 = ''ZB8'' then c.revenue_recognition_date end) as IAS_EE,
        max(case when a.prod_tier6 = ''ZC4'' then c.revenue_recognition_date end) as IAS_SE,
        max(case when a.prod_tier6 = ''ZJ8'' then c.revenue_recognition_date end) as IAS_Java_Edit,
        max(case when a.prod_tier6 = ''ZX2'' then c.revenue_recognition_date end) as BPEL_PROCESS_MGR,
        max(case when a.prod_tier6 = ''YO6'' then c.revenue_recognition_date end) as AS_OTHER,
        max(case when a.prod_tier6 = ''ZX5'' then c.revenue_recognition_date end) as PORTAL,
        max(case when a.prod_tier6 = ''Z3B'' then c.revenue_recognition_date end) as IAS_SEO,
        max(case when a.prod_tier6 = ''ZUH'' then c.revenue_recognition_date end) as PROV_PACK,
        max(case when a.prod_tier6 = ''ZS5'' then c.revenue_recognition_date end) as CHA_MGMT_PACK,
        max(case when a.prod_tier6 = ''ZA6'' then c.revenue_recognition_date end) as ADV_SECURITY,
        max(case when a.prod_tier6 = ''ZS1'' then c.revenue_recognition_date end) as LABEL_SECURITY,
        max(case when a.prod_tier6 = ''FH9'' then c.revenue_recognition_date end) as ORA_SECUR,
        max(case when a.prod_tier6 = ''EC2'' then c.revenue_recognition_date end) as ORA_SEC_BK,
        max(case when a.prod_tier6 = ''ZG3'' then c.revenue_recognition_date end) as PARTITIONING,
        max(case when a.prod_tier6 = ''ZS2'' then c.revenue_recognition_date end) as SPATIAL,
        max(case when a.prod_tier6 = ''ZH6'' then c.revenue_recognition_date end) as DATA_MINING,
        max(case when a.prod_tier5 = ''YH1'' then c.revenue_recognition_date end) as EBS,
        max(case when a.prod_tier5 = ''YO1'' then c.revenue_recognition_date end) as EBS_SPECIAL_EDIT,
        max(case when a.prod_tier5 = ''MA4'' then c.revenue_recognition_date end) as TOOLS_INST,
        max(case when a.prod_tier6 = ''Z07'' then c.revenue_recognition_date end) as IDS,
        max(case when a.prod_tier5 = ''YC5'' then c.revenue_recognition_date end) as HUMAN_RES,
        max(case when a.prod_tier4 = ''KR7'' then c.revenue_recognition_date end) as BI_TIER4,
        max(case when a.prod_tier6 = ''ZY7'' then c.revenue_recognition_date end) as BI_SE,
        max(case when a.prod_tier5 = ''EB7'' then c.revenue_recognition_date end) as BI_TECH,
        max(case when a.prod_tier6 = ''ZK3'' then c.revenue_recognition_date end) as E_BI,
        max(case when a.prod_tier4 = ''YI6'' then c.revenue_recognition_date end) as COLLABORATION,
        max(case when a.prod_tier6 = ''ZI8'' then c.revenue_recognition_date end) as COLLABORATION_SUITE,
        max(case when a.prod_tier6 = ''Z1D'' then c.revenue_recognition_date end) as RECORDS_MGMT,
        max(case when a.prod_tier6 = ''ZJ1'' then c.revenue_recognition_date end) as CONTENT_SERIVCES,

        max(case when a.prod_tier5 = ''YC1'' then c.revenue_recognition_date end) as MANUFACTURING,
        max(case when a.prod_tier5 = ''YB8'' then c.revenue_recognition_date end) as ORDER_MGMT,
        max(case when a.prod_tier5 = ''YC2'' then c.revenue_recognition_date end) as FINANCIALS,
        max(case when a.prod_tier5 = ''YC4'' then c.revenue_recognition_date end) as PROCUREMENT,
        max(case when a.prod_tier5 = ''YV9'' then c.revenue_recognition_date end) as SCM, -- SUPPLY_CHAIN_MANAGEMENT
        max(case when a.prod_tier5 = ''WA5'' then c.revenue_recognition_date end) as PSFT_ENTERPRISE_ERP,
        max(case when a.prod_tier5 = ''WA9'' then c.revenue_recognition_date end) as JDE_WORLD_ERP,
        max(case when a.prod_tier5 = ''WA8'' then c.revenue_recognition_date end) as JDE_ENTERPRISEONE_ERP,
        max(case when a.prod_tier5 = ''SY3'' then c.revenue_recognition_date end) as SIEBEL_ANALYTICS,
        max(case when a.prod_tier4 = ''YV1'' then c.revenue_recognition_date end) as ACQUIR_RETAIL_APPL,
        max(case when a.prod_tier4 = ''WD3'' then c.revenue_recognition_date end) as ORACLE_RETAIL_APPL,
        max(case when a.prod_tier4 = ''YO2'' then c.revenue_recognition_date end) as ORACLE_VERTICAL_APPL,
        max(case when a.prod_tier3 = ''YF3'' then c.revenue_recognition_date end) as MIDDLEWARE,
        max(case when a.prod_tier4 = ''ZV5'' then c.revenue_recognition_date end) as IDENTITY_MGMT,
        max(case when a.prod_tier6 = ''ZX8'' then c.revenue_recognition_date end) as FORMS_AND_REPORTS,
        max(case when a.prod_tier6 = ''Z80'' then c.revenue_recognition_date end) as OLAP,
        max(case when a.prod_tier6 = ''ZC3'' then c.revenue_recognition_date end) as DATA_WAREHOUSE,
        max(case when a.prod_tier6 = ''ZG2'' then c.revenue_recognition_date end) as RAC,
        max(case when a.prod_tier6 = ''ZS3'' then c.revenue_recognition_date end) as TUNING_PACK,
        max(case when a.prod_tier6 = ''ZS4'' then c.revenue_recognition_date end) as DIAGNOSTICS_PACK,
        max(case when a.prod_tier5 = ''EC1'' then c.revenue_recognition_date end) as SECURE_BACKUP,
        max(case when a.prod_tier5 = ''XY1'' then c.revenue_recognition_date end) as SECURE_ENTERP_SEARCH,
        max(case when a.prod_tier5 = ''YO4'' then c.revenue_recognition_date end) as LEARNING_MGMT,
        max(case when a.prod_tier4 = ''YQ8'' then c.revenue_recognition_date end) as PSFT_VERTICAL_APPS
        from gsrt.gsrt_ref c, gcd_dw.list_build_organizations_eu b,
             gsrt.gsrt_prod_hierarchy_staging a
        where c.duns_number = b.duns_number
              and c.prod_code = a.prod_code
              -- and c.revenue_recognition_date > add_months(sysdate,-60) -- 5 yrs back
            and (a.prod_tier2 = ''YF0'' -- DB_INST
                 or a.prod_tier6 = ''Z10'' -- DB_EE
    	         or a.prod_tier6 = ''Z58'' -- DB_SE
            	 or a.prod_tier6 = ''ZW3'' -- DB_SEO
            	 or a.prod_tier5 = ''YM9'' -- DB_ENTERPRISE_MGMT

            	 or a.prod_tier4 = ''DM2'' -- ORACLE CRM
            	 or a.prod_tier4 = ''DM3'' -- PSFT_CRM
            	 or a.prod_tier4 = ''GW1'' -- SIEBEL_CRM
                 or a.prod_tier5 = ''Y26'' -- AS

                 or a.prod_tier6 = ''Z3B'' -- IAS_SEO
                 or a.prod_tier6 = ''ZB8'' -- IAS_EE
                 or a.prod_tier6 = ''ZC4'' -- IAS_SE
                 or a.prod_tier6 = ''ZJ8'' -- IAS_Java_edit
                 or a.prod_tier6 = ''ZX2'' -- BPEL_PROCESS_MGR
                 or a.prod_tier6 = ''YO6'' -- AS_OTHER
                 or a.prod_tier6 = ''ZX5'' -- PORTAL
                 or a.prod_tier6 = ''ZUH'' -- PROV_PACK
                 or a.prod_tier6 = ''ZS5'' -- CHA_MGMT_PACK
                 or a.prod_tier6 = ''ZA6'' -- ADVANCED_SECURITY
                 or a.prod_tier6 = ''ZS1'' -- LABLE_SECURITY
                 or a.prod_tier6 = ''FH9'' -- ORA_SECUR
                 or a.prod_tier6 = ''EC2'' -- ORA_SEC_BK
                 or a.prod_tier6 = ''ZG3'' -- ORA_PAR
                 or a.prod_tier6 = ''ZS2'' -- SPATIAL
                 or a.prod_tier6 = ''ZH6'' -- DATA MINING
                 or a.prod_tier5 = ''YH1'' -- EBS
                 or a.prod_tier5 = ''YO1'' -- EBS_SPECIAL_EDITION
                 or a.prod_tier5 = ''MA4'' -- TOOLS_INST
                 or a.prod_tier6 = ''Z07'' -- INTERNET DEVELOPER SUITE
                 or a.prod_tier5 = ''YC5'' -- HUMAN RESSOURCES
                 or a.prod_tier5 = ''YC1'' -- MANUFACTURING
                 or a.prod_tier5 = ''YB8'' -- ORDER MANAGEMENT
                 or a.prod_tier4 = ''KR7'' -- BI_TIER4
                 or a.prod_tier6 = ''ZY7'' -- BUSINESS INTELLIGENCE
                 or a.prod_tier5 = ''EB7'' -- BI_TECH
                 or a.prod_tier6 = ''ZK3'' -- E-BUSINESS INTELLIGENCE
                 or a.prod_tier4 = ''YI6'' -- COLLABORATION
                 or a.prod_tier6 = ''ZI8'' -- COLLABORATION SUITE
                 or a.prod_tier6 = ''Z1D'' -- RECORDS MGMT
                 or a.prod_tier6 = ''ZJ1'' -- CONTENT SERVICES
                 or a.prod_tier5 = ''YC1'' -- MANUFACTURING
                 or a.prod_tier5 = ''YC2'' -- FINANCIALS
                 or a.prod_tier5 = ''YC4'' -- PROCUREMENT
                 or a.prod_tier5 = ''YV9'' -- SCM -- SUPPLY_CHAIN_MANAGEMENT
                 or a.prod_tier5 = ''WA5'' -- PSFT_ENTERPRISE_ERP
                 or a.prod_tier5 = ''WA9'' -- JDE_WORLD_ERP
                 or a.prod_tier5 = ''WA8'' -- JDE_ENTERPRISEONE_ERP
                 or a.prod_tier5 = ''SY3'' -- SIEBEL ANALYTICS
                 or a.prod_tier4 = ''YV1'' -- ACQUIRED_RETAIL_APPLIC
                 or a.prod_tier4 = ''WD3'' -- ORACLE_RETAIL_APPLIC
                 or a.prod_tier4 = ''YO2'' -- ORACLE_VERTICAL_APPLIC

                 or a.prod_tier3 = ''YF3'' -- MIDDLEWARE
                 or a.prod_tier4 = ''ZV5'' -- IDENTITY MANAGEMENT

                 or a.prod_tier6 = ''ZX8'' -- FORMS_AND_REPORT
                 or a.prod_tier6 = ''Z80'' -- OLAP
                 or a.prod_tier6 = ''ZC3'' -- DATA_WAREHOUSE
                 or a.prod_tier6 = ''ZG2'' -- RAC
                 or a.prod_tier6 = ''ZS3'' -- TUNING_PACK
                 or a.prod_tier6 = ''ZS4'' -- DIAGNOSTICS_PACK

                 or a.prod_tier5 = ''EC1'' -- SECURE_BACKUP
                 or a.prod_tier5 = ''XY1'' -- SECURE_ENTERP_SEARCH

                 or a.prod_tier2 = ''YY1'' -- APPLICATIONS tier2
                 or a.prod_tier5 = ''YO4'' -- LEARNING MGMT

                 or a.prod_tier4 = ''YQ8'' -- PSFT_VERTICAL_APPS

            )
            and b.marketing_status not in (''BAD DATA'',''DELETED'')
        group by b.country_id, b.duns_number';

            begin
                execute immediate 'drop table PRODS_EMEA_A_gsrt2';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

        begin
            if is_table_populated('gsrt.gsrt_ref') then
                execute immediate sqlstmt;
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt2 create', sysdate,'CREATED');
            end if;
        exception when others then
            err_msg := SUBSTR(SQLERRM, 1, 100);
            insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt2 create', sysdate,'!! '  || err_msg);
        end;

        begin
            if is_table_populated('PRODS_EMEA_A_gsrt2') then

                -- drop PRODS_EMEA_A_bak
              begin
                execute immediate 'drop table PRODS_EMEA_A_gsrt_bak';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                    commit;
              end;

                -- PRODS_EMEA_A -> PRODS_EMEA_A_bak
              begin
                execute immediate 'alter table PRODS_EMEA_A_gsrt rename to PRODS_EMEA_A_gsrt_bak';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt rename -> PRODS_EMEA_A_gsrt_bak', sysdate,'RENAMED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
              end;

                -- PRODS_EMEA_A2 -> PRODS_EMEA_A
              begin
                execute immediate 'alter table PRODS_EMEA_A_gsrt2 rename to PRODS_EMEA_A_gsrt';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt2 rename -> PRODS_EMEA_A_gsrt', sysdate,'RENAMED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt2 rename -> PRODS_EMEA_A_gsrt', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate 'DROP INDEX BT_PRODS_EMEA_A_gsrt_org_id';
                execute immediate 'DROP INDEX BM_PRODS_EMEA_A_gsrt_country_id';
                execute immediate 'DROP INDEX BT_PRODS_EMEA_A_gsrt_duns_no';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt indexes', sysdate,'DROPPED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate '
                    CREATE Unique INDEX BT_PRODS_EMEA_A_gsrt_duns_no ON PRODS_EMEA_A_gsrt (  duns_number  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_PRODS_EMEA_A_gsrt_org_id ON PRODS_EMEA_A_gsrt (  org_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE Bitmap INDEX BM_PRODS_EMEA_A_gsrt_country_id ON PRODS_EMEA_A_gsrt (  country_id  )
                    COMPUTE STATISTICS';
                    insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt indexes', sysdate,'CREATED');
                    commit;
               EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate 'GRANT SELECT ON PRODS_EMEA_A_gsrt TO public';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt GRANT to public', sysdate, 'GRANTED');
                commit;
              exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt GRANT to public', sysdate,'NOT GRANTED - ' || err_msg);
                commit;
              end;

            end if;
        end;

END;

PROCEDURE PROC_prods_emea_flags_JUPITER
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_name varchar2(30) := 'PRODS_EMEA_FLAGS';

begin
    insert into prods_emea_log values (PRODS_EMEA_seq.NEXTVAL,'PROC_' || table_name, sysdate,'START');
    commit;
    if chrispack.is_table_populated('kcierpisz.' || table_name || '@jupiter_kcierpisz') then
        insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'kcierpisz.' || table_name || '@jupiter_kcierpisz', sysdate,'POPULATED');
        commit;

            begin
                execute immediate 'drop table ' || table_name || '2';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || '2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || '2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;
            begin
                execute immediate '
                        create table ' || table_name || '2 nologging as
                        select *
                    from kcierpisz. ' || table_name || '@jupiter_kcierpisz
                        ';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || '2 create', sysdate,'CREATED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || '2 create', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

        if chrispack.is_table_populated(table_name || '2') then
            insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || '2', sysdate,'POPULATED');
            commit;

            --- emea_optins -> emea_optins_bak

            begin
                execute immediate 'drop table ' || table_name || '_bak';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || '_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || '_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                   commit;
            end;

            begin
                execute immediate 'alter table ' || table_name || ' rename to ' || table_name || '_bak';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || ' rename -> ' || table_name || '_bak', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || ' rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;


            --- emea_inds2 -> emea_inds
            begin
                --execute immediate 'alter table emea_inds2 rename to emea_inds';
                --insert into emea_inds_log values (emea_inds_seq.NEXTVAL,'emea_inds2 rename -> emea_inds', sysdate,'RENAMED');
                execute immediate 'create table ' || table_name || ' nologging as
                                    select *
                                    from ' || table_name || '2';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || ' created from ' || table_name || '2', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || ' not created from ' || table_name || '2', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'DROP INDEX BT_' || table_name || '_duns';
                execute immediate 'DROP INDEX BT_' || table_name || '_org_id';
                execute immediate 'DROP INDEX BM_' || table_name || '_country_id';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || ' indexes', sysdate,'DROPPED');
                commit;
           EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate '
                    CREATE Unique INDEX BT_' || table_name || '_duns ON ' || table_name || ' (  duns_number  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_' || table_name || '_org_id ON ' || table_name || ' (  org_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE Bitmap INDEX BM_' || table_name || '_country_id ON ' || table_name || ' (  country_id  )
                    COMPUTE STATISTICS';
                    insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || ' indexes', sysdate,'CREATED');
                    commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


        else
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,table_name || '2', sysdate,'NOT POPULATED');

        end if;

    else

        insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'kcierpisz.' || table_name || '@jupiter_kcierpisz', sysdate,'NOT POPULATED ending');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON ' || table_name || ' TO public';
    end;

    insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'PROC_PRODS_EMEA_FLAGS', sysdate,'END');
    commit;

end;

/*
PROCEDURE PROC_PRODS_EMEA_FLAGS
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   sqlstmt varchar2(32767) := '';
BEGIN

    PROC_PRODS_EMEA_GSRT();
    PROC_PRODS_EMEA();


    sqlstmt := '
create table prods_emea_flags2 as
select
	   country_id, org_id,
max(DUNS_NUMBER) as DUNS_NUMBER,
max(PROD_DB_INST) as PROD_DB_INST,
max(TAR_DB_INST) as TAR_DB_INST,
max(DB_INST) as DB_INST,
max(PROD_DB_EE) as PROD_DB_EE,
max(TAR_DB_EE) as TAR_DB_EE,
max(DB_EE) as DB_EE,
max(PROD_DB_SE) as PROD_DB_SE,
max(TAR_DB_SE) as TAR_DB_SE,
max(DB_SE) as DB_SE,
max(PROD_DB_SEO) as PROD_DB_SEO,
max(TAR_DB_SEO) as TAR_DB_SEO,
max(DB_SEO) as DB_SEO,
max(PROD_DB_ENTERPRISE_MGMT) as PROD_DB_ENTERPRISE_MGMT,
max(TAR_DB_ENTERPRISE_MGMT) as TAR_DB_ENTERPRISE_MGMT,
max(DB_ENTERPRISE_MGMT) as DB_ENTERPRISE_MGMT,
max(PROD_APPLICATIONS) as PROD_APPLICATIONS,
max(APPLICATIONS) as APPLICATIONS,
max(TAR_APPLICATIONS) as TAR_APPLICATIONS,

max(PROD_timesten) as PROD_timesten,
max(timesten) as timesten,
max(TAR_timesten) as TAR_timesten,
max(PROD_demantra) as PROD_demantra,
max(demantra) as demantra,
max(TAR_demantra) as TAR_demantra,

max(PROD_ORACLE_CRM_APPL) as PROD_ORACLE_CRM_APPL,
max(TAR_ORACLE_CRM_APPL) as TAR_ORACLE_CRM_APPL,
max(ORACLE_CRM_APPL) as ORACLE_CRM_APPL,
max(PROD_PSFT_CRM_APPL) as PROD_PSFT_CRM_APPL,
max(TAR_PSFT_CRM_APPL) as TAR_PSFT_CRM_APPL,
max(PSFT_CRM_APPL) as PSFT_CRM_APPL,
max(PROD_SIEBEL_CRM_APPL) as PROD_SIEBEL_CRM_APPL,
max(TAR_SIEBEL_CRM_APPL) as TAR_SIEBEL_CRM_APPL,
max(SIEBEL_CRM_APPL) as SIEBEL_CRM_APPL,
max(PROD_APPLIC_SERVERS) as PROD_APPLIC_SERVERS,
max(TAR_APPLIC_SERVERS) as TAR_APPLIC_SERVERS,
max(APPLIC_SERVERS) as APPLIC_SERVERS,
max(PROD_IAS_EE) as PROD_IAS_EE,
max(TAR_IAS_EE) as TAR_IAS_EE,
max(IAS_EE) as IAS_EE,
max(PROD_IAS_SE) as PROD_IAS_SE,
max(TAR_IAS_SE) as TAR_IAS_SE,
max(IAS_SE) as IAS_SE,
max(PROD_IAS_JAVE_EDIT) as PROD_IAS_JAVE_EDIT,
max(TAR_IAS_JAVA_EDIT) as TAR_IAS_JAVA_EDIT,
max(IAS_JAVA_EDIT) as IAS_JAVA_EDIT,
max(PROD_BPEL_PROCESS_MGR) as PROD_BPEL_PROCESS_MGR,
max(TAR_BPEL_PROCESS_MGR) as TAR_BPEL_PROCESS_MGR,
max(BPEL_PROCESS_MGR) as BPEL_PROCESS_MGR,
max(PROD_AS_OTHER) as PROD_AS_OTHER,
max(TAR_AS_OTHER) as TAR_AS_OTHER,
max(AS_OTHER) as AS_OTHER,
max(PROD_PORTAL) as PROD_PORTAL,
max(TAR_PORTAL) as TAR_PORTAL,
max(PORTAL) as PORTAL,
max(PROD_IAS_SEO) as PROD_IAS_SEO,
max(TAR_IAS_SEO) as TAR_IAS_SEO,
max(IAS_SEO) as IAS_SEO,
max(PROD_PROV_PACK) as PROD_PROV_PACK,
max(TAR_PROV_PACK) as TAR_PROV_PACK,
max(PROV_PACK) as PROV_PACK,
max(PROD_CHA_MGMT_PACK) as PROD_CHA_MGMT_PACK,
max(TAR_CHA_MGMT_PACK) as TAR_CHA_MGMT_PACK,
max(CHA_MGMT_PACK) as CHA_MGMT_PACK,
max(PROD_ADV_SECURITY) as PROD_ADV_SECURITY,
max(TAR_ADV_SECURITY) as TAR_ADV_SECURITY,
max(ADV_SECURITY) as ADV_SECURITY,
max(PROD_LABEL_SECURITY) as PROD_LABEL_SECURITY,
max(TAR_LABEL_SECURITY) as TAR_LABEL_SECURITY,
max(LABEL_SECURITY) as LABEL_SECURITY,
max(PROD_ORA_SECUR) as PROD_ORA_SECUR,
max(TAR_ORA_SECUR) as TAR_ORA_SECUR,
max(ORA_SECUR) as ORA_SECUR,
max(PROD_ORA_SEC_BK) as PROD_ORA_SEC_BK,
max(TAR_ORA_SEC_BK) as TAR_ORA_SEC_BK,
max(ORA_SEC_BK) as ORA_SEC_BK,
max(PROD_PARTITIONING) as PROD_PARTITIONING,
max(TAR_PARTITIONING) as TAR_PARTITIONING,
max(PARTITIONING) as PARTITIONING,
max(PROD_SPATIAL) as PROD_SPATIAL,
max(TAR_SPATIAL) as TAR_SPATIAL,
max(SPATIAL) as SPATIAL,
max(PROD_DATA_MINING) as PROD_DATA_MINING,
max(TAR_DATA_MINING) as TAR_DATA_MINING,
max(DATA_MINING) as DATA_MINING,
max(PROD_EBS) as PROD_EBS,
max(TAR_EBS) as TAR_EBS,
max(EBS) as EBS,
max(PROD_EBS_SPECIAL_EDIT) as PROD_EBS_SPECIAL_EDIT,
max(TAR_EBS_SPECIAL_EDIT) as TAR_EBS_SPECIAL_EDIT,
max(EBS_SPECIAL_EDIT) as EBS_SPECIAL_EDIT,
max(PROD_TOOLS_INST) as PROD_TOOLS_INST,
max(TAR_TOOLS_INST) as TAR_TOOLS_INST,
max(TOOLS_INST) as TOOLS_INST,
max(PROD_IDS) as PROD_IDS,
max(TAR_IDS) as TAR_IDS,
max(IDS) as IDS,
max(PROD_HUMAN_RES) as PROD_HUMAN_RES,
max(TAR_HUMAN_RES) as TAR_HUMAN_RES,
max(HUMAN_RES) as HUMAN_RES,
max(PROD_BI_TIER4) as PROD_BI_TIER4,
max(TAR_BI_TIER4) as TAR_BI_TIER4,
max(BI_TIER4) as BI_TIER4,
max(PROD_BI_SE) as PROD_BI_SE,
max(TAR_BI_SE) as TAR_BI_SE,
max(BI_SE) as BI_SE,
max(PROD_BI_TECH) as PROD_BI_TECH,
max(TAR_BI_TECH) as TAR_BI_TECH,
max(BI_TECH) as BI_TECH,
max(PROD_E_BI) as PROD_E_BI,
max(TAR_E_BI) as TAR_E_BI,
max(E_BI) as E_BI,
max(PROD_COLLABORATION) as PROD_COLLABORATION,
max(TAR_COLLABORATION) as TAR_COLLABORATION,
max(COLLABORATION) as COLLABORATION,
max(PROD_COLLABORATION_SUITE) as PROD_COLLABORATION_SUITE,
max(TAR_COLLABORATION_SUITE) as TAR_COLLABORATION_SUITE,
max(COLLABORATION_SUITE) as COLLABORATION_SUITE,
max(PROD_RECORDS_MGMT) as PROD_RECORDS_MGMT,
max(TAR_RECORDS_MGMT) as TAR_RECORDS_MGMT,
max(RECORDS_MGMT) as RECORDS_MGMT,
max(PROD_CONTENT_SERIVCES) as PROD_CONTENT_SERIVCES,
max(TAR_CONTENT_SERIVCES) as TAR_CONTENT_SERIVCES,
max(CONTENT_SERIVCES) as CONTENT_SERIVCES,
max(PROD_JDE) as PROD_JDE,
max(PROD_MANUFACTURING) as PROD_MANUFACTURING,
max(TAR_MANUFACTURING) as TAR_MANUFACTURING,
max(MANUFACTURING) as MANUFACTURING,
max(PROD_ORDER_MGMT) as PROD_ORDER_MGMT,
max(TAR_ORDER_MGMT) as TAR_ORDER_MGMT,
max(ORDER_MGMT) as ORDER_MGMT,
max(PROD_FINANCIALS) as PROD_FINANCIALS,
max(TAR_FINANCIALS) as TAR_FINANCIALS,
max(FINANCIALS) as FINANCIALS,
max(PROD_PROCUREMENT) as PROD_PROCUREMENT,
max(TAR_PROCUREMENT) as TAR_PROCUREMENT,
max(PROCUREMENT) as PROCUREMENT,
max(PROD_SCM) as PROD_SCM,
max(TAR_SCM) as TAR_SCM,
max(SCM) as SCM,
max(PROD_PSFT_ENTERPRISE_ERP) as PROD_PSFT_ENTERPRISE_ERP,
max(TAR_PSFT_ENTERPRISE_ERP) as TAR_PSFT_ENTERPRISE_ERP,
max(PSFT_ENTERPRISE_ERP) as PSFT_ENTERPRISE_ERP,
max(PROD_JDE_WORLD_ERP) as PROD_JDE_WORLD_ERP,
max(TAR_JDE_WORLD_ERP) as TAR_JDE_WORLD_ERP,
max(JDE_WORLD_ERP) as JDE_WORLD_ERP,
max(PROD_JDE_ENTERPRISEONE_ERP) as PROD_JDE_ENTERPRISEONE_ERP,
max(TAR_JDE_ENTERPRISEONE_ERP) as TAR_JDE_ENTERPRISEONE_ERP,
max(JDE_ENTERPRISEONE_ERP) as JDE_ENTERPRISEONE_ERP,
max(PROD_SIEBEL_ANALYTICS) as PROD_SIEBEL_ANALYTICS,
max(TAR_SIEBEL_ANALYTICS) as TAR_SIEBEL_ANALYTICS,
max(SIEBEL_ANALYTICS) as SIEBEL_ANALYTICS,
max(PROD_ACQUIR_RETAIL_APPL) as PROD_ACQUIR_RETAIL_APPL,
max(TAR_ACQUIR_RETAIL_APPL) as TAR_ACQUIR_RETAIL_APPL,
max(ACQUIR_RETAIL_APPL) as ACQUIR_RETAIL_APPL,
max(PROD_ORACLE_RETAIL_APPL) as PROD_ORACLE_RETAIL_APPL,
max(TAR_ORACLE_RETAIL_APPL) as TAR_ORACLE_RETAIL_APPL,
max(ORACLE_RETAIL_APPL) as ORACLE_RETAIL_APPL,
max(PROD_ORACLE_VERTICAL_APPL) as PROD_ORACLE_VERTICAL_APPL,
max(TAR_ORACLE_VERTICAL_APPL) as TAR_ORACLE_VERTICAL_APPL,
max(ORACLE_VERTICAL_APPL) as ORACLE_VERTICAL_APPL,
max(PROD_MIDDLEWARE) as PROD_MIDDLEWARE,
max(TAR_MIDDLEWARE) as TAR_MIDDLEWARE,
max(MIDDLEWARE) as MIDDLEWARE,
max(PROD_IDENTITY_MGMT) as PROD_IDENTITY_MGMT,
max(TAR_IDENTITY_MGMT) as TAR_IDENTITY_MGMT,
max(IDENTITY_MGMT) as IDENTITY_MGMT,
max(PROD_FORMS_AND_REPORTS) as PROD_FORMS_AND_REPORTS,
max(TAR_FORMS_AND_REPORTS) as TAR_FORMS_AND_REPORTS,
max(FORMS_AND_REPORTS) as FORMS_AND_REPORTS,
max(PROD_OLAP) as PROD_OLAP,
max(TAR_OLAP) as TAR_OLAP,
max(OLAP) as OLAP,
max(PROD_DATA_WAREHOUSE) as PROD_DATA_WAREHOUSE,
max(TAR_DATA_WAREHOUSE) as TAR_DATA_WAREHOUSE,
max(DATA_WAREHOUSE) as DATA_WAREHOUSE,
max(PROD_RAC) as PROD_RAC,
max(TAR_RAC) as TAR_RAC,
max(RAC) as RAC,
max(PROD_TUNING_PACK) as PROD_TUNING_PACK,
max(TAR_TUNING_PACK) as TAR_TUNING_PACK,
max(TUNING_PACK) as TUNING_PACK,
max(PROD_DIAGNOSTICS_PACK) as PROD_DIAGNOSTICS_PACK,
max(TAR_DIAGNOSTICS_PACK) as TAR_DIAGNOSTICS_PACK,
max(DIAGNOSTICS_PACK) as DIAGNOSTICS_PACK,
max(PROD_SECURE_BACKUP) as PROD_SECURE_BACKUP,
max(TAR_SECURE_BACKUP) as TAR_SECURE_BACKUP,
max(SECURE_BACKUP) as SECURE_BACKUP,
max(PROD_SECURE_ENTERP_SEARCH) as PROD_SECURE_ENTERP_SEARCH,
max(TAR_SECURE_ENTERP_SEARCH) as TAR_SECURE_ENTERP_SEARCH,
max(SECURE_ENTERP_SEARCH) as SECURE_ENTERP_SEARCH,
max(PROD_LEARNING_MGMT) as PROD_LEARNING_MGMT,
max(TAR_LEARNING_MGMT) as TAR_LEARNING_MGMT,
max(psft_vertical_apps) as psft_vertical_apps,
max(LEARNING_MGMT) as LEARNING_MGMT,
max(PROD_DB2) as PROD_DB2,
max(PROD_AS400) as PROD_AS400,
max(PROD_MSSQL) as PROD_MSSQL,
max(PROD_SYBASE) as PROD_SYBASE,
max(PROD_MYSQL) as PROD_MYSQL,
max(PROD_INGRES) as PROD_INGRES,
max(PROD_SAP) as PROD_SAP,
max(PROD_QAD) as PROD_QAD,
max(PROD_MICROSOFT) as PROD_MICROSOFT,
max(PROD_BAAN) as PROD_BAAN
from
(
select
       a.country_id, a.org_id, a.duns_number, null as prod_db_inst,
       null as tar_db_inst, a.db_inst, null as prod_db_ee, null as tar_db_ee, a.db_ee,
       null as prod_db_se, null as tar_db_se, a.db_se, null as prod_db_seo, null as tar_db_seo,
       a.db_seo, null as prod_db_enterprise_mgmt, null as tar_db_enterprise_mgmt,
       a.db_enterprise_mgmt, null as prod_applications, a.applications,
       null as tar_applications,
       null as prod_timesten,
       null as tar_timesten, a.timesten,
       null as prod_demantra,
       null as tar_demantra, a.demantra,
       null as prod_oracle_crm_appl,
       null as tar_oracle_crm_appl, a.oracle_crm_appl, null as prod_psft_crm_appl,
       null as tar_psft_crm_appl, a.psft_crm_appl, null as prod_siebel_crm_appl,
       null as tar_siebel_crm_appl, a.siebel_crm_appl, null as prod_applic_servers,
       null as tar_applic_servers, a.applic_servers, null as prod_ias_ee,
       null as tar_ias_ee, a.ias_ee, null as prod_ias_se, null as tar_ias_se, a.ias_se,
       null as prod_ias_jave_edit, null as tar_ias_java_edit, a.ias_java_edit,
       null as prod_bpel_process_mgr, null as tar_bpel_process_mgr,
       a.bpel_process_mgr, null as prod_as_other, null as tar_as_other, a.as_other,
       null as prod_portal, null as tar_portal, a.portal, null as prod_ias_seo,
       null as tar_ias_seo, a.ias_seo, null as prod_prov_pack, null as tar_prov_pack,
       a.prov_pack, null as prod_cha_mgmt_pack, null as tar_cha_mgmt_pack,
       a.cha_mgmt_pack, null as prod_adv_security, null as tar_adv_security,
       a.adv_security, null as prod_label_security, null as tar_label_security,
       a.label_security, null as prod_ora_secur, null as tar_ora_secur, a.ora_secur,
       null as prod_ora_sec_bk, null as tar_ora_sec_bk, a.ora_sec_bk,
       null as prod_partitioning, null as tar_partitioning, a.partitioning,
       null as prod_spatial, null as tar_spatial, a.spatial, null as prod_data_mining,
       null as tar_data_mining, a.data_mining, null as prod_ebs, null as tar_ebs, a.ebs,
       null as prod_ebs_special_edit, null as tar_ebs_special_edit,
       a.ebs_special_edit, null as prod_tools_inst, null as tar_tools_inst,
       a.tools_inst, null as prod_ids, null as tar_ids, a.ids, null as prod_human_res,
       null as tar_human_res, a.human_res,
       null as prod_bi_tier4, null as tar_bi_tier4, a.bi_tier4,
       null as prod_bi_se, null as tar_bi_se, a.bi_se,
       null as prod_bi_tech, null as tar_bi_tech, a.bi_tech, null as prod_e_bi,
       null as tar_e_bi, a.e_bi, null as prod_collaboration, null as tar_collaboration,
       a.collaboration, null as prod_collaboration_suite,
       null as tar_collaboration_suite, a.collaboration_suite,
       null as prod_records_mgmt, null as tar_records_mgmt, a.records_mgmt,
       null as prod_content_serivces, null as tar_content_serivces,
       a.content_serivces, null as prod_jde, null as prod_manufacturing,
       null as tar_manufacturing, a.manufacturing, null as prod_order_mgmt,
       null as tar_order_mgmt, a.order_mgmt, null as prod_financials,
       null as tar_financials, a.financials, null as prod_procurement,
       null as tar_procurement, a.procurement, null as prod_scm, null as tar_scm, a.scm,
       null as prod_psft_enterprise_erp, null as tar_psft_enterprise_erp,
       a.psft_enterprise_erp,
        null as prod_jde_world_erp,
       null as tar_jde_world_erp, a.jde_world_erp,
       null as prod_jde_enterpriseone_erp,
       null as tar_jde_enterpriseone_erp, a.jde_enterpriseone_erp,
       null as prod_siebel_analytics, null as tar_siebel_analytics,
       a.siebel_analytics, null as prod_acquir_retail_appl,
       null as tar_acquir_retail_appl, a.acquir_retail_appl,
       null as prod_oracle_retail_appl, null as tar_oracle_retail_appl,
       a.oracle_retail_appl, null as prod_oracle_vertical_appl,
       null as tar_oracle_vertical_appl, a.oracle_vertical_appl,
       null as prod_middleware, null as tar_middleware, a.middleware,
       null as prod_identity_mgmt, null as tar_identity_mgmt, a.identity_mgmt,
       null as prod_forms_and_reports, null as tar_forms_and_reports,
       a.forms_and_reports, null as prod_olap, null as tar_olap, a.olap,
       null as prod_data_warehouse, null as tar_data_warehouse, a.data_warehouse,
       null as prod_rac, null as tar_rac, a.rac, null as prod_tuning_pack,
       null as tar_tuning_pack, a.tuning_pack, null as prod_diagnostics_pack,
       null as tar_diagnostics_pack, a.diagnostics_pack, null as prod_secure_backup,
       null as tar_secure_backup, a.secure_backup,
       null as prod_secure_enterp_search, null as tar_secure_enterp_search,
       a.secure_enterp_search,
	   a.psft_vertical_apps,
	   null as prod_learning_mgmt,
       null as tar_learning_mgmt, a.learning_mgmt, null as prod_db2, null as prod_as400,
       null as prod_mssql, null as prod_sybase, null as prod_mysql, null as prod_ingres,
       null as prod_sap, null as prod_qad, null as prod_microsoft, null as prod_baan
from prods_emea_gsrt a
union all
select
       a.country_id, a.org_id, a.duns_number, a.prod_db_inst,
       a.tar_db_inst, a.db_inst, a.prod_db_ee, a.tar_db_ee, a.db_ee,
       a.prod_db_se, a.tar_db_se, a.db_se, a.prod_db_seo, a.tar_db_seo,
       a.db_seo, a.prod_db_enterprise_mgmt, a.tar_db_enterprise_mgmt,
       a.db_enterprise_mgmt, a.prod_applications, a.applications,
       a.tar_applications,
       a.prod_timesten,
       a.tar_timesten, a.timesten,
       a.prod_demantra,
       a.tar_demantra, a.demantra,
       a.prod_oracle_crm_appl,
       a.tar_oracle_crm_appl, a.oracle_crm_appl, a.prod_psft_crm_appl,
       a.tar_psft_crm_appl, a.psft_crm_appl, a.prod_siebel_crm_appl,
       a.tar_siebel_crm_appl, a.siebel_crm_appl, a.prod_applic_servers,
       a.tar_applic_servers, a.applic_servers, a.prod_ias_ee,
       a.tar_ias_ee, a.ias_ee, a.prod_ias_se, a.tar_ias_se, a.ias_se,
       a.prod_ias_jave_edit, a.tar_ias_java_edit, a.ias_java_edit,
       a.prod_bpel_process_mgr, a.tar_bpel_process_mgr,
       a.bpel_process_mgr, a.prod_as_other, a.tar_as_other, a.as_other,
       a.prod_portal, a.tar_portal, a.portal, a.prod_ias_seo,
       a.tar_ias_seo, a.ias_seo, a.prod_prov_pack, a.tar_prov_pack,
       a.prov_pack, a.prod_cha_mgmt_pack, a.tar_cha_mgmt_pack,
       a.cha_mgmt_pack, a.prod_adv_security, a.tar_adv_security,
       a.adv_security, a.prod_label_security, a.tar_label_security,
       a.label_security, a.prod_ora_secur, a.tar_ora_secur, a.ora_secur,
       a.prod_ora_sec_bk, a.tar_ora_sec_bk, a.ora_sec_bk,
       a.prod_partitioning, a.tar_partitioning, a.partitioning,
       a.prod_spatial, a.tar_spatial, a.spatial, a.prod_data_mining,
       a.tar_data_mining, a.data_mining, a.prod_ebs, a.tar_ebs, a.ebs,
       a.prod_ebs_special_edit, a.tar_ebs_special_edit,
       a.ebs_special_edit, a.prod_tools_inst, a.tar_tools_inst,
       a.tools_inst, a.prod_ids, a.tar_ids, a.ids, a.prod_human_res,
       a.tar_human_res, a.human_res,
       a.prod_bi_tier4, a.tar_bi_tier4, a.bi_tier4,
       a.prod_bi_se, a.tar_bi_se, a.bi_se,
       a.prod_bi_tech, a.tar_bi_tech, a.bi_tech, a.prod_e_bi,
       a.tar_e_bi, a.e_bi, a.prod_collaboration, a.tar_collaboration,
       a.collaboration, a.prod_collaboration_suite,
       a.tar_collaboration_suite, a.collaboration_suite,
       a.prod_records_mgmt, a.tar_records_mgmt, a.records_mgmt,
       a.prod_content_serivces, a.tar_content_serivces,
       a.content_serivces, a.prod_jde, a.prod_manufacturing,
       a.tar_manufacturing, a.manufacturing, a.prod_order_mgmt,
       a.tar_order_mgmt, a.order_mgmt, a.prod_financials,
       a.tar_financials, a.financials, a.prod_procurement,
       a.tar_procurement, a.procurement, a.prod_scm, a.tar_scm, a.scm,
       a.prod_psft_enterprise_erp, a.tar_psft_enterprise_erp,
       a.psft_enterprise_erp,
        a.prod_jde_world_erp,
       a.tar_jde_world_erp, a.jde_world_erp,
       a.prod_jde_enterpriseone_erp,
       a.tar_jde_enterpriseone_erp, a.jde_enterpriseone_erp,
       a.prod_siebel_analytics, a.tar_siebel_analytics,
       a.siebel_analytics, a.prod_acquir_retail_appl,
       a.tar_acquir_retail_appl, a.acquir_retail_appl,
       a.prod_oracle_retail_appl, a.tar_oracle_retail_appl,
       a.oracle_retail_appl, a.prod_oracle_vertical_appl,
       a.tar_oracle_vertical_appl, a.oracle_vertical_appl,
       a.prod_middleware, a.tar_middleware, a.middleware,
       a.prod_identity_mgmt, a.tar_identity_mgmt, a.identity_mgmt,
       a.prod_forms_and_reports, a.tar_forms_and_reports,
       a.forms_and_reports, a.prod_olap, a.tar_olap, a.olap,
       a.prod_data_warehouse, a.tar_data_warehouse, a.data_warehouse,
       a.prod_rac, a.tar_rac, a.rac, a.prod_tuning_pack,
       a.tar_tuning_pack, a.tuning_pack, a.prod_diagnostics_pack,
       a.tar_diagnostics_pack, a.diagnostics_pack, a.prod_secure_backup,
       a.tar_secure_backup, a.secure_backup,
       a.prod_secure_enterp_search, a.tar_secure_enterp_search,
       a.secure_enterp_search,
	     	   null as psft_vertical_apps,
	   a.prod_learning_mgmt,
       a.tar_learning_mgmt, a.learning_mgmt, a.prod_db2, a.prod_as400,
       a.prod_mssql, a.prod_sybase, a.prod_mysql, a.prod_ingres,
       a.prod_sap, a.prod_qad, a.prod_microsoft, a.prod_baan
from prods_emea a
)
group by country_id, org_id';


            begin
                execute immediate 'drop table prods_emea_FLAGS2';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_FLAGS2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_FLAGS2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

        begin
            if is_table_populated('PRODS_EMEA_GSRT') and is_table_populated('PRODS_EMEA') then
                execute immediate sqlstmt;
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_FLAGS2 create', sysdate,'CREATED');
            end if;
        exception when others then
            err_msg := SUBSTR(SQLERRM, 1, 100);
            insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'GSRT prods_emea_gsrt2 create', sysdate,'!! '  || err_msg);
        end;

        begin
            if is_table_populated('prods_emea_flags2') then

                -- drop prods_emea_bak
              begin
                execute immediate 'drop table prods_emea_flags_bak';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_flags_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_flags_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                    commit;
              end;

                -- prods_emea -> prods_emea_bak
              begin
                execute immediate 'alter table prods_emea_flags rename to prods_emea_flags_bak';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_flags rename -> prods_emea_flags_bak', sysdate,'RENAMED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_flags rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
              end;

                -- prods_emea2 -> prods_emea
              begin
                execute immediate 'alter table prods_emea_flags2 rename to prods_emea_flags';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_flags2 rename -> prods_emea_flags', sysdate,'RENAMED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_flags2 rename -> prods_emea_flags', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate 'DROP INDEX BT_prods_emea_flags_org_id';
                execute immediate 'DROP INDEX BM_prods_emea_flags_country_id';
                execute immediate 'DROP INDEX BT_prods_emea_flags_duns_no';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_flags indexes', sysdate,'DROPPED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_flags indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate '
                    CREATE Unique INDEX BT_prods_emea_flags_duns_no ON prods_emea_flags (  duns_number  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_prods_emea_flags_org_id ON prods_emea_flags (  org_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE Bitmap INDEX BM_prods_emea_flags_country_id ON prods_emea_flags (  country_id  )
                    COMPUTE STATISTICS';
                    insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_flags indexes', sysdate,'CREATED');
                    commit;
               EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_flags indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate 'GRANT SELECT ON prods_emea_flags TO public';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_flags GRANT to public', sysdate, 'GRANTED');
                commit;
              exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'FLAGS prods_emea_flags GRANT to public', sysdate,'NOT GRANTED - ' || err_msg);
                commit;
              end;

            end if;
        end;

END;
*/

PROCEDURE PROC_PRODS_EMEA_A_FLAGS
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   sqlstmt varchar2(32767) := '';
BEGIN
    PROC_PRODS_EMEA_A_GSRT();
    PROC_PRODS_EMEA_A();


    sqlstmt := '
create table PRODS_EMEA_A_flags2 as
select
	   country_id, org_id,
max(DUNS_NUMBER) as DUNS_NUMBER,
max(PROD_DB_INST) as PROD_DB_INST,
max(TAR_DB_INST) as TAR_DB_INST,
max(DB_INST) as DB_INST,
max(PROD_DB_EE) as PROD_DB_EE,
max(TAR_DB_EE) as TAR_DB_EE,
max(DB_EE) as DB_EE,
max(PROD_DB_SE) as PROD_DB_SE,
max(TAR_DB_SE) as TAR_DB_SE,
max(DB_SE) as DB_SE,
max(PROD_DB_SEO) as PROD_DB_SEO,
max(TAR_DB_SEO) as TAR_DB_SEO,
max(DB_SEO) as DB_SEO,
max(PROD_DB_ENTERPRISE_MGMT) as PROD_DB_ENTERPRISE_MGMT,
max(TAR_DB_ENTERPRISE_MGMT) as TAR_DB_ENTERPRISE_MGMT,
max(DB_ENTERPRISE_MGMT) as DB_ENTERPRISE_MGMT,
max(PROD_APPLICATIONS) as PROD_APPLICATIONS,
max(APPLICATIONS) as APPLICATIONS,
max(TAR_APPLICATIONS) as TAR_APPLICATIONS,

max(PROD_timesten) as PROD_timesten,
max(timesten) as timesten,
max(TAR_timesten) as TAR_timesten,
max(PROD_demantra) as PROD_demantra,
max(demantra) as demantra,
max(TAR_demantra) as TAR_demantra,

max(PROD_ORACLE_CRM_APPL) as PROD_ORACLE_CRM_APPL,
max(TAR_ORACLE_CRM_APPL) as TAR_ORACLE_CRM_APPL,
max(ORACLE_CRM_APPL) as ORACLE_CRM_APPL,
max(PROD_PSFT_CRM_APPL) as PROD_PSFT_CRM_APPL,
max(TAR_PSFT_CRM_APPL) as TAR_PSFT_CRM_APPL,
max(PSFT_CRM_APPL) as PSFT_CRM_APPL,
max(PROD_SIEBEL_CRM_APPL) as PROD_SIEBEL_CRM_APPL,
max(TAR_SIEBEL_CRM_APPL) as TAR_SIEBEL_CRM_APPL,
max(SIEBEL_CRM_APPL) as SIEBEL_CRM_APPL,
max(PROD_APPLIC_SERVERS) as PROD_APPLIC_SERVERS,
max(TAR_APPLIC_SERVERS) as TAR_APPLIC_SERVERS,
max(APPLIC_SERVERS) as APPLIC_SERVERS,
max(PROD_IAS_EE) as PROD_IAS_EE,
max(TAR_IAS_EE) as TAR_IAS_EE,
max(IAS_EE) as IAS_EE,
max(PROD_IAS_SE) as PROD_IAS_SE,
max(TAR_IAS_SE) as TAR_IAS_SE,
max(IAS_SE) as IAS_SE,
max(PROD_IAS_JAVE_EDIT) as PROD_IAS_JAVE_EDIT,
max(TAR_IAS_JAVA_EDIT) as TAR_IAS_JAVA_EDIT,
max(IAS_JAVA_EDIT) as IAS_JAVA_EDIT,
max(PROD_BPEL_PROCESS_MGR) as PROD_BPEL_PROCESS_MGR,
max(TAR_BPEL_PROCESS_MGR) as TAR_BPEL_PROCESS_MGR,
max(BPEL_PROCESS_MGR) as BPEL_PROCESS_MGR,
max(PROD_AS_OTHER) as PROD_AS_OTHER,
max(TAR_AS_OTHER) as TAR_AS_OTHER,
max(AS_OTHER) as AS_OTHER,
max(PROD_PORTAL) as PROD_PORTAL,
max(TAR_PORTAL) as TAR_PORTAL,
max(PORTAL) as PORTAL,
max(PROD_IAS_SEO) as PROD_IAS_SEO,
max(TAR_IAS_SEO) as TAR_IAS_SEO,
max(IAS_SEO) as IAS_SEO,
max(PROD_PROV_PACK) as PROD_PROV_PACK,
max(TAR_PROV_PACK) as TAR_PROV_PACK,
max(PROV_PACK) as PROV_PACK,
max(PROD_CHA_MGMT_PACK) as PROD_CHA_MGMT_PACK,
max(TAR_CHA_MGMT_PACK) as TAR_CHA_MGMT_PACK,
max(CHA_MGMT_PACK) as CHA_MGMT_PACK,
max(PROD_ADV_SECURITY) as PROD_ADV_SECURITY,
max(TAR_ADV_SECURITY) as TAR_ADV_SECURITY,
max(ADV_SECURITY) as ADV_SECURITY,
max(PROD_LABEL_SECURITY) as PROD_LABEL_SECURITY,
max(TAR_LABEL_SECURITY) as TAR_LABEL_SECURITY,
max(LABEL_SECURITY) as LABEL_SECURITY,
max(PROD_ORA_SECUR) as PROD_ORA_SECUR,
max(TAR_ORA_SECUR) as TAR_ORA_SECUR,
max(ORA_SECUR) as ORA_SECUR,
max(PROD_ORA_SEC_BK) as PROD_ORA_SEC_BK,
max(TAR_ORA_SEC_BK) as TAR_ORA_SEC_BK,
max(ORA_SEC_BK) as ORA_SEC_BK,
max(PROD_PARTITIONING) as PROD_PARTITIONING,
max(TAR_PARTITIONING) as TAR_PARTITIONING,
max(PARTITIONING) as PARTITIONING,
max(PROD_SPATIAL) as PROD_SPATIAL,
max(TAR_SPATIAL) as TAR_SPATIAL,
max(SPATIAL) as SPATIAL,
max(PROD_DATA_MINING) as PROD_DATA_MINING,
max(TAR_DATA_MINING) as TAR_DATA_MINING,
max(DATA_MINING) as DATA_MINING,
max(PROD_EBS) as PROD_EBS,
max(TAR_EBS) as TAR_EBS,
max(EBS) as EBS,
max(PROD_EBS_SPECIAL_EDIT) as PROD_EBS_SPECIAL_EDIT,
max(TAR_EBS_SPECIAL_EDIT) as TAR_EBS_SPECIAL_EDIT,
max(EBS_SPECIAL_EDIT) as EBS_SPECIAL_EDIT,
max(PROD_TOOLS_INST) as PROD_TOOLS_INST,
max(TAR_TOOLS_INST) as TAR_TOOLS_INST,
max(TOOLS_INST) as TOOLS_INST,
max(PROD_IDS) as PROD_IDS,
max(TAR_IDS) as TAR_IDS,
max(IDS) as IDS,
max(PROD_HUMAN_RES) as PROD_HUMAN_RES,
max(TAR_HUMAN_RES) as TAR_HUMAN_RES,
max(HUMAN_RES) as HUMAN_RES,
max(PROD_BI_TIER4) as PROD_BI_TIER4,
max(TAR_BI_TIER4) as TAR_BI_TIER4,
max(BI_TIER4) as BI_TIER4,
max(PROD_BI_SE) as PROD_BI_SE,
max(TAR_BI_SE) as TAR_BI_SE,
max(BI_SE) as BI_SE,
max(PROD_BI_TECH) as PROD_BI_TECH,
max(TAR_BI_TECH) as TAR_BI_TECH,
max(BI_TECH) as BI_TECH,
max(PROD_E_BI) as PROD_E_BI,
max(TAR_E_BI) as TAR_E_BI,
max(E_BI) as E_BI,
max(PROD_COLLABORATION) as PROD_COLLABORATION,
max(TAR_COLLABORATION) as TAR_COLLABORATION,
max(COLLABORATION) as COLLABORATION,
max(PROD_COLLABORATION_SUITE) as PROD_COLLABORATION_SUITE,
max(TAR_COLLABORATION_SUITE) as TAR_COLLABORATION_SUITE,
max(COLLABORATION_SUITE) as COLLABORATION_SUITE,
max(PROD_RECORDS_MGMT) as PROD_RECORDS_MGMT,
max(TAR_RECORDS_MGMT) as TAR_RECORDS_MGMT,
max(RECORDS_MGMT) as RECORDS_MGMT,
max(PROD_CONTENT_SERIVCES) as PROD_CONTENT_SERIVCES,
max(TAR_CONTENT_SERIVCES) as TAR_CONTENT_SERIVCES,
max(CONTENT_SERIVCES) as CONTENT_SERIVCES,
max(PROD_JDE) as PROD_JDE,
max(PROD_MANUFACTURING) as PROD_MANUFACTURING,
max(TAR_MANUFACTURING) as TAR_MANUFACTURING,
max(MANUFACTURING) as MANUFACTURING,
max(PROD_ORDER_MGMT) as PROD_ORDER_MGMT,
max(TAR_ORDER_MGMT) as TAR_ORDER_MGMT,
max(ORDER_MGMT) as ORDER_MGMT,
max(PROD_FINANCIALS) as PROD_FINANCIALS,
max(TAR_FINANCIALS) as TAR_FINANCIALS,
max(FINANCIALS) as FINANCIALS,
max(PROD_PROCUREMENT) as PROD_PROCUREMENT,
max(TAR_PROCUREMENT) as TAR_PROCUREMENT,
max(PROCUREMENT) as PROCUREMENT,
max(PROD_SCM) as PROD_SCM,
max(TAR_SCM) as TAR_SCM,
max(SCM) as SCM,
max(PROD_PSFT_ENTERPRISE_ERP) as PROD_PSFT_ENTERPRISE_ERP,
max(TAR_PSFT_ENTERPRISE_ERP) as TAR_PSFT_ENTERPRISE_ERP,
max(PSFT_ENTERPRISE_ERP) as PSFT_ENTERPRISE_ERP,
max(PROD_JDE_WORLD_ERP) as PROD_JDE_WORLD_ERP,
max(TAR_JDE_WORLD_ERP) as TAR_JDE_WORLD_ERP,
max(JDE_WORLD_ERP) as JDE_WORLD_ERP,
max(PROD_JDE_ENTERPRISEONE_ERP) as PROD_JDE_ENTERPRISEONE_ERP,
max(TAR_JDE_ENTERPRISEONE_ERP) as TAR_JDE_ENTERPRISEONE_ERP,
max(JDE_ENTERPRISEONE_ERP) as JDE_ENTERPRISEONE_ERP,
max(PROD_SIEBEL_ANALYTICS) as PROD_SIEBEL_ANALYTICS,
max(TAR_SIEBEL_ANALYTICS) as TAR_SIEBEL_ANALYTICS,
max(SIEBEL_ANALYTICS) as SIEBEL_ANALYTICS,
max(PROD_ACQUIR_RETAIL_APPL) as PROD_ACQUIR_RETAIL_APPL,
max(TAR_ACQUIR_RETAIL_APPL) as TAR_ACQUIR_RETAIL_APPL,
max(ACQUIR_RETAIL_APPL) as ACQUIR_RETAIL_APPL,
max(PROD_ORACLE_RETAIL_APPL) as PROD_ORACLE_RETAIL_APPL,
max(TAR_ORACLE_RETAIL_APPL) as TAR_ORACLE_RETAIL_APPL,
max(ORACLE_RETAIL_APPL) as ORACLE_RETAIL_APPL,
max(PROD_ORACLE_VERTICAL_APPL) as PROD_ORACLE_VERTICAL_APPL,
max(TAR_ORACLE_VERTICAL_APPL) as TAR_ORACLE_VERTICAL_APPL,
max(ORACLE_VERTICAL_APPL) as ORACLE_VERTICAL_APPL,
max(PROD_MIDDLEWARE) as PROD_MIDDLEWARE,
max(TAR_MIDDLEWARE) as TAR_MIDDLEWARE,
max(MIDDLEWARE) as MIDDLEWARE,
max(PROD_IDENTITY_MGMT) as PROD_IDENTITY_MGMT,
max(TAR_IDENTITY_MGMT) as TAR_IDENTITY_MGMT,
max(IDENTITY_MGMT) as IDENTITY_MGMT,
max(PROD_FORMS_AND_REPORTS) as PROD_FORMS_AND_REPORTS,
max(TAR_FORMS_AND_REPORTS) as TAR_FORMS_AND_REPORTS,
max(FORMS_AND_REPORTS) as FORMS_AND_REPORTS,
max(PROD_OLAP) as PROD_OLAP,
max(TAR_OLAP) as TAR_OLAP,
max(OLAP) as OLAP,
max(PROD_DATA_WAREHOUSE) as PROD_DATA_WAREHOUSE,
max(TAR_DATA_WAREHOUSE) as TAR_DATA_WAREHOUSE,
max(DATA_WAREHOUSE) as DATA_WAREHOUSE,
max(PROD_RAC) as PROD_RAC,
max(TAR_RAC) as TAR_RAC,
max(RAC) as RAC,
max(PROD_TUNING_PACK) as PROD_TUNING_PACK,
max(TAR_TUNING_PACK) as TAR_TUNING_PACK,
max(TUNING_PACK) as TUNING_PACK,
max(PROD_DIAGNOSTICS_PACK) as PROD_DIAGNOSTICS_PACK,
max(TAR_DIAGNOSTICS_PACK) as TAR_DIAGNOSTICS_PACK,
max(DIAGNOSTICS_PACK) as DIAGNOSTICS_PACK,
max(PROD_SECURE_BACKUP) as PROD_SECURE_BACKUP,
max(TAR_SECURE_BACKUP) as TAR_SECURE_BACKUP,
max(SECURE_BACKUP) as SECURE_BACKUP,
max(PROD_SECURE_ENTERP_SEARCH) as PROD_SECURE_ENTERP_SEARCH,
max(TAR_SECURE_ENTERP_SEARCH) as TAR_SECURE_ENTERP_SEARCH,
max(SECURE_ENTERP_SEARCH) as SECURE_ENTERP_SEARCH,
max(PROD_LEARNING_MGMT) as PROD_LEARNING_MGMT,
max(TAR_LEARNING_MGMT) as TAR_LEARNING_MGMT,
max(psft_vertical_apps) as psft_vertical_apps,
max(LEARNING_MGMT) as LEARNING_MGMT,
max(PROD_DB2) as PROD_DB2,
max(PROD_AS400) as PROD_AS400,
max(PROD_MSSQL) as PROD_MSSQL,
max(PROD_SYBASE) as PROD_SYBASE,
max(PROD_MYSQL) as PROD_MYSQL,
max(PROD_INGRES) as PROD_INGRES,
max(PROD_SAP) as PROD_SAP,
max(PROD_QAD) as PROD_QAD,
max(PROD_MICROSOFT) as PROD_MICROSOFT,
max(PROD_BAAN) as PROD_BAAN
from
(
select
       a.country_id, a.org_id, a.duns_number, null as prod_db_inst,
       null as tar_db_inst, a.db_inst, null as prod_db_ee, null as tar_db_ee, a.db_ee,
       null as prod_db_se, null as tar_db_se, a.db_se, null as prod_db_seo, null as tar_db_seo,
       a.db_seo, null as prod_db_enterprise_mgmt, null as tar_db_enterprise_mgmt,
       a.db_enterprise_mgmt, null as prod_applications, a.applications,
       null as tar_applications,

       null as prod_timesten,
       null as tar_timesten, a.timesten,
       null as prod_demantra,
       null as tar_demantra, a.demantra,

       null as prod_oracle_crm_appl,
       null as tar_oracle_crm_appl, a.oracle_crm_appl, null as prod_psft_crm_appl,
       null as tar_psft_crm_appl, a.psft_crm_appl, null as prod_siebel_crm_appl,
       null as tar_siebel_crm_appl, a.siebel_crm_appl, null as prod_applic_servers,
       null as tar_applic_servers, a.applic_servers, null as prod_ias_ee,
       null as tar_ias_ee, a.ias_ee, null as prod_ias_se, null as tar_ias_se, a.ias_se,
       null as prod_ias_jave_edit, null as tar_ias_java_edit, a.ias_java_edit,
       null as prod_bpel_process_mgr, null as tar_bpel_process_mgr,
       a.bpel_process_mgr, null as prod_as_other, null as tar_as_other, a.as_other,
       null as prod_portal, null as tar_portal, a.portal, null as prod_ias_seo,
       null as tar_ias_seo, a.ias_seo, null as prod_prov_pack, null as tar_prov_pack,
       a.prov_pack, null as prod_cha_mgmt_pack, null as tar_cha_mgmt_pack,
       a.cha_mgmt_pack, null as prod_adv_security, null as tar_adv_security,
       a.adv_security, null as prod_label_security, null as tar_label_security,
       a.label_security, null as prod_ora_secur, null as tar_ora_secur, a.ora_secur,
       null as prod_ora_sec_bk, null as tar_ora_sec_bk, a.ora_sec_bk,
       null as prod_partitioning, null as tar_partitioning, a.partitioning,
       null as prod_spatial, null as tar_spatial, a.spatial, null as prod_data_mining,
       null as tar_data_mining, a.data_mining, null as prod_ebs, null as tar_ebs, a.ebs,
       null as prod_ebs_special_edit, null as tar_ebs_special_edit,
       a.ebs_special_edit, null as prod_tools_inst, null as tar_tools_inst,
       a.tools_inst, null as prod_ids, null as tar_ids, a.ids, null as prod_human_res,
       null as tar_human_res, a.human_res,
       null as prod_bi_tier4, null as tar_bi_tier4, a.bi_tier4,
       null as prod_bi_se, null as tar_bi_se, a.bi_se,
       null as prod_bi_tech, null as tar_bi_tech, a.bi_tech, null as prod_e_bi,
       null as tar_e_bi, a.e_bi, null as prod_collaboration, null as tar_collaboration,
       a.collaboration, null as prod_collaboration_suite,
       null as tar_collaboration_suite, a.collaboration_suite,
       null as prod_records_mgmt, null as tar_records_mgmt, a.records_mgmt,
       null as prod_content_serivces, null as tar_content_serivces,
       a.content_serivces, null as prod_jde, null as prod_manufacturing,
       null as tar_manufacturing, a.manufacturing, null as prod_order_mgmt,
       null as tar_order_mgmt, a.order_mgmt, null as prod_financials,
       null as tar_financials, a.financials, null as prod_procurement,
       null as tar_procurement, a.procurement, null as prod_scm, null as tar_scm, a.scm,
       null as prod_psft_enterprise_erp, null as tar_psft_enterprise_erp,
       a.psft_enterprise_erp,
        null as prod_jde_world_erp,
       null as tar_jde_world_erp, a.jde_world_erp,
       null as prod_jde_enterpriseone_erp,
       null as tar_jde_enterpriseone_erp, a.jde_enterpriseone_erp,
       null as prod_siebel_analytics, null as tar_siebel_analytics,
       a.siebel_analytics, null as prod_acquir_retail_appl,
       null as tar_acquir_retail_appl, a.acquir_retail_appl,
       null as prod_oracle_retail_appl, null as tar_oracle_retail_appl,
       a.oracle_retail_appl, null as prod_oracle_vertical_appl,
       null as tar_oracle_vertical_appl, a.oracle_vertical_appl,
       null as prod_middleware, null as tar_middleware, a.middleware,
       null as prod_identity_mgmt, null as tar_identity_mgmt, a.identity_mgmt,
       null as prod_forms_and_reports, null as tar_forms_and_reports,
       a.forms_and_reports, null as prod_olap, null as tar_olap, a.olap,
       null as prod_data_warehouse, null as tar_data_warehouse, a.data_warehouse,
       null as prod_rac, null as tar_rac, a.rac, null as prod_tuning_pack,
       null as tar_tuning_pack, a.tuning_pack, null as prod_diagnostics_pack,
       null as tar_diagnostics_pack, a.diagnostics_pack, null as prod_secure_backup,
       null as tar_secure_backup, a.secure_backup,
       null as prod_secure_enterp_search, null as tar_secure_enterp_search,
       a.secure_enterp_search,
	   a.psft_vertical_apps,
	   null as prod_learning_mgmt,
       null as tar_learning_mgmt, a.learning_mgmt, null as prod_db2, null as prod_as400,
       null as prod_mssql, null as prod_sybase, null as prod_mysql, null as prod_ingres,
       null as prod_sap, null as prod_qad, null as prod_microsoft, null as prod_baan
from PRODS_EMEA_A_gsrt a
union all
select
       a.country_id, a.org_id, a.duns_number, a.prod_db_inst,
       a.tar_db_inst, a.db_inst, a.prod_db_ee, a.tar_db_ee, a.db_ee,
       a.prod_db_se, a.tar_db_se, a.db_se, a.prod_db_seo, a.tar_db_seo,
       a.db_seo, a.prod_db_enterprise_mgmt, a.tar_db_enterprise_mgmt,
       a.db_enterprise_mgmt, a.prod_applications, a.applications,
       a.tar_applications,

       a.prod_timesten,
       a.tar_timesten, a.timesten,
       a.prod_demantra,
       a.tar_demantra, a.demantra,

       a.prod_oracle_crm_appl,
       a.tar_oracle_crm_appl, a.oracle_crm_appl, a.prod_psft_crm_appl,
       a.tar_psft_crm_appl, a.psft_crm_appl, a.prod_siebel_crm_appl,
       a.tar_siebel_crm_appl, a.siebel_crm_appl, a.prod_applic_servers,
       a.tar_applic_servers, a.applic_servers, a.prod_ias_ee,
       a.tar_ias_ee, a.ias_ee, a.prod_ias_se, a.tar_ias_se, a.ias_se,
       a.prod_ias_jave_edit, a.tar_ias_java_edit, a.ias_java_edit,
       a.prod_bpel_process_mgr, a.tar_bpel_process_mgr,
       a.bpel_process_mgr, a.prod_as_other, a.tar_as_other, a.as_other,
       a.prod_portal, a.tar_portal, a.portal, a.prod_ias_seo,
       a.tar_ias_seo, a.ias_seo, a.prod_prov_pack, a.tar_prov_pack,
       a.prov_pack, a.prod_cha_mgmt_pack, a.tar_cha_mgmt_pack,
       a.cha_mgmt_pack, a.prod_adv_security, a.tar_adv_security,
       a.adv_security, a.prod_label_security, a.tar_label_security,
       a.label_security, a.prod_ora_secur, a.tar_ora_secur, a.ora_secur,
       a.prod_ora_sec_bk, a.tar_ora_sec_bk, a.ora_sec_bk,
       a.prod_partitioning, a.tar_partitioning, a.partitioning,
       a.prod_spatial, a.tar_spatial, a.spatial, a.prod_data_mining,
       a.tar_data_mining, a.data_mining, a.prod_ebs, a.tar_ebs, a.ebs,
       a.prod_ebs_special_edit, a.tar_ebs_special_edit,
       a.ebs_special_edit, a.prod_tools_inst, a.tar_tools_inst,
       a.tools_inst, a.prod_ids, a.tar_ids, a.ids, a.prod_human_res,
       a.tar_human_res, a.human_res,
       a.prod_bi_tier4, a.tar_bi_tier4, a.bi_tier4,
       a.prod_bi_se, a.tar_bi_se, a.bi_se,
       a.prod_bi_tech, a.tar_bi_tech, a.bi_tech, a.prod_e_bi,
       a.tar_e_bi, a.e_bi, a.prod_collaboration, a.tar_collaboration,
       a.collaboration, a.prod_collaboration_suite,
       a.tar_collaboration_suite, a.collaboration_suite,
       a.prod_records_mgmt, a.tar_records_mgmt, a.records_mgmt,
       a.prod_content_serivces, a.tar_content_serivces,
       a.content_serivces, a.prod_jde, a.prod_manufacturing,
       a.tar_manufacturing, a.manufacturing, a.prod_order_mgmt,
       a.tar_order_mgmt, a.order_mgmt, a.prod_financials,
       a.tar_financials, a.financials, a.prod_procurement,
       a.tar_procurement, a.procurement, a.prod_scm, a.tar_scm, a.scm,
       a.prod_psft_enterprise_erp, a.tar_psft_enterprise_erp,
       a.psft_enterprise_erp,
        a.prod_jde_world_erp,
       a.tar_jde_world_erp, a.jde_world_erp,
       a.prod_jde_enterpriseone_erp,
       a.tar_jde_enterpriseone_erp, a.jde_enterpriseone_erp,
       a.prod_siebel_analytics, a.tar_siebel_analytics,
       a.siebel_analytics, a.prod_acquir_retail_appl,
       a.tar_acquir_retail_appl, a.acquir_retail_appl,
       a.prod_oracle_retail_appl, a.tar_oracle_retail_appl,
       a.oracle_retail_appl, a.prod_oracle_vertical_appl,
       a.tar_oracle_vertical_appl, a.oracle_vertical_appl,
       a.prod_middleware, a.tar_middleware, a.middleware,
       a.prod_identity_mgmt, a.tar_identity_mgmt, a.identity_mgmt,
       a.prod_forms_and_reports, a.tar_forms_and_reports,
       a.forms_and_reports, a.prod_olap, a.tar_olap, a.olap,
       a.prod_data_warehouse, a.tar_data_warehouse, a.data_warehouse,
       a.prod_rac, a.tar_rac, a.rac, a.prod_tuning_pack,
       a.tar_tuning_pack, a.tuning_pack, a.prod_diagnostics_pack,
       a.tar_diagnostics_pack, a.diagnostics_pack, a.prod_secure_backup,
       a.tar_secure_backup, a.secure_backup,
       a.prod_secure_enterp_search, a.tar_secure_enterp_search,
       a.secure_enterp_search,
	     	   null as psft_vertical_apps,
	   a.prod_learning_mgmt,
       a.tar_learning_mgmt, a.learning_mgmt, a.prod_db2, a.prod_as400,
       a.prod_mssql, a.prod_sybase, a.prod_mysql, a.prod_ingres,
       a.prod_sap, a.prod_qad, a.prod_microsoft, a.prod_baan
from PRODS_EMEA_A a
)
group by country_id, org_id';


            begin
                execute immediate 'drop table PRODS_EMEA_A_FLAGS2';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_FLAGS2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_FLAGS2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

        begin
            if is_table_populated('PRODS_EMEA_A_GSRT') and is_table_populated('PRODS_EMEA_A') then
                execute immediate sqlstmt;
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_FLAGS2 create', sysdate,'CREATED');
            end if;
        exception when others then
            err_msg := SUBSTR(SQLERRM, 1, 100);
            insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'GSRT PRODS_EMEA_A_gsrt2 create', sysdate,'!! '  || err_msg);
        end;

        begin
            if is_table_populated('PRODS_EMEA_A_flags2') then

                -- drop PRODS_EMEA_A_bak
              begin
                execute immediate 'drop table PRODS_EMEA_A_flags_bak';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_flags_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_flags_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                    commit;
              end;

                -- PRODS_EMEA_A -> PRODS_EMEA_A_bak
              begin
                execute immediate 'alter table PRODS_EMEA_A_flags rename to PRODS_EMEA_A_flags_bak';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_flags rename -> PRODS_EMEA_A_flags_bak', sysdate,'RENAMED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_flags rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
              end;

                -- PRODS_EMEA_A2 -> PRODS_EMEA_A
              begin
                execute immediate 'alter table PRODS_EMEA_A_flags2 rename to PRODS_EMEA_A_flags';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_flags2 rename -> PRODS_EMEA_A_flags', sysdate,'RENAMED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_flags2 rename -> PRODS_EMEA_A_flags', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate 'DROP INDEX BT_PRODS_EMEA_A_flags_org_id';
                execute immediate 'DROP INDEX BM_PRODS_EMEA_A_flags_country_id';
                execute immediate 'DROP INDEX BT_PRODS_EMEA_A_flags_duns_no';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_flags indexes', sysdate,'DROPPED');
                commit;
              EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_flags indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate '
                    CREATE Unique INDEX BT_PRODS_EMEA_A_flags_duns_no ON PRODS_EMEA_A_flags (  duns_number  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_PRODS_EMEA_A_flags_org_id ON PRODS_EMEA_A_flags (  org_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE Bitmap INDEX BM_PRODS_EMEA_A_flags_country_id ON PRODS_EMEA_A_flags (  country_id  )
                    COMPUTE STATISTICS';
                    insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_flags indexes', sysdate,'CREATED');
                    commit;
               EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_flags indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
              end;

              begin
                execute immediate 'GRANT SELECT ON PRODS_EMEA_A_flags TO public';
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_flags GRANT to public', sysdate, 'GRANTED');
                commit;
              exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into PRODS_EMEA_log values (PRODS_EMEA_seq.NEXTVAL,'FLAGS PRODS_EMEA_A_flags GRANT to public', sysdate,'NOT GRANTED - ' || err_msg);
                commit;
              end;

            end if;
        end;

END;

procedure PROC_PRODS_HIERARCHY
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_info_new varchar2(4000);
   table_info_bak varchar2(4000);
   no_of_rows_new number;
   no_of_rows_bak number;
   changed boolean := false; -- if the prods_hier has changed from the bak table
   no_of_changed_rows number := 0;

begin
    --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_EMEA_OPTINS', sysdate,'START');
    --commit;
    if chrispack.is_table_populated('gcd_dw.GCD_PRODUCTS') then
        --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_eu', sysdate,'POPULATED');
        --commit;

            begin
                execute immediate 'drop table PRODS_HIER2';
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2 drop', sysdate,'DROPPED');
                --commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                --commit;
            end;

            begin
                execute immediate '
                    create table prods_hier2 NOLOGGING as
                    select distinct
                       to_char(a.prod_tier1) || '' - '' || a.prod_tier1_desc as tier1,
                       to_char(a.prod_tier2) || '' - '' || a.prod_tier2_desc as tier2,
                       to_char(a.prod_tier3) || '' - '' || a.prod_tier3_desc as tier3,
                       to_char(a.prod_tier4) || '' - '' || a.prod_tier4_desc as tier4,
                       to_char(a.prod_tier5) || '' - '' || a.prod_tier5_desc as tier5,
                       to_char(a.prod_tier6) || '' - '' || a.prod_tier6_desc as tier6,
                       to_char(a.prod_code) || '' - '' || a.prod_name as product
                    from gcd_dw.gcd_products a
                    where a.prod_tier1 is not null
                        and a.prod_name not in (''UNKNOWN'')
                    order by tier1, tier2, tier3, tier4, tier5, tier6, product';

                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2 create', sysdate,'CREATED');
                --commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2 create', sysdate,'NOT CREATED - ' || err_msg);
                --commit;
            end;

        if chrispack.is_table_populated('PRODS_HIER2') then
            --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2', sysdate,'POPULATED');
            --commit;

            --- PRODS_HIER -> PRODS_HIER_bak

            begin
                execute immediate 'drop table PRODS_HIER_bak';
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins_bak drop', sysdate,'DROPPED');
                --commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                    --commit;
            end;

            begin
                execute immediate 'alter table PRODS_HIER rename to PRODS_HIER_bak';
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins rename -> emea_optins_bak', sysdate,'RENAMED');
                --commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins rename', sysdate,'NOT RENAMED - ' || err_msg);
                --commit;
            end;


            --- PRODS_HIER2 -> PRODS_HIER
            begin
                execute immediate 'alter table PRODS_HIER2 rename to PRODS_HIER';

                select get_info('prods_hier') into table_info_new from dual;

                select count(*) into no_of_rows_new from prods_hier;
                select count(*) into no_of_rows_bak from prods_hier_bak;

                if no_of_rows_new <> no_of_rows_bak then
                    changed := true;
                else -- the same no_of_rows

                    select count(*) into no_of_changed_rows from
                    ( select * from prods_hier
                      minus
                      select * from prods_hier_bak
                    );

                    if no_of_changed_rows <> 0 then
                        changed := true;
                    end if;
                end if;

                if changed then
                    table_info_new := 'CHNAGED!!! ' || table_info_new || ' CHANGED!!!';
-- uncomment when UTL_SMTP is ready again !!!!

                    demo_mail.mail('krzysztof.cierpisz@oracle.com',
                                    'krzysztof.cierpisz@oracle.com',
                                    'PRODS_HIER_CHANGED',
                                    'See logs under prods_hier_log ' || chr(13) || table_info_new);

                end if;

                insert into prods_hier_log values (prods_hier_seq.NEXTVAL,'prods_hier created', sysdate,table_info_new);
                commit;

            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins not created from emea_optins2', sysdate,'NOT RENAMED - ' || err_msg);
                --commit;
            end;

         --else
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2', sysdate,'NOT POPULATED');
        end if;

    else

        insert into PRODS_HIER_log values (PRODS_HIER_seq.NEXTVAL,'gcd_dw.GCD_PRODUCTS', sysdate,'NOT POPULATED ending NO CREATION OF HIERARCHY');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON prods_hier TO public';
    end;


    --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_EMEA_OPTINS', sysdate,'END');
    --commit;

end;


procedure PROC_PRODS_HIER_GSRT
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_info_new varchar2(4000);
   table_info_bak varchar2(4000);
   no_of_rows_new number;
   no_of_rows_bak number;
   changed boolean := false; -- if the prods_hier has changed from the bak table
   no_of_changed_rows number := 0;

begin
    --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_EMEA_OPTINS', sysdate,'START');
    --commit;
    if chrispack.is_table_populated('gsrt.gsrt_prod_hierarchy_staging') then
        --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_eu', sysdate,'POPULATED');
        --commit;

            begin
                execute immediate 'drop table PRODS_HIER_GSRT2';
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2 drop', sysdate,'DROPPED');
                --commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                --commit;
            end;

            begin
                execute immediate '
                    create table prods_hier_gsrt2 NOLOGGING as
                    select distinct
                       to_char(a.prod_tier1) || '' - '' || a.prod_tier1_desc as tier1,
                       to_char(a.prod_tier2) || '' - '' || a.prod_tier2_desc as tier2,
                       to_char(a.prod_tier3) || '' - '' || a.prod_tier3_desc as tier3,
                       to_char(a.prod_tier4) || '' - '' || a.prod_tier4_desc as tier4,
                       to_char(a.prod_tier5) || '' - '' || a.prod_tier5_desc as tier5,
                       to_char(a.prod_tier6) || '' - '' || a.prod_tier6_desc as tier6,
                       to_char(a.prod_code) || '' - '' || a.product_description as product
                    from gsrt.gsrt_prod_hierarchy_staging a
                    where a.prod_tier1 is not null
                    order by tier1, tier2, tier3, tier4, tier5, tier6, product';

                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2 create', sysdate,'CREATED');
                --commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2 create', sysdate,'NOT CREATED - ' || err_msg);
                --commit;
                dbms_output.put_line('wrong: ' || err_msg);
                raise;
            end;

        if chrispack.is_table_populated('PRODS_HIER_GSRT2') then
            --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2', sysdate,'POPULATED');
            --commit;

            --- PRODS_HIER -> PRODS_HIER_bak

            begin
                execute immediate 'drop table PRODS_HIER_GSRT_bak';
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins_bak drop', sysdate,'DROPPED');
                --commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                    --commit;
            end;

            begin
                execute immediate 'alter table PRODS_HIER_GSRT rename to PRODS_HIER_GSRT_bak';
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins rename -> emea_optins_bak', sysdate,'RENAMED');
                --commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins rename', sysdate,'NOT RENAMED - ' || err_msg);
                --commit;
            end;


            --- PRODS_HIER2 -> PRODS_HIER
            begin
                execute immediate 'alter table PRODS_HIER_GSRT2 rename to PRODS_HIER_GSRT';

                select get_info('prods_hier_gsrt') into table_info_new from dual;

                select count(*) into no_of_rows_new from prods_hier_gsrt;
                select count(*) into no_of_rows_bak from prods_hier_gsrt_bak;

                if no_of_rows_new <> no_of_rows_bak then
                    changed := true;
                else -- the same no_of_rows

                    select count(*) into no_of_changed_rows from
                    ( select * from prods_hier_gsrt
                      minus
                      select * from prods_hier_gsrt_bak
                    );

                    if no_of_changed_rows <> 0 then
                        changed := true;
                    end if;
                end if;

                if changed then
                    table_info_new := 'CHNAGED!!! ' || table_info_new || ' CHANGED!!!';
-- uncomment when UTL_SMTP is ready again
                    demo_mail.mail('krzysztof.cierpisz@oracle.com',
                                    'krzysztof.cierpisz@oracle.com',
                                    'PRODS_HIER_GSRT_CHANGED',
                                    'See logs under prods_hier_log ' || chr(13) || table_info_new);

                end if;

                insert into prods_hier_log values (prods_hier_seq.NEXTVAL,'prods_hier_gsrt created', sysdate,table_info_new);
                commit;

            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins not created from emea_optins2', sysdate,'NOT RENAMED - ' || err_msg);
                --commit;
            end;

         --else
                --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2', sysdate,'NOT POPULATED');
        end if;

    else

        insert into PRODS_HIER_log values (PRODS_HIER_seq.NEXTVAL,'gsrt.gsrt_prod_hierarchy_staging', sysdate,'NOT POPULATED ending NO CREATION OF HIERARCHY');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON prods_hier_gsrt TO public';
    end;


    --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_EMEA_OPTINS', sysdate,'END');
    --commit;

end;


procedure PROC_EMEA_OPTINS
is
   err_num NUMBER;
   err_msg VARCHAR2(100);

begin
    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_EMEA_OPTINS', sysdate,'START');
    commit;
    if chrispack.is_table_populated('gcd_dw.list_build_individuals_eu') then
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_eu', sysdate,'POPULATED');
        commit;

            begin
                execute immediate 'drop table emea_optins2';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;
            begin
                execute immediate '
                        create table emea_optins2 nologging as
                        select distinct a.individual_id,
                        a.email_address,
                        (case when b.email_address is null
                            and a.email_address like ''_%@_%.__%''
                            and ( c.individual_id is not null and c.permission_given_flg = ''Y'' and c.correspondence_type_id = 1
                                or
                                  d.individual_id is not null and d.service_type_id = 39 and d.news_letter_flg in (''Y'',''1'')
                            )
                            and e.individual_id is null
                            then ''Y''
                        else (
                            case when a.sub_region_name in (''MIDDLE EAST'',''AFRICA'') and a.country_id not in (223,195) and nvl(a.contact_email,''Y'') = ''Y''
                            then ''Y'' else ''N'' end) end) as contact_email
                    from gcd_dw.list_build_individuals_eu a,
                         dm_metrics.email_suppression b, gcd_dw.gcd_correspondence_details c,
                         gcd_dw.gcd_individual_services d, dm_metrics.email_optout e
                         --GCD_DW.GCD_INDS_INDUSTRIES_VW@dwprd.us.oracle.com INDUST
                    where
                        upper(a.email_address) = b.email_address (+)
                        and a.individual_id = c.individual_id (+)
                        and a.individual_id = d.individual_id (+)
                        and a.individual_id = e.individual_id (+)
                        ';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2 create', sysdate,'CREATED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2 create', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

        if chrispack.is_table_populated('emea_optins2') then
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2', sysdate,'POPULATED');
            commit;

            --- emea_optins -> emea_optins_bak

            begin
                execute immediate 'drop table emea_optins_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                   commit;
            end;

            begin
                execute immediate 'alter table emea_optins rename to emea_optins_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins rename -> emea_optins_bak', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;


            --- emea_inds2 -> emea_inds
            begin
                --execute immediate 'alter table emea_inds2 rename to emea_inds';
                --insert into emea_inds_log values (emea_inds_seq.NEXTVAL,'emea_inds2 rename -> emea_inds', sysdate,'RENAMED');
                execute immediate 'create table emea_optins nologging as
                                    select individual_id, email_address
                                    from emea_optins2 where contact_email = ''Y''';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins created from emea_optins2', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins not created from emea_optins2', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'DROP INDEX BT_emea_optins_ind_id';
                execute immediate 'DROP INDEX BT_emea_optins_email';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins indexes', sysdate,'DROPPED');
                commit;
           EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate '
                    CREATE Unique INDEX BT_emea_optins_ind_id ON emea_optins (  individual_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_emea_optins_email ON emea_optins (  email_address  )
                    COMPUTE STATISTICS';
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins indexes', sysdate,'CREATED');
                    commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


        else
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins2', sysdate,'NOT POPULATED');

        end if;

    else

        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_eu', sysdate,'NOT POPULATED ending');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON emea_optins TO public';
    end;

/*
              begin
                execute immediate 'GRANT SELECT ON prods_emea TO public';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea GRANT to public', sysdate, 'GRANTED');
                commit;
              exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea GRANT to public', sysdate,'NOT GRANTED - ' || err_msg);
                commit;
              end;
*/

    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_EMEA_OPTINS', sysdate,'END');
    commit;

end;

procedure PROC_EMEA_OPTINS_alt
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_name varchar2(30) := 'EMEA_OPTINS_ALT';

begin
    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'START');
    commit;
    if chrispack.is_table_populated('gcd_dw.list_build_individuals_eu') then
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_eu', sysdate,'POPULATED');
        commit;

            begin
                execute immediate 'drop table ' || table_name || '2';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;
            begin
                execute immediate '
                        create table ' || table_name || '2 nologging as
                        select distinct a.individual_id,
                        a.email_address,
                        (case when b.email_address is null
                            and a.email_address like ''_%@_%.__%''
                            and ( c.individual_id is not null and c.permission_given_flg = ''Y'' and c.correspondence_type_id = 1
                                or
                                  d.individual_id is not null and d.service_type_id = 39 and d.news_letter_flg in (''Y'',''1'')
                            )
                            --and e.individual_id is null
                            then ''Y''
                        else ''N'' end) as contact_email
                    from gcd_dw.list_build_individuals_eu a,
                         dm_metrics.email_suppression b, gcd_dw.gcd_correspondence_details c,
                         gcd_dw.gcd_individual_services d--, dm_metrics.email_optout e
                         --GCD_DW.GCD_INDS_INDUSTRIES_VW@dwprd.us.oracle.com INDUST
                    where
                        upper(a.email_address) = b.email_address (+)
                        and a.individual_id = c.individual_id (+)
                        and a.individual_id = d.individual_id (+)
                        --and a.individual_id = e.individual_id (+)
                        ';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'CREATED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

        if chrispack.is_table_populated(table_name || '2') then
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'POPULATED');
            commit;

            --- emea_optins -> emea_optins_bak

            begin
                execute immediate 'drop table ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                   commit;
            end;

            begin
                execute immediate 'alter table ' || table_name || ' rename to ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename -> ' || table_name || '_bak', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;


            --- emea_inds2 -> emea_inds
            begin
                --execute immediate 'alter table emea_inds2 rename to emea_inds';
                --insert into emea_inds_log values (emea_inds_seq.NEXTVAL,'emea_inds2 rename -> emea_inds', sysdate,'RENAMED');
                execute immediate 'create table ' || table_name || ' nologging as
                                    select individual_id, email_address
                                    from ' || table_name || '2 where contact_email = ''Y''';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' created from ' || table_name || '2', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' not created from ' || table_name || '2', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'DROP INDEX BT_' || table_name || '_ind_id';
                execute immediate 'DROP INDEX BT_' || table_name || '_email';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'DROPPED');
                commit;
           EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate '
                    CREATE Unique INDEX BT_' || table_name || '_ind_id ON ' || table_name || ' (  individual_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_' || table_name || '_email ON ' || table_name || ' (  email_address  )
                    COMPUTE STATISTICS';
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'CREATED');
                    commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


        else
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'NOT POPULATED');

        end if;

    else

        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_eu', sysdate,'NOT POPULATED ending');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON ' || table_name || ' TO public';
    end;

/*
              begin
                execute immediate 'GRANT SELECT ON prods_emea TO public';
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea GRANT to public', sysdate, 'GRANTED');
                commit;
              exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into prods_emea_log values (prods_emea_seq.NEXTVAL,'prods_emea GRANT to public', sysdate,'NOT GRANTED - ' || err_msg);
                commit;
              end;
*/

    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_EMEA_OPTINS_ALT', sysdate,'END');
    commit;

end;


procedure PROC_EMEA_OPTINS_1stOct
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_name varchar2(30) := 'EMEA_OPTINS_O';
   emea_optins_log    varchar2(30) := 'EMEA_OPTINS_LOG';


begin
    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_EMEA_OPTINS_1st_oct', sysdate,'START');
    commit;
    if chrispack.is_table_populated('gcd_dw.list_build_individuals_eu') then
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_eu', sysdate,'POPULATED');
        commit;

            begin
                execute immediate 'drop table ' || table_name || '2';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;
            begin
                execute immediate '
                        create table ' || table_name || '2 nologging as
                        select distinct a.sub_region_name, a.country_id, a.individual_id,
                        a.email_address,
                        (case when b.email_address is null
                            and a.email_address like ''_%@_%.__%''
                            and ( c.individual_id is not null and c.permission_given_flg = ''Y'' and c.correspondence_type_id = 1
                                or
                                  d.individual_id is not null and d.service_type_id = 39 and d.news_letter_flg in (''Y'',''1'')
                            )
                            and e.individual_id is null
                            then ''Y''
                        else ''N'' end) as contact_email
                    from gcd_dw.list_build_individuals_eu a,
                         dm_metrics.email_suppression b, gcd_dw.gcd_correspondence_details c,
                         gcd_dw.gcd_individual_services d, dm_metrics.email_optout e
                    where
                        upper(a.email_address) = b.email_address (+)
                        and a.individual_id = c.individual_id (+)
                        and a.individual_id = d.individual_id (+)
                        and a.individual_id = e.individual_id (+)
                        ';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'CREATED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

        if chrispack.is_table_populated(table_name || '2') then
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'POPULATED');
            commit;

            --- emea_optins -> emea_optins_bak

            begin
                execute immediate 'drop table ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                   commit;
            end;

            begin
                execute immediate 'alter table ' || table_name || ' rename to ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename -> ' || table_name || '_bak', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;


            --- emea_inds2 -> emea_inds
            begin
                --execute immediate 'alter table emea_inds2 rename to emea_inds';
                --insert into emea_inds_log values (emea_inds_seq.NEXTVAL,'emea_inds2 rename -> emea_inds', sysdate,'RENAMED');
                execute immediate 'create table ' || table_name || ' nologging as
                                    select sub_region_name, country_id, individual_id, email_address
                                    from ' || table_name || '2 where contact_email = ''Y''';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' created from ' || table_name || '2', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' not created from ' || table_name || '2', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'DROP INDEX BT_' || table_name || '_ind_id';
                execute immediate 'DROP INDEX BT_' || table_name || '_email';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'DROPPED');
                commit;
           EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate '
                    CREATE Unique INDEX BT_' || table_name || '_ind_id ON ' || table_name || ' (  individual_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_' || table_name || '_email ON ' || table_name || ' (  email_address  )
                    COMPUTE STATISTICS';
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'CREATED');
                    commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


        else
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'NOT POPULATED');

        end if;

    else

        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.lb_individuals_eu_vw', sysdate,'NOT POPULATED ending');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON ' || table_name || ' TO public';
    end;

    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_EMEA_OPTINS_1st_oct', sysdate,'END');
    commit;

end;

procedure PROC_EMEA_OPTINS_me
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_name varchar2(30) := 'EMEA_OPTINS_ME';
   emea_optins_log    varchar2(30) := 'EMEA_OPTINS_LOG';


begin
    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_EMEA_OPTINS_ME', sysdate,'START');
    commit;
    if chrispack.is_table_populated('gcd_dw.list_build_individuals_eu') then
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_eu', sysdate,'POPULATED');
        commit;

            begin
                execute immediate 'drop table ' || table_name || '2';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;
            begin
                execute immediate '
                        create table ' || table_name || '2 nologging as
                        select distinct a.sub_region_name, a.country_id, a.individual_id,
                        a.email_address,
                        (case when b.email_address is null
                            and a.email_address like ''_%@_%.__%''
                            and ( c.individual_id is not null and c.permission_given_flg = ''Y'' and c.correspondence_type_id = 1
                                or
                                  d.individual_id is not null and d.service_type_id = 39 and d.news_letter_flg in (''Y'',''1'')
                            )
                            and e.individual_id is null
                            then ''Y''
                        else (
                            case when a.sub_region_name in (''MIDDLE EAST'',''AFRICA'') and a.country_id not in (223,195) and nvl(a.contact_email,''Y'') = ''Y''
                                      and b.email_address is null
                                      and e.individual_id is null
                            then ''Y'' else ''N'' end) end) as contact_email
                    from gcd_dw.list_build_individuals_eu a,
                         dm_metrics.email_suppression b, gcd_dw.gcd_correspondence_details c,
                         gcd_dw.gcd_individual_services d, dm_metrics.email_optout e
                    where
                        upper(a.email_address) = b.email_address (+)
                        and a.individual_id = c.individual_id (+)
                        and a.individual_id = d.individual_id (+)
                        and a.individual_id = e.individual_id (+)
                        ';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'CREATED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

        if chrispack.is_table_populated(table_name || '2') then
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'POPULATED');
            commit;

            --- emea_optins -> emea_optins_bak

            begin
                execute immediate 'drop table ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                   commit;
            end;

            begin
                execute immediate 'alter table ' || table_name || ' rename to ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename -> ' || table_name || '_bak', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;


            --- emea_inds2 -> emea_inds
            begin
                --execute immediate 'alter table emea_inds2 rename to emea_inds';
                --insert into emea_inds_log values (emea_inds_seq.NEXTVAL,'emea_inds2 rename -> emea_inds', sysdate,'RENAMED');
                execute immediate 'create table ' || table_name || ' nologging as
                                    select sub_region_name, country_id, individual_id, email_address
                                    from ' || table_name || '2 where contact_email = ''Y''';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' created from ' || table_name || '2', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' not created from ' || table_name || '2', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'DROP INDEX BT_' || table_name || '_ind_id';
                execute immediate 'DROP INDEX BT_' || table_name || '_email';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'DROPPED');
                commit;
           EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate '
                    CREATE Unique INDEX BT_' || table_name || '_ind_id ON ' || table_name || ' (  individual_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_' || table_name || '_email ON ' || table_name || ' (  email_address  )
                    COMPUTE STATISTICS';
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'CREATED');
                    commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


        else
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'NOT POPULATED');

        end if;

    else

        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.lb_individuals_eu_vw', sysdate,'NOT POPULATED ending');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON ' || table_name || ' TO public';
    end;

    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_EMEA_OPTINS_ME', sysdate,'END');
    commit;

end;

procedure PROC_EMEA_OPTINS_prfl_b -- changed from main to _b
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_name varchar2(30) := 'EMEA_OPTINS_PRFL_b';
   emea_optins_log    varchar2(30) := 'EMEA_OPTINS_LOG';
   sqlstmt varchar2(4000) := '';

begin
    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'START');
    commit;
    if chrispack.is_table_populated('gcd_dw.list_build_individuals_eu') then
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_eu', sysdate,'POPULATED');
        commit;

            begin
                execute immediate 'drop table ' || table_name || '2';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;
            begin
                execute immediate '
                        create table ' || table_name || '2 nologging as
                        select distinct a.sub_region_name, a.country_id, a.individual_id,
                        a.email_address,
                        (case when b.email_address is null
                            and a.email_address like ''_%@_%.__%''
                            and ( c.individual_id is not null and c.permission_given_flg = ''Y'' and c.correspondence_type_id = 1
                                or
                                  d.individual_id is not null and d.service_type_id = 39 and d.news_letter_flg in (''Y'',''1'')
                            )
                            and e.individual_id is null
                            then ''Y''
                        else (
                            case when a.sub_region_name in (''MIDDLE EAST'',''AFRICA'') and a.country_id not in (223,195) and nvl(a.contact_email,''Y'') = ''Y''
                                      and b.email_address is null
                                      and e.individual_id is null
                            then ''Y'' else ''N'' end) end) as contact_email
                    from gcd_dw.list_build_individuals_eu a,
                         dm_metrics.email_suppression b, gcd_dw.gcd_correspondence_details c,
                         gcd_dw.gcd_individual_services d, dm_metrics.email_optout e
                    where
                        upper(a.email_address) = b.email_address (+)
                        and a.individual_id = c.individual_id (+)
                        and c.correspondence_type_id (+) = 1
                        and a.individual_id = d.individual_id (+)
                        and d.service_type_id (+) = 39
                        and a.individual_id = e.individual_id (+)
                        ';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'CREATED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;
            
            -- update with profile Y

            begin
                sqlstmt := 'update ' || table_name || '2 set
                            contact_email=''Y''
                            where individual_id in
                            (select new_individual_id from dm_metrics.vg_prfl_email_subscriptions
                                where case <>''OTHERS'' AND
                                      EMAIL_OPT_IN_FLAG_AFTR_SUP=''Y'')';
                execute immediate sqlstmt;
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 update with PROFILE', sysdate,'NOT UPDATED - ' || err_msg);
                commit;
            end;

            -- update with profile N

            begin
                sqlstmt := 'update ' || table_name || '2 set
                            contact_email=''N''
                            where individual_id in
                            (select new_individual_id from dm_metrics.vg_prfl_email_subscriptions
                                where case <>''OTHERS'' AND
                                      EMAIL_OPT_IN_FLAG_AFTR_SUP=''N'')';
                execute immediate sqlstmt;
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 update with PROFILE N', sysdate,'NOT UPDATED - ' || err_msg);
                commit;
            end;

            ------------------------

        if chrispack.is_table_populated(table_name || '2') then
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'POPULATED');
            commit;

            --- emea_optins -> emea_optins_bak

            begin
                execute immediate 'drop table ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                   commit;
            end;

            begin
                execute immediate 'alter table ' || table_name || ' rename to ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename -> ' || table_name || '_bak', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;


            --- emea_inds2 -> emea_inds
            begin
                --execute immediate 'alter table emea_inds2 rename to emea_inds';
                --insert into emea_inds_log values (emea_inds_seq.NEXTVAL,'emea_inds2 rename -> emea_inds', sysdate,'RENAMED');
                execute immediate 'create table ' || table_name || ' nologging as
                                    select sub_region_name, country_id, individual_id, email_address
                                    from ' || table_name || '2 where contact_email = ''Y'' and email_address is not null';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' created from ' || table_name || '2', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' not created from ' || table_name || '2', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'DROP INDEX BT_' || table_name || '_ind_id';
                execute immediate 'DROP INDEX BT_' || table_name || '_email';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'DROPPED');
                commit;
           EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate '
                    CREATE Unique INDEX BT_' || table_name || '_ind_id ON ' || table_name || ' (  individual_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_' || table_name || '_email ON ' || table_name || ' (  email_address  )
                    COMPUTE STATISTICS';
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'CREATED');
                    commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


        else
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'NOT POPULATED');

        end if;

    else

        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.lb_individuals_eu_vw', sysdate,'NOT POPULATED ending');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON ' || table_name || ' TO public';
    end;

    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'END');
    commit;

end;

procedure PROC_EMEA_OPTINS_prfl_c -- changed from main to _c
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_name varchar2(30) := 'EMEA_OPTINS_PRFL_C';
   emea_optins_log    varchar2(30) := 'EMEA_OPTINS_LOG';
   sqlstmt varchar2(4000) := '';

begin
    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'START');
    commit;
    --if chrispack.is_table_populated('gcd_dw.list_build_individuals_eu') then
    if chrispack.is_table_populated('gcd_dw.gcd_individuals') then
--        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_eu', sysdate,'POPULATED');
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'POPULATED');
        commit;

            begin
                execute immediate 'drop table ' || table_name || '2';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;
            begin
                execute immediate '
                        create table ' || table_name || '2 nologging as
                        select distinct ''region'' sub_region_name, a.country_id, a.individual_id,
                        a.email_address,
                        (case when b.email_address is null
                            --and a.email_address like ''_%@_%.__%''
                            and ( c.individual_id is not null and c.permission_given_flg = ''Y'' and c.correspondence_type_id = 1
                                or
                                  d.individual_id is not null and d.service_type_id = 39 and d.news_letter_flg in (''Y'',''1'')
                            )
                            and e.individual_id is null
                            then ''Y''
                        else (
                            case when 
                                     a.country_id in (3,6,17,23,28,34,35,37,39,41,42,48,49,50,53,59,64,66,67,69,79,80,83,91,92,103,104,110,112,116,120,121,122,123,129,130,133,137,138,146,147,149,157,158,163,175,179,185,186,187,188,189,194,195,201,204,207,210,212,216,221,223,235,236,238,239,246)
                                      and a.country_id not in (223,195) 
                                      and b.email_address is null
                                      and e.individual_id is null
                            then ''Y'' else ''N'' end) end) as contact_email
                    from gcd_dw.gcd_individuals a,
                         dm_metrics.email_suppression b, gcd_dw.gcd_correspondence_details c,
                         gcd_dw.gcd_individual_services d, dm_metrics.email_optout e
                    where
                        a.country_id in (2,3,5,6,11,14,15,17,20,21,23,27,28,33,34,35,37,39,41,42,48,49,50,53,54,56,57,58,59,64,66,67,68,69,70,71,73,74,75,76,79,80,81,82,83,84,85,86,88,91,92,96,99,100,103,104,105,106,107,110,111,112,116,117,119,120,121,122,123,124,125,126,128,129,130,133,134,136,137,138,139,142,143,146,147,149,152,153,154,157,158,162,163,172,173,175,176,177,178,179,184,185,186,187,188,189,191,192,194,195,196,197,199,200,201,203,204,205,206,207,209,210,212,216,217,218,221,222,223,224,228,235,236,238,239,242,243,244,246,247)
                        and upper(a.email_address) = b.email_address (+)
                        and a.individual_id = c.individual_id (+)
                        and c.correspondence_type_id (+) = 1
                        and a.individual_id = d.individual_id (+)
                        and d.service_type_id (+) = 39
                        and a.individual_id = e.individual_id (+)
                        ';
                        
                        -- --a.sub_region_name in (''MIDDLE EAST'',''AFRICA'')
                        -- -- and nvl(a.contact_email,''Y'') = ''Y''
                        -- --                    from gcd_dw.list_build_individuals_eu a,
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'CREATED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            -- update with profile Y

            begin
                /*
                sqlstmt := 'update ' || table_name || '2 set
                            contact_email=''Y''
                            where individual_id in
                            (select new_individual_id from dm_metrics.vg_prfl_email_subscriptions
                                where case <>''OTHERS'' AND
                                      EMAIL_OPT_IN_FLAG_AFTR_SUP=''Y'')';
                */

sqlstmt := 'update ' || table_name || '2 b set
(contact_email, email_address) =
(select min(email_opt_in_flag_aftr_sup), min(a.email)
from dm_metrics.vg_prfl_email_subscriptions a
where case <>''OTHERS'' and a.new_individual_id = b.individual_id
and a.use_this_email = ''Y''
and a.email_opt_in_flag_aftr_sup = ''Y''
group by a.new_individual_id)
where exists (select 1 from dm_metrics.vg_prfl_email_subscriptions c
where c.new_individual_id = b.individual_id
and c.case <> ''OTHERS'' and c.use_this_email = ''Y''
and c.email_opt_in_flag_aftr_sup = ''Y'')';

                dbms_output.put_line(sqlstmt);
                execute immediate sqlstmt;
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 update with PROFILE', sysdate,'NOT UPDATED - ' || err_msg);
                commit;
            end;

            -- update with profile N

            begin
                sqlstmt := 'update ' || table_name || '2 set
                            contact_email=''N''
                            where individual_id in
                            (select new_individual_id from dm_metrics.vg_prfl_email_subscriptions
                                where case <>''OTHERS'' AND
                                      EMAIL_OPT_IN_FLAG_AFTR_SUP=''N'')';
                execute immediate sqlstmt;
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 update with PROFILE N', sysdate,'NOT UPDATED - ' || err_msg);
                commit;
            end;

            ------------------------

        if chrispack.is_table_populated(table_name || '2') then
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'POPULATED');
            commit;

            --- emea_optins -> emea_optins_bak

            begin
                execute immediate 'drop table ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                   commit;
            end;

            begin
                execute immediate 'alter table ' || table_name || ' rename to ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename -> ' || table_name || '_bak', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;


            --- emea_inds2 -> emea_inds
            begin
                --execute immediate 'alter table emea_inds2 rename to emea_inds';
                --insert into emea_inds_log values (emea_inds_seq.NEXTVAL,'emea_inds2 rename -> emea_inds', sysdate,'RENAMED');
                execute immediate 'create table ' || table_name || ' nologging as
                                    select sub_region_name, country_id, individual_id, email_address
                                    from ' || table_name || '2 where contact_email = ''Y'' and email_address like ''_%@_%.__%'''; -- is not null
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' created from ' || table_name || '2', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' not created from ' || table_name || '2', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'DROP INDEX BT_' || table_name || '_ind_id';
                execute immediate 'DROP INDEX BT_' || table_name || '_email';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'DROPPED');
                commit;
           EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate '
                    CREATE Unique INDEX BT_' || table_name || '_ind_id ON ' || table_name || ' (  individual_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_' || table_name || '_email ON ' || table_name || ' (  email_address  )
                    COMPUTE STATISTICS';
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'CREATED');
                    commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


        else
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'NOT POPULATED');

        end if;

    else

        --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.lb_individuals_eu_vw', sysdate,'NOT POPULATED ending');
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'NOT POPULATED ending');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON ' || table_name || ' TO public';
    end;

    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'END');
    commit;

end;

procedure PROC_EMEA_OPTINS_prfl  -- changed from _c to main
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_name varchar2(30) := 'EMEA_OPTINS_PRFL';
   emea_optins_log    varchar2(30) := 'EMEA_OPTINS_LOG';
   sqlstmt varchar2(4000) := '';

begin
    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'START');
    commit;
    --if chrispack.is_table_populated('gcd_dw.list_build_individuals_eu') then
    if chrispack.is_table_populated('gcd_dw.gcd_individuals') then
--        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_eu', sysdate,'POPULATED');
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'POPULATED');
        commit;

            begin
                execute immediate 'drop table ' || table_name || '2';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;
            begin
                execute immediate '
                        create table ' || table_name || '2 nologging as
                        select distinct ''region'' sub_region_name, a.country_id, a.individual_id,
                        a.email_address,
                        (case when b.email_address is null
                            --and a.email_address like ''_%@_%.__%''
                            and
                            ( c.individual_id is not null and c.permission_given_flg = ''Y'' and c.correspondence_type_id = 1
                                or
                                  d.individual_id is not null and d.service_type_id = 39 and d.news_letter_flg in (''Y'',''1'')
                            )
                            --and e.individual_id is null
                            then ''Y''
                        else (
                            case when
                                     a.country_id in (3,6,17,23,28,34,35,37,39,41,42,48,49,50,53,59,64,66,67,69,79,80,83,91,92,103,104,110,112,116,120,121,122,123,129,130,133,137,138,146,147,149,157,158,163,175,179,185,186,187,188,189,194,195,201,204,207,210,212,216,221,223,235,236,238,239,246)
                                      and a.country_id not in (223,195)
                                      and b.email_address is null
                                      --and e.individual_id is null
                            then ''Y'' else ''N'' end) end) as contact_email
                            ,a.prospect_rowid, a.contact_rowid  -- added 04.08.2008
                    from gcd_dw.gcd_individuals a,
                         dm_metrics.email_suppression b,
                         gcd_dw.gcd_correspondence_details c,
                         gcd_dw.gcd_individual_services d--, dm_metrics.email_optout e
                    where
                        a.country_id in (2,3,5,6,11,14,15,17,20,21,23,27,28,33,34,35,37,39,41,42,48,49,50,53,54,56,57,58,59,64,66,67,68,69,70,71,73,74,75,76,79,80,81,82,83,84,85,86,88,91,92,96,99,100,103,104,105,106,107,110,111,112,116,117,119,120,121,122,123,124,125,126,128,129,130,133,134,136,137,138,139,142,143,146,147,149,152,153,154,157,158,162,163,172,173,175,176,177,178,179,184,185,186,187,188,189,191,192,194,195,196,197,199,200,201,203,204,205,206,207,209,210,212,216,217,218,221,222,223,224,228,235,236,238,239,242,243,244,246,247)
--                        a.country_id in (82,172)
                        and upper(a.email_address) = b.email_address (+)
                        and a.individual_id = c.individual_id (+)
                        and c.correspondence_type_id (+) = 1
                        and a.individual_id = d.individual_id (+)
                        and d.service_type_id (+) = 39
                        --and a.individual_id = e.individual_id (+)
                        ';

                        -- --a.sub_region_name in (''MIDDLE EAST'',''AFRICA'')
                        -- -- and nvl(a.contact_email,''Y'') = ''Y''
                        -- --                    from gcd_dw.list_build_individuals_eu a,
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'CREATED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            -- update with profile Y


           if chrispack.is_table_populated('dm_metrics.vg_prfl_email_subscriptions') then
            begin

            sqlstmt := 'drop table prfl_vani';

                dbms_output.put_line(sqlstmt);
                execute immediate sqlstmt;
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'prfl_vani DROP', sysdate,'DROPEED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'prfl_vani DROP', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
            sqlstmt := 'create table prfl_vani nologging as
            select a.new_individual_id, min(a.email_opt_in_flag_aftr_sup) contact_email,
		      min(a.email) email_address from dm_metrics.vg_prfl_email_subscriptions a
		      --min(a.email) email_address from vg_prfl_email_subscriptions_bk a
                where a.case <> ''OTHERS'' and a.use_this_email = ''Y''
                group by a.new_individual_id';

                   dbms_output.put_line(sqlstmt);
                execute immediate sqlstmt;
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'prfl_vani CREATE', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'prfl_vani CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


            begin
            
            sqlstmt := 'create index bt_prfl_vani_new_ind on prfl_vani (new_individual_id)';
            execute immediate sqlstmt;
            sqlstmt := 'create index bt_prfl_vani_contact_email on prfl_vani (contact_email)';
            execute immediate sqlstmt;
            sqlstmt := 'create index bt_prfl_vani_email on prfl_vani (email_address)';
            execute immediate sqlstmt;

                dbms_output.put_line(sqlstmt);
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'prfl_vani indexes CREATE', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'prfl_vani indexes CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            else
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'dm_metrics.vg_prfl_email_subscriptions not populated', sysdate,'NOT POPULATED - ');
                commit;
            end if;

            begin
                sqlstmt := 'drop table ' || table_name || '2_p';
            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2_p DROP', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2_p DROP', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


            begin
            sqlstmt := 'create table ' || table_name || '2_p nologging as
                        select a.*, b.contact_email as prfl_contact_email, b.email_address as prfl_email_address
                        from ' || table_name || '2 a, prfl_vani b
                        where a.individual_id = b.new_individual_id (+)';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2_p CREATE', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2_p CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


            begin
                sqlstmt := 'drop table ' || table_name || '3';
            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '3 DROP', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '3 DROP', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


            begin
            sqlstmt := 'create table ' || table_name || '3 nologging as
                        select a.individual_id, a.sub_region_name, a.country_id,
                        (case when a.prfl_contact_email is not null then a.prfl_email_address else a.email_address end) email_address,
                        (case when a.prfl_contact_email is not null then a.prfl_contact_email else a.contact_email end) contact_email
                        ,a.prospect_rowid, a.contact_rowid  -- added 04.08.2008
                        from ' || table_name || '2_p a';
            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '3 CREATE', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '3 CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


           ------------------------

        if chrispack.is_table_populated(table_name || '3') then
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '3', sysdate,'POPULATED');
            commit;

            --- emea_optins -> emea_optins_bak

            begin
                execute immediate 'drop table ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                   commit;
            end;

            begin
                execute immediate 'alter table ' || table_name || ' rename to ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename -> ' || table_name || '_bak', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;


            --- emea_inds2 -> emea_inds
            begin
                --execute immediate 'alter table emea_inds2 rename to emea_inds';
                --insert into emea_inds_log values (emea_inds_seq.NEXTVAL,'emea_inds2 rename -> emea_inds', sysdate,'RENAMED');
                execute immediate 'create table ' || table_name || ' nologging as
                                    select sub_region_name, country_id, individual_id, email_address
                                    ,prospect_rowid, contact_rowid  -- added 04.08.2008
                                    from ' || table_name || '3 where contact_email = ''Y'' and email_address like ''_%@_%.__%'''; -- is not null
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' created from ' || table_name || '3', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' not created from ' || table_name || '3', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'DROP INDEX BT_' || table_name || '_ind_id';
                execute immediate 'DROP INDEX BT_' || table_name || '_email';
                execute immediate 'DROP INDEX FB_' || table_name || '_rowid';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'DROPPED');
                commit;
           EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate '
                    CREATE Unique INDEX BT_' || table_name || '_ind_id ON ' || table_name || ' (  individual_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_' || table_name || '_email ON ' || table_name || ' (  email_address  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX FB_' || table_name || '_rowid ON ' || table_name || ' (  nvl(contact_rowid,prospect_rowid)  )
                    COMPUTE STATISTICS';

                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'CREATED');
                    commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


        else
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'NOT POPULATED');

        end if;

    else

        --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.lb_individuals_eu_vw', sysdate,'NOT POPULATED ending');
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'NOT POPULATED ending');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON ' || table_name || ' TO public';
    end;

    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'END');
    commit;

end;

procedure PROC_EMEA_OPTINS_prfl_bef1910
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_name varchar2(30) := 'EMEA_OPTINS_PRFL';
   emea_optins_log    varchar2(30) := 'EMEA_OPTINS_LOG';
   sqlstmt varchar2(4000) := '';

begin
    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'START');
    commit;
    --if chrispack.is_table_populated('gcd_dw.list_build_individuals_eu') then
    if chrispack.is_table_populated('gcd_dw.gcd_individuals') then
--        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_eu', sysdate,'POPULATED');
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'POPULATED');
        commit;

            begin
                execute immediate 'drop table ' || table_name || '2';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;
            begin
                execute immediate '
                        create table ' || table_name || '2 nologging as
                        select distinct ''region'' sub_region_name, a.country_id, a.individual_id,
                        a.email_address,
                        (case when b.email_address is null
                            --and a.email_address like ''_%@_%.__%''
                            and
                            ( c.individual_id is not null and c.permission_given_flg = ''Y'' and c.correspondence_type_id = 1
                                or
                                  d.individual_id is not null and d.service_type_id = 39 and d.news_letter_flg in (''Y'',''1'')
                            )
                            --and e.individual_id is null
                            then ''Y''
                        else (
                            case when
                                     a.country_id in (3,6,17,23,28,34,35,37,39,41,42,48,49,50,53,59,64,66,67,69,79,80,83,91,92,103,104,110,112,116,120,121,122,123,129,130,133,137,138,146,147,149,157,158,163,175,179,185,186,187,188,189,194,195,201,204,207,210,212,216,221,223,235,236,238,239,246)
                                      and a.country_id not in (223,195)
                                      and b.email_address is null
                                      --and e.individual_id is null
                            then ''Y'' else ''N'' end) end) as contact_email
                            ,a.prospect_rowid, a.contact_rowid  -- added 04.08.2008
                    from gcd_dw.gcd_individuals a,
                         dm_metrics.email_suppression b,
                         gcd_dw.gcd_correspondence_details c,
                         gcd_dw.gcd_individual_services d--, dm_metrics.email_optout e
                    where
                        a.country_id in (2,3,5,6,11,14,15,17,20,21,23,27,28,33,34,35,37,39,41,42,48,49,50,53,54,56,57,58,59,64,66,67,68,69,70,71,73,74,75,76,79,80,81,82,83,84,85,86,88,91,92,96,99,100,103,104,105,106,107,110,111,112,116,117,119,120,121,122,123,124,125,126,128,129,130,133,134,136,137,138,139,142,143,146,147,149,152,153,154,157,158,162,163,172,173,175,176,177,178,179,184,185,186,187,188,189,191,192,194,195,196,197,199,200,201,203,204,205,206,207,209,210,212,216,217,218,221,222,223,224,228,235,236,238,239,242,243,244,246,247)
--                        a.country_id in (82,172)
                        and upper(a.email_address) = b.email_address (+)
                        and a.individual_id = c.individual_id (+)
                        and c.correspondence_type_id (+) = 1
                        and a.individual_id = d.individual_id (+)
                        and d.service_type_id (+) = 39
                        --and a.individual_id = e.individual_id (+)
                        ';

                        -- --a.sub_region_name in (''MIDDLE EAST'',''AFRICA'')
                        -- -- and nvl(a.contact_email,''Y'') = ''Y''
                        -- --                    from gcd_dw.list_build_individuals_eu a,
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'CREATED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            -- update with profile Y

            begin

            sqlstmt := 'drop table prfl_vani';

                dbms_output.put_line(sqlstmt);
                execute immediate sqlstmt;
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'prfl_vani DROP', sysdate,'DROPEED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'prfl_vani DROP', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
            sqlstmt := 'create table prfl_vani nologging as
            select a.new_individual_id, min(a.email_opt_in_flag_aftr_sup) contact_email,
		      min(a.email) email_address from dm_metrics.vg_prfl_email_subscriptions a
                where a.case <> ''OTHERS'' and a.use_this_email = ''Y''
                group by a.new_individual_id';

                   dbms_output.put_line(sqlstmt);
                execute immediate sqlstmt;
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'prfl_vani CREATE', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'prfl_vani CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


            begin

            sqlstmt := 'create index bt_prfl_vani_new_ind on prfl_vani (new_individual_id)';
            execute immediate sqlstmt;
            sqlstmt := 'create index bt_prfl_vani_contact_email on prfl_vani (contact_email)';
            execute immediate sqlstmt;

                dbms_output.put_line(sqlstmt);
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'prfl_vani indexes CREATE', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'prfl_vani indexes CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


            begin
                sqlstmt := 'drop table ' || table_name || '2_p';
            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2_p DROP', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2_p DROP', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


            begin
            sqlstmt := 'create table ' || table_name || '2_p nologging as
                        select a.*, b.contact_email as prfl_contact_email, b.email_address as prfl_email_address
                        from ' || table_name || '2 a, prfl_vani b
                        where a.individual_id = b.new_individual_id (+)';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2_p CREATE', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2_p CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


            begin
                sqlstmt := 'drop table ' || table_name || '3';
            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '3 DROP', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '3 DROP', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


            begin
            sqlstmt := 'create table ' || table_name || '3 nologging as
                        select a.individual_id, a.sub_region_name, a.country_id,
                        (case when a.prfl_contact_email is not null then a.prfl_email_address else a.email_address end) email_address,
                        (case when a.prfl_contact_email is not null then a.prfl_contact_email else a.contact_email end) contact_email
                        ,a.prospect_rowid, a.contact_rowid  -- added 04.08.2008
                        from ' || table_name || '2_p a';
            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '3 CREATE', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '3 CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


           ------------------------

        if chrispack.is_table_populated(table_name || '3') then
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '3', sysdate,'POPULATED');
            commit;

            --- emea_optins -> emea_optins_bak

            begin
                execute immediate 'drop table ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                   commit;
            end;

            begin
                execute immediate 'alter table ' || table_name || ' rename to ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename -> ' || table_name || '_bak', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;


            --- emea_inds2 -> emea_inds
            begin
                --execute immediate 'alter table emea_inds2 rename to emea_inds';
                --insert into emea_inds_log values (emea_inds_seq.NEXTVAL,'emea_inds2 rename -> emea_inds', sysdate,'RENAMED');
                execute immediate 'create table ' || table_name || ' nologging as
                                    select sub_region_name, country_id, individual_id, email_address
                                    ,prospect_rowid, contact_rowid  -- added 04.08.2008
                                    from ' || table_name || '3 where contact_email = ''Y'' and email_address like ''_%@_%.__%'''; -- is not null
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' created from ' || table_name || '3', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' not created from ' || table_name || '3', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'DROP INDEX BT_' || table_name || '_ind_id';
                execute immediate 'DROP INDEX BT_' || table_name || '_email';
                execute immediate 'DROP INDEX FB_' || table_name || '_rowid';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'DROPPED');
                commit;
           EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate '
                    CREATE Unique INDEX BT_' || table_name || '_ind_id ON ' || table_name || ' (  individual_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_' || table_name || '_email ON ' || table_name || ' (  email_address  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX FB_' || table_name || '_rowid ON ' || table_name || ' (  nvl(contact_rowid,prospect_rowid)  )
                    COMPUTE STATISTICS';

                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'CREATED');
                    commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


        else
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'NOT POPULATED');

        end if;

    else

        --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.lb_individuals_eu_vw', sysdate,'NOT POPULATED ending');
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'NOT POPULATED ending');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON ' || table_name || ' TO public';
    end;

    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'END');
    commit;

end;

procedure drop_table(table_name in varchar2, log_table in varchar2)
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   sqlstmt varchar2(4000) := '';
   log_stmt varchar2(4000) := '';
begin
    sqlstmt := 'drop table ' || table_name || ' purge';
    dbms_output.put_line(sqlstmt);
    execute immediate sqlstmt;
--    log_stmt := '
--    insert into ' || log_table || ' values (emea_optins_seq.NEXTVAL,''' || table_name || ' DROP'', sysdate, ''DROPPED'')';
    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' DROP',sysdate,'DROPPED - ');
    --execute immediate log_stmt;
    commit;
exception when others then
    err_msg := SUBSTR(SQLERRM, 1, 100);
    --log_stmt := '
    --insert into ' || log_table || ' values (emea_optins_seq.NEXTVAL,''' || table_name || ' DROP'', sysdate,''NOT DROPPED - '' || err_msg || '')';
    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' DROP',sysdate,'NOT DROPPED - ' || err_msg);
    --execute immediate log_stmt;
    commit;
end;

procedure drop_table2(table_name in varchar2, log_table in varchar2)
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   sqlstmt varchar2(4000) := '';
   log_stmt varchar2(4000) := '';
begin
    sqlstmt := 'drop table ' || table_name || ' purge';
    dbms_output.put_line(sqlstmt);
    execute immediate sqlstmt;
--    log_stmt := '
--    insert into ' || log_table || ' values (emea_optins_seq.NEXTVAL,''' || table_name || ' DROP'', sysdate, ''DROPPED'')';
    insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || ' DROP',sysdate,'DROPPED - ');
    --execute immediate log_stmt;
    commit;
exception when others then
    err_msg := SUBSTR(SQLERRM, 1, 100);
    --log_stmt := '
    --insert into ' || log_table || ' values (emea_optins_seq.NEXTVAL,''' || table_name || ' DROP'', sysdate,''NOT DROPPED - '' || err_msg || '')';
    insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || ' DROP',sysdate,'NOT DROPPED - ' || err_msg);
    --execute immediate log_stmt;
    commit;
end;

procedure PROC_EMEA_OPTINS_flags  -- new 04.08.2008
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_name varchar2(30) := 'EMEA_OPTINS_FLAGS';
   emea_optins_tab varchar2(30) := 'EMEA_OPTINS_PRFL';
   emea_optins_log    varchar2(30) := 'EMEA_OPTINS_LOG';
   sqlstmt varchar2(4000) := '';
   view_stmt varchar2(4000) := '';

begin
    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'START');
    commit;
    if chrispack.is_table_populated('gcd_dw.gcd_individuals') then
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'POPULATED');
        commit;

/*
            begin
                sqlstmt := 'drop table ' || table_name || '_tmp1';
            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp1 DROP', sysdate,'CREATED');
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp1 DROP', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;
*/
            drop_table(table_name || '_TMP1', emea_optins_log);
            drop_table(table_name || '_TMP2', emea_optins_log);
            drop_table(table_name || '_TMP3', emea_optins_log);
            drop_table(table_name || '_TMP4', emea_optins_log);
            drop_table(table_name || '_TMP5', emea_optins_log);
            drop_table(table_name || '_TMP6', emea_optins_log);

            begin
            sqlstmt := 'create table ' || table_name || '_tmp1 nologging as
                        select c.name as sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid, a.prospect_rowid
                        from gcd_dw.gcd_individuals a, gcd_dw.gcd_countries b, gcd_dw.gcd_regions c
                        where
                        a.country_id in (2,3,5,6,11,14,15,17,20,21,23,27,28,33,34,35,37,39,41,42,48,49,50,53,54,56,57,58,59,64,66,67,68,69,70,71,73,74,75,76,79,80,81,82,83,84,85,86,88,91,92,96,99,100,103,104,105,106,107,110,111,112,116,117,119,120,121,122,123,124,125,126,128,129,130,133,134,136,137,138,139,142,143,146,147,149,152,153,154,157,158,162,163,172,173,175,176,177,178,179,184,185,186,187,188,189,191,192,194,195,196,197,199,200,201,203,204,205,206,207,209,210,212,216,217,218,221,222,223,224,228,235,236,238,239,242,243,244,246,247)
                        and a.country_id = b.country_id
                        and b.region_id = c.region_id';
                        --and a.email_address like ''_%@_%.__%''';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp1 CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp1 CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

         if chrispack.is_table_populated('kcierpisz.' || emea_optins_tab) then
            begin
            sqlstmt := 'create table ' || table_name || '_tmp2 nologging as
                select a.sub_region_name, a.country_id, a.individual_id, a.contact_rowid, a.prospect_rowid,
                (case when b.individual_id is not null then b.email_address else a.email_address end) email_address,
                (case when b.individual_id is not null then 1 else 0 end) kcierpisz_optin
                from emea_optins_flags_tmp1 a, kcierpisz.emea_optins_prfl b
                where a.individual_id = b.individual_id (+)';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp2 CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp2 CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;
          end if;

         if chrispack.is_table_populated('kcierpisz.prfl_vani') then
            begin
            sqlstmt := 'create table ' || table_name || '_tmp3 nologging as
                select a.sub_region_name, a.country_id, a.individual_id, a.contact_rowid, a.prospect_rowid,
                (case when b.email_address is not null then b.email_address else a.email_address end) email_address,
                a.kcierpisz_optin, b.contact_email vani_prfl
                from emea_optins_flags_tmp2 a, prfl_vani b
                where a.individual_id  = b.new_individual_id (+)';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp3 CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp3 CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;
            
            begin
            sqlstmt := 'alter table ' || table_name || '_tmp3 add (vani_prfl_email varchar2(1))';
            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp3 ALTER add vani_prfl_email', sysdate,'ALTERED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp3 ALTER add vani_prfl_email', sysdate,'NOT ALTERED - ' || err_msg);
                commit;
            end;

            --create index bt_em_tmp3_email on em_tmp3 (email_address);

            begin
            sqlstmt := 'create index bt_' || table_name || '_tmp3_em on ' || table_name || '_tmp3 (email_address)';
            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp3 INDEX on email_address', sysdate,'INDEX CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp3 INDEX on email_address', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            
            begin
            sqlstmt := 'update ' || table_name || '_tmp3 a set
                        a.vani_prfl_email = (select min(contact_email) from prfl_vani b
                                                                where b.email_address = a.email_address)';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp3 UPDATE based on email from profile', sysdate,'UPDATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp3 UPDATE based on email from profile', sysdate,'NOT UPDATED - ' || err_msg);
                commit;
            end;

          end if;

         if chrispack.is_table_populated('dm_metrics.email_suppression') then
            begin
            sqlstmt := 'create table ' || table_name || '_tmp4 nologging as
                select a.sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid, a.prospect_rowid,
                a.kcierpisz_optin, a.vani_prfl,a.vani_prfl_email, max(case when b.email_address is not null then 1 end) suppression
                from emea_optins_flags_tmp3 a, dm_metrics.email_suppression b
                where a.email_address = b.email_address (+)
                group by a.sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid,
                a.prospect_rowid, a.kcierpisz_optin, a.vani_prfl, a.vani_prfl_email';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp4 CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp4 CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;
          end if;

         if chrispack.is_table_populated('gcd_dw.gcd_individual_services') then
            begin
            sqlstmt := 'create table ' || table_name || '_tmp5 nologging as
                select a.*, (case when b.individual_id is not null and b.news_letter_flg in (''Y'',''1'') then ''Y''
				                  when b.individual_id is not null and b.news_letter_flg in (''N'') then ''N'' end) gcd_services
                from emea_optins_flags_tmp4 a, gcd_dw.gcd_individual_services b
                where a.individual_id = b.individual_id (+)
                and b.service_type_id (+) = 39';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp5 CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp5 CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;
          end if;

         if chrispack.is_table_populated('gcd_dw.gcd_correspondence_details') then
            begin
            sqlstmt := 'create table ' || table_name || '_tmp6 nologging as
                select a.*,  b.permission_given_flg correspondence1
                from emea_optins_flags_tmp5 a, gcd_dw.gcd_correspondence_details b
                where a.individual_id = b.individual_id (+)
                and b.correspondence_type_id (+) = 1';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp6 CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp6 CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;
          end if;

           ------------------------

        if chrispack.is_table_populated(table_name || '_tmp6') then
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp6', sysdate,'POPULATED');
            commit;

            --- emea_optins -> emea_optins_bak

/*
            begin
                execute immediate 'drop table ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                   commit;
            end;
*/
            drop_table(table_name || '_BAK', emea_optins_log);
            
            begin
                execute immediate 'alter table ' || table_name || ' rename to ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename -> ' || table_name || '_bak', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;


            --- emea_inds2 -> emea_inds
            begin
                execute immediate 'create table ' || table_name || ' nologging as
                select a.sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid, a.prospect_rowid,
                a.kcierpisz_optin, a.vani_prfl, a.vani_prfl_email, a.gcd_services, a.suppression,
                a.correspondence1,
                (case when a.vani_prfl = ''Y'' or a.vani_prfl_email = ''Y'' then ''Y''
                	  when a.vani_prfl = ''N'' or a.vani_prfl_email = ''N'' then ''N''
                	  --when a.kcierpisz_optin = 1 then ''Y''
                	  --when a.suppression = 1 or a.gcd_services = ''N'' or a.correspondence1 = ''N'' then ''N''
                	  when a.suppression = 1 or a.gcd_services = ''N'' then ''N''
                	  when a.gcd_services = ''Y'' or a.correspondence1 = ''Y'' then ''Y''
                	  end) optin
                    from emea_optins_flags_tmp6 a
                    where email_address like ''_%@_%.__%''';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' created from ' || table_name || '_tmp6', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' not created from ' || table_name || '_tmp6', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'DROP INDEX BT_' || table_name || '_ind_id';
                execute immediate 'DROP INDEX BT_' || table_name || '_email';
                execute immediate 'DROP INDEX FB_' || table_name || '_rowid';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'DROPPED');
                commit;
           EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate '
                    CREATE Unique INDEX BT_' || table_name || '_ind_id ON ' || table_name || ' (  individual_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_' || table_name || '_email ON ' || table_name || ' (  email_address  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX FB_' || table_name || '_rowid ON ' || table_name || ' (  nvl(contact_rowid,prospect_rowid)  )
                    COMPUTE STATISTICS';

                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'CREATED');
                    commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


                view_stmt := 'create or replace view emea_optins_vw as
                    select a.sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid, a.prospect_rowid,
                    a.optin
                    from ' || table_name || ' a';
                    
                begin
                    execute immediate view_stmt;
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins_vw view', sysdate,'CREATED');
                    commit;
                exception when others then
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'emea_optins_vw view', sysdate,'NOT CREATED - ' || err_msg);
                    commit;
                end;
                
        else
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_tmp6', sysdate,'NOT POPULATED');

        end if;

    else

        --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.lb_individuals_eu_vw', sysdate,'NOT POPULATED ending');
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'NOT POPULATED ending');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON emea_optins_vw TO public';
    end;

    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'END');
    commit;

end;

procedure PROC_EMAIL_OPTINS  -- new 04.08.2008
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_name varchar2(30) := 'EMAIL_OPTINS';
   --emea_optins_tab varchar2(30) := 'EMEA_OPTINS_PRFL';
   email_optins_log    varchar2(30) := 'email_optins_log';
   sqlstmt varchar2(4000) := '';
   view_stmt varchar2(4000) := '';
   
   all_opportunities = 'KCIERPISZ.SUMANT_OPPTS1';
   opts_details      = 'KCIERPISZ.OPPTS3';
   opt_contacts      = 'oppt3110_distinct';
   opt_emails        = 'oppt3110_email_distinct';
   
   tars              = 'lm_emea.emea_gcm_tar_summary';
   tar1              = 'tar_tmp1'

begin
    insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'PROC_' || table_name, sysdate,'START');
    commit;
    if chrispack.is_table_populated('gcd_dw.gcd_individuals') then
        insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'POPULATED');
        commit;

            drop_table2(table_name || '_TMP1', email_optins_log);
            drop_table2(table_name || '_TMP2', email_optins_log);
            drop_table2(table_name || '_TMP3', email_optins_log);
            drop_table2(table_name || '_TMP4', email_optins_log);
            drop_table2(table_name || '_TMP5', email_optins_log);
            drop_table2(table_name || '_TMP6', email_optins_log);

            begin
            sqlstmt := 'create table ' || table_name || '_tmp1 nologging as
                        select c.name as sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid, a.prospect_rowid
                        from gcd_dw.gcd_individuals a, gcd_dw.gcd_countries b, gcd_dw.gcd_regions c
                        where
                        --a.country_id in (2,3,5,6,11,14,15,17,20,21,23,27,28,33,34,35,37,39,41,42,48,49,50,53,54,56,57,58,59,64,66,67,68,69,70,71,73,74,75,76,79,80,81,82,83,84,85,86,88,91,92,96,99,100,103,104,105,106,107,110,111,112,116,117,119,120,121,122,123,124,125,126,128,129,130,133,134,136,137,138,139,142,143,146,147,149,152,153,154,157,158,162,163,172,173,175,176,177,178,179,184,185,186,187,188,189,191,192,194,195,196,197,199,200,201,203,204,205,206,207,209,210,212,216,217,218,221,222,223,224,228,235,236,238,239,242,243,244,246,247)
                        a.country_id in (2,3,5,6) -- TEST ONLY
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

/*
         if chrispack.is_table_populated('kcierpisz.' || emea_optins_tab) then
            begin
            sqlstmt := 'create table ' || table_name || '_tmp2 nologging as
                select a.sub_region_name, a.country_id, a.individual_id, a.contact_rowid, a.prospect_rowid,
                (case when b.individual_id is not null then b.email_address else a.email_address end) email_address,
                (case when b.individual_id is not null then 1 else 0 end) kcierpisz_optin
                from emea_optins_flags_tmp1 a, kcierpisz.emea_optins_prfl b
                where a.individual_id = b.individual_id (+)';

            dbms_output.put_line(sqlstmt);
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp2 CREATE', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp2 CREATE', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;
          end if;
*/

            -- prepare vg_prfl table
            if chrispack.is_table_populated('dm_metrics.vg_prfl_email_subscriptions') then
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
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'dm_metrics.vg_prfl_email_subscriptions not populated', sysdate,'NOT POPULATED - ');
                commit;
            end if;
            --------------------------

         if chrispack.is_table_populated('vg_prfl') then
            begin
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp2 CREATE', sysdate,'CREATING...');
            commit;
            sqlstmt := 'create table ' || table_name || '_tmp2 nologging as
                select a.sub_region_name, a.country_id, a.individual_id, a.contact_rowid, a.prospect_rowid,
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

         if chrispack.is_table_populated('dm_metrics.email_suppression') then
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp3 CREATE', sysdate,'CREATING...');
            commit;
            begin
            sqlstmt := 'create table ' || table_name || '_tmp3 nologging as
                select a.sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid, a.prospect_rowid,
                a.contact_email_prfl,a.contact_email_prfl2, max(case when b.email_address is not null then 1 end) suppression
                from ' || table_name || '_tmp2 a, dm_metrics.email_suppression b
                where a.email_address = b.email_address (+)
                group by a.sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid,
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

         if chrispack.is_table_populated('gcd_dw.gcd_individual_services') then
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp4 CREATE', sysdate,'CREATING...');
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

         if chrispack.is_table_populated('gcd_dw.gcd_correspondence_details') then
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp5 CREATE', sysdate,'CREATING...');
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


        if chrispack.is_table_populated(table_name || '_tmp5') then
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


            --- emea_inds2 -> emea_inds
            begin
                execute immediate 'create table ' || table_name || '_FLAGS nologging as
                select a.sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid, a.prospect_rowid,
                a.contact_email_prfl, a.contact_email_prfl2, a.gcd_services, a.suppression,
                a.correspondence1,
                (case when a.contact_email_prfl = ''Y'' or a.contact_email_prfl2 = ''Y'' then ''Y''
                	  when a.contact_email_prfl = ''N'' or a.contact_email_prfl2 = ''N'' then ''N''
                	  when a.suppression = 1 or a.gcd_services = ''N'' then ''N''
                	  when a.gcd_services = ''Y'' or a.correspondence1 = ''Y'' then ''Y''
                	  end) optin
                    from ' || table_name || '_tmp5 a
                    where email_address like ''_%@_%.__%''';
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS created from ' || table_name || '_tmp5', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS not created from ' || table_name || '_tmp5', sysdate,'NOT RENAMED - ' || err_msg);
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


           ---------preparing opportunities tables ---------------

            --   all_opportunities = 'KCIERPISZ.SUMANT_OPPTS1';
            --   opts_details      = 'KCIERPISZ.OPPTS3';

         if chrispack.is_table_populated(all_opportunities) and
            chrispack.is_table_populated(opts_details)
         then
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,all_opportunities || ' and ' || opts_details || ' POPULATED', sysdate,'POPULATED');
            commit;

                drop_table2(opt_contacts, email_optins_log);
                drop_table2(opt_emails,log_table);

            begin
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,opt_contacts || ' CREATE', sysdate,'CREATING...');
            commit;
            sqlstmt := 'create table ' || opt_contacts || ' as
                        select a."Contact or Prospect ID" contact_prospect_rowid,
                        max(upper(trim(a."Email Address"))) email_address,
                        max(to_date(b."Opened Date",''YYYY-MM-DD HH24:MI:SS'')) op_date
                        from ' || all_opportunities || ' a, ' || opt_details || ' b
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
                        from ' || all_opportunities || ' a, ' || opt_details || ' b
                        where a.row_id = b.row_id
                        and b."Opty Status" not in (''Lead:Declined'',''No Opportunity'',''Lost'')
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
            sqlstmt := 'create unique index bt_' || opt_emails || '_email on ' || opt_emails || ' (email_address)';
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

         if chrispack.is_table_populated(tars)
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
            sqlstmt := 'create unique index bt_' || tar1 || '_email on ' || tar1 || ' (gsi_party_id)';
            execute immediate sqlstmt;
            sqlstmt := 'create unique index bt_' || tar1 || '_duns on ' || tar1 || ' (duns_number)';
            execute immediate sqlstmt;
            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tar1 || ' INDEXes gsi_party_id and duns_number', sysdate,'CREATED');
            commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,tar1 || ' INDEXes gsi_party_id and duns_number', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

           ----------------------------

            ----------------- LORI rules ----------------
                       if chrispack.is_table_populated(table_name || '_FLAGS')
                          and CHRISPACK.is_table_populated(opt_contacts)
                          and CHRISPACK.is_table_populated(opt_emails)
                        then

                            drop_table2(table_name || '_FLAGS2', email_optins_log);

                            sqlstmt := 'create table ' || table_name || '_FLAGS2 nologging as
                                        select a.*, b.op_date op_date1, c.op_date op_date2,
                                        replace(greatest(nvl(b.op_date,to_date(''01011900'',''DDMMYYYY'')),
                                                         nvl(c.op_date,to_date(''01011900'',''DDMMYYYY''))),
                                                         to_date(''01011900'',''DDMMYYYY''),null
                                                ) op_date
                                        from ' || table_name || '_FLAGS a, ' || opt_contacts || ' b, ' || opt_emails || ' c
                                        where b.contact_prospect_rowid (+) = coalesce(a.contact_rowid, a.prospect_rowid)
                                        and a.email_address = c.email_address (+)
                                        ';
                            execute immediate sqlstmt;
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS2 created from ' || table_name || '_FLAGS', sysdate,'CREATED');
                            commit;


                       else
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS', sysdate,'NOT POPULATED');

                       end if;


                       if chrispack.is_table_populated(table_name || '_FLAGS2') then

                        drop table emea_optins_flags3;
create table emea_optins_flags3 as
select a.*, b.org_id, b.org_party_id, b.last_email_contacted_date from emea_optins_flags2 a, gcd_dw.gcd_individuals b
where a.individual_id = b.individual_id;

                        drop_table2(table_name || '_FLAGS3', email_optins_log);


                       else
                            insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_FLAGS2', sysdate,'NOT POPULATED');
                       end if;
            
            ---------------------------------------------


                view_stmt := 'create or replace view emea_optins_vw as
                    select a.sub_region_name, a.country_id, a.individual_id, a.email_address, a.contact_rowid, a.prospect_rowid,
                    a.optin
                    from ' || table_name || ' a';

                begin
                    execute immediate view_stmt;
                    insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'emea_optins_vw view', sysdate,'CREATED');
                    commit;
                exception when others then
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'emea_optins_vw view', sysdate,'NOT CREATED - ' || err_msg);
                    commit;
                end;

        else
                insert into email_optins_log values (email_optins_log_seq.NEXTVAL,table_name || '_tmp6', sysdate,'NOT POPULATED');

        end if;

    else

        --insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'gcd_dw.lb_individuals_eu_vw', sysdate,'NOT POPULATED ending');
        insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'NOT POPULATED ending');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON emea_optins_vw TO public';
    end;

    insert into email_optins_log values (email_optins_log_seq.NEXTVAL,'PROC_' || table_name, sysdate,'END');
    commit;

end;


procedure PROC_LAD_OPTINS_prfl -- changed from _b to main
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_name varchar2(30) := 'LAD_OPTINS_PRFL';
   emea_optins_log    varchar2(30) := 'EMEA_OPTINS_LOG';
   sqlstmt varchar2(4000) := '';

begin
    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'START');
    commit;
    --if chrispack.is_table_populated('gcd_dw.list_build_individuals_eu') then
    if chrispack.is_table_populated('gcd_dw.gcd_individuals') then
--        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_eu', sysdate,'POPULATED');
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'POPULATED');
        commit;

            begin
                execute immediate 'drop table ' || table_name || '2';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;
            begin
                execute immediate '
                        create table ' || table_name || '2 nologging as
                        select distinct ''region'' sub_region_name, a.country_id, a.individual_id,
                        a.email_address,
                        (case when b.email_address is null
                            --and a.email_address like ''_%@_%.__%''
                            and ( c.individual_id is not null and c.permission_given_flg = ''Y'' and c.correspondence_type_id = 1
                                or
                                  d.individual_id is not null and d.service_type_id = 39 and d.news_letter_flg in (''Y'',''1'')
                            )
                            and e.individual_id is null
                            then ''Y''
                        else ''N'' end) as contact_email
                    from gcd_dw.gcd_individuals a,
                         dm_metrics.email_suppression b, gcd_dw.gcd_correspondence_details c,
                         gcd_dw.gcd_individual_services d, dm_metrics.email_optout e
                    where
                        a.country_id in (7,9,10,12,16,19,22,24,26,30,40,43,47,52,60,61,63,65,87,90,93,94,97,108,140,145,156,166,168,169,174,180,181,182,202,215,219,227,230,232,233)
                        and upper(a.email_address) = b.email_address (+)
                        and a.individual_id = c.individual_id (+)
                        and c.correspondence_type_id (+) = 1
                        and a.individual_id = d.individual_id (+)
                        and d.service_type_id (+) = 39
                        and a.individual_id = e.individual_id (+)
                        ';

                        -- --a.sub_region_name in (''MIDDLE EAST'',''AFRICA'')
                        -- -- and nvl(a.contact_email,''Y'') = ''Y''
                        -- --                    from gcd_dw.list_build_individuals_eu a,
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'CREATED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            -- update with profile Y

            begin
                /*
                sqlstmt := 'update ' || table_name || '2 set
                            contact_email=''Y''
                            where individual_id in
                            (select new_individual_id from dm_metrics.vg_prfl_email_subscriptions
                                where case <>''OTHERS'' AND
                                      EMAIL_OPT_IN_FLAG_AFTR_SUP=''Y'')';
                */

sqlstmt := 'update ' || table_name || '2 b set
(contact_email, email_address) =
(select min(email_opt_in_flag_aftr_sup), min(a.email)
from dm_metrics.vg_prfl_email_subscriptions a
where case <>''OTHERS'' and a.new_individual_id = b.individual_id
and a.use_this_email = ''Y''
and a.email_opt_in_flag_aftr_sup = ''Y''
group by a.new_individual_id)
where exists (select 1 from dm_metrics.vg_prfl_email_subscriptions c
where c.new_individual_id = b.individual_id
and c.case <> ''OTHERS'' and c.use_this_email = ''Y''
and c.email_opt_in_flag_aftr_sup = ''Y'')';

                dbms_output.put_line(sqlstmt);
                execute immediate sqlstmt;
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 update with PROFILE', sysdate,'NOT UPDATED - ' || err_msg);
                commit;
            end;

            -- update with profile N

            begin
                sqlstmt := 'update ' || table_name || '2 set
                            contact_email=''N''
                            where individual_id in
                            (select new_individual_id from dm_metrics.vg_prfl_email_subscriptions
                                where case <>''OTHERS'' AND
                                      EMAIL_OPT_IN_FLAG_AFTR_SUP=''N'')';
                execute immediate sqlstmt;
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 update with PROFILE N', sysdate,'NOT UPDATED - ' || err_msg);
                commit;
            end;

            ------------------------

        if chrispack.is_table_populated(table_name || '2') then
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'POPULATED');
            commit;

            --- emea_optins -> emea_optins_bak

            begin
                execute immediate 'drop table ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                   commit;
            end;

            begin
                execute immediate 'alter table ' || table_name || ' rename to ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename -> ' || table_name || '_bak', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;


            --- emea_inds2 -> emea_inds
            begin
                --execute immediate 'alter table emea_inds2 rename to emea_inds';
                --insert into emea_inds_log values (emea_inds_seq.NEXTVAL,'emea_inds2 rename -> emea_inds', sysdate,'RENAMED');
                execute immediate 'create table ' || table_name || ' nologging as
                                    select sub_region_name, country_id, individual_id, email_address
                                    from ' || table_name || '2 where contact_email = ''Y'' and email_address like ''_%@_%.__%'''; -- is not null
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' created from ' || table_name || '2', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' not created from ' || table_name || '2', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'DROP INDEX BT_' || table_name || '_ind_id';
                execute immediate 'DROP INDEX BT_' || table_name || '_email';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'DROPPED');
                commit;
           EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate '
                    CREATE Unique INDEX BT_' || table_name || '_ind_id ON ' || table_name || ' (  individual_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_' || table_name || '_email ON ' || table_name || ' (  email_address  )
                    COMPUTE STATISTICS';
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'CREATED');
                    commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


        else
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'NOT POPULATED');

        end if;

    else

        --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.lb_individuals_eu_vw', sysdate,'NOT POPULATED ending');
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'NOT POPULATED ending');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON ' || table_name || ' TO public';
    end;

    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'END');
    commit;

end;

procedure PROC_NA_OPTINS_prfl -- changed from _b to main
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_name varchar2(30) := 'NA_OPTINS_PRFL';
   emea_optins_log    varchar2(30) := 'EMEA_OPTINS_LOG';
   sqlstmt varchar2(4000) := '';

begin
    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'START');
    commit;
    --if chrispack.is_table_populated('gcd_dw.list_build_individuals_eu') then
    if chrispack.is_table_populated('gcd_dw.gcd_individuals') then
--        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_eu', sysdate,'POPULATED');
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'POPULATED');
        commit;

            begin
                execute immediate 'drop table ' || table_name || '2';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;
            begin
                execute immediate '
                        create table ' || table_name || '2 nologging as
                        select distinct ''region'' sub_region_name, a.country_id, a.individual_id,
                        a.email_address,
                        (case when b.email_address is null
                            --and a.email_address like ''_%@_%.__%''
                            and ( c.individual_id is not null and c.permission_given_flg = ''Y'' and c.correspondence_type_id = 1
                                or
                                  d.individual_id is not null and d.service_type_id = 39 and d.news_letter_flg in (''Y'',''1'')
                            )
                            --and e.individual_id is null
                            then ''Y''
                        else ''N'' end) as contact_email
                    from gcd_dw.gcd_individuals a,
                         dm_metrics.email_suppression b, gcd_dw.gcd_correspondence_details c,
                         gcd_dw.gcd_individual_services d--, dm_metrics.email_optout e
                    where
                        a.country_id in (8,29,38,55,225,226)
                        and upper(a.email_address) = b.email_address (+)
                        and a.individual_id = c.individual_id (+)
                        and c.correspondence_type_id (+) = 1
                        and a.individual_id = d.individual_id (+)
                        and d.service_type_id (+) = 39
                        --and a.individual_id = e.individual_id (+)
                        ';

                        -- --a.sub_region_name in (''MIDDLE EAST'',''AFRICA'')
                        -- -- and nvl(a.contact_email,''Y'') = ''Y''
                        -- --                    from gcd_dw.list_build_individuals_eu a,
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'CREATED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            -- update with profile Y

            begin
                /*
                sqlstmt := 'update ' || table_name || '2 set
                            contact_email=''Y''
                            where individual_id in
                            (select new_individual_id from dm_metrics.vg_prfl_email_subscriptions
                                where case <>''OTHERS'' AND
                                      EMAIL_OPT_IN_FLAG_AFTR_SUP=''Y'')';
                */

sqlstmt := 'update ' || table_name || '2 b set
(contact_email, email_address) =
(select min(email_opt_in_flag_aftr_sup), min(a.email)
from dm_metrics.vg_prfl_email_subscriptions a
where case <>''OTHERS'' and a.new_individual_id = b.individual_id
and a.use_this_email = ''Y''
and a.email_opt_in_flag_aftr_sup = ''Y''
group by a.new_individual_id)
where exists (select 1 from dm_metrics.vg_prfl_email_subscriptions c
where c.new_individual_id = b.individual_id
and c.case <> ''OTHERS'' and c.use_this_email = ''Y''
and c.email_opt_in_flag_aftr_sup = ''Y'')';

                dbms_output.put_line(sqlstmt);
                execute immediate sqlstmt;
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 update with PROFILE', sysdate,'NOT UPDATED - ' || err_msg);
                commit;
            end;

            -- update with profile N

            begin
                sqlstmt := 'update ' || table_name || '2 set
                            contact_email=''N''
                            where individual_id in
                            (select new_individual_id from dm_metrics.vg_prfl_email_subscriptions
                                where case <>''OTHERS'' AND
                                      EMAIL_OPT_IN_FLAG_AFTR_SUP=''N'')';
                execute immediate sqlstmt;
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 update with PROFILE N', sysdate,'NOT UPDATED - ' || err_msg);
                commit;
            end;

            ------------------------

        if chrispack.is_table_populated(table_name || '2') then
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'POPULATED');
            commit;

            --- emea_optins -> emea_optins_bak

            begin
                execute immediate 'drop table ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                   commit;
            end;

            begin
                execute immediate 'alter table ' || table_name || ' rename to ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename -> ' || table_name || '_bak', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;


            --- emea_inds2 -> emea_inds
            begin
                --execute immediate 'alter table emea_inds2 rename to emea_inds';
                --insert into emea_inds_log values (emea_inds_seq.NEXTVAL,'emea_inds2 rename -> emea_inds', sysdate,'RENAMED');
                execute immediate 'create table ' || table_name || ' nologging as
                                    select sub_region_name, country_id, individual_id, email_address
                                    from ' || table_name || '2 where contact_email = ''Y'' and email_address like ''_%@_%.__%'''; -- is not null
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' created from ' || table_name || '2', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' not created from ' || table_name || '2', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'DROP INDEX BT_' || table_name || '_ind_id';
                execute immediate 'DROP INDEX BT_' || table_name || '_email';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'DROPPED');
                commit;
           EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate '
                    CREATE Unique INDEX BT_' || table_name || '_ind_id ON ' || table_name || ' (  individual_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_' || table_name || '_email ON ' || table_name || ' (  email_address  )
                    COMPUTE STATISTICS';
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'CREATED');
                    commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


        else
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'NOT POPULATED');

        end if;

    else

        --insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.lb_individuals_eu_vw', sysdate,'NOT POPULATED ending');
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.gcd_individuals', sysdate,'NOT POPULATED ending');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON ' || table_name || ' TO public';
    end;

    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'END');
    commit;

end;

procedure PROC_APAC_OPTINS_prfl
is
   err_num NUMBER;
   err_msg VARCHAR2(100);
   table_name varchar2(30) := 'APAC_OPTINS_PRFL';
   emea_optins_log    varchar2(30) := 'EMEA_OPTINS_LOG';
   sqlstmt varchar2(4000) := '';

begin
    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'START');
    commit;
    if chrispack.is_table_populated('gcd_dw.list_build_individuals_ap') then
        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.list_build_individuals_ap', sysdate,'POPULATED');
        commit;

            begin
                execute immediate 'drop table ' || table_name || '2';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'DROPPED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 drop', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;
            begin
                execute immediate '
                        create table ' || table_name || '2 nologging as
                        select distinct a.sub_region_name, a.country_id, a.individual_id,
                        a.email_address,
                        (case when b.email_address is null
                            --and a.email_address like ''_%@_%.__%''
                            and ( c.individual_id is not null and c.permission_given_flg = ''Y'' and c.correspondence_type_id = 1
                                or
                                  d.individual_id is not null and d.service_type_id = 39 and d.news_letter_flg in (''Y'',''1'')
                            )
                            and e.individual_id is null
                            then ''Y''
                        else ''N''
                        end) as contact_email
                    from gcd_dw.list_build_individuals_ap a,
                         dm_metrics.email_suppression b, gcd_dw.gcd_correspondence_details c,
                         gcd_dw.gcd_individual_services d, dm_metrics.email_optout e
                    where
                        upper(a.email_address) = b.email_address (+)
                        and a.individual_id = c.individual_id (+)
                        and c.correspondence_type_id (+) = 1
                        and a.individual_id = d.individual_id (+)
                        and d.service_type_id (+) = 39
                        and a.individual_id = e.individual_id (+)
                        ';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'CREATED');
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 create', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;

            -- update with profile Y

            begin
                sqlstmt := 'update ' || table_name || '2 set
                            contact_email=''Y''
                            where individual_id in
                            (select new_individual_id from dm_metrics.vg_prfl_email_subscriptions
                                where case <>''OTHERS'' AND
                                      EMAIL_OPT_IN_FLAG_AFTR_SUP=''Y'')';
                execute immediate sqlstmt;
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 update with PROFILE', sysdate,'NOT UPDATED - ' || err_msg);
                commit;
            end;

            -- update with profile N

            begin
                sqlstmt := 'update ' || table_name || '2 set
                            contact_email=''N''
                            where individual_id in
                            (select new_individual_id from dm_metrics.vg_prfl_email_subscriptions
                                where case <>''OTHERS'' AND
                                      EMAIL_OPT_IN_FLAG_AFTR_SUP=''N'')';
                execute immediate sqlstmt;
                commit;
            exception when others then
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2 update with PROFILE N', sysdate,'NOT UPDATED - ' || err_msg);
                commit;
            end;

            ------------------------

        if chrispack.is_table_populated(table_name || '2') then
            insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'POPULATED');
            commit;

            --- emea_optins -> emea_optins_bak

            begin
                execute immediate 'drop table ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'DROPPED');
                commit;
                EXCEPTION WHEN OTHERS THEN
                    err_msg := SUBSTR(SQLERRM, 1, 100);
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '_bak drop', sysdate,'NOT DROPPED - ' || err_msg);
                   commit;
            end;

            begin
                execute immediate 'alter table ' || table_name || ' rename to ' || table_name || '_bak';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename -> ' || table_name || '_bak', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' rename', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;


            --- emea_inds2 -> emea_inds
            begin
                --execute immediate 'alter table emea_inds2 rename to emea_inds';
                --insert into emea_inds_log values (emea_inds_seq.NEXTVAL,'emea_inds2 rename -> emea_inds', sysdate,'RENAMED');
                execute immediate 'create table ' || table_name || ' nologging as
                                    select sub_region_name, country_id, individual_id, email_address
                                    from ' || table_name || '2 where contact_email = ''Y'' and email_address is not null';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' created from ' || table_name || '2', sysdate,'RENAMED');
                commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' not created from ' || table_name || '2', sysdate,'NOT RENAMED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate 'DROP INDEX BT_' || table_name || '_ind_id';
                execute immediate 'DROP INDEX BT_' || table_name || '_email';
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'DROPPED');
                commit;
           EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT DROPPED - ' || err_msg);
                commit;
            end;

            begin
                execute immediate '
                    CREATE Unique INDEX BT_' || table_name || '_ind_id ON ' || table_name || ' (  individual_id  )
                    COMPUTE STATISTICS';
                execute immediate '
                    CREATE INDEX BT_' || table_name || '_email ON ' || table_name || ' (  email_address  )
                    COMPUTE STATISTICS';
                    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'CREATED');
                    commit;
            EXCEPTION WHEN OTHERS THEN
                err_msg := SUBSTR(SQLERRM, 1, 100);
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || ' indexes', sysdate,'NOT CREATED - ' || err_msg);
                commit;
            end;


        else
                insert into emea_optins_log values (emea_optins_seq.NEXTVAL,table_name || '2', sysdate,'NOT POPULATED');

        end if;

    else

        insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'gcd_dw.lb_individuals_ap_vw', sysdate,'NOT POPULATED ending');
        commit;
    end if;

    begin
      execute immediate 'GRANT SELECT ON ' || table_name || ' TO public';
    end;

    insert into emea_optins_log values (emea_optins_seq.NEXTVAL,'PROC_' || table_name, sysdate,'END');
    commit;

end;


function get_token(
    the_list  varchar2,
    the_index number,
    delim     varchar2 := ','
)
    return    varchar2
is
    start_pos number;
    end_pos   number;
begin
    if the_index = 1 then
        start_pos := 1;
    else
        start_pos := instr(the_list,delim,1,the_index - 1);
        if start_pos = 0 then
            return null;
        else
            start_pos := start_pos + length(delim);
        end if;
    end if;

    end_pos := instr(the_list,delim,start_pos,1);

    if end_pos = 0 then
        return substr(the_list,start_pos);
    else
        return substr(the_list,start_pos,end_pos - start_pos);
    end if;
end get_token;

function get_info
    (tableName in varchar2)
    return varchar2 is

    result varchar2(4000) := '';
    rows_no number := 0;
    creation_date date := '';

    table_name varchar2(100) := '';
    table_name_tmp varchar2(100) := '';
    host_name  varchar2(100) := '';
    schema_name varchar2(100) := '';
    sql_date   varchar2(4000) := '';

    type cur_ref is ref cursor;
    tab cur_ref;

begin
    open tab for 'select count(*) from ' || tableName;
        loop
            exit when tab%notfound;
            fetch tab into rows_no;
            result := tableName || ' check returned ' || rows_no || ' rows';
        end loop;
    close tab;

    table_name_tmp := get_token(tableName,1,'@');
    if instr(table_name_tmp,'.') > 0 then
        table_name := get_token(table_name_tmp,2,'.');
        schema_name := get_token(table_name_tmp,1,'.');
    else
        table_name := table_name_tmp;
    end if;
    host_name  := get_token(tableName,2,'@');

    sql_date := 'SELECT created FROM all_objects';
    if host_name is not null then
        sql_date := sql_date || '@' || host_name;
    end if;
    sql_date := sql_date || ' WHERE object_type in (''TABLE'',''VIEW'') AND object_name = upper(''' || table_name || ''')';

    if schema_name is not null then
        sql_date := sql_date || ' AND owner = ''' || upper(schema_name) || '''';
    end if;

    open tab for sql_date;
        --loop
        --    exit when tab%notfound;
            fetch tab into creation_date;
            result := result || ' created on: ' || to_char(creation_date + 9/24,'yyyy-mm-dd HH24:MI');
        --end loop;
    close tab;

    return result;
exception when others then
    result := tableName || ' check returned an EXCEPTION';
    return result;
end;

/*
function get_count( tableName is varchar2)
    return number
is
    result number;
    type cur_ref is ref cursor;
    tab    cur_ref;
begin
    open tab for 'select count(*) from '  || tableName;
    loop
         exit when tab%notfound;
         fetch tab into result;
    end loop;
    close tab;

    return result;
end;
*/

function get_details (tableName in varchar2)
         return varchar2
is
    result                  varchar2(4000) := '';

    type cur_ref is ref cursor;
    tab                     cur_ref;

    owner                   varchar2(30) := '';
    table_name_link         varchar2(60) := '';
    table_name_tmp          varchar2(60) := '';
    table_name_owner        varchar2(30) := '';
    database_link           varchar2(30) := '';
    indexes_info            varchar2(4000) := '';
begin
    if instr(tableName,'@') > 0 then
        table_name_link := get_token(tableName,1,'@');
        database_link := get_token(tableName,2,'@');
    else
        table_name_link := tableName;
    end if;

    if instr(table_name_link,'.') > 0 then
        table_name_owner := get_token(table_name_link,1,'.');
        table_name_tmp := get_token(table_name_link,2,'.');
    else
        table_name_tmp := table_name_link;

        open tab for 'SELECT sys_context(''USERENV'', ''CURRENT_SCHEMA'') FROM dual';
        loop
            exit when tab%notfound;
            fetch tab into table_name_owner;
        end loop;
        close tab;

    end if;

    if is_table_populated(tableName) then
        open tab for 'select count(*) || '':'' || to_row(a.column_name)as indexes
from all_ind_columns a, all_indexes b
where a.index_name = b.index_name
and a.table_name = ''' || upper(table_name_tmp) ||
''' and a.table_owner = ''' || upper(table_name_owner) ||
''' and a.column_name not like ''%$%''';
        loop
            exit when tab%notfound;
            fetch tab into indexes_info;
        end loop;
        close tab;
        result := table_name_owner || '.' || table_name_tmp ||
                  '-' || indexes_info;
    else
        result := 'table not populated: ' || table_name_owner || '.' || table_name_tmp;
    end if;

    return result;
end;

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

function is_table_populated2
    (tableName in varchar2)
    return varchar2 is

    result varchar2(10) := 'false';
    rows_no number := 0;

    type cur_ref is ref cursor;
    tab cur_ref;

begin

        open tab for 'select count(*) from ' || tableName || ' where rownum < 2';
        loop
            exit when tab%notfound;
            fetch tab into rows_no;
            if rows_no > 0 then
                result := 'true';
            end if;
        end loop;
    close tab;

    return result;
exception when others then
    result := 'false';
    return result;
end;

END CHRISPACK;

