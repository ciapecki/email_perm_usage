alter table email_optins_diff1 add (created_at date);

update email_optins_diff1 set created_at = sysdate - 2;
commit;

create table email_optins_diff2 as
select * from EMAIL_OPTINS_DIFF1 a
union all
select a.*, sysdate from 
(
select * from email_optins_bak
minus
select * from email_optins
) a

select a.created_at, count(*) from email_optins_diff2 a
group by a.created_at

drop table diff_test;
create table diff_test as
select * from email_optins_diff2;

select count(*) from diff_test -- 38.499
select * from diff_test a where a.created_at > sysdate - 2

delete from diff_test a where a.created_at > sysdate - 2
;commit;

drop table diff_test;
create table diff_test as select * from diff_test_bak;

call email_pkg.populate_diff_table('diff_test');

select * from email_optins_log order by id desc


create table diff_test2 as
                    select * from diff_test a
                    union all
                    select a.*, sysdate from
                    (
                        select * from email_optins_bak
                        minus
                        select * from email_optins
                    ) a
                    
select count(*) from diff_test_bak -- 37.528
select count(*) from diff_test -- 37.528

select * from diff_test a

    delete from diff_test a where a.created_at > sysdate - 1;
    commit;
    
      select count(*) from diff_test ; -- 49558
      
      call email_pkg.populate_diff_table('diff_test');

      select count(*) from diff_test ; -- 49558

