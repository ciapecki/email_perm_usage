PACKAGE EMAIL_PKG
  IS

  function is_table_populated (tableName in varchar2) return boolean;
  procedure drop_table2(table_name in varchar2, log_table in varchar2);
  procedure populate_diff_table(table_name in varchar2);
  procedure PROC_EMAIL_OPTINS;

END; -- Package spec

