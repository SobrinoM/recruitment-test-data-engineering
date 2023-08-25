USE codetest;


create table `dwh_f_l1_natality` ( 
  `fk_date_of_birth` int null,
  `fk_full_name` int null,
  `fk_city` int null
);


INSERT INTO codetest.adm_migration_history(script_name) VALUES("script_004_f_l1_natality");
