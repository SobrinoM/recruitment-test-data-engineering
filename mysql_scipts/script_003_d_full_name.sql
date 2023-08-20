USE codetest;


create table `dwh_d_full_name` (
  `pk_full_name` int not null auto_increment,
  `given_name` varchar(80) default null,
  `family_name` varchar(80) default null,
  primary key (`pk_full_name`)
);

ALTER TABLE dwh_d_full_name ADD UNIQUE list_unique_index(given_name, family_name);


DELIMITER //

CREATE PROCEDURE spUpsert_dwh_d_full_name()
BEGIN

INSERT
INTO    dwh_d_full_name (given_name, family_name)
SELECT  given_name, family_name
FROM    stg_d_full_name 
ON DUPLICATE KEY
UPDATE
        dwh_d_full_name.family_name = stg_d_full_name.family_name;

END //

DELIMITER ;

INSERT INTO codetest.adm_migration_history(script_name) VALUES("script_003_d_full_name");
