USE codetest;


create table `dwh_d_city` (
  `pk_city` int not null auto_increment,
  `city` varchar(80) default null,
  `county` varchar(80) default null,
  `country` varchar(80) default null,
  primary key (`pk_city`)
);

ALTER TABLE dwh_d_city ADD UNIQUE list_unique_index(city, county, country);


DELIMITER //

CREATE PROCEDURE spUpsert_dwh_d_city()
BEGIN

INSERT
INTO    dwh_d_city (city, county, country)
SELECT  city, county, country
FROM    stg_d_city 
ON DUPLICATE KEY
UPDATE
        dwh_d_city.country = stg_d_city.country;

END //

DELIMITER ;




INSERT INTO codetest.adm_migration_history(script_name) VALUES("script_002_d_city");
