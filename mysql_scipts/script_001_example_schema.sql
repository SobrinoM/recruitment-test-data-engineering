USE codetest;

drop table if exists examples;

create table `examples` (
  `id` int not null auto_increment,
  `name` varchar(80) default null,
  primary key (`id`)
);

INSERT INTO codetest.adm_migration_history(script_name) VALUES("script001_example_schema");
