USE codetest;

CREATE TABLE IF NOT EXISTS adm_migration_history(
        script_name VARCHAR(255),
        exec_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

INSERT INTO codetest.adm_migration_history(script_name) VALUES("script000_init");