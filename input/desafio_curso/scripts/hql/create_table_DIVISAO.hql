CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.divisao ( 
        Division string,
        Division_Name string
    )
COMMENT 'Tabela divisao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/DIVISAO/'
TBLPROPERTIES ("skip.header.line.count"="1");
