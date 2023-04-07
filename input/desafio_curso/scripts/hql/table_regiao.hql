CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.regiao ( 
        Region_Code string,
        Region_Name string
    )
COMMENT 'Tabela regiao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/REGIAO/'
TBLPROPERTIES ("skip.header.line.count"="1");