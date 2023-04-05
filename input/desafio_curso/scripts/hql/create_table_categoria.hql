CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.clientes ( 
        id_categoria string,
        ds_categoria string,
        perc_parceiro string
    )
COMMENT 'Tabela de Categoria'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/categoria/'
TBLPROPERTIES ("skip.header.line.count"="1");

#TABELA CLIENTES

CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.clientes ( 
        Address_Number string,
        Business_Family string,
        Business_Unit string,
        Customer string,
        Customer_Key string,
        Customer_Type string,
        Division string,
        Line_of_Business string,
        Phone string,
        Region_Code string,
        Regional_Sales_Mgr string,
        Search_Type string
    )
COMMENT 'Tabela de clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/CLIENTES/'
TBLPROPERTIES ("skip.header.line.count"="1");

#TABELA DIVISAO#

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


#TABELA ENDERECO#

CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.endereco ( 
        Address_Number string,
        City string,
        Country string,
        Customer_Address_1 string,
        Customer_Address_2 string,
        Customer_Address_3 string,
        Customer_Address_4 string,
        State string,
        Zip_Code string
    )
COMMENT 'Tabela endereco'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/ENDERECO/'
TBLPROPERTIES ("skip.header.line.count"="1");



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

#TABELA VENDAS#
CREATE EXTERNAL TABLE IF NOT EXISTS vendas(
    Actual_Delivery_Date string,
    Customer_key string,
    Datekey string,
    Discount_Amount string,
    Invoice_Date string,
    Invoice_Number string,
    Item_Class string,
    Item_Number string,
    Item string,
    Line_Number string,
    List_Price string,
    Order_Number string,
    Promised_Delivery_Date string,
    Sales_Amount string,
    Sales_Amount_Based_on_List_Price string,
    Sales_Cost_Amount string,
    Sales_Margin_Amount string,
    Sales_Price string,
    Sales_Quantity string,
    Sales_Rep string,
    `U/M` string
)
COMMENT 'Tabela vendas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/datalake/raw/VENDAS/'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.vendas(
    
)
COMMENT 'Tabela vendas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/datalake/raw/VENDAS/'
TBLPROPERTIES ("skip.header.line.count"="1");
