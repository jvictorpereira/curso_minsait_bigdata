CREATE TABLE IF NOT EXISTS desafio.pedidos (
    id_pedido string,
    dt_pedito string,
    id_parceiro string,
    id_cliente string,
    id_filial string,
    vr_total_pago string

)
COMMENT 'Tabela de Pedidos'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/pedido/'
TBLPROPERTIES ("skip.header.line.count"="1")