#So para formatação


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



select * from pedidos as p
inner join item_pedido as ip on p.id_pedido = ip.id_pedido
limit
vav










sql_filial = ''' select * from filia f
         join cidade c on f.id_cidade = c.id_cidade
         join estado e on f.id_estado = c.id_estado
    '''

df_filial = spark.sql(sql_filial)

df_filial.show(10)