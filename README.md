#Desafio Big Data/Bii
Este repositório contém o código desenvolvido para o Desafio Big Data/BI. O objetivo do desafio era realizar a ingestão de dados de uma pasta /raw com alguns arquivos .csv de um banco relacional de vendas e fornecer dados em uma pasta desafio_curso/gold em formato .csv para serem consumidos por um relatório em PowerBI.

#Tecnologias utilizadas
Apache Hadoop
Apache Hive
Apache Spark
Jupyter Notebook
PowerBI

#Etapas do desafio
O desafio foi dividido em várias etapas, que foram realizadas conforme descrito abaixo:

#Etapa 1 - Enviar os arquivos para o HDFS
Nesta etapa, foram enviados os arquivos .csv para o HDFS. Para isso, foi criado um shell script que fazia o trabalho repetitivo de enviar os arquivos para o HDFS.

#Etapa 2 - Criar o banco DEASFIO_CURSO e dentro tabelas no Hive usando o HQL e executando um script shell dentro do hive server na pasta scripts/pre_process.
Nesta etapa, foi criado o banco DEASFIO_CURSO e dentro dele, as tabelas no Hive usando o HQL e executando um script shell dentro do hive server na pasta scripts/pre_process. As tabelas são:

TBL_VENDAS
TBL_CLIENTES
TBL_ENDERECO
TBL_REGIAO
TBL_DIVISAO

#Etapa 3 - Processar os dados no Spark Efetuando suas devidas transformações criando os arquivos com a modelagem de BI.
Nesta etapa, foram processados os dados no Spark efetuando suas devidas transformações e criando os arquivos com a modelagem de BI. O desenvolvimento foi feito no Jupyter, mas o código final ficou no arquivo desafio_curso/scripts/process/process.py.

#Etapa 4 - Gravar as informações em tabelas dimensionais em formato cvs delimitado por ';'.
Nesta etapa, as informações foram gravadas em tabelas dimensionais em formato csv delimitado por ';'. As tabelas são:

FT_VENDAS
DIM_CLIENTES
DIM_TEMPO
DIM_LOCALIDADE

#Etapa 5 - Exportar os dados para a pasta desafio_curso/gold
Nesta etapa, os dados foram exportados para a pasta desafio_curso/gold.

#Etapa 6 - Criar e editar o PowerBI com os dados que você trabalhou.
Nesta etapa, foi criado e editado o PowerBI com os dados trabalhados. Foram criados gráficos de vendas.

#Etapa 7 - Criar uma documentação com os testes e etapas do projeto.
Nesta etapa, foi criada uma documentação com os testes e etapas do projeto.