from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType

def criar_spark_session():
    """
    Cria e retorna uma sessão Spark.
    É necessário ter o driver JDBC do PostgreSQL no classpath.
    Exemplo de como executar o script:
    spark-submit --jars /path/to/postgresql-42.2.23.jar etl_pyspark.py
    """
    spark = SparkSession.builder \
        .appName("ETL Global Retail") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
        .getOrCreate()
    return spark

def extrair_dados(spark, url, properties):
    """
    Extrai dados do banco de dados transacional usando uma query consolidada.
    """
    query = """
    (SELECT
        v.id_venda,
        v.data_venda,
        iv.id_produto,
        iv.qtd_vendida,
        iv.preco_venda,
        p.nome_produto,
        cp.nome_categoria_produto,
        c.id_cliente,
        c.nome_cliente,
        c.idade,
        c.genero,
        cc.nome_categoria_cliente,
        l.id_localidade,
        l.cidade,
        l.estado,
        l.regiao,
        pf.custo_compra_unitario
    FROM vendas v
    JOIN item_vendas iv ON v.id_venda = iv.id_venda
    JOIN produto p ON iv.id_produto = p.id_produto
    JOIN categoria_produto cp ON p.id_categoria_produto = cp.id_categoria_produto
    JOIN cliente c ON v.id_cliente = c.id_cliente
    JOIN categoria_cliente cc ON c.id_categoria_cliente = cc.id_categoria_cliente
    JOIN localidade l ON c.id_localidade = l.id_localidade
    LEFT JOIN produto_fornecedor pf ON p.id_produto = pf.id_produto) as vendas_completas
    """
    df = spark.read.jdbc(url=url, table=query, properties=properties)
    return df

def transformar_dados(df):
    """
    Aplica as transformações de limpeza e padronização nos dados.
    """
    # Tratamento de valores nulos
    # Calcula a mediana da idade para preenchimento
    idade_mediana = df.select("idade").na.drop().agg({"idade": "median"}).collect()[0][0]
    df = df.na.fill({
        "idade": int(idade_mediana) if idade_mediana is not None else 35,
        "genero": "Não informado",
        "nome_produto": "Produto não informado",
        "custo_compra_unitario": 0
    })

    # Limpeza e Padronização
    df = df.withColumn("genero",
        f.when(f.col("genero") == "M", "Masculino")
        .when(f.col("genero") == "F", "Feminino")
        .otherwise(f.col("genero"))
    )
    df = df.withColumn("estado", f.upper(f.col("estado")))

    # Tratamento de Datas
    # Converte para data e remove linhas com datas inválidas
    df = df.withColumn("data_venda", f.to_date(f.col("data_venda"), "yyyy-MM-dd"))
    df = df.na.drop(subset=["data_venda"])

    # Cálculos de negócio
    df = df.withColumn("valor_total", f.round(f.col("qtd_vendida") * f.col("preco_venda"), 2))

    # Criação das Dimensões
    dim_cliente = df.select("id_cliente", "nome_cliente", "idade", "genero", "nome_categoria_cliente").distinct()
    dim_produto = df.select("id_produto", "nome_produto", "nome_categoria_produto").distinct()
    dim_localidade = df.select("id_localidade", "cidade", "estado", "regiao").distinct()

    dim_tempo = df.select("data_venda").distinct() \
        .withColumn("ano", f.year(f.col("data_venda"))) \
        .withColumn("mes", f.month(f.col("data_venda"))) \
        .withColumn("dia", f.dayofmonth(f.col("data_venda"))) \
        .withColumn("trimestre", f.quarter(f.col("data_venda")))

    return dim_cliente, dim_produto, dim_localidade, dim_tempo, df

def carregar_dados(dim_cliente, dim_produto, dim_localidade, dim_tempo, df_transformado, url_dw, properties_dw):
    """
    Carrega os DataFrames de dimensão e fato no Data Warehouse.
    """
    # Ordem de carga: Dimensões primeiro
    dim_cliente.write.jdbc(url=url_dw, table="dim_cliente", mode="overwrite", properties=properties_dw)
    dim_produto.write.jdbc(url=url_dw, table="dim_produto", mode="overwrite", properties=properties_dw)
    dim_localidade.write.jdbc(url=url_dw, table="dim_localidade", mode="overwrite", properties=properties_dw)
    dim_tempo.write.jdbc(url=url_dw, table="dim_tempo", mode="overwrite", properties=properties_dw)

    print("Dimensões carregadas com sucesso.")

    # Ler dimensões de volta para obter as Surrogate Keys (SKs)
    spark = SparkSession.getActiveSession()
    dim_cliente_sk = spark.read.jdbc(url=url_dw, table="dim_cliente", properties=properties_dw)
    dim_produto_sk = spark.read.jdbc(url=url_dw, table="dim_produto", properties=properties_dw)
    dim_localidade_sk = spark.read.jdbc(url=url_dw, table="dim_localidade", properties=properties_dw)
    dim_tempo_sk = spark.read.jdbc(url=url_dw, table="dim_tempo", properties=properties_dw)

    # Join para enriquecer o DataFrame principal com as SKs
    fato_vendas = df_transformado \
        .join(dim_cliente_sk, "id_cliente") \
        .join(dim_produto_sk, "id_produto") \
        .join(dim_localidade_sk, "id_localidade") \
        .join(dim_tempo_sk, "data_venda") \
        .select(
            f.col("sk_cliente"),
            f.col("sk_produto"),
            f.col("sk_localidade"),
            f.col("sk_tempo"),
            f.col("id_venda"),
            f.col("qtd_vendida"),
            f.col("preco_venda"),
            f.col("valor_total"),
            f.col("custo_compra_unitario")
        )

    # Carregar tabela de fatos
    fato_vendas.write.jdbc(url=url_dw, table="fato_vendas", mode="overwrite", properties=properties_dw)
    print("Tabela de fatos carregada com sucesso.")


def main():
    """
    Orquestra o processo de ETL.
    """
    spark = criar_spark_session()

    # Configurações de conexão
    db_url_crm = "jdbc:postgresql://localhost:5432/crm"
    db_properties_crm = {"user": "postgres", "password": "password", "driver": "org.postgresql.Driver"}

    db_url_dw = "jdbc:postgresql://localhost:5432/dw"
    db_properties_dw = {"user": "postgres", "password": "password", "driver": "org.postgresql.Driver"}

    # Extração
    df_origem = extrair_dados(spark, db_url_crm, db_properties_crm)
    print("Extração concluída.")

    # Transformação
    dim_cliente, dim_produto, dim_localidade, dim_tempo, df_transformado = transformar_dados(df_origem)
    print("Transformação concluída.")

    # Carga
    carregar_dados(dim_cliente, dim_produto, dim_localidade, dim_tempo, df_transformado, db_url_dw, db_properties_dw)
    print("Carga concluída.")

    spark.stop()

if __name__ == "__main__":
    main()
