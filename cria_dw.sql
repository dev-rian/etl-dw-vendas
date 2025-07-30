CREATE TABLE dim_cliente (
    sk_cliente SERIAL PRIMARY KEY,
    id_cliente INT,
    nome_cliente VARCHAR(255),
    idade INT,
    genero VARCHAR(50),
    nome_categoria_cliente VARCHAR(100)
);

CREATE TABLE dim_produto (
    sk_produto SERIAL PRIMARY KEY,
    id_produto INT,
    nome_produto VARCHAR(255),
    nome_categoria_produto VARCHAR(100)
);

CREATE TABLE dim_localidade (
    sk_localidade SERIAL PRIMARY KEY,
    id_localidade INT,
    cidade VARCHAR(100),
    estado VARCHAR(50),
    regiao VARCHAR(50)
);

CREATE TABLE dim_tempo (
    sk_tempo SERIAL PRIMARY KEY,
    data_venda DATE,
    ano INT,
    mes INT,
    dia INT,
    trimestre INT
);

CREATE TABLE fato_vendas (
    sk_cliente INT REFERENCES dim_cliente(sk_cliente),
    sk_produto INT REFERENCES dim_produto(sk_produto),
    sk_localidade INT REFERENCES dim_localidade(sk_localidade),
    sk_tempo INT REFERENCES dim_tempo(sk_tempo),
    id_venda INT,
    qtd_vendida INT,
    preco_venda DECIMAL(10, 2),
    valor_total DECIMAL(10, 2),
    custo_compra_unitario DECIMAL(10, 2),
    PRIMARY KEY (sk_cliente, sk_produto, sk_localidade, sk_tempo, id_venda)
);