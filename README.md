# Semantix
## Exercícios de Spart

### Respostas:

#### Qual o objetivo do comando cache em Spark?
O Objetivo é gravar os resultados em memória para não ter que executar novamente um procedimento que já foi feito.

#### O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?
Porque o Mapreduce usa disco e depende das velocidades de leitura e escrita, enquanto o Spark realiza os procedimentos in-memory.

#### Qual é a função do SparkContext?
Serve para criar um client que vai se conectar ao cluster do spark e usar a engine do spark para rodar os jobs.

#### Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
É um processamento massivo paralelo de dados in-memory, ele distribui o dataset em diversas máquinas para ter uma velocidade maior de execução

#### GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
Não sei responder ainda.

#### Explique o que o código Scala abaixo faz.
```scala
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
.map(word => (word, 1))
.reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")
```
Não tenho conhecimento em Scala ainda, mas pela similaridade ao python acredito que ele está fazendo text mining, lendo um arquivo de texto, separando palavra por palavra e criando um índice delas.


