# Post LinkedIn — DataRadar

> Rascunho para publicacao. Anexar screenshot do dashboard como imagem.

---

Construi um pipeline de dados end-to-end que monitora 72 comunidades tech do Reddit em tempo real.

O DataRadar extrai milhares de posts e comentarios por dia, processa tudo em camadas (Bronze -> Silver -> Gold) e serve os resultados num dashboard interativo com dados reais.

A stack:
-> Apache Airflow orquestrando a ingestao (3 DAGs, extracao horaria)
-> AWS S3 como data lake (camada Bronze)
-> AWS Lambda reagindo a novos arquivos (event-driven, custo zero quando inativo)
-> Databricks processando com PySpark + Delta Lake (Silver/Gold)
-> FastAPI + SQL Warehouse servindo dados reais no dashboard

O maior desafio tecnico foi lidar com o rate limiting da API publica do Reddit. Com 72 subreddits e centenas de posts por extracao, implementei um sistema de concorrencia controlada no Airflow (Pools) com retry exponencial que respeita o header Retry-After. Parece simples, mas a diferenca entre "funciona no teste" e "funciona em producao com 72 subreddits" e enorme.

Tudo open source, com CI/CD (GitHub Actions), 55+ testes automatizados e documentacao de decisoes tecnicas.

GitHub: https://github.com/wesleyolvr/DataRadar

#DataEngineering #Python #ApacheAirflow #Databricks #AWS #OpenSource #Pipeline #DataPipeline
