### Este projeto permite consultar em linguagem natural a base de históricos de preços de combustíveis da ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis), fornecendo respostas rápidas e precisas com uso de IA.

### Fonte dos dados: [ANP](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis)

### Funcionalidades
✅ Download automatizado dos datasets da ANP

✅ Limpeza e processamento eficiente com Polars

✅ Armazenamento em DuckDB para consultas rápidas

✅ Interface de consulta via IA usando Ollama e Phi-3

✅ Respostas em linguagem natural para perguntas como:

"Qual a média do preço de venda da gasolina em 2020?"

"Qual a média de preço do diesel em 2017 por região?"

"Mostre a evolução do preço do etanol em São Paulo nos últimos 5 anos."

**🛠️ Tecnologias Utilizadas**

**Ingestão de Dados:**  Requests, Polars, BeautifulSoup

**Processamento:** Polars

**Banco de Dados:** DuckDB

**IA:** Ollama com modelo Phi-3