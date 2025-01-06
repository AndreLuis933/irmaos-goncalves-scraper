# Web Scraping Supermercado Irmãos Gonçalves

## Índice
1. [Descrição](#descrição)
2. [Funcionalidades](#funcionalidades)
3. [Tecnologias Utilizadas](#tecnologias-utilizadas)
4. [Pré-requisitos](#pré-requisitos)
5. [Instalação](#instalação)
6. [Estrutura do Projeto](#estrutura-do-projeto)
7. [Como Usar](#como-usar)
8. [Considerações de Desempenho](#considerações-de-desempenho)
9. [Licença](#licença)

## Descrição
Este projeto realiza web scraping assíncrono do site do supermercado Irmãos Gonçalves, extraindo informações de produtos como nome, preço e link. Os dados são armazenados em um banco de dados SQLite usando SQLAlchemy e manipulados com Pandas. As requisições HTTP são otimizadas com asyncio para melhor performance, e as imagens dos produtos são baixadas e armazenadas localmente.

## Funcionalidades
- Web scraping assíncrono do site do Irmãos Gonçalves usando asyncio e aiohttp
- Extração de nome, preço e link dos produtos
- Armazenamento dos dados em banco de dados SQLite usando SQLAlchemy
- Manipulação eficiente de dados com Pandas
- Download de imagens dos produtos
- Seleção de cidade para coleta de dados
- Processamento automatizado através do script principal

## Tecnologias Utilizadas
- **asyncio**: Para operações assíncronas e melhoria de performance
- **aiohttp**: Para requisições HTTP assíncronas
- **BeautifulSoup**: Para parsing de HTML
- **SQLAlchemy**: Para ORM e operações de banco de dados
- **Pandas**: Para manipulação e análise de dados
- **SQLite**: Como banco de dados
- **Selenium**: Para casos onde a extração direta falha

## Pré-requisitos
- Python 3.12+
- Navegador web compatível com Selenium (ex: Chrome, Firefox)

## Instalação
1. Clone o repositório:
   ```bash
   git clone https://github.com/AndreLuis933/irmaos-goncalves-scraper
   cd irmaos-goncalves-scraper
   ```

2. Crie e ative um ambiente virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/MacOS
   venv\Scripts\activate     # Windows
   ```

3. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

## Estrutura do Projeto
```
IRMAOS-GONSALVES/
│
├── database/
│   ├── __init__.py
│   ├── db_operations.py
│   └── db_setup.py
│
├── modules/
│   ├── __init__.py
│   ├── baixar_imagem.py
│   ├── baixar_site.py
│   ├── extrair_link_imagem.py
│   └── prever_imagens.py
│
├── .gitignore
├── LICENSE
├── main.py
├── selecionar_regiao.py
└── README.md
```

## Como Usar
1. Execute o script para selecionar a região:
   ```bash
   python selecionar_regiao.py
   ```

2. Após selecionar a região, execute o script principal:
   ```bash
   python main.py
   ```

3. O script realizará o scraping de forma assíncrona, fazendo múltiplas requisições simultaneamente para melhorar a performance.

4. Os dados coletados serão armazenados no banco SQLite usando SQLAlchemy à medida que são baixados.

5. Após a coleta inicial dos dados (cerca de 1 minuto), o script começará a baixar as imagens dos produtos.

> **Nota importante:**
> - Você pode interromper o script a qualquer momento após o download inicial do site.
> - Todo o trabalho realizado até o momento da interrupção será salvo automaticamente.
> - Isso permite que você obtenha dados parciais mesmo se não puder esperar pela conclusão total do processo.

6. Se deixado em execução, o script continuará o download das imagens, o que pode levar várias horas.

7. Após a conclusão (ou interrupção), você encontrará:
   - Os dados dos produtos no banco de dados SQLite
   - As imagens dos produtos baixadas localmente

## Considerações de Desempenho

O script opera em duas fases principais, cada uma com características de desempenho distintas:

1. **Scraping e Download de Dados (Rápido)**
   - Esta fase inicial é relativamente rápida, geralmente concluída em cerca de 1 minuto.
   - Os dados são salvos no banco de dados à medida que são coletados.

2. **Download das Imagens (Demorado)**
   - Esta fase é significativamente mais lenta e pode levar várias horas para ser concluída completamente.

**Flexibilidade de Uso:**
- O script é projetado para ser interrompido com segurança a qualquer momento após a fase inicial de scraping.
- Todos os dados e imagens processados até o momento da interrupção são salvos automaticamente.
- Isso permite que você obtenha resultados parciais úteis mesmo sem esperar pela conclusão total do processo.

## Licença
Este projeto está licenciado sob a Licença MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.