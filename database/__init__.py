# Importa configurações de conexão
from .connection import DATABASE_TYPE, ENGINE, Session
from .gap import close_gap, log_execucao

# Importa modelos do banco de dados
from .models import Base, Cidade, DisponibilidadeCidade, HistoricoPreco, Imagem, LogExecucao, Produto, init_db

# Importa operações de cidades
from .operations.cidades import set_cidades

# Importa operações de disponibilidade
from .operations.disponibilidade import salvar_disponibilidade

# Importa operações de imagens
from .operations.imagens import (
    get_count_products_without_images,
    get_image_links,
    get_produtos_sem_imagens,
    images_id,
    save_images,
)

# Importa operações de preços
from .operations.precos import price_change, salvar_preco, verificar_mudancas_preco

# Importa operações de produtos
from .operations.produtos import (
    get_link_produto,
    get_null_product_category,
    get_produtos_sem_categoria,
    salvar_produto,
    update_categoria,
)

# Importa utilitários
from .operations.utils import (
    atualizar_em_lotes,
    last_execution,
    gerenciador_transacao,
    inserir_com_conflito,
    obter_mapeamento_id,
)

# Importa processadores de dados
from .processors import processar_dados_brutos

# Importa schemas (estruturas de dados)
from .schemas import DadosProcessados, DisponibilidadeInfo, PrecoInfo, PrecoVariavel, ProdutoInfo

# inicializa o banco de dados
init_db()

# Define quais símbolos são exportados com "from database import *"

__all__ = [
    "DATABASE_TYPE",
    "ENGINE",
    "Base",
    "Cidade",
    "DadosProcessados",
    "DisponibilidadeCidade",
    "DisponibilidadeInfo",
    "HistoricoPreco",
    "Imagem",
    "LogExecucao",
    "PrecoInfo",
    "PrecoVariavel",
    "Produto",
    "ProdutoInfo",
    "Session",
    "atualizar_em_lotes",
    "close_gap",
    "last_execution",
    "gerenciador_transacao",
    "get_count_products_without_images",
    "get_image_links",
    "get_link_produto",
    "get_null_product_category",
    "get_produtos_sem_categoria",
    "get_produtos_sem_imagens",
    "images_id",
    "init_db",
    "inserir_com_conflito",
    "log_execucao",
    "obter_mapeamento_id",
    "price_change",
    "processar_dados_brutos",
    "salvar_disponibilidade",
    "salvar_preco",
    "salvar_produto",
    "save_images",
    "set_cidades",
    "update_categoria",
    "verificar_mudancas_preco",
]
