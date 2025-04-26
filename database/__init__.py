# Importa configurações de conexão
from .connection import DATABASE_TYPE, ENGINE, Session

# Importa modelos do banco de dados
from .models import Base, Cidade, DisponibilidadeCidade, HistoricoPreco, Imagem, Produto, init_db

# Importa operações de cidades
from .operations.cidades import set_cidades

# Importa operações de disponibilidade
from .operations.disponibilidade import salvar_disponibilidade

# Importa operações de imagens
from .operations.imagens import get_count_products_without_images, get_image_links, images_id, save_images

# Importa operações de preços
from .operations.precos import price_change, salvar_preco, verificar_mudancas_preco

# Importa operações de produtos
from .operations.produtos import get_link_produto, get_null_product_category, salvar_produto

# Importa utilitários
from .operations.utils import (
    atualizar_em_lotes,
    execute_today,
    gerenciador_transacao,
    get_dataframe,
    inserir_com_conflito,
    obter_data_atual,
    obter_mapeamento_id,
)

# Importa processadores de dados
from .processors import processar_dados_brutos

# Importa schemas (estruturas de dados)
from .schemas import DadosProcessados, DisponibilidadeInfo, PrecoInfo, PrecoVariavel, ProdutoInfo

init_db()
# Define quais símbolos são exportados com "from database import *"
__all__ = [
    # Modelos
    "Produto",
    "Imagem",
    "Cidade",
    "HistoricoPreco",
    "DisponibilidadeCidade",
    "Base",
    "init_db",
    # Conexão
    "ENGINE",
    "Session",
    "DATABASE_TYPE",
    "salvar_disponibilidade",
    # Schemas
    "ProdutoInfo",
    "PrecoInfo",
    "PrecoVariavel",
    "DisponibilidadeInfo",
    "DadosProcessados",
    # Processadores
    "processar_dados_brutos",
    # Operações - Produtos
    "salvar_produto",
    "get_link_produto",
    "get_null_product_category",
    # Operações - Preços
    "salvar_preco",
    "verificar_mudancas_preco",
    "price_change",
    "execute_today",
    # Operações - Imagens
    "save_images",
    "get_image_links",
    "images_id",
    "get_count_products_without_images",
    # Operações - Cidades
    "set_cidades",
    "get_dataframe",
    # Utilitários
    "atualizar_em_lotes", "gerenciador_transacao",
    "obter_data_atual",
    "obter_mapeamento_id",
    "inserir_com_conflito",
]
