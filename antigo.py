# Criar objetos para inserção, filtrando produtos não encontrados e já registrados
objetos = [
    HistoricoPreco(produto_id=link_to_id.get(link), preco=preco, data_atualizacao=hoje)
    for link, preco in dados
    if link_to_id.get(link) and link_to_id.get(link) not in produtos_com_registro
]