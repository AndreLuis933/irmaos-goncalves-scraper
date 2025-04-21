from collections import defaultdict

from .schemas import DadosProcessados, DisponibilidadeInfo, PrecoInfo, PrecoVariavel, ProdutoInfo


def processar_dados_brutos(resultados):
    produtos: set[ProdutoInfo] = set()
    precos_por_link = defaultdict(list)
    disponibilidades = []

    # Extrair produtos e preços
    for produtos_raw, links_precos, cidade in resultados:
        for nome, link, categoria in produtos_raw:
            produtos.add(ProdutoInfo(nome=nome, link=link, categoria=categoria))
            disponibilidades.append(DisponibilidadeInfo(produto_link=link, cidade=cidade))

        for link, preco in links_precos:
            precos_por_link[link].append((preco, cidade))

    # Classificar preços uniformes e variáveis
    precos_uniformes: list[PrecoInfo] = []
    precos_variaveis: list[PrecoVariavel] = []

    for link, precos_cidades in precos_por_link.items():
        primeiro_preco = precos_cidades[0][0]
        if len(precos_cidades) == 1 or all(preco == primeiro_preco for preco, _ in precos_cidades[1:]):
            precos_uniformes.append(PrecoInfo(link=link, preco=primeiro_preco))
        else:
            for preco, cidade in precos_cidades:
                precos_variaveis.append(PrecoVariavel(link=link, preco=preco, cidade=cidade))

    return DadosProcessados(
        produtos=list(produtos),
        precos_uniformes=precos_uniformes,
        precos_variaveis=precos_variaveis,
        disponibilidades=disponibilidades,
    )
