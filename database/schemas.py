from __future__ import annotations

from typing import NamedTuple


class PrecoInfo(NamedTuple):
    link: str
    preco: float


class PrecoVariavel(NamedTuple):
    link: str
    preco: float
    cidade: str


class DisponibilidadeInfo(NamedTuple):
    produto_link: str
    cidade: str

class ProdutoInfo(NamedTuple):
    nome: str
    link: str
    categoria: str | None

class DadosProcessados(NamedTuple):
    produtos: list[ProdutoInfo]
    precos_uniformes: list[PrecoInfo]
    precos_variaveis: list[PrecoVariavel]
    disponibilidades: list[DisponibilidadeInfo]



