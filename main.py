from modules.baixar_imagem import baixar_imagem
from modules.baixar_site import baixar_site
from modules.extrair_link_imagem import extrair_link_selenium
from modules.prever_imagens import prever_imagem


def main():
    baixar_site()
    prever_imagem()
    extrair_link_selenium()
    baixar_imagem()


if __name__ == "__main__":
    main()
