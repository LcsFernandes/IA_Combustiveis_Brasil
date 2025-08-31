from src.script.transform_data import load_data
from src.script.download_data import baixar_relatorios_combustiveis_automotivos


def main():
    url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis"
    path = r".\src\data\Bronze\Relatorio_Combustiveis_Brasil"

    baixar_relatorios_combustiveis_automotivos(url, path)
    
    load_data()

if __name__ == "__main__":
    main()