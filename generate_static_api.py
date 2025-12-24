import sqlite3
import json
import os


def clean(value, upper=True):
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned.upper() if upper else cleaned


def save_json(path, data):
    """Cria diretórios e salva o arquivo JSON formatado"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def generate():
    # Conecta ao SQLite (usando o banco que populamos anteriormente)
    conn = sqlite3.connect('db.sqlite')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    base_path = "geonames/json"
    print("🚀 Iniciando geração da estrutura de arquivos JSON...")

    # 1. Gerar regions.json
    print("📁 Gerando regions.json")
    regions = [dict(row) for row in cursor.execute("SELECT * FROM regions order by name ASC")]
    save_json(f"{base_path}/regions.json", regions)

    # 2. Gerar countries.json (Lista global de países)
    print("📁 Gerando countries.json")
    countries = [dict(row) for row in cursor.execute("SELECT * FROM countries order by name ASC")]

    for c in countries:
        # Decodifica os campos que salvamos como string JSON no SQLite
        c['timezones'] = json.loads(c['timezones']) if c['timezones'] else []
        c['translations'] = json.loads(c['translations']) if c['translations'] else {}

    save_json(f"{base_path}/countries.json", countries)

    # 3. Processar cada País para criar a estrutura /ISO3/index.json
    for country in countries:
        iso3 = clean(country['iso3'])  # Garante que o nome da pasta seja limpo (ex: "BGD")
        country_name = country['name']

        print(f"🌍 Processando País: {iso3} ({country_name})")

        # Busca estados filtrando pelo nome do país (conforme sua nova estrutura)
        states = [dict(row) for row in cursor.execute(
            "SELECT * FROM states WHERE country = ? order by name ASC", (country_name,)
        )]

        # Salvar /geonames/BRA/index.json (Lista de estados do país)
        save_json(f"{base_path}/{iso3}/index.json", states)

        # 4. Processar cada Estado para criar a estrutura /ISO3/Estado/index.json
        for state in states:
            # Limpa o nome do estado para evitar o erro de "[Errno 2]" (espaço no final)
            # Também removemos barras que podem existir em nomes de províncias
            state_folder = clean(state['iso2'], upper=False).replace("/", "-")
            state_name_db = state['name']

            # Busca cidades filtrando pelo nome do estado
            cities = [dict(row) for row in cursor.execute(
                "SELECT * FROM cities WHERE state = ? order by name ASC", (state_name_db,)
            )]

            # Salvar /geonames/BRA/SAO PAULO/index.json (Lista de cidades do estado)
            if states:
                save_json(f"{base_path}/{iso3}/{state_folder}/index.json", cities)

    conn.close()
    print(f"\n✅ Sucesso! Estrutura completa gerada em: ./{base_path}")


if __name__ == "__main__":
    generate()