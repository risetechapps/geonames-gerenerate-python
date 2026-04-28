import sqlite3
import json
import os
from ui import console


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


def get_sqlite_database():
    """Lê o nome do banco SQLite do config.json ou usa padrão"""
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
            return config.get('sqlite', {}).get('database', 'db.sqlite')
    except (FileNotFoundError, json.JSONDecodeError):
        return 'db.sqlite'


def generate(output_path=None):
    # Conecta ao SQLite (usando o banco que populamos anteriormente)
    db_name = get_sqlite_database()
    conn = sqlite3.connect(db_name)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    base_path = output_path if output_path else "geonames/json"
    console.print("[bold green]🚀 Iniciando geração da estrutura de arquivos JSON...[/bold green]")

    # 1. Gerar regions.json
    console.print("[cyan]📁 Gerando regions.json[/cyan]")
    regions = [dict(row) for row in cursor.execute("SELECT * FROM regions order by name ASC")]
    save_json(f"{base_path}/regions.json", regions)

    # 2. Gerar countries.json (Lista global de países)
    console.print("[cyan]📁 Gerando countries.json[/cyan]")
    countries = [dict(row) for row in cursor.execute("SELECT * FROM countries order by name ASC")]

    for c in countries:
        # Decodifica os campos que salvamos como string JSON no SQLite
        c['timezones'] = json.loads(c['timezones']) if c['timezones'] else []
        c['translations'] = json.loads(c['translations']) if c['translations'] else {}
        c['zip_code_format'] = json.loads(c['zip_code_format']) if c['zip_code_format'] else None
        c['zip_code_regex'] = json.loads(c['zip_code_regex']) if c['zip_code_regex'] else None
        c['telephone_format'] = json.loads(c['telephone_format']) if c['telephone_format'] else None
        c['telephone_regex'] = json.loads(c['telephone_regex']) if c['telephone_regex'] else None
        c['cellphone_format'] = json.loads(c['cellphone_format']) if c['cellphone_format'] else None
        c['cellphone_regex'] = json.loads(c['cellphone_regex']) if c['cellphone_regex'] else None
        c['documents'] = json.loads(c['documents']) if c.get('documents') else []

    save_json(f"{base_path}/countries.json", countries)

    # 3. Processar cada País para criar a estrutura /ISO3/index.json
    for country in countries:
        iso3 = clean(country['iso3'])  # Garante que o nome da pasta seja limpo (ex: "BGD")
        country_name = country['name']

        console.print(f"[yellow]🌍 Processando País: {iso3} ({country_name})[/yellow]")

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
    console.print(f"\n[bold green]✅ Sucesso! Estrutura completa gerada em: ./{base_path}[/bold green]")


if __name__ == "__main__":
    generate()