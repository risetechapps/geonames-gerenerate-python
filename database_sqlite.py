import sqlite3
import json

from languages import LANG_MAP, get_language
from country_formats import get_country_formats
from country_documents import get_country_documents


def setup_db(database='db.sqlite'):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    # Tabela de Regiões
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS regions
                   (
                       id
                       INTEGER
                       PRIMARY
                       KEY,
                       name
                       TEXT
                       NOT
                       NULL
                   )
                   ''')

    # Tabela de Países
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS countries
                   (
                       id
                       INTEGER
                       PRIMARY
                       KEY,
                       name
                       TEXT,
                       iso3
                       TEXT,
                       iso2
                       TEXT,
                       phonecode
                       TEXT,
                       capital
                       TEXT,
                       currency
                       TEXT,
                       currency_name
                       TEXT,
                       currency_symbol
                       TEXT,
                       tld
                       TEXT,
                       native
                       TEXT,
                       region
                       TEXT,
                       subregion
                       TEXT,
                       nationality
                       TEXT,
                       zip_code_format
                       TEXT,
                       zip_code_regex
                       TEXT,
                       telephone_format
                       TEXT,
                       telephone_regex
                       TEXT,
                       cellphone_format
                       TEXT,
                       cellphone_regex
                       TEXT,
                       timezones
                       TEXT,
                       translations
                       TEXT,
                       latitude
                       TEXT,
                       longitude
                       TEXT,
                       emoji
                       TEXT,
                       emojiU
                       TEXT,
                       language_code
                       TEXT,
                       documents
                       TEXT,
                       FOREIGN
                       KEY(region) REFERENCES regions (name)
                   )
                   ''')

    # Tabela de Estados
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS states
                   (
                       id
                       INTEGER
                       PRIMARY
                       KEY,
                       name
                       TEXT,
                       iso2
                       TEXT,
                       country
                       TEXT,
                       country_iso2
                       TEXT,
                       country_iso3
                       TEXT,
                       country_native
                       TEXT,
                       timezone
                       TEXT,
                       FOREIGN
                       KEY(country) REFERENCES countries(name)
                       )
                   ''')

    # Tabela de Cidades
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS cities
                   (
                       id
                       INTEGER
                       PRIMARY
                       KEY,
                       name
                       TEXT,
                       state
                       Text,
                       state_iso2
                       TEXT,
                       country
                       TEXT,
                       country_iso2
                       TEXT,
                       country_iso3
                       TEXT,
                       country_native
                       TEXT,
                       FOREIGN
                       KEY(state) REFERENCES states(name),
                       FOREIGN KEY(country) REFERENCES countries(name)
                       )
                   ''')

    conn.commit()
    return conn

def insert_regions(conn, data):
    cursor = conn.cursor()

    print("Iniciando processamento hierárquico Sqlite...")

    for region in data:
        cursor.execute('''
                    INSERT OR REPLACE INTO regions (id, name)
                    VALUES (?, ?)
                ''', (region.get('id'), clean(region.get('name'))))

    conn.commit()

    return conn


def insert_data(conn, data):
    cursor = conn.cursor()


    for c in data:
        # 1. Inserir País
        language = get_language(clean(c.get('name')))

        # Buscar formatos do país
        iso3 = clean(c.get('iso3'))
        formats = get_country_formats(iso3)
        documents = get_country_documents(iso3)

        # Usar formatos da API se existirem, senão usar os do nosso módulo
        zip_code_format = c.get('postal_code_format') or json.dumps(formats.get('zip_code_format'), ensure_ascii=False)
        zip_code_regex = c.get('postal_code_regex') or json.dumps(formats.get('zip_code_regex'), ensure_ascii=False)
        telephone_format = json.dumps(formats.get('telephone_format'), ensure_ascii=False)
        telephone_regex = json.dumps(formats.get('telephone_regex'), ensure_ascii=False)
        cellphone_format = json.dumps(formats.get('cellphone_format'), ensure_ascii=False)
        cellphone_regex = json.dumps(formats.get('cellphone_regex'), ensure_ascii=False)
        documents_json = json.dumps(documents, ensure_ascii=False)

        dados_pais = (
            c.get('id'), clean(c.get('name')), clean(c.get('iso3')), clean(c.get('iso2')),
            c.get('phonecode'), clean(c.get('capital')), c.get('currency'), clean(c.get('currency_name')),
            c.get('currency_symbol'), c.get('tld'), clean(c.get('native')), clean(c.get('region')),
            clean(c.get('subregion')), clean(c.get('nationality')),
            zip_code_format, zip_code_regex,
            telephone_format, telephone_regex, cellphone_format, cellphone_regex,
            json.dumps(c.get('timezones'), ensure_ascii=False), json.dumps(c.get('translations'), ensure_ascii=False),
            c.get('latitude'), c.get('longitude'), c.get('emoji'), c.get('emojiU'), language,
            documents_json
        )
        cursor.execute(f"INSERT OR REPLACE INTO countries VALUES ({','.join(['?'] * 28)})", dados_pais)

        # 2. Processar Estados do País
        for s in c.get('states', []):
            dados_estado = (
                s.get('id'), clean(s.get('name')),s.get('iso2'), clean(c.get('name')),
                clean(c.get('iso2')), clean(c.get('iso3')), clean(c.get('native')), s.get('timezone')
            )
            cursor.execute("INSERT OR REPLACE INTO states VALUES (?,?,?,?,?,?,?,?)", dados_estado)

            # 3. Processar Cidades do Estado
            for city in s.get('cities', []):
                dados_cidade = (
                    city.get('id'), clean(city.get('name')),
                    clean((s.get('name'))), clean(s.get('iso2')),
                    clean(c.get('name')), clean(c.get('iso2')), clean(c.get('iso3')), clean(c.get('native')),
                )
                cursor.execute("INSERT OR REPLACE INTO cities VALUES (?,?,?,?,?,?,?,?)", dados_cidade)

    conn.commit()
    print("Importação para Sqlite finalizada!")

def clean(value, upper=True):
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned.upper() if upper else cleaned