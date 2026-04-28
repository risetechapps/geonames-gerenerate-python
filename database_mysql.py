import mysql.connector
import json

from languages import get_language
from country_formats import get_country_formats
from country_documents import get_country_documents


def setup_db(config):
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

    # 1. Tabela de Regiões
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS regions
                   (
                       id
                       INT
                       PRIMARY
                       KEY,
                       name
                       VARCHAR
                   (
                       100
                   ) NOT NULL
                       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                   ''')

    # 2. Tabela de Países (26 colunas para bater com seu novo SQLite)
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS countries
                   (
                       id
                       INT
                       PRIMARY
                       KEY,
                       name
                       VARCHAR
                   (
                       255
                   ),
                       iso3 CHAR
                   (
                       3
                   ),
                       iso2 CHAR
                   (
                       2
                   ),
                       phonecode VARCHAR
                   (
                       20
                   ),
                       capital VARCHAR
                   (
                       255
                   ),
                       currency VARCHAR
                   (
                       50
                   ),
                       currency_name VARCHAR
                   (
                       100
                   ),
                       currency_symbol VARCHAR
                   (
                       10
                   ),
                       tld VARCHAR
                   (
                       10
                   ),
                       native VARCHAR
                   (
                       255
                   ),
                       region VARCHAR
                   (
                       100
                   ),
                       subregion VARCHAR
                   (
                       100
                   ),
                       nationality VARCHAR
                   (
                       100
                   ),
                       zip_code_format VARCHAR
                   (
                       100
                   ),
                       zip_code_regex VARCHAR
                   (
                       255
                   ),
                       telephone_format VARCHAR
                   (
                       100
                   ),
                       telephone_regex VARCHAR
                   (
                       100
                   ),
                       cellphone_format VARCHAR
                   (
                       100
                   ),
                       cellphone_regex VARCHAR
                   (
                       100
                   ),
                       timezones JSON,
                       translations JSON,
                       latitude VARCHAR
                   (
                       50
                   ),
                       longitude VARCHAR
                   (
                       50
                   ),
                       emoji VARCHAR
                   (
                       10
                   ),
                       emojiU VARCHAR
                   (
                       50
                   ),
                       language_code VARCHAR
                   (
                       50
                   ),
                       documents JSON
                       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                   ''')

    # 3. Tabela de Estados (8 colunas conforme seu SQLite)
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS states
                   (
                       id
                       INT
                       PRIMARY
                       KEY,
                       name
                       VARCHAR
                   (
                       255
                   ),
                       iso2 VARCHAR
                   (
                       10
                   ),
                       country VARCHAR
                   (
                       255
                   ),
                       country_iso2 CHAR
                   (
                       2
                   ),
                       country_iso3 CHAR
                   (
                       3
                   ),
                       country_native VARCHAR
                   (
                       255
                   ),
                       timezone VARCHAR
                   (
                       100
                   )
                       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                   ''')

    # 4. Tabela de Cidades (8 colunas conforme seu SQLite)
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS cities
                   (
                       id
                       INT
                       PRIMARY
                       KEY,
                       name
                       VARCHAR
                   (
                       255
                   ),
                       state VARCHAR
                   (
                       255
                   ),
                       state_iso2 VARCHAR
                   (
                       10
                   ),
                       country VARCHAR
                   (
                       255
                   ),
                       country_iso2 CHAR
                   (
                       2
                   ),
                       country_iso3 CHAR
                   (
                       3
                   ),
                       country_native VARCHAR
                   (
                       255
                   )
                       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                   ''')

    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    conn.commit()
    return conn


def insert_regions(conn, data):
    cursor = conn.cursor()

    print("Iniciando importação de Regiões no MySQL...")

    sql = "INSERT IGNORE INTO regions (id, name) VALUES (%s, %s)"
    for region in data:
        cursor.execute(sql, (region.get('id'), clean(region.get('name'))))
    conn.commit()


def insert_data(conn, data):
    cursor = conn.cursor()

    for c in data:

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

        # 1. País (27 colunas)
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
        sql_pais = f"INSERT IGNORE INTO countries VALUES ({','.join(['%s'] * 28)})"
        cursor.execute(sql_pais, dados_pais)

        # 2. Estados (8 colunas)
        for s in c.get('states', []):
            dados_estado = (
                s.get('id'), clean(s.get('name')), s.get('iso2'), clean(c.get('name')),
                clean(c.get('iso2')), clean(c.get('iso3')), clean(c.get('native')), s.get('timezone')
            )
            sql_estado = "INSERT IGNORE INTO states VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql_estado, dados_estado)

            # 3. Cidades (8 colunas)
            for city in s.get('cities', []):
                dados_cidade = (
                    city.get('id'), clean(city.get('name')),
                    clean(s.get('name')), clean(s.get('iso2')),
                    clean(c.get('name')), clean(c.get('iso2')), clean(c.get('iso3')), clean(c.get('native'))
                )
                sql_cidade = "INSERT IGNORE INTO cities VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql_cidade, dados_cidade)

    conn.commit()
    print("Importação para MySQL finalizada com sucesso!")


def clean(value, upper=True):
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned.upper() if upper else cleaned
