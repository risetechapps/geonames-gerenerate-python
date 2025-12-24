import psycopg2
import json

from languages import get_language


def setup_db(config):
    """
    config = {
        'host': 'localhost',
        'database': 'geonames',
        'user': 'postgres',
        'password': 'sua_senha'
    }
    """
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()

    # 1. Tabela de Regiões
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS regions
                   (
                       id
                       INTEGER
                       PRIMARY
                       KEY,
                       name
                       VARCHAR
                   (
                       255
                   ) NOT NULL
                       )
                   ''')

    # 2. Tabela de Países (26 colunas)
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS countries
                   (
                       id
                       INTEGER
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
                       255
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
                       255
                   ),
                       subregion VARCHAR
                   (
                       255
                   ),
                       nationality VARCHAR
                   (
                       255
                   ),
                       zip_code_format VARCHAR
                   (
                       255
                   ),
                       zip_code_regex VARCHAR
                   (
                       255
                   ),
                       telephone_format VARCHAR
                   (
                       255
                   ),
                       telephone_regex VARCHAR
                   (
                       255
                   ),
                       cellphone_format VARCHAR
                   (
                       255
                   ),
                       cellphone_regex VARCHAR
                   (
                       255
                   ),
                       timezones JSONB,
                       translations JSONB,
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
                   )
                       )
                   ''')

    # 3. Tabela de Estados (8 colunas)
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS states
                   (
                       id
                       INTEGER
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
                       timezone TEXT
                       )
                   ''')

    # 4. Tabela de Cidades (8 colunas)
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS cities
                   (
                       id
                       INTEGER
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
                       )
                   ''')

    conn.commit()
    return conn


def insert_regions(conn, data):
    cursor = conn.cursor()

    print("Iniciando processamento hierárquico no PostgreSQL...")

    sql = "INSERT INTO regions (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING"
    for region in data:
        cursor.execute(sql, (region.get('id'), clean(region.get('name'))))
    conn.commit()


def insert_data(conn, data):
    cursor = conn.cursor()

    for c in data:

        language = get_language(clean(c.get('name')))

        # 1. País (26 colunas)
        dados_pais = (
            c.get('id'), clean(c.get('name')), clean(c.get('iso3')), clean(c.get('iso2')),
            c.get('phonecode'), clean(c.get('capital')), c.get('currency'), clean(c.get('currency_name')),
            c.get('currency_symbol'), c.get('tld'), clean(c.get('native')), clean(c.get('region')),
            clean(c.get('subregion')), clean(c.get('nationality')),
            c.get('postal_code_format'), c.get('postal_code_regex'),
            None, None, None, None,
            json.dumps(c.get('timezones')), json.dumps(c.get('translations')),
            c.get('latitude'), c.get('longitude'), c.get('emoji'), c.get('emojiU'), language
        )

        placeholders = ",".join(["%s"] * 27)
        sql_pais = f"INSERT INTO countries VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING"
        cursor.execute(sql_pais, dados_pais)

        # 2. Estados (8 colunas)
        for s in c.get('states', []):
            dados_estado = (
                s.get('id'), clean(s.get('name')), s.get('iso2'), clean(c.get('name')),
                clean(c.get('iso2')), clean(c.get('iso3')), clean(c.get('native')), s.get('timezone')
            )
            sql_estado = "INSERT INTO states VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING"
            cursor.execute(sql_estado, dados_estado)

            # 3. Cidades (8 colunas)
            for city in s.get('cities', []):
                dados_cidade = (
                    city.get('id'), clean(city.get('name')),
                    clean(s.get('name')), clean(s.get('iso2')),
                    clean(c.get('name')), clean(c.get('iso2')), clean(c.get('iso3')), clean(c.get('native'))
                )
                sql_cidade = "INSERT INTO cities VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING"
                cursor.execute(sql_cidade, dados_cidade)

    conn.commit()
    print("Importação para PostgreSQL finalizada com sucesso!")


def clean(value, upper=True):
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned.upper() if upper else cleaned