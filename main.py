import json
from urllib.request import urlopen
import database_sqlite
import database_mysql
import database_psql
import database_mongo
import generate_static_api

URL_REGIOES = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/regions.json"
URL_FULL = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/countries%2Bstates%2Bcities.json"


def run():
    try:
        print(f"Baixando base de dados completa (isso pode demorar um pouco)...")

        with urlopen(URL_REGIOES) as response:
            full_data_regios = json.loads(response.read().decode())


        with urlopen(URL_FULL) as response:
            full_data = json.loads(response.read().decode())

        print("Conectando ao banco de dados...")

        connSqlite = database_sqlite.setup_db()

        connMysql = database_mysql.setup_db({
            'host': 'localhost',
            'user': 'root',
            'password': '',
            'database': 'geonames'
        })

        connPsql = database_psql.setup_db({
            'host': 'localhost',
            'user': 'postgres',
            'password': '',
            'database': 'geonames'
        })

        connMongo = database_mongo.setup_db({
            'host': 'localhost',
            'port': 27017,
            # 'username': 'softhouse1',
            # 'password': 'LJrryEvOvG6zynMZ'
        })

        # connMongo = database_mongo.setup_db_uri("mongodb+srv://softhouse1:LJrryEvOvG6zynMZ@cluster0.fxel7ab.mongodb.net/")

        database_sqlite.insert_regions(connSqlite, full_data_regios)
        database_sqlite.insert_data(connSqlite, full_data)

        database_mysql.insert_regions(connMysql, full_data_regios)
        database_mysql.insert_data(connMysql, full_data)

        database_psql.insert_regions(connPsql, full_data_regios)
        database_psql.insert_data(connPsql, full_data)

        database_mongo.insert_regions(connMongo, full_data_regios)
        database_mongo.insert_data(connMongo, full_data)

        connSqlite.close()
        connMysql.close()
        connPsql.close()
        connMongo.client.close()

        generate_static_api.generate()

    except Exception as e:
        print(f"Erro crítico: {e}")


if __name__ == "__main__":
    run()