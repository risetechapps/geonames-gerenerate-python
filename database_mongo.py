from pymongo import MongoClient
import certifi # Necessário para evitar erros de SSL em alguns sistemas

from languages import get_language
from country_formats import get_country_formats
from country_documents import get_country_documents


def setup_db(config):
    """
    config = {
        'host': 'localhost',
        'port': 27017,
        'username': 'admin',
        'password': 'password',
        'authSource': 'admin'
    }
    """
    # Se for local sem senha, basta MongoClient('mongodb://localhost:27017/')
    client = MongoClient(**config)
    db = client['geonames_db']

    # No MongoDB não "criamos" tabelas, apenas acessamos as coleções
    return db


def setup_db_uri(uri):

    try:
        import ssl

        # Cria um contexto SSL que força TLS 1.2+
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
        ssl_context.check_hostname = True
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.load_verify_locations(certifi.where())

        # Configurações robustas para Atlas
        client = MongoClient(
            uri,
            tls=True,
            tlsCAFile=certifi.where(),
            tlsAllowInvalidCertificates=False,
            retryWrites=True,
            maxPoolSize=50,
            serverSelectionTimeoutMS=30000,
            socketTimeoutMS=45000,
            connectTimeoutMS=30000
        )

        # O Atlas cria o banco automaticamente ao inserir dados
        db = client['geonames']

        # Testar a conexão
        client.admin.command('ping')
        print("✅ Conectado com sucesso ao MongoDB Atlas!")

        return db
    except Exception as e:
        print(f"❌ Erro ao conectar ao Atlas: {e}")
        raise

def insert_regions(db, data):

    print("Iniciando processamento hierárquico no MongoDB...")

    collection = db['regions']

    # Limpamos os dados antes de inserir (strip/upper)
    cleaned_regions = []
    for r in data:
        cleaned_regions.append({
            "_id": r.get('id'),  # O MongoDB usa _id como chave primária
            "name": str(r.get('name')).strip().upper()
        })

    collection.delete_many({})
    collection.insert_many(cleaned_regions)
    print(f"Sucesso! {len(cleaned_regions)} regiões importadas.")


def insert_data(db, data):
    collection = db['countries']

    # No MongoDB, vamos salvar a estrutura hierárquica completa
    # Isso torna a busca "País -> Estado -> Cidade" extremamente rápida

    for c in data:

        language = get_language(str(c.get('name')).strip())

        # Buscar formatos do país
        iso3 = str(c.get('iso3')).strip().upper()
        formats = get_country_formats(iso3)
        documents = get_country_documents(iso3)

        # Usar formatos da API se existirem, senão usar os do nosso módulo
        zip_code_format = c.get('postal_code_format') or formats.get('zip_code_format')
        zip_code_regex = c.get('postal_code_regex') or formats.get('zip_code_regex')

        country_doc = {
            "_id": c.get('id'),
            "name": str(c.get('name')).strip(),
            "iso3": iso3,
            "iso2": str(c.get('iso2')).strip().upper(),
            "phonecode": c.get('phonecode'),
            "capital": c.get('capital'),
            "currency": c.get('currency'),
            "currency_name": c.get('currency_name'),
            "currency_symbol": c.get('currency_symbol'),
            "tld": c.get('tld'),
            "native": c.get('native'),
            "region": str(c.get('region')).strip(),
            "subregion": c.get('subregion'),
            "nationality": c.get('nationality'),
            "zip_code_format": zip_code_format,
            "zip_code_regex": zip_code_regex,
            "telephone_format": formats.get('telephone_format'),
            "telephone_regex": formats.get('telephone_regex'),
            "cellphone_format": formats.get('cellphone_format'),
            "cellphone_regex": formats.get('cellphone_regex'),
            "documents": documents,  # Lista de documentos do país
            "timezones": c.get('timezones'),  # No Mongo salvamos como LISTA real
            "translations": c.get('translations'),  # No Mongo salvamos como OBJETO real
            "latitude": c.get('latitude'),
            "longitude": c.get('longitude'),
            "emoji": c.get('emoji'),
            "emojiU": c.get('emojiU'),
            "language_code": language,
            "states": []  # Vamos popular abaixo
        }

        # 2. Processar Estados e Cidades (Aninhados)
        for s in c.get('states', []):
            state_doc = {
                "id": s.get('id'),
                "name": str(s.get('name')).strip(),
                "iso2": s.get('iso2'),
                "timezone": s.get('timezone'),
                "cities": []
            }

            for city in s.get('cities', []):
                state_doc['cities'].append({
                    "id": city.get('id'),
                    "name": str(city.get('name')).strip(),
                    "native": city.get('native')
                })

            country_doc['states'].append(state_doc)

        # Inserir ou atualizar o país inteiro com tudo dentro
        collection.replace_one({"_id": country_doc["_id"]}, country_doc, upsert=True)

    print("Importação para MongoDB finalizada!")