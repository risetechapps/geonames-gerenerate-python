import json
from urllib.request import urlopen
import sys
import time

import database_sqlite
import database_mysql
import database_psql
import database_mongo
import generate_static_api
from ui import (
    show_header, show_config_status, show_section_title,
    show_success_summary, show_error_message, show_info_message,
    DatabaseProgressTracker, console
)
from rich.progress import Progress, SpinnerColumn, TextColumn

URL_REGIOES = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/regions.json"
URL_FULL = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/countries%2Bstates%2Bcities.json"


def load_config():
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        show_error_message("Arquivo config.json não encontrado.\nCopie config.example.json para config.json e configure.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        show_error_message(f"config.json inválido: {e}")
        sys.exit(1)


def download_data():
    """Download dados com barra de progresso visual."""
    show_section_title("Baixando Dados", "⬇️")

    full_data_regioes = None
    full_data = None

    with Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold blue]{task.description}"),
        console=console
    ) as progress:
        # Download regions
        task1 = progress.add_task("[cyan]Baixando regions.json...", total=None)
        try:
            with urlopen(URL_REGIOES) as response:
                full_data_regioes = json.loads(response.read().decode())
            progress.update(task1, description="[green]✓ regions.json baixado", completed=100)
        except Exception as e:
            progress.update(task1, description=f"[red]✗ Erro: {e}", completed=100)
            raise

        # Download full data
        task2 = progress.add_task("[cyan]Baixando countries+states+cities.json (pode demorar)...", total=None)
        try:
            with urlopen(URL_FULL) as response:
                full_data = json.loads(response.read().decode())
            progress.update(task2, description="[green]✓ Dados completos baixados", completed=100)
        except Exception as e:
            progress.update(task2, description=f"[red]✗ Erro: {e}", completed=100)
            raise

    show_info_message("Download concluído com sucesso!", "green")
    return full_data_regioes, full_data


def process_sqlite(config, full_data_regioes, full_data, tracker):
    tracker.start_db("SQLite")

    if not config.get('enabled', False):
        tracker.disabled()
        return

    try:
        conn = database_sqlite.setup_db(config.get('database', 'db.sqlite'))
        database_sqlite.insert_regions(conn, full_data_regioes)
        database_sqlite.insert_data(conn, full_data)
        conn.close()
        tracker.success("Dados importados")
    except Exception as e:
        tracker.error(str(e))


def process_mysql(config, full_data_regioes, full_data, tracker):
    tracker.start_db("MySQL")

    if not config.get('enabled', False):
        tracker.disabled()
        return

    try:
        # Remove 'enabled' from config before passing to MySQL
        db_config = {k: v for k, v in config.items() if k != 'enabled' and v is not None}
        conn = database_mysql.setup_db(db_config)
        database_mysql.insert_regions(conn, full_data_regioes)
        database_mysql.insert_data(conn, full_data)
        conn.close()
        tracker.success("Dados importados")
    except Exception as e:
        tracker.error(str(e))


def process_postgresql(config, full_data_regioes, full_data, tracker):
    tracker.start_db("PostgreSQL")

    if not config.get('enabled', False):
        tracker.disabled()
        return

    try:
        # Remove 'enabled' from config before passing to PostgreSQL
        db_config = {k: v for k, v in config.items() if k != 'enabled' and v is not None}
        conn = database_psql.setup_db(db_config)
        database_psql.insert_regions(conn, full_data_regioes)
        database_psql.insert_data(conn, full_data)
        conn.close()
        tracker.success("Dados importados")
    except Exception as e:
        tracker.error(str(e))


def process_mongodb(config, full_data_regioes, full_data, tracker):
    tracker.start_db("MongoDB")

    if not config.get('enabled', False):
        tracker.disabled()
        return

    try:
        if config.get('uri'):
            db = database_mongo.setup_db_uri(config['uri'])
        else:
            db_config = {k: v for k, v in config.items() if k in ['host', 'port', 'username', 'password', 'authSource'] and v is not None}
            db = database_mongo.setup_db(db_config)

        database_mongo.insert_regions(db, full_data_regioes)
        database_mongo.insert_data(db, full_data)

        if hasattr(db, 'client'):
            db.client.close()
        tracker.success("Dados importados")
    except Exception as e:
        tracker.error(str(e))


def run():
    # Mostra cabeçalho
    show_header()

    # Carrega config
    config = load_config()
    show_config_status(config)

    # Download dados
    try:
        full_data_regioes, full_data = download_data()
    except Exception as e:
        show_error_message(f"Falha ao baixar dados: {e}")
        sys.exit(1)

    # Processa bancos
    show_section_title("Importando para Bancos de Dados", "🗄️")

    tracker = DatabaseProgressTracker()

    process_sqlite(config.get('sqlite', {}), full_data_regioes, full_data, tracker)
    process_mysql(config.get('mysql', {}), full_data_regioes, full_data, tracker)
    process_postgresql(config.get('postgresql', {}), full_data_regioes, full_data, tracker)
    process_mongodb(config.get('mongodb', {}), full_data_regioes, full_data, tracker)

    # Gera API estática (se habilitada)
    api_config = config.get('static_api', {})
    if api_config.get('enabled', True):
        show_section_title("Gerando API Estática", "📁")

        try:
            with Progress(
                SpinnerColumn(style="green"),
                TextColumn("[bold green]{task.description}"),
                console=console
            ) as progress:
                task = progress.add_task("[green]Gerando estrutura de arquivos JSON...", total=None)
                output_path = api_config.get('output_path', 'geonames/json')
                generate_static_api.generate(output_path=output_path)
                progress.update(task, description="[green]✓ API estática gerada", completed=100)

        except Exception as e:
            show_error_message(f"Erro ao gerar API estática: {e}")
            sys.exit(1)
    else:
        show_info_message("Geração de API estática desabilitada na configuração.", "dim white")

    # Calcula totais e mostra resumo
    total_countries = len(full_data) if full_data else 0
    total_states = sum(len(c.get('states', [])) for c in full_data) if full_data else 0
    total_cities = sum(
        sum(len(s.get('cities', [])) for s in c.get('states', []))
        for c in full_data
    ) if full_data else 0

    show_success_summary(total_countries, total_states, total_cities)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        console.print("\n")
        from rich.prompt import Confirm
        try:
            if Confirm.ask("⛔ Deseja realmente encerrar o programa?", default=True):
                show_error_message("Programa interrompido pelo usuário.")
                sys.exit(0)
            else:
                console.print("[yellow]⚠️ Não é possível retomar. Execute o programa novamente.[/yellow]")
                sys.exit(0)
        except KeyboardInterrupt:
            # Se pressionar Ctrl+C novamente na confirmação, sai imediatamente
            sys.exit(0)
