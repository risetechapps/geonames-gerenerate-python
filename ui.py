"""
Interface visual elegante para o terminal usando Rich.
"""

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.layout import Layout
from rich.text import Text
from rich import box
import time

console = Console()


def show_header():
    """Mostra o cabeçalho do programa."""
    console.print()
    console.print(Panel.fit(
        Text("🌍 Geonames Generator", justify="center", style="bold cyan") +
        Text("\n", style="") +
        Text("Download e processamento de dados geográficos", justify="center", style="dim white"),
        border_style="cyan",
        box=box.ROUNDED
    ))
    console.print()


def show_config_status(config):
    """Mostra status da configuração em uma tabela elegante."""
    table = Table(title="📋 Configuração", box=box.ROUNDED, border_style="blue")
    table.add_column("Componente", style="cyan", no_wrap=True)
    table.add_column("Status", style="green")
    table.add_column("Detalhes", style="white")

    # Bancos de dados
    databases = [
        ("SQLite", config.get('sqlite', {}).get('enabled', False), config.get('sqlite', {}).get('database', 'db.sqlite')),
        ("MySQL", config.get('mysql', {}).get('enabled', False), config.get('mysql', {}).get('host', 'localhost')),
        ("PostgreSQL", config.get('postgresql', {}).get('enabled', False), config.get('postgresql', {}).get('host', 'localhost')),
        ("MongoDB", config.get('mongodb', {}).get('enabled', False), config.get('mongodb', {}).get('host', 'localhost')),
    ]

    for name, enabled, location in databases:
        status = "✅ Ativado" if enabled else "⛔ Desabilitado"
        status_style = "green" if enabled else "dim white"
        table.add_row(name, Text(status, style=status_style), location)

    # API Estática
    api_config = config.get('static_api', {})
    api_enabled = api_config.get('enabled', True)
    api_path = api_config.get('output_path', 'geonames/json')
    api_status = "✅ Ativada" if api_enabled else "⛔ Desabilitada"
    api_status_style = "green" if api_enabled else "dim white"
    table.add_row("API Estática", Text(api_status, style=api_status_style), api_path)

    console.print(table)
    console.print()


def create_download_progress():
    """Cria uma barra de progresso para download."""
    return Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=40, complete_style="cyan", finished_style="green"),
        TaskProgressColumn(),
        console=console,
        transient=False
    )


def create_processing_progress():
    """Cria uma barra de progresso para processamento."""
    return Progress(
        SpinnerColumn(style="green"),
        TextColumn("[bold green]{task.description}"),
        BarColumn(bar_width=40, complete_style="green", finished_style="cyan"),
        TaskProgressColumn(),
        console=console,
        transient=False
    )


def show_download_progress(task_name, urls):
    """Mostra progresso de download múltiplo."""
    with Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=40),
        TaskProgressColumn(),
        console=console
    ) as progress:
        tasks = []
        for url in urls:
            task_id = progress.add_task(f"⬇️  Baixando {url}...", total=100)
            tasks.append(task_id)

        # Simula progresso
        for i in range(101):
            for task_id in tasks:
                progress.update(task_id, completed=i)
            time.sleep(0.01)


def show_processing_table():
    """Mostra tabela de processamento em tempo real."""
    table = Table(box=box.ROUNDED, border_style="green", title="🔄 Processando Dados")
    table.add_column("Etapa", style="cyan")
    table.add_column("Status", style="white")
    table.add_column("Itens", justify="right", style="green")

    return table


def show_database_results(results):
    """Mostra resultados do processamento em bancos em tabela."""
    console.print()
    table = Table(title="📊 Resultados do Processamento", box=box.ROUNDED, border_style="cyan")
    table.add_column("Banco de Dados", style="bold cyan")
    table.add_column("Status", style="white")
    table.add_column("Detalhes", style="dim white")

    for db_name, status, details in results:
        if status == "success":
            status_text = Text("✅ Concluído", style="green")
        elif status == "error":
            status_text = Text("❌ Erro", style="red")
        elif status == "disabled":
            status_text = Text("⏭️  Pulado", style="dim white")
        else:
            status_text = Text("⏳ Processando...", style="yellow")

        table.add_row(db_name, status_text, details)

    console.print(table)
    console.print()


def show_success_summary(total_countries, total_states, total_cities):
    """Mostra resumo de sucesso."""
    console.print(Panel(
        Text("✨ Processamento Concluído!", justify="center", style="bold green") +
        Text("\n\n", style="") +
        Text(f"🗂️  Países: {total_countries}\n", style="white") +
        Text(f"📍 Estados: {total_states}\n", style="white") +
        Text(f"🏙️  Cidades: {total_cities}", style="white"),
        border_style="green",
        box=box.ROUNDED
    ))
    console.print()


def show_error_message(error_msg):
    """Mostra mensagem de erro."""
    console.print()
    console.print(Panel(
        Text("❌ Erro", style="bold red") +
        Text(f"\n\n{error_msg}", style="white"),
        border_style="red",
        box=box.ROUNDED
    ))
    console.print()


def show_info_message(message, style="cyan"):
    """Mostra mensagem informativa."""
    console.print(f"[{style}]ℹ️  {message}[/{style}]")


def show_section_title(title, emoji="📦"):
    """Mostra título de seção."""
    console.print()
    console.print(f"[bold cyan]{emoji} {title}[/bold cyan]")
    console.print("─" * 60, style="dim cyan")


def show_progress_task(description, total=100):
    """Mostra uma tarefa de progresso simples."""
    return Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold]{task.description}"),
        BarColumn(bar_width=40),
        TaskProgressColumn(),
        console=console
    )


class DatabaseProgressTracker:
    """Rastreador de progresso para processamento de bancos."""

    def __init__(self):
        self.results = []
        self.current_db = None

    def start_db(self, db_name):
        """Inicia processamento de um banco."""
        self.current_db = db_name
        console.print(f"[cyan]🔄 Conectando a {db_name}...[/cyan]")

    def success(self, details=""):
        """Marca banco atual como sucesso."""
        if self.current_db:
            console.print(f"  [green]✅ {self.current_db} - {details}[/green]")
            self.results.append((self.current_db, "success", details))

    def error(self, error_msg):
        """Marca banco atual como erro."""
        if self.current_db:
            console.print(f"  [red]❌ {self.current_db} - {error_msg}[/red]")
            self.results.append((self.current_db, "error", error_msg))

    def disabled(self):
        """Marca banco atual como desabilitado."""
        if self.current_db:
            console.print(f"  [dim white]⏭️  {self.current_db} - Desabilitado[/dim white]")
            self.results.append((self.current_db, "disabled", "Não configurado"))

    def get_results(self):
        """Retorna resultados."""
        return self.results
