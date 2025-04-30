from datetime import datetime, timezone
from zoneinfo import ZoneInfo


def obter_data_atual():
    """Retorna data atual em UTC."""
    return datetime.now(timezone.utc).astimezone(ZoneInfo("America/Cuiaba")).date()
