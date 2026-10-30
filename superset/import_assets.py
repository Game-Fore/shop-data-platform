"""Импорт дашборда из git при запуске Superset: дашборд живёт в YAML (superset/dashboards), а не в метабазе.

Поэтому он переживает reset.ps1, а его изменения видны в git diff. Повторный импорт перезаписывает
объекты с теми же uuid: правки, сделанные только в интерфейсе, при перезапуске пропадут, их нужно
перенести в YAML. Объект, удалённый из YAML, из метабазы сам не исчезнет.

CLI `superset import-dashboards` здесь не подходит: его overwrite=True действует только на сам
дашборд, а чарты и датасеты внутри него импортируются с overwrite=False и молча не обновляются.
Поэтому каждый тип объектов импортируется своей командой.

Пароль в git не хранится: в файле подключения лежит заглушка, значение подставляется только в памяти.
"""
import os
from pathlib import Path
from urllib.parse import quote

from superset.app import create_app

SOURCE = Path("/app/dashboards")
PLACEHOLDER = "__CLICKHOUSE_BI_PASSWORD__"

app = create_app()
with app.app_context():
    # импортировать можно только после создания приложения: модели используют его контекст
    from superset import security_manager
    from superset.commands.chart.importers.v1 import ImportChartsCommand
    from superset.commands.dashboard.importers.v1 import ImportDashboardsCommand
    from superset.commands.database.importers.v1 import ImportDatabasesCommand
    from superset.commands.dataset.importers.v1 import ImportDatasetsCommand
    from superset.utils.core import override_user

    password = quote(os.environ["CLICKHOUSE_BI_PASSWORD"], safe="")
    contents = {path.relative_to(SOURCE).as_posix(): path.read_text(encoding="utf-8").replace(PLACEHOLDER, password)
                for path in SOURCE.rglob("*.yaml") if path.name != "metadata.yaml"}

    # порядок важен: каждый следующий тип ссылается на предыдущий. Тип в metadata.yaml проверяется командой.
    commands = [(ImportDatabasesCommand, "Database"), (ImportDatasetsCommand, "SqlaTable"),
                (ImportChartsCommand, "Slice"), (ImportDashboardsCommand, "Dashboard")]
    with override_user(security_manager.find_user(username="admin")):
        for command, model_type in commands:
            metadata = f"version: 1.0.0\ntype: {model_type}\ntimestamp: '2026-01-01T00:00:00+00:00'\n"
            command({**contents, "metadata.yaml": metadata}, overwrite=True).run()
            print(f"импортировано: {model_type}")
