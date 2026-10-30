# Официальный образ "лёгкий": драйверов баз данных в нём нет, ставим нужные в его venv.
FROM apache/superset:6.1.0

USER root
RUN uv pip install --python /app/.venv/bin/python \
    psycopg2-binary==2.9.* \
    clickhouse-connect==0.8.*

COPY compile_translations.py /tmp/compile_translations.py
RUN /app/.venv/bin/python /tmp/compile_translations.py /app/superset/translations/ru/LC_MESSAGES

COPY superset_config.py /app/pythonpath/superset_config.py
COPY bootstrap.sh /app/bootstrap.sh
RUN chmod +x /app/bootstrap.sh

USER superset
ENTRYPOINT ["/app/bootstrap.sh"]
