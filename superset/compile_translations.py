"""Компиляция русского перевода Superset при сборке образа.

В официальном образе лежит только исходник messages.po: скомпилированные файлы он
собирает лишь при сборке из исходников с флагом BUILD_TRANSLATIONS. Нужны два файла:
messages.mo для бэкенда и messages.json (формат Jed) для фронтенда.
"""
import json
import sys
from pathlib import Path

from babel.messages.mofile import write_mo
from babel.messages.pofile import read_po

directory = Path(sys.argv[1])
with open(directory / "messages.po", "rb") as f:
    catalog = read_po(f, locale="ru", domain="superset")

messages = {"": {"domain": "superset", "lang": "ru", "plural_forms": catalog.plural_forms}}
for message in catalog:
    if not message.id or message.fuzzy or not message.string:
        continue
    plural = isinstance(message.id, tuple)
    key = message.id[0] if plural else message.id
    if message.context:
        key = f"{message.context}\x04{key}"  # так Jed хранит сообщения с контекстом
    values = list(message.string) if plural else [message.string]
    if all(values):
        messages[key] = values

with open(directory / "messages.mo", "wb") as f:
    write_mo(f, catalog)
with open(directory / "messages.json", "w", encoding="utf-8") as f:
    json.dump({"domain": "superset", "locale_data": {"superset": messages}}, f, ensure_ascii=False)
print(f"ru: {len(messages) - 1} сообщений")
