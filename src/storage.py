"""
Низкоуровневые функции для работы с файлами базы данных.

Ключевая идея хранения данных:
    Каждая строка в файле данных имеет фиксированную длину — ровно 501 символ.

Формат индексного файла:
    Каждая строка индекса: "ключ:номер строки в файле данных"
    Индекс всегда отсортирован по ключу.
    Индекс целиком загружается в память — он маленький и нужен для быстрого
    поиска номера строки по ключу.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Generator, TypeVar

from pydantic import BaseModel

from config import DELIMITER, RECORD_DATA_LEN, RECORD_LEN


T = TypeVar("T", bound=BaseModel)


def ensure_file_exists(filename: str) -> None:
    """
    Создаёт файл, если он ещё не существует.
    """
    Path(filename).touch(exist_ok=True)


def value_to_string(value: object) -> str:
    """
    Преобразует значение поля модели в строку для записи в txt.
    Обрабатывает str, datetime, Decimal, Enum.
    """
    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, Enum):
        return str(value.value)

    return str(value)


def serialize(model: BaseModel) -> str:
    """
    Превращает Pydantic-объект в строку фиксированной длины для записи в файл.
    """
    data = model.model_dump()
    row = DELIMITER.join(value_to_string(value) for value in data.values())

    if len(row) > RECORD_DATA_LEN:
        raise ValueError(
            f"Длина записи превышает {RECORD_DATA_LEN} символов: {row}"
        )

    return row.ljust(RECORD_DATA_LEN) + "\n"


def deserialize(line: str, model_class: type[T]) -> T:
    """
    Превращает строку из файла обратно в Pydantic-объект.
    """
    stripped = line.rstrip("\n").rstrip()
    values = stripped.split(DELIMITER)

    field_names = list(model_class.model_fields.keys())

    if len(values) != len(field_names):
        raise ValueError(
            f"Неверное число полей для {model_class.__name__}: "
            f"ожидалось {len(field_names)}, получено {len(values)}"
        )

    data = dict(zip(field_names, values))
    return model_class(**data)


def append_record(filename: str, record: BaseModel) -> None:
    """
    Добавляет одну запись в конец файла данных.
    """
    ensure_file_exists(filename)

    with open(filename, "a", encoding="utf-8", newline="\n") as file:
        file.write(serialize(record))


def iter_records(filename: str, model_class: type[T]) -> Generator[tuple[int, T], None, None]:
    """
    Последовательно читает файл и возвращает пары (номер строки, объект).
    """
    ensure_file_exists(filename)

    with open(filename, "r", encoding="utf-8", newline="\n") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue
            yield line_number, deserialize(line, model_class)


def read_record_by_line(filename: str, line_number: int, model_class: type[T]) -> T | None:
    """
    Читает конкретную строку из файла без чтения всего файла.
    """
    if line_number < 1:
        return None

    ensure_file_exists(filename)

    with open(filename, "r", encoding="utf-8", newline="\n") as file:
        offset = (line_number - 1) * RECORD_LEN
        file.seek(offset)
        line = file.readline()

        if not line:
            return None

        return deserialize(line, model_class)


def rewrite_file(filename: str, records: list[BaseModel]) -> None:
    """
    Полностью перезаписывает файл переданным списком записей.
    """
    with open(filename, "w", encoding="utf-8", newline="\n") as file:
        for record in records:
            file.write(serialize(record))


def rebuild_index(data_filename: str, index_filename: str, model_class: type[T]) -> None:
    """
    Полностью перестраивает индексный файл по данным из файла данных.
    """
    ensure_file_exists(data_filename)

    pairs: list[tuple[str, int]] = []

    for line_number, record in iter_records(data_filename, model_class):
        index_key = record.index()
        pairs.append((index_key, line_number))

    pairs.sort(key=lambda item: item[0])

    with open(index_filename, "w", encoding="utf-8", newline="\n") as file:
        for key, line_number in pairs:
            file.write(f"{key}:{line_number}\n")


def find_line_number(index_filename: str, key: str) -> int | None:
    """
    Ищет номер строки по ключу в индексном файле.
    """
    ensure_file_exists(index_filename)

    with open(index_filename, "r", encoding="utf-8", newline="\n") as file:
        for line in file:
            stripped = line.rstrip("\n").strip()
            if not stripped:
                continue

            index_key, line_number = stripped.split(":", maxsplit=1)

            if index_key == key:
                return int(line_number)

    return None


def record_exists(index_filename: str, key: str) -> bool:
    """
    Проверяет, есть ли запись в индексе.
    Возвращает True если есть, False если нет.
    """
    return find_line_number(index_filename, key) is not None


def get_record_by_key(
    data_filename: str,
    index_filename: str,
    key: str,
    model_class: type[T],
) -> T | None:
    """
    Находит и возвращает запись по ключу.
    """
    line_number = find_line_number(index_filename, key)

    if line_number is None:
        return None

    return read_record_by_line(data_filename, line_number, model_class)
