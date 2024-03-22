from .local_column import LocalColumn
from .local_row import LocalRow
from ..sql.row import Row
from typing import Any, Dict, List, TypedDict, Union
import random
import time



def _random_string_from_timestamp(length=10):
    timestamp_ms = int(time.time_ns() / 1e6)  # Convert nanoseconds to milliseconds
    random.seed(timestamp_ms)
    characters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    return ''.join(random.choice(characters) for _ in range(length))

# class TableMetadata():
#     id: int
#     name: str
#     type: Any

class Table(TypedDict):
    data: List[LocalRow]
    metadata: List[LocalColumn]

type Warehouse = Dict[str, Dict[str, Table]]

class WarehouseManager():

    column_count: int = 0
    
    def __init__(self) -> None:
        self.default_context = "default"
        self.warehouse: Warehouse = {
            "default": { }
        }

    @property
    def column_id(self) -> int:
        WarehouseManager.column_count += 1
        return WarehouseManager.column_count

    def append(self, table_path: str, data: List[Union[LocalRow]]) -> None:
        pass

    def create_database(self, database_name: str, ignore_if_exists: bool = True) -> None:
        if (database_name not in self.warehouse.keys()):
            self.warehouse[database_name] = { }
            return
        
        if (not ignore_if_exists):
            raise Exception("Database already exists")

    def create_table(self, data: Union[List[LocalRow], List[Row]], table_name: str) -> Table:
        if (len(data) == 0):
            raise Exception("The functionality to create a dataframe without rows is not yet avaliable")
        
        local_columns: List[LocalColumn] = self.generate_local_columns(data[0])

        self.create_database(table_name.split(".")[0])
        self.warehouse[table_name.split(".")[0]][table_name.split(".")[1]] = {
            "data": data,
            "metadata": local_columns
        }

        return self.warehouse[table_name.split(".")[0]][table_name.split(".")[1]]

    def create_temp_table(self, data: Union[List[LocalRow], List[Row]]) -> Table:
        return self.create_table(data, f"default.{_random_string_from_timestamp()}")

    def generate_local_columns(self, data_sample: Row) -> List[LocalColumn]:
        columns = List[LocalColumn] = []

        if (not hasattr(data_sample, "_fields")):
            for i, v in enumerate(data_sample._values):
                columns.append(LocalColumn(None, {
                    "id": self.column_id,
                    "name": f"_c{i}"
                }))

        else:
            for column_name in data_sample.asDict(False).keys():
                columns.append(LocalRow(None, {
                    "id": self.column_id,
                    "name": column_name
                }))

        return columns

    def table(self, table_path: str) -> Table:
        database_name: str = ""
        table_name: str = ""

        if ("." in table_path):
            database_name = table_path.split(".")[0]
            table_name = table_path.split(".")[1:]
        else:
            database_name = self.default_context
            table_name = table_path

        return self.warehouse[database_name][table_name]

    def overwrite(self, table_path: str, data: List[Union[LocalRow]]) -> None:
        pass