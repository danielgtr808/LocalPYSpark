from .local_column import LocalColumn
from .local_row import LocalRow
from .utils import agg_rows, ColumnType, map_column_to_local_column
from typing import Callable, Dict, List, Union



type DataCallback = Callable[[],  Union[Dict[str, Union[Dict[str, str], List[LocalRow]]]]]

class LocalGroupedData():


    def __init__(self, data_callback: DataCallback, group_by_rows: List[LocalColumn]) -> None:
        self.data_callback = data_callback
        self.group_by_rows = group_by_rows

    def agg(self, *expr: ColumnType) -> "LocalDataframe":
        _columns: List[LocalColumn] = map_column_to_local_column(list(expr))

        from .local_dataframe import LocalDataframe 
        return LocalDataframe((
            lambda: agg_rows(self.group_by_rows, _columns, self.data_callback())
        ))