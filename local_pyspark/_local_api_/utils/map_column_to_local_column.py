from ..local_column import LocalColumn
from ...sql.column import Column
from typing import List, Union


type ColumnType = Union[Column, LocalColumn]

def map_column_to_local_column(columns: List[ColumnType]) -> List[LocalColumn]:
    return list(map(
        lambda x:
        x._lc if (isinstance(x, Column)) else x,
        columns
    ))
