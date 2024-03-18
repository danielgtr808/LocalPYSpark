from .local_column import LocalColumn
from .local_grouped_data import LocalGroupedData
from .local_row import LocalRow
from .utils import ColumnType, group_rows, map_column_to_local_column, order_rows
from ..sql.column import Column
from typing import Callable, List



type DataCallback = Callable[[], List[LocalRow]]

class LocalDataframe():
    
    def __init__(self, data_callback: DataCallback) -> None:
        self.data_callback = data_callback

    def groupBy(self, *cols: ColumnType) -> LocalGroupedData:
        _columns: List[LocalColumn] = map_column_to_local_column(list(cols))
        
        return LocalGroupedData(
            (
                lambda: group_rows(_columns, self.data_callback())
            ),
            _columns
        )

    def orderBy(self, *cols: ColumnType) -> "LocalDataframe":
        _columns: List[LocalColumn] = map_column_to_local_column(list(cols))
        
        return LocalDataframe(
            (
                lambda: order_rows(
                    _columns,
                    group_rows(
                        _columns,
                        self.data_callback()
                    )
                )
            )
        )

    def select(self, *columns: ColumnType) -> "LocalDataframe":
        _columns: List[LocalColumn] = map_column_to_local_column(list(columns))

        def _():
            callback_data: List[LocalRow] = []

            for row in self.data_callback:
                new_row = LocalRow({})

                for col in _columns:
                    new_row.add(
                        col,
                        col.operator(row) if (col.operator is not None) else row[col]
                    )

                callback_data.append(new_row)

            return callback_data

        return LocalDataframe(
            (
                lambda: _()
            )
        )

    def where(self, conditional: ColumnType) -> "LocalDataframe":
        _conditional: LocalColumn = conditional._jc if (isinstance(_conditional, Column)) else conditional

        return LocalDataframe(
            (
                lambda: list(filter(lambda x: _conditional(x), self.data_callback()))
            )
        )
