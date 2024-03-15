from types import *
from typing import Dict, Union

type LocalRowData = Dict["LocalColumn", Union[float, int, str]]



class LocalRow():
    
    def __init__(self, data: LocalRowData):
        self._data = data

    @property
    def data(self) -> LocalRowData:
        return self._data
    
    def add(self, column: "LocalColumn", data: Union[float, int, str]) -> None:
        self._data[column] = data

    def clone(self) -> "LocalRow":
        return LocalRow({**self.data})

    def merge(self, other_row: "LocalRow") -> "LocalRow":
        return LocalRow(
            {
                **self.data,
                **other_row.data
            }
        )

    def remove(self, column: "LocalColumn") -> None:
        try:
            del self._data[column]
        except KeyError:
            return

    def __getitem__(self, key: "LocalColumn") -> Union[float, int, str]:
        return self.data[key]