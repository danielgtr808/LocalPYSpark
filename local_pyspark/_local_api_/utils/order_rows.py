from ..local_column import LocalColumn
from ..local_row import LocalRow
from typing import Any, Dict, List, Union


def order_rows(columns: List[LocalColumn], grouped_data: Union[Dict[str, Union[Dict[str, str], List[LocalRow]]]]) -> Union[Dict[str, Any], List["LocalColumn"]]:
    if (len(columns) == 0):
        return grouped_data
    
    ordered_data: List[LocalRow] = []
    column: "LocalColumn" = columns[0]

    for value in column(list(grouped_data.keys())):
        ordered_data = ordered_data + order_rows(columns[1:], grouped_data[value])

    return ordered_data