from ..local_column import LocalColumn
from ..local_row import LocalRow
from typing import Dict, List, Union



def group_rows(columns: List[LocalColumn], rows: List[LocalRow]) -> Union[Dict[str, Union[Dict[str, str], List[LocalRow]]]]:
    if (len(columns) == 0):
        return rows
    
    grouped_data: Dict = { }
    column: LocalColumn = columns[0]

    for value in set(map(lambda x: column(x), rows)):
        grouped_data[value] = group_rows(
            columns[1:],
            list(
                filter(lambda x: column.equal_to(value)(x), rows)
            )
        )

    return grouped_data