from ..local_column import LocalColumn
from ..local_row import LocalRow
from typing import Dict, List, Union


def agg_rows(columns: List[LocalColumn], agg: List[LocalColumn], grouped_data: Union[Dict, List[LocalRow]]):
    if (isinstance(grouped_data, Dict)):
        agg_data: List[LocalRow] = []

        for key in list(grouped_data.keys()):
            agg_data = agg_data + agg_rows(columns, agg, grouped_data[key])

        return agg_data
    
    agg_row: LocalRow = grouped_data[0].clone()
    for agg_column in agg:
        agg_row.add(agg_column, agg_column(grouped_data))

    columns_to_keep = [*columns, *agg]
    for key in list(agg_row.data.keys()):
        if (key not in columns_to_keep):
            agg_row.remove(key)

    return [agg_row]