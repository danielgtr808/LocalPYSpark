from typing import Any, Dict, List, Union



class Row():

    def __init__(self, *args, **kwargs) -> None:
        if (args and kwargs):
            raise Exception("Cannot set args and kwargs together")
        
        if (kwargs):
            self._fields: List[str] = list(kwargs.keys())
            self._values: List[Any] = list(kwargs.values())
        else:
            self._values = args

    def asDict(self, recursive: bool = False) ->  Dict[str, Any]:
        if (not hasattr(self, "_fields")):
            raise Exception("Cannot convert list to dict")
        
        row_as_dict = dict(zip(self._fields, self._values)) 
        if (recursive):
            for key in row_as_dict.keys():
                if (isinstance(row_as_dict[key], Row)):
                    row_as_dict[key] = row_as_dict[key].asDict()

        return row_as_dict

    def __getitem__(self, item: Union[int, slice, str]) -> Any:
        if (isinstance(item, (int, slice))):
            return self._values[item]
        
        return self[self._fields.index(item)]
    


def _rows_are_equal(rows: List[Row]) -> bool:
    for i, r in enumerate(rows[:-1]):
        if ((not hasattr(r, "_fields"))):
            if ((r._values != rows[i + 1]._values)):
                return False
            
        elif (r.asDict(True) != rows[i + 1].asDict(True)):
            return False

    return True