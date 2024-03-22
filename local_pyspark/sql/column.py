from .._local_api_.local_column import LocalColumn

def _column_builder(operator, other: object) -> "Column":
    return Column(operator(
        other._lc if isinstance(other, Column) else other
    ))

class Column():

    def __init__(self, lc: LocalColumn):
        self._lc = lc

    def __and__(self, other: object) -> "Column":
        return _column_builder(self._lc.and_, other)
    
    def __eq__(self, __value: object) -> "Column":
        return _column_builder(self._lc.equal_to, __value)
    
    def __ge__(self, __value: object) -> "Column":
        return _column_builder(self._lc.ge, __value)
    
    def __gt__(self, __value: object) -> "Column":
        return _column_builder(self._lc.gt, __value)
    
    def __le__(self, __value: object) -> "Column":
        return _column_builder(self._lc.le, __value)
    
    def __lt__(self, __value: object) -> "Column":
        return _column_builder(self._lc.lt, __value)
    
    def __ne__(self, __value: object) -> "Column":
        return _column_builder(self._lc.not_equal, __value)
    
    def __or__(self, other: object) -> "Column":
        return _column_builder(self._lc.or_, other)