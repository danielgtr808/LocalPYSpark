from .local_row import LocalRow
from statistics import mean
from typing import Any, List, Literal, Optional, TypedDict, Union

import operator

type ColumnTypes = Literal["aggregation", "alias", "arithmetic", "comparison", "logical", "ordering"]
type ColumnOperators = Union[BinaryOperator, None, UnaryOperator]
type OperandType = List[Union[LiteralType, LocalColumn]]
type LiteralType = Union[float, int, str]


class AggregationOperator():

    def __call__(self, rows_to_agg: List[LocalRow]) -> Any:
        return self.operator(map(
            lambda x:
            x[self.column],
            rows_to_agg
        ))

    def __init__(self, column, operator) -> None:
        self.column = column
        self.operator = operator
   
class BinaryOperator():

    def __call__(self, row: List[LocalRow]):
        operator_values: List = []

        for operator in [self.left_operand, self.right_operand]:
            operator_values.append(
                operator(row)
                if (isinstance(operator, LocalColumn)) else
                operator
            )

        return self.operator(*operator_values)

    def __init__(self, left_operand: OperandType, right_operand: OperandType, operator) -> None:
        self.left_operand = left_operand
        self.operator = operator
        self.right_operand = right_operand

class UnaryOperator():

    def __call__(self, rows: Union[List[Union[LiteralType, LocalRow]], LocalRow]):
        if (isinstance(rows, List)):
            if (len(rows) == 0):
                return []

            return self.operator(
                list(map(lambda x: self.left_operand(x), rows))
                if (isinstance(rows[0], LocalRow)) else
                rows
            )
        
        return self.operator(rows)

    def __init__(self, left_operand: OperandType, operator) -> None:
        self.left_operand = left_operand
        self.operator = operator

class LocalColumnMetadata(TypedDict):
    id: Optional[int]
    name: str
    operator: ColumnOperators
    type: Optional[ColumnTypes]

class LocalColumn():

    def __call__(self, row: LocalRow) -> Any:
        if (self.operator is None):
            return row[self]
        
        return self.operator(row)

    def __init__(self, base_column: Union["LocalColumn", None], metadata: LocalColumnMetadata):
        self.base_column = base_column
        self.metadata = metadata

    @property
    def name(self) -> str:
        return self.metadata["name"]
    
    @property
    def operator(self) -> ColumnOperators:
        if ("operator" in self.metadata):
            return self.metadata["operator"]
        
        return None
    
    def and_(self, other: Union[LiteralType, "LocalColumn"]) -> "LocalColumn":
        return LocalColumn(
            self,
            {
                "name": f"({self} AND {other})",
                "operator": BinaryOperator(self, other, operator.and_),
                "type": "logical"
            }
        )

    def as_(self, alias: str) -> "LocalColumn":
        return LocalColumn(
            self,
            {
                "name": f"{self} as {alias}",
                "operator": UnaryOperator(self, (lambda x: self(x))),
                "type": "alias"
            }
        )
  
    def avg(self) -> "LocalColumn":        
        return LocalColumn(
            self,
            {
                "name": f"avg({self})",
                "operator": AggregationOperator(self, mean),
                "type": "aggregation"
            }
        )
      
    def divide(self, other: Union[LiteralType, "LocalColumn"]) -> "LocalColumn":
        return LocalColumn(
            self,
            {
                "name": f"({self} / {other})",
                "operator": BinaryOperator(self, other, operator.truediv),
                "type": "arithmetic"
            }
        )

    def equal_to(self, other: Union[LiteralType, "LocalColumn"]) -> "LocalColumn":
        return LocalColumn(
            self,
            {
                "name": f"({self} = {other})",
                "operator": BinaryOperator(self, other, operator.eq),
                "type": "comparison"
            }
        )
    
    def max(self) -> "LocalColumn":        
        return LocalColumn(
            self,
            {
                "name": f"max({self})",
                "operator": AggregationOperator(self, max),
                "type": "aggregation"
            }
        )

    def min(self) -> "LocalColumn":        
        return LocalColumn(
            self,
            {
                "name": f"min({self})",
                "operator": AggregationOperator(self, min),
                "type": "aggregation"
            }
        )
    
    def minus(self, other: Union[LiteralType, "LocalColumn"]) -> "LocalColumn":
        return LocalColumn(
            self,
            {
                "name": f"({self} - {other})",
                "operator": BinaryOperator(self, other, operator.sub),
                "type": "arithmetic"
            }
        )
    
    def mod(self, other: Union[LiteralType, "LocalColumn"]) -> "LocalColumn":
        return LocalColumn(
            self,
            {
                "name": f"({self} % {other})",
                "operator": BinaryOperator(self, other, operator.mod),
                "type": "arithmetic"
            }
        )
    
    def multiply(self, other: Union[LiteralType, "LocalColumn"]) -> "LocalColumn":
        return LocalColumn(
            self,
            {
                "name": f"({self} * {other})",
                "operator": BinaryOperator(self, other, operator.mul),
                "type": "arithmetic"
            }
        )
    
    def not_(self) -> "LocalColumn":
        return LocalColumn(
            self,
            {
                "name": f"(NOT {self})",
                "operator": BinaryOperator(self, None, (lambda x, y: operator.not_(x))),
                "type": "logical"
            }
        )
    
    def not_equal(self, other: Union[LiteralType, "LocalColumn"]) -> "LocalColumn":
        return LocalColumn(
            self,
            {
                "name": f"NOT ({self} = {other})",
                "operator": BinaryOperator(self, other, operator.ne),
                "type": "comparison"
            }
        )
    
    def or_(self, other: Union[LiteralType, "LocalColumn"]) -> "LocalColumn":
        return LocalColumn(
            self,
            {
                "name": f"({self} OR {other})",
                "operator": BinaryOperator(self, other, operator.or_),
                "type": "logical"
            }
        )
    
    def plus(self, other: Union[LiteralType, "LocalColumn"]) -> "LocalColumn":
        return LocalColumn(
            self,
            {
                "name": f"({self} + {other})",
                "operator": BinaryOperator(self, other, operator.add),
                "type": "arithmetic"
            }
        )

    def __str__(self) -> str:
        return self.name