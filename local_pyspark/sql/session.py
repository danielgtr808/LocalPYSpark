from __future__ import annotations

from .conf import RuntimeConfig
from .row import Row
from .types import StructType, _infer_schema, _merge_type
from .._local_api_.warehouse import WarehouseManager
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .dataframe import DataFrame



class classproperty(property):
    def __get__(self, instance: Any, owner: Any = None) -> "SparkSession.Builder":
        return classmethod(self.fget).__get__(None, owner)()  # type: ignore
    
class SparkSession():

    def __init__(self, options: Dict[str, Any], warehouse_manager: Optional[WarehouseManager] = None) -> None:
        self._conf: RuntimeConfig = RuntimeConfig(options)
        self._warehouse_manager = warehouse_manager or WarehouseManager()
    
    @property
    def conf(self) -> RuntimeConfig:
        return self._conf

    class Builder():

        def __init__(self) -> None:
            self._options: Dict[str, Any] = { }

        def appName(self, name: str) -> "SparkSession.Builder":
            return self.config("spark.app.name", name)

        def config(self, key: str, value: Any) -> "SparkSession.Builder":
            self._options[key] = value
            return self
        
        def enableHiveSupport(self) -> "SparkSession.Builder":
            return self.config("spark.sql.catalogImplementation", "hive")
        
        def getOrCreate(self) -> "SparkSession":
            return SparkSession(self._options)
            
        def master(self, master: str) -> "SparkSession.Builder":
            return self.config("spark.master", master)

    @classproperty
    def builder(cls) -> "Builder":
        return cls.Builder()    
    
    def createDataFrame(self, data: List[Row]) -> "DataFrame":
        return DataFrame(
            self._warehouse_manager.create_temp_table(data),
            self
        )
    
    def _inferSchemaFromList(self, data: List[Row]) -> StructType:
        if ((not data) or (len(data) == 0)):
            raise Exception("CANNOT_INFER_EMPTY_SCHEMA")

        schemas = list(map(lambda x: _infer_schema(x), data))
        schema = schemas[0]

        for s in schemas[1:]:
            for field in schema:
                field.dataType = _merge_type(field.dataType, s[field.name].dataType)

        return schema

