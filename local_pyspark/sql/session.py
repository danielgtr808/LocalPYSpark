from local_pyspark.sql.conf import RuntimeConfig
from typing import Any, Dict



class SparkSession():

    def __init__(self, options: Dict[str, Any]) -> None:
        self._conf: RuntimeConfig = RuntimeConfig(options)
    
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
        
    def builder() -> Builder:
        return SparkSession.Builder()