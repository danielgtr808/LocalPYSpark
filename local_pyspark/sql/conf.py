from typing import Any, Dict
import tempfile
import os



DEFAULT_CONFIG = {
    "localspark.warehouse.dir": None,
    "spark.sql.warehouse.dir": os.path.join(tempfile.gettempdir(), "warehouse")
}

class RuntimeConfig():

    def __init__(self, options: Dict[str, Any]) -> None:
        self._options: Dict[str, Any] = options

    def get(self, key: str, default: Any = None) -> Any:
        if (key in self._options.keys()):
            return self._options[key]
        elif (default is not None):
            self.set(key, default)
            return default
        
        raise Exception(f"The SQL config \"{key}\" cannot be found...")
        
        
    
    def getAll(self) -> Dict[str, Any]:
        return self._options

    def set(self, key: str, value: Any) -> None:
        self._options[key] = value

    def unset(self, key: str) -> None:
        del self._options[key]
