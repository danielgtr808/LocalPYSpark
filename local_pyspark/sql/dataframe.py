from .session import SparkSession
from .._local_api_.local_dataframe import LocalDataframe



class DataFrame():

    def __init__(self, ldf: LocalDataframe, session: SparkSession) -> None:
        self._session = session
        self._ldf = ldf
        self._schema = None

    @property
    def sparkSession(self) -> SparkSession:
        return self._session
    
    def collect(self):
        pass