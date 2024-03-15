from typing import List
from ..__local_api__.local_dataframe import LocalDataframe
from .session import SparkSession


class DataFrame():

    def __init__(self, ldf: LocalDataframe, session: SparkSession) -> None:
        self._session = session
        self._ldf = ldf
        self._schema = None

    @property
    def columns(self) -> List[str]:
        return [f.name for f in self.schema.fields]

    @property
    def schema(self):
        return self._schema

    @property
    def sparkSession(self) -> SparkSession:
        return self._session
    
    def collect(self):
        pass

    def __getitem__(self, item):

        # Criar coluna como lc (local column)

    