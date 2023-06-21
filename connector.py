import snowflake.connector
from elasticsearch import Elasticsearch 
from elasticsearch.helpers import streaming_bulk
import yaml
import json, os
import tqdm
from loguru import logger

class Connector:

    def __init__(self):

        logger.info("loading configs")
        with open('config/connector.yml', 'r') as file:
            c = yaml.safe_load(file)

        self._c = c

        logger.info("Connecting snowflake ...")
        conn_sf = snowflake.connector.connect(
            user=c['snowflake']['username'],
            password=c['snowflake']['password'],
            account=c['snowflake']['account'],
            warehouse=c['snowflake']['warehouse'],
            database=c['snowflake']['database'],
            schema=c['snowflake']['scheme']
        )
    
        self._sf = conn_sf.cursor()

        logger.info("Connecting elasticsearch ...")
        conn_es = Elasticsearch(
            c['elasticsearch']['host'],
            ca_certs = c['elasticsearch']['ca_cert'],
            basic_auth = (c['elasticsearch']['username'], c['elasticsearch']['password'])
        )

        self._es = conn_es
    
    def getColumns(self):

        c = "show columns in table " + self._c['snowflake']['table']
        result = self._sf.execute(c)
        col = []
        for row in result:
            col.append(row[2])
       
        return col
    
    def getCount(self):

        c = "select count(*) as count from " + self._c['snowflake']['table']
        result = self._sf.execute(c)

        for row in result:
            count = row[0]
  
        logger.info("Total records found " + str(count))
        return count
    
    def pull(self, offset):
 
        col = self.getColumns()

        select_columns = ', '.join(col)

        q = "select "+select_columns+" from "+self._c['snowflake']['table']+" limit " + str(self._c['snowflake']['limit'])+ " offset " + str(offset);
        result = self._sf.execute(q)
        
        for row in result:
            doc = dict(zip(col, row))
            yield doc

    def stream(self, offset):

        for ok, action in streaming_bulk(
            client=self._es, index=self._c['elasticsearch']['index'], actions=self.pull(offset),):
            self._success += 1
            self._progress.update(1)


    def push(self):

        logger.info("Data transfer started")
        
        self._count = self.getCount()
        self._success = 0 
        batch_no = 1
        limit = self._c['snowflake']['limit']

        self._progress = tqdm.tqdm(unit="docs", total=self._count)

        while self._success < self._count:
            offset = (batch_no - 1) * limit 
            self.stream(offset)
            batch_no += 1


    def run(self):
        self.push()
        logger.success("Data transfer completed")
