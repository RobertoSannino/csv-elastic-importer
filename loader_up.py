from elasticsearch import Elasticsearch
from elasticsearch import helpers
import csv
import os

class ExportInElastic:

    def __init__(self):
        self.root_path = "/Users/roberto/Apps/neo4j-community-4.2.1/import"
        self.csv_paths = [self.root_path + "/", self.root_path + "/2016", self.root_path + "/2017", self.root_path + "/2018", self.root_path + "/2019"]

        self.index_name = 'uffici_provinciali'
        self.csv_files_to_read = ['ufficio-provinciale-0-2019','ufficio-provinciale-0-2018','ufficio-provinciale-0-2017','ufficio-provinciale-0-2016']

    def read_file(self,path,file):
        header = ("zzCode","codiceUfficioProvinciale","annoRiferimento","ufficio","label")

        resultList = []

        with open(os.path.join(path,file), "r") as f:
            reader = csv.DictReader(f,fieldnames = header)
            for line in reader:
                resultList.append(line)
        return resultList

    def __get_elastic_client(self):
        return Elasticsearch("localhost:9200")

    def req_elastic_index(self):
        return {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "whitespace_with_lowercase": {
                            "tokenizer": "whitespace",
                            "filter": [
                                "lowercase"
                            ]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    #"codiceFiscale": {
                     #   "type": "text",
                      #  "analyzer": "keyword"
                    #},
                    "ufficio": {
                        "type": "text",
                        "analyzer": "whitespace_with_lowercase"
                    }
                }
            }
        }

    def json_bulk_suffix(self, doc):
        return {
            '_op_type': 'index',
            '_index': 'uffici_provinciali',
            '_source': { #con doc gli dai tutto il documento
                'zzCode': doc.get('zzCode'),
                'ufficio': doc.get('ufficio')
            }
        }

    def run(self):
        print("=== Connecting to Elastic ===")
        as_client = self.__get_elastic_client()

        print("=== Creating Index ===")
        #as_client.indices.delete(index=self.index_name)
        as_client.indices.create(index=self.index_name, body=self.req_elastic_index())

        for dir_name in self.csv_paths:
            for file_name in os.listdir(dir_name):
                path = os.path.join(dir_name, file_name)
                if os.path.isdir(path) or file_name[:-4] not in self.csv_files_to_read :
                    continue

                print("=== File: ", file_name)
                elastic_documents = self.read_file(dir_name, file_name)
                print("=== Document Read")
                print("=== Start Importing In Elasticsearch ===")

                json_list = []
                for doc in elastic_documents:
                    json_list.append(self.json_bulk_suffix(doc))

                helpers.bulk(client=as_client, actions=json_list)

if __name__ == '__main__':
    exp=ExportInElastic()
    exp.run()
    del exp