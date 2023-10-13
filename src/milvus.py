from pymilvus import CollectionSchema, FieldSchema, DataType, connections, db, utility, Collection
import argparse
import requests
import json
import logging
import numpy as np
from flask import Flask, request, jsonify, Blueprint
logging.basicConfig(level=logging.INFO)
app = Flask(__name__)


class VDBHandeler:

    def __init__(self, args) -> None:
        self.init_milvus(args.reset)
        self.bp = Blueprint("VectorDB", __name__)
        self.register_routes()

    def register_routes(self):
        self.bp.add_url_rule("/upsert", "upsert",
                             self.upsert_Data, methods=["POST"])
        self.bp.add_url_rule("/delete", "delete",
                             self.delete_Data, methods=["POST"])
        self.bp.add_url_rule(
            "/query", "query", self.query_Data, methods=["POST"])
        self.bp.add_url_rule("/", "alive", self.alive, methods=["GET"])

    def alive(self):
        return "GOOD"

    def upsert_Data(self):
        data = request.get_json()
        # logging.info(data)
        # table = Collection(data["tableName"])
        table = Collection("aitown")
        upsert_data = [[data["vector"]["id"]], [np.array(data["vector"]["values"])],  [
            data["vector"]["metadata"]["playerId"]],]
        logging.info(upsert_data)
        mr = table.insert(upsert_data)
        response = {"message": "Data upsert successfully :)"}
        logging.info(response)
        return response

    def delete_Data(self):
        data = request.get_json()
        # logging.info(data)
        if "deleteAll" in data.keys() and data["deleteAll"] == True:
            self.init_milvus()
        else:
            table = Collection("aitown")
            table.delete("id = "+str(data["id"]))
        response = {"message": "Data delete successfully :)"}
        logging.info(response)
        return response

    def query_Data(self):
        # logging.error(2222)
        # logging.info(1111)
        data = request.get_json()
        logging.info(data["filter"])
        # table = Collection(data["tableName"])
        table = Collection("aitown")
        table.load()
        search_params = {
            "metric_type": "L2",
            "offset": 1,
            "ignore_growing": False,
            "params": {"nprobe": 4}
        }
        res = table.search(data=[data["embedding"]], limit=data["topK"],
                           param=search_params, anns_field="values", consistency_level="Strong", expr="playerId == '"+str(data["filter"]["playerId"])+"'")
        ret = []
        print(res[0].ids, res[0].distances)
        for i, s in zip(res[0].ids, res[0].distances):
            ret.append({"id": i, "score": s})
        response = str(ret).replace("'", '"')
        logging.info("Q:"+response)
        return response

    def init_milvus(self, reset=False):
        connection = connections.connect(host="localhost", port=19530)
        if "aitown" not in db.list_database():
            vdb = db.create_database("aitown")
        db.using_database("aitown")
        if reset is True:
            #
            utility.drop_collection("aitown")
            print("Successfully deleted collections")
        if not utility.has_collection("aitown"):
            memory_id = FieldSchema(
                name="id",
                dtype=DataType.VARCHAR,
                max_length=200,
                is_primary=True,
            )
            player_id = FieldSchema(
                name="playerId",
                dtype=DataType.VARCHAR,
                max_length=200,
                # The default value will be used if this field is left empty during data inserts or upserts.
            )
            embeddings = FieldSchema(
                name="values",
                dtype=DataType.FLOAT_VECTOR,
                dim=1536
            )
            schema = CollectionSchema(
                fields=[memory_id, embeddings, player_id],
                description="memorydb",
                enable_dynamic_field=True
            )
            collection_name = "aitown"
            c = Collection(
                name=collection_name,
                schema=schema,
                using='default',
                shards_num=2
            )
            index_params = {
                "metric_type": "L2",
                "index_type": "IVF_FLAT",
                "params": {"nlist": 256}
            }
            c = Collection("aitown")
            c.create_index(field_name="values", index_params=index_params)
            print('Successfully created collections')
        print(utility.has_collection("aitown"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start milvus service.')
    parser.add_argument('--reset', action="store_true",
                        help="reset vector database.")

    args = parser.parse_args()
    print(args)
    vdb = VDBHandeler(args)
    app.register_blueprint(vdb.bp)
    app.run(host='localhost', port=5001)
