import configparser
import json

from google.cloud import storage
from gremlin_python.driver import client as cl, serializer

# read config files

config = configparser.ConfigParser()
config.read("../filenames.ini")

# global object for gremlin

ENDPOINT = 'wss://intership-assignments-2020.gremlin.cosmos.azure.com:443/'
DATABASE = 'graph-data-ambuj-kumar'
COLLECTION = 'graph-data-ambuj-kumar'
PRIMARY_KEY = \
    ''

client_grem = cl.Client(
    message_serializer=serializer.GraphSONSerializersV2d0(),
    url=ENDPOINT, traversal_source='g',
    username="/dbs/" + DATABASE + "/colls/" +
             COLLECTION,
    password=PRIMARY_KEY
    )


# function takes in json and type and creates query iterating over keys

def gen_key_val_query(i, type_):
    if type_ == "node":
        ini_s = "g.addV('" + i["Label"][0] + "').property['id','" + i[
            "IdUnique"] + "']"  # static section of query
        del i["Label"]
        del i["IdUnique"]  # removing Label array and IDUnique already added
        for j in i.keys(): # iterate add node property from json keys
            ini_s = ini_s + ".property('" + j + "','" + str(i[j]) + "')"
        print(ini_s)
    else:
        ini_s = "g.V().has('IdObject', '" + i[
            "FromIdObject"] + "').has('Label', '" \
                + i["FromLabel"] + "')as('a').V().has('IdObject', '" \
                + i["ToIdObject"] + "').has('Label', '" + i["ToLabel"] + \
                "').addE('" + i["Type"] + "').from('a')"  # static section of query
        del i["Label"]
        del i["Type"]
        for j in i.keys(): # iterate add rel property from json keys
            ini_s = ini_s + ".property('" + j + "','" + str(i[j]) + "')"
        print(ini_s)
    return ini_s


# checks if relationship exists before adding

def relationship_exists(i):
    query = "g.V().has('IdObject', '" + i["FromIdObject"] + "').has('Label', '" \
            + i["FromLabel"] + "').outE('" + i[
                "Type"] + "').as('e').V().has('IdObject', '" \
            + i["ToIdObject"] + "').select('e')"
    callback = client_grem.submitAsync(query)  # check and return true
    if callback.result():
        return True
    else:
        print("Incorrect query: {0}".format(query))


# processes graph queries and calls other functions

def process_graph_queries(blob_s):
    # read blob as string
    data = json.loads(blob_s.download_as_string())
    # iterate to get individual json
    for i in data:
        print(i)
        type_ = i["Kind"]
        if type_ == "node": # handle according to kind
            del i["Kind"]
            query = gen_key_val_query(i, type_)  # create custom query
        elif type_ == "relationship":
            del i["Kind"]
            if i["DeDuplication"]:
                if relationship_exists(i):  #check rel exists
                    continue
            query = gen_key_val_query(i, type_)  # create custom query

        callback = client_grem.submitAsync(query)  #insert node/rel
        if callback.result() is not None:
            print("Inserted this vertex/edge:\n{0}".format(callback.result(
            ).one()))
        else:
            print("Incorrect query: {0}".format(query))

    return "Successful"


# handler function

def handler():
    # read blobs from gs
    gs_client = storage.Client.create_anonymous_client()

    bucket = gs_client.bucket('data-engineering-intern-data')

    blobs = bucket.list_blobs(prefix="graph-data/transform")

    for blob in blobs:
        print(blob.name)
        try:
            config["Default"][blob.name]  # check if blob already has been
            # processed and continue
            continue
        except KeyError:  # else proceed
            print("Key Not in Processed List Continue Processing")

        # if successfully processed add the filename to conf

        if process_graph_queries(blob) == "Successful":
            config.set("Default", blob.name, "True")


if __name__ == '__main__':
    handler()
