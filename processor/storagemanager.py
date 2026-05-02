import pickle
import time
import re

import pymongo
from pymongo import UpdateOne
from unidecode import unidecode

# Used to split camel and snake case aliases into tokens
aliastokensre = re.compile(r'[._-]?([A-Z]*[a-z]+)')

class StorageInterface:

    def __init__(self, host='localhost', port=27017):
        self.client = pymongo.MongoClient(host, port)
        self.db = self.client.explorer_database

    def init_indexes(self):
        """
        Only needs to be called once on startup
        """

        # For holding select data from the graph, 1 for ascending sort
        self.db.nodedata.create_index([('pub_key', 1)],
                                      unique=True)
        self.db.nodedata.create_index([('alias', 1)],
                                      unique=False)
        self.db.channeldata.create_index([('channel_id', 1)],
                                      unique=True)
        self.db.channeldata.create_index([('node1_pub', 1),
                                          ('node2_pub', 1)],
                                      unique=False)

        # Drop and create, because the text indexes get renamed often
        self.db.nodesearchindex.drop_indexes()
        self.db.nodesearchindex.create_index([('pub_key', 1)], unique=True)
        self.db.nodesearchindex.create_index([('pub_key', 'text'),
                                              ('alias', 'text'),
                                              ('alias_ascii', 'text'),
                                              ('alias_tokens', 'text'),
                                              ],
                                             unique=False)

    def writenodeinfo(self, nodeinfo):
        pubkey = nodeinfo['pub_key']

        # update actually replaces all the node data since it's in a dict
        # but we need to keep the report and settings data

        self.db.nodedata.update_one({'pub_key': pubkey},
                                    {'$set':{'nodeinfo':nodeinfo}},
                                    upsert=True)

        nodeindex = {
                      'pub_key':nodeinfo['pub_key'],
                      'alias':nodeinfo['alias'],
                      # Emojis impede mongo's searching
                      'alias_ascii': unidecode(nodeinfo['alias']),
                      }

        # Split the ascii alias into tokens
        alias_split = aliastokensre.split(nodeindex['alias_ascii'])

        # Above uses the ascii name to handle accents
        # The regex actually does pretty well with emojis

        # Split leaves behind some empty strings, causing extra joins
        alias_tokens = ' '.join(alias_split).replace('  ', ' ').strip()

        nodeindex['alias_tokens'] = alias_tokens


        # update the search index
        self.db.nodesearchindex.update_one({'pub_key': pubkey},
                                    {'$set':nodeindex},
                                    upsert=True)

    def writenoderecords(self, node_records):
        bulk_operations = [
            UpdateOne({'pub_key': pubkey},
                              {'$set': {'nodeinfo': nodedata['nodeinfo']}
                              },
                              upsert=True)
            for pubkey, nodedata in node_records.items()
        ]
        # Need to handle settings specially to prevent clobber
        bulk_operations.extend([
            UpdateOne({'pub_key': pubkey},
                              {'$set': {f'nodesettings.{s}': sv
                                for s, sv in nodedata['nodesettings'].items()}
                              },
                              upsert=True)
            for pubkey, nodedata in node_records.items()
        ])

        r1 = self.db.nodedata.bulk_write(bulk_operations, ordered=False)

        nodeindex = [{
                      'pub_key': pubkey,
                      'alias': nodedata['nodeinfo']['alias'],
                      # Emojis impede mongo's searching
                      'alias_ascii': unidecode(nodedata['nodeinfo']['alias']),
                      } for pubkey, nodedata in node_records.items()]

        # Split the ascii alias into tokens
        for node in nodeindex:
            alias_split = aliastokensre.split(node['alias_ascii'])
            # Above uses the ascii name to handle accents
            # The regex actually does pretty well with emojis

            # Split leaves behind some empty strings, causing extra joins
            alias_tokens = ' '.join(alias_split).replace('  ', ' ').strip()

            node['alias_tokens'] = alias_tokens

        bulk_operations = [
            UpdateOne({'pub_key': nodeidx['pub_key']},
                              {'$set': nodeidx},
                              upsert=True)
            for nodeidx in nodeindex
        ]

        # update the search index
        r2 = self.db.nodesearchindex.bulk_write(bulk_operations, ordered=False)

        return r1

    def readnodeinfo(self, pub_key):
        return self.db.nodedata.find_one({'pub_key': pub_key})['nodeinfo']

    def writechanneldata(self, channeldata):
        chan_id = channeldata['channel_id']
        self.db.channeldata.update_one({'channel_id': chan_id},
                                    {'$set': channeldata},
                                    upsert=True)

    def writechannelrecords(self, channel_records):
        self.db.channeldata.delete_many({})
        bulk_operations = [
            UpdateOne({'channel_id': chan_id},
                              {'$set': chan_data},
                              upsert=True)
            for chan_id, chan_data in channel_records.items()
        ]
        r = self.db.channeldata.bulk_write(bulk_operations, ordered=False)
        return r

    def readchanneldatabyid(self, channel_id):
        return self.db.channeldata.find_one({'channel_id': channel_id})

    def readchannelsbykey(self, pub_key):
        q = {'$or': [
                {'node1_pub': pub_key},
                {'node2_pub': pub_key}
                ]
             }

        cursor = self.db.channeldata.find(q)
        # Unlikely to have a situation where cursor is preferred
        return list(cursor)

    def writereport(self, pub_key, reporttype, status, results={}):
        # TODO test
        k = 'nodereports.' + reporttype

        self.db.nodedata.update_one(
            {'pub_key': pub_key},
            {'$set':{k:{'status':status, 'results':results,
                        'last_update':time.time()}}}, upsert=True)

    def readreport(self, pub_key, reporttype):
        r = self.db.nodedata.find_one({'pub_key': pub_key}
                            )['nodereports'][reporttype]
        return r

    def setsetting(self, pubkey, setting, value):
        k = 'nodesettings.' + setting

        self.db.nodedata.update_one({'pub_key': pubkey},
            {'$set':{k: value}}, upsert=True)

    def readsetting(self, pubkey, setting):

        r = self.db.nodedata.find_one({'pub_key': pubkey})

        return r['nodesettings'][setting]

    def countactivepreviews(self):
        q = {'nodesettings.userrequests.channelreportpreview.requested': True}
        r = self.db.nodedata.count_documents(q)
        return r

    def searchnodes(self, searchtext, limit=10):
        results = self.db.nodesearchindex.find({"$text": {"$search": searchtext}}, limit=limit)

        return results

    def deleteoldpreviews(self, cutoff):
        # where lt cutoff and results available, might need a join
        query = {'nodesettings.userrequests.channelreportpreview.timestamp':{'$lt':cutoff}}

        cursor = self.db.nodedata.find(query)

        # Set results to expired and [],

        for result in cursor:
            k = result['pub_key']
            self.writereport(k, 'previewsuggestions', 'expired', {'suggestions':[]})

        # TODO: the above and below db txns could probably be combined

        # Also set requested to false
        r = self.db.nodedata.update_many(query,
                   {'$set':{'nodesettings.userrequests.channelreportpreview.requested': False
                }})

        return r.modified_count

    def writeblob(self, key, blob):
        r = self.db.blobs.replace_one({'blob_key':key},
                                      {'blob_key':key, 'blob': blob},
                                      upsert=True)
        return r

    def readblob(self, key):
        r = self.db.blobs.find_one({'blob_key':key})
        if r is None: return None
        return r['blob']

    def addjob(self):
        # TODO write new job API
        pass
