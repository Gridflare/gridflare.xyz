"""
Contains all database queries the frontend makes
Seperates parameter parsing and data retrieval,
should improve readability

"""


from motor.motor_asyncio import AsyncIOMotorClient

def getdb():
    mongoclient = AsyncIOMotorClient('mongo', 27017)
    db = mongoclient.explorer_database
    return db

async def getgraphstats():
    db = getdb()
    r = await db.blobs.find_one({'blob_key':'graphstats'})
    return r['blob']

async def searchnode(q: int, count:int=100, skip:int=0):
    db = getdb()
    cursor = db.nodesearchindex.find({"$text": {"$search": q}},
                 projection={'alias':True, 'pub_key':True,
                             'score': {'$meta': 'textScore'}})

    cursor.sort([('score', {'$meta': 'textScore'})])
    if skip > 0: cursor.skip(skip)

    results = await cursor.to_list(length=count)

    return results

async def getnodedata(pub_key):
    db = getdb()
    data = await db.nodedata.find_one({'pub_key': pub_key})
    return data

async def getnodeinfo(pub_key):
    db = getdb()
    data = await db.nodedata.find_one({'pub_key': pub_key},
                                      projection=['nodeinfo'])
    return data['nodeinfo']

async def getrandomnode():
    db = getdb()
    async for nodedata in db.nodedata.aggregate([{'$sample': {'size':1}}]):
        break

    return nodedata

async def getallpubkeys(limit=50_000):
    db = getdb()
    cursor = db.nodedata.find({}, projection={'pub_key': True})

    nodelist = await cursor.to_list(length=50000)

    return nodelist

async def getnodechannels(pub_key):
    db = getdb()
    q = {'$or': [
            {'node1_pub': pub_key},
            {'node2_pub': pub_key}
        ]
    }

    cursor = db.channeldata.find(q)
    cursor.sort([('capacity', -1), ('channel_id', -1)])

    chanlist = await cursor.to_list(length=50000)

    return chanlist

async def getchanneldata(channel_id):
    db = getdb()
    chandata = await db.channeldata.find_one({'channel_id': channel_id})

    del chandata['_id']

    return chandata




