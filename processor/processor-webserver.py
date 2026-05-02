import time

from quart import Quart, jsonify
from motor.motor_asyncio import AsyncIOMotorClient
from google.protobuf.json_format import MessageToDict

import process_workers
import reports

process_workers.start(n_processes=1)

app = Quart(__name__)

def getdb():
    mongoclient = AsyncIOMotorClient('mongo', 27017)
    db = mongoclient.explorer_database
    return db

@app.route('/', methods=['GET'])
async def root():
    return jsonify({'message':'You have reached the processor'})

@app.route('/attemptmongo', methods=['GET'])
async def attemptmongo():
    try:
        mongoclient = AsyncIOMotorClient('mongo', 27017)
        await mongoclient.admin.command('ping')
    except Exception as e:
        log.exception('Mongo connection failed')
        return jsonify({'message':'Mongo connection failed'})

    return jsonify({'message':'Mongo connected'})

@app.route('/lndinfo', methods=['GET'])
async def lndinfo():
    ni = process_workers.getnodeinterface()
    info = MessageToDict(
        ni.GetInfo(),
        preserving_proto_field_name=True,
    )
    return jsonify(info)

@app.route('/report/suggestchannelspreview/<string:pub_key>', methods=['POST'])
async def request_channel_suggestions(pub_key):

    assert len(pub_key) == 66

    db = getdb()

    await db.nodedata.update_one(
            {'pub_key': pub_key},
            {'$set':{'nodesettings.userrequests': {
                            'channelreportpreview': {
                                                'requested':True,
                                                'timestamp':time.time(),
                                                  }},
                    'nodereports.previewsuggestions':
                        {'status':'pending', 'results':{'suggestions':[]}},
                    }
            }, upsert=True)

    process_workers.jobqueues['suggestionpreview'].put({'jobname':'suggestionpreview',
                                                        'pub_key': pub_key})

    return 'Job queued', 202

