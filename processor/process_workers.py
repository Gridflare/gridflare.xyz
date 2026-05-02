from multiprocessing import Process, Queue, Lock, Event
from threading import Thread
from queue import Empty
import logging
import logging.handlers
import time
import configparser
import pickle
import os

#TODO: logging to a single file from multiple processes is unsafe

log = logging.getLogger('processor')
log.setLevel('INFO')
h = logging.handlers.RotatingFileHandler('processor.log', maxBytes=1e7, backupCount=4)
f = logging.Formatter('%(asctime)s %(processName)-11s %(name)s %(levelname)s %(message)s')
h.setFormatter(f)
log.addHandler(h)
log = logging.getLogger('processor.work')

import storagemanager
import loadgraph
import reports
from nodeinterface import NodeInterface

previewrequesttimeout = 3*24*60*60
mainrefreshinterval = 1*60*60
graphrefreshinterval = 0.8*60*60
previewrefreshinterval = 22*60*60
addpeerinterval = 0.8*60*60

nodeparametersofinterest = ['pub_key', 'alias','capacity',
                            'num_channels', 'color', 'last_update',
                            'addresses']

chanparametersofinterest = ["channel_id", "chan_point", "last_update",
                            "node1_pub", "node2_pub", "capacity",
                            "node1_policy", "node2_policy"]


config = configparser.ConfigParser()
config.read('processor/node.conf')
config = config['Node1']

# Extract bools and convert from strings
forcegraphrefresh = int(config['refreshgraphonstart'])
use_include_unannounced_workaround = int(config['use_include_unannounced_workaround'])
hide_listener_node = int(config['hide_listener_node'])
randomgossipconnections = int(config['randomgossipconnections'])

jobqueues = {n:Queue() for n in
            ['graphrefresh', 'suggestionpreview', 'suggestionreport']}

# Ensure graph is not read while being written
graphrwlock = Lock()
graphrefreshedflag = Event()
graphfile = 'data/graph.pkl'

def getnow(q):
    """
    Fetches from queue, returning None if empty.
    """
    try:
        return q.get_nowait()
    except Empty:
        return None

def getgraph(checktimeout=False):
    """Retrieve graph from disk"""
    with graphrwlock:
        with open(graphfile, 'rb') as f:
            gdata = pickle.load(f)

    if checktimeout and time.time() - gdata['timestamp'] > graphrefreshinterval:
        raise ValueError('Graph is expired')

    return gdata['graph']

def writegraph(newgraph):
    with graphrwlock:
        with open(graphfile, 'wb') as f:
            gdata = {'graph': newgraph,
                     'timestamp':time.time()}
            pickle.dump(gdata, f)

def getnodeinterface():
    return NodeInterface(server=config['server'],
               tlspath=config['tlspath'],
               macpath=config['macpath'])

def getgraphlnd():
    """Retrieve graph from lnd"""
    ni = getnodeinterface()
    log.info('Fetching graph from lnd...')
    t = time.time()
    g = loadgraph.lnGraph.fromlnd(
            lndnode=ni,
            include_unannounced=use_include_unannounced_workaround
            )
    log.info(f'Fetched graph from lnd in {time.time()-t:.1f}s')
    return g

def connectrandomnode(storage):
    """
    Connect to a random clearnet node for gossip
    Targets medium sized nodes that have not been recently updated
    """

    too_recent_threshold = time.time() - 2 * 60 * 60

    # Filtering by last channel update will weed out large nodes

    pipeline = [{'$match': {'nodeinfo.num_channels': {
                                     '$gt':10,
                                     # ~ '$lt': 500
                                     },
                            'nodeinfo.last_update': {
                                '$lt': too_recent_threshold},
                            'nodeinfo.last_channel_update': {
                                '$lt': too_recent_threshold},
                             }
                 },
                {'$unwind': "$nodeinfo.addresses"},
                {'$sample': {'size':16}},
                {'$sort': {'nodeinfo.last_channel_update': 1}},
                ]

    log.info('Searching for potential gossip peers')
    aggregation = storage.db.nodedata.aggregate(pipeline)

    ni = getnodeinterface()
    for candidate in aggregation:
        nodeinfo = candidate['nodeinfo']
        pubkey = nodeinfo['pub_key']
        addr = nodeinfo['addresses']['addr']
        last_seen = time.ctime(nodeinfo['last_channel_update'])

        log.info(f'Selected {pubkey}@{addr} Last seen {last_seen}')
        try:
            response = ni.addPeer(pubkey, addr, timeout=1)
            log.info(f'Connection attempt completed {response}')
            break

        except Exception as e:
            log.error(f'Error connecting to gossip candidate: {e.details()}')
    else:
        log.warning('Failed to connect to any gossip peer')

def refreshgraph(storage, jobname, force):
    assert jobname == 'graphrefresh'
    log.info('Starting graph refresh job')

    newgraph = None
    if not os.path.isfile(graphfile):
        log.info(f'{graphfile} does not exist, will create')
        force = True
    elif storage.db.nodedata.count_documents({}) == 0:
        log.info('Graph was found but database is unpopulated')
        force = True

    if force:
        newgraph = getgraphlnd()
    else:
        try:
            getgraph(checktimeout=True)
            log.info('Graph has not expired and will not be refreshed')
        except ValueError:
            newgraph = getgraphlnd()

    # Can safely short circuit now
    if not newgraph:
        return True

    # Calculate node farness and betweenness
    # and save into the graph
    log.info('Running centrality measures')
    newgraph = reports.recordcentralities(newgraph)

    # Cannot write graph file to database, it is too large
    log.info('Writing fresh graph to disk')
    writegraph(newgraph)

    graphstats = {'num_nodes': len(newgraph.nodes),
                  'num_chans': len(newgraph.edges),
                  'last_update': max(filter( # Filter in case LND gives a funky time value
                                     lambda t: t < time.time()*1.01,
                                     (ed[2]['last_update']
                                     for ed in newgraph.edges(data=True)))),
                  }
    storage.writeblob('graphstats', graphstats)

    # Refresh the node data
    log.info('Collecting node records')
    alias_cache = {} # To record aliases in channel data
    node_records = {}
    for node in newgraph.nodes.values():
        alias_cache[node['pub_key']] = node['alias']
        nodedict = {}
        for k in nodeparametersofinterest:
            try:
                nodedict[k] = node[k]
            except KeyError:
                log.exception(f'Node was missing a key {node}')

        # Record a node's report eligibility
        pub_key = node['pub_key']
        eligible = reports.nodeiseligible(pub_key, newgraph)
        nodesettings = {'eligibility': {
                        'suggestionpreview': eligible,
                        'suggestionreport': False}}

        node_records[pub_key] = {'nodeinfo': nodedict,
                                 'nodesettings': nodesettings}

    # Refresh the channel data
    def recordlatestchanupdate(node_pub, last_channel_update):
        l = node_records[node_pub].get('last_channel_update', 0)
        if last_channel_update > l:
            node_records[node_pub]['nodeinfo']['last_channel_update'] = last_channel_update

    log.info('Collecting channel records')
    channel_records = {}
    for chan in newgraph.edges.values():
        chandict = {}
        for k in chanparametersofinterest:
            try:
                chandict[k] = chan[k]
            except KeyError:
                log.exception(f'Channel was missing a key {chan}')

        chandict['node1_alias'] = alias_cache[chandict['node1_pub']]
        chandict['node2_alias'] = alias_cache[chandict['node2_pub']]

        recordlatestchanupdate(chandict['node1_pub'], chandict['last_update'])
        recordlatestchanupdate(chandict['node2_pub'], chandict['last_update'])

        channel_records[chan['channel_id']] = chandict

        #TODO: also record channel centralities

    # Try to work around LND missing some channel data after running under neutrino
    # If graph was fetched with include_unannounced, need to prune private channels from the graph
    if use_include_unannounced_workaround:
        ni = getnodeinterface()
        private_channels = ni.ListChannels(private_only=True).channels

        node_records[config['listener_node_pub']]['nodeinfo']['num_channels'] -= len(private_channels)

        for chan in private_channels:
            del channel_records[chan.chan_id]
            node_records[config['listener_node_pub']]['nodeinfo']['capacity'] -= chan.capacity

    log.info('Updating node records')
    storage.writenoderecords(node_records)

    log.info('Updating channel records')
    storage.writechannelrecords(channel_records)

    if hide_listener_node:
        # Don't want this node to be found on the site
        storage.db.nodedata.delete_one({'pub_key': config['listener_node_pub']})
        storage.db.nodesearchindex.delete_one({'pub_key': config['listener_node_pub']})

    log.info('Updated records')

    # Channel suggestion refreshes are not handled here
    # They are the responsibility of the refresh thread

    return True

def suggestionpreview(storage, jobname, pub_key):
    assert jobname == 'suggestionpreview'
    log.info('Starting suggestion preview job')

    # Assume node is already eligible at this point, now is rather late to check
    graph = getgraph()
    config = configparser.ConfigParser()
    config.read('processor/improvecentrality-preview.conf')

    try:
        peersuggestions = reports.suggestchannelspreview(pub_key, graph, config)
        status = 'available'
    except Exception as e:
        log.exception('failed to generate report')
        peersuggestions = []

    if len(peersuggestions) == 0:
        status = 'error'

    storage.writereport(pub_key, 'previewsuggestions', status,
                        results={'suggestions':peersuggestions,
                                 'timestamp':time.time()})

    log.info('Completed suggestion preview job')

    return True

def logqsizes():
    qsizes = {k:q.qsize() for k, q in jobqueues.items()}
    if any(qsizes.values()):
        s  = ' '.join([str(t) for t in qsizes.items()])
        log.info('Current queue sizes ' + s)

def worker():
    """
    Worker mainloop, grabs jobs from queue and executes.
    """
    storage = storagemanager.StorageInterface('mongo', 27017)

    # Start mainloop
    while True:
        try:
            logqsizes()

            # Check for graph refresh job, there should be one sent during startup
            if (grjob := getnow(jobqueues['graphrefresh'])):
                try:
                    success = refreshgraph(storage, **grjob)
                except Exception as e:
                    log.exception('Error refreshing graph')

                # Unblock mainloop regardless of success
                graphrefreshedflag.set()

            # Check for channel suggestion preview job
            elif (grjob := getnow(jobqueues['suggestionpreview'])):
                success = suggestionpreview(storage, **grjob)

            # Check for report job
            elif (grjob := getnow(jobqueues['suggestionreport'])):
                pass

            # Else, sleep for a few seconds
            else:
                time.sleep(2)

        except Exception as e:
            log.exception('Error during processing')

def peridodicrefresh():
    """
    Background mainloop.
    Performs setup and periodically adds jobs to queues.
    """
    storage = storagemanager.StorageInterface('mongo', 27017)
    storage.init_indexes()

    # Need to clear pending status during startup
    stuckpending = storage.db.nodedata.find(
                    {'nodereports.previewsuggestions.status': {
                        '$in': ['pending', 'error']}},
                    projection=['pub_key'])
    stuckpending = list(stuckpending)
    if len(stuckpending) > 0:
        log.warning(f'Found {len(stuckpending)} pending or errored reports')
        for node in stuckpending:
            storage.writereport(node['pub_key'], 'previewsuggestions',
                                    'interrupted', {'suggestions':[]})
        log.info('Stuck reports reset')

    lastgraphpulltime = 0
    lastpeeraddtime = 0

    while True:
        loopstarttime = time.time()

        if loopstarttime - lastgraphpulltime >= graphrefreshinterval:

            # Submit graph refresh job to queue
            graphrefreshedflag.clear()
            jobqueues['graphrefresh'].put({'jobname':'graphrefresh',
                                           'force': forcegraphrefresh})

            # Wait for graph refresh to complete
            graphrefreshedflag.wait()
            lastgraphpulltime = time.time()

        # For every node in nodereports
        # Check if they have a recent channel suggestion preview request
        cutoff = loopstarttime - previewrequesttimeout
        minage = loopstarttime - 3*60*60 # don't regen if fresh, no longer needed?

        n = storage.deleteoldpreviews(cutoff)
        log.info(f'Expired {n} report requests')

        n = storage.countactivepreviews()
        log.info(f'There are {n} active report requests')

        # Get all active preview requests
        cursor = storage.db.nodedata.find({
            'nodesettings.userrequests.channelreportpreview.requested': True,
            'nodesettings.userrequests.channelreportpreview.timestamp':{
                '$gt':cutoff, '$lt':minage},
            })

        # Check which requests are too old
        rerun_keys = []
        reruncutoff = loopstarttime - previewrefreshinterval
        for r in cursor:
            pub_key = r['pub_key']
            report = storage.readreport(pub_key, 'previewsuggestions')
            timestamp = report['last_update']
            if timestamp < reruncutoff:
                rerun_keys.append(pub_key)

        # TODO: combine the above queries

        log.info(f'Found {len(rerun_keys)} reports for reprocessing')

        # Submit channel suggestion preview job to queue
        # Sleep several seconds before the next iteration to avoid flooding the queue
        for k in rerun_keys:
            jobqueues['suggestionpreview'].put({'jobname':'suggestionpreview',
                                                          'pub_key': k})
            log.info(f'Submitted {k} for preview reprocessing')
            time.sleep(40)

        # Randomly add peers to improve network knowledge
        if randomgossipconnections and loopstarttime - lastpeeraddtime >= addpeerinterval:
            connectrandomnode(storage)
            lastpeeraddtime = loopstarttime

        # Use runtime and desired interval time to calculate a new sleep time
        runtime = time.time() - loopstarttime
        sleeptime = mainrefreshinterval - runtime
        log.info(f'All periodic tasks complete, sleeping for {sleeptime:.1f}s')
        sleeptime = max(0, sleeptime) # Negative values could be dangerous
        time.sleep(sleeptime)


def start(n_processes=1):
    log.info('=========== START ===========')
    log.info('Starting workers')
    for i in range(n_processes):
        Process(target=worker).start()

    Thread(target=peridodicrefresh).start()
