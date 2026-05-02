
import random
import time
import logging
from os import environ

import numpy as np
import networkx as nx
import requests

from processor import fastcentrality


log = logging.getLogger('processor.reports')
log.setLevel('INFO')

def check_graph_recent(g, hours=12):
    t= time.time()
    mostrecentupdate = 0
    for e in g.edges.values():
        if e['last_update'] > mostrecentupdate:
            mostrecentupdate = e['last_update']

    return t - mostrecentupdate > hours*60*60

def nodeiseligible(pubkey, graph):
    nodedata = graph.nodes.get(pubkey)
    if nodedata is None: return False

    cond = (
            graph.degree(pubkey) >= 3,
            sum((c >= 5e5 for c in nodedata['capacities'])) > 2,
            )
    log.debug(f'Eligibility for {pubkey} {cond}')
    return all(cond)

def countdisabledchannels(candidatekey, graph):
    # Report reliability-related data

    node = graph.nodes[candidatekey]

    chankeypairs = graph.edges(candidatekey)
    disabledcount = {'sending':0, 'receiving':0}

    for chankeypair in chankeypairs:
        chan = graph.edges[chankeypair]

        if None in [chan['node1_policy'], chan['node2_policy']]:
            continue # TODO: Might want to add a penalty to this

        if candidatekey == chan['node1_pub']:
            if chan['node1_policy']['disabled']:
                disabledcount['sending'] += 1
            if chan['node2_policy']['disabled']:
                disabledcount['receiving'] += 1
        elif candidatekey == chan['node2_pub']:
            if chan['node2_policy']['disabled']:
                disabledcount['sending'] += 1
            if chan['node1_policy']['disabled']:
                disabledcount['receiving'] += 1
        else:
            assert False

    return disabledcount

blockheightcache = {'timestamp':0,'height':0}
def getblockheight():

    if time.time() - 10*60*60 > blockheightcache['timestamp']:
        # probably faster than asking LND
        currblockheight = requests.get(
                        "http://processor:5000/lndinfo"
                        ).json()['block_height']
        blockheightcache['height'] = currblockheight
        blockheightcache['timestamp'] = time.time()

    return blockheightcache['height']

def calcavgchanage(candidatekey, graph):

    currblockheight = getblockheight()

    ages = []
    for chankeypair in graph.edges(candidatekey):
        chan = graph.edges[chankeypair]
        chanblk = chan['channel_id'] >> 40
        ageblks = currblockheight - chanblk
        ages.append(ageblks)

    if len(ages) == 0:
        return -1

    return np.mean(ages)

def nodeisgoodcandidate(n, graph, filters, debug=False):
    nkey = n['pub_key']

    # Conditions a node must meet to be considered for further connection analysis
    minchancount = filters.getint('minchancount')
    mincapacity = int(filters.getfloat('mincapacitybtc')*1e8)
    minavgchan = filters.getint('minavgchan')
    minmedchan = filters.getint('minmedchan')
    minreliability = filters.getfloat('minreliability')
    minavgchanageblks = filters.getint('minavgchanageblks')

    reliability = 1 - countdisabledchannels(nkey, graph)['receiving'] / graph.degree(nkey)

    cond = (len(n['addresses']) > 0, # Node must be connectable
            minchancount <= n['num_channels'],
            mincapacity <= n['capacity'],
            n['capacity']/n['num_channels'] > minavgchan, # avg chan size
            np.median(n['capacities']) >= minmedchan,
            reliability >= minreliability,
            sum((c >= 2e6 for c in n['capacities'])) > 4, # At least some sizeable channels
            sum((c >= 4e6 for c in n['capacities'])) > 2, # At least some sizeable channels
            calcavgchanage(nkey, graph) >= minavgchanageblks,
            )

    if debug:
        return cond
    else:
        return all(cond)

def selectinitialcandidates(mynodekey, graph, filters):
    # If already connected or is ourself, drop from list
    f1 = [n for n in graph.nodes.values()
             if all((not graph.has_edge(mynodekey, n['pub_key']),
                     mynodekey != n['pub_key'],
                     graph.degree(n['pub_key']) > 0))]

    f2 = [n['pub_key'] for n in
            filter(lambda n: nodeisgoodcandidate(n, graph, filters), f1)
           ]

    # Further refine candidates using the static half of the score
    staticattractivenessscores = []
    for c in f2:
        staticattractivenessscores.append(
            calculatestaticattractivenessscore(c, graph))

    minstaticscore = np.percentile(staticattractivenessscores, 50)

    f3 = [c for c in f2
          if graph.nodes[c]['staticscore'] >= minstaticscore]

    return f2

def filtergraph(graph, graphfilters):
    minrelevantchan = graphfilters.getint('minrelevantchan')

    t = time.time()
    def filter_node(nk):
        n = graph.nodes[nk]

        cond = (
                # A node that hasn't updated in this time might be dead
                # ~ t - n['last_update'] < 7 *24*60*60 ,
                # Demonstrably untrue, some of these "stale nodes"
                # have actually been requesting suggestions

                graph.degree(nk) > 0, # doesn't seem to fix the issue
            )

        return all(cond)

    def filter_edge(n1, n2):
        e = graph.edges[n1, n2]

        cond = (
                # A channel that hasn't updated in this time might be dead
                t - e['last_update'] <=  1.5 *24*60*60,

                # Remove economically irrelevant channels
                e['capacity'] >= minrelevantchan,
                )

        return all(cond)

    gfilt = nx.subgraph_view(graph, filter_node=filter_node, filter_edge=filter_edge)

    return gfilt

def recordcentralities(graph):
    """Calcuate farness and betweenness and save into the graph"""
    closenessscores = fastcentrality.closeness(graph)
    betweennessscores = fastcentrality.betweenness(graph)

    for nk in graph.nodes.keys():
        graph.nodes[nk]['closeness'] = closenessscores[nk]
        graph.nodes[nk]['farness'] = 1/closenessscores[nk]
        graph.nodes[nk]['betweenness'] = betweennessscores[nk]

    return graph


def calculatestaticattractivenessscore(peer2add, graph):
    """This is the rapid to compute half of the score
    It is not dependent on persepctive
    Record the value to the graph and returns
    """

    farness = graph.nodes[peer2add]['farness']
    betweenness = graph.nodes[peer2add]['betweenness']

    staticscore = farness/1000 + np.cbrt(betweenness/10000)

    graph.nodes[peer2add]['staticscore'] = staticscore

    return staticscore


def calculatedynamicattractivenessscore(peer2add, myfarness, graphcopy, mynodekey):
    """
    Slower component of the score
    Adds a perspective-dependent component to the existing static score
    """

    # Modify the graph with a simulated channel
    graphcopy.add_edge(peer2add, mynodekey)

    mynewfarness = 1/fastcentrality.closeness(graphcopy, mynodekey)

    myfarnessdelta = mynewfarness - myfarness

    # Since this function is batched, and making a fresh copy is slow,
    # Make sure all changes are undone
    graphcopy.remove_edge(peer2add, mynodekey)


    staticscore = graphcopy.nodes[peer2add]['staticscore']

    # This is where the magic happens
    # Nodes that reduce our farness,
    # as well as nodes with a high farness,
    # are prioritized. But especially nodes that have both.
    farnessscore = np.sqrt(abs(myfarnessdelta)) + staticscore

    return farnessscore

def calculatebetweennessdelta(peer2add, graphcopy, mynodekey, mybetweenness):
    """Calculate how much a new channel can improve betweenness"""

    # Modify the graph with a simulated channel
    graphcopy.add_edge(peer2add, mynodekey)

    mynewbetweenness = fastcentrality.betweenness(graphcopy, mynodekey)

    mybetweennnessdelta = mynewbetweenness - mybetweenness

    # Since this function is batched, and making a fresh copy is slow,
    # Make sure all changes are undone
    graphcopy.remove_edge(peer2add, mynodekey)

    return mybetweennnessdelta


def suggestchannelspreview(pubkey, graph, config):
    if not nodeiseligible(pubkey, graph):
        log.warning(f'Node was not eligible {pubkey}')
        return []

    log.info(f'Starting channel suggestion preview for {pubkey}')

    # Set config
    finalsuggestioncount = 4
    toppoolsize = 16
    preselectionsize = 300
    random.seed(int(time.time()/(24*3600)))

    nodefilters = config['CandidateFilters']
    graphfilters = config['GraphFilters']

    # Get simplified graph
    g2 = filtergraph(graph, graphfilters)
    if len(g2.nodes) == 0:
        log.error('No relevant nodes in graph')
        return []

    # Select candidates
    t = time.time()
    candidates = selectinitialcandidates(pubkey, g2, nodefilters)
    if len(candidates) == 0:
        log.error('No candidate nodes found')
        return []

    log.info(f'Found {len(candidates)} suggestion candiates')

    # Shuffle candiates to remove any sorting residue
    random.shuffle(candidates)

    log.info(f'Found initial candidates in {time.time()-t:.1f}s')

    t = time.time()

    farnesscores = {}

    myfarness = 1/fastcentrality.closeness(g2, pubkey)
    gcopy = g2.copy()

    # Prevent error in simplified tests
    preselectionsize = min(preselectionsize, len(candidates))

    while len(farnesscores) < preselectionsize:
        c = candidates.pop()
        mfscore = calculatedynamicattractivenessscore(c, myfarness, gcopy, pubkey)
        farnesscores[c] = mfscore

    log.info(f'Found mf scores in {time.time()-t:.1f}s')

    topcandidates = [i[0] for i in
                     sorted(farnesscores.items(),
                     key=lambda n: -n[1])][:toppoolsize]

    # To ensure the results have variety
    # We'll find 2 large and 2 small nodes from the set
    # On half set measured by channel count, the other by capacity

    random.shuffle(topcandidates)

    # This happens in tests, hopefully not in reality
    if len(topcandidates) <= 4:
        return topcandidates

    l2 = len(topcandidates)//2
    chanhalf = topcandidates[l2:]
    caphalf = topcandidates[:l2]

    chanhalf.sort(key=lambda k: g2.nodes[k]['num_channels'])
    caphalf.sort(key=lambda k: g2.nodes[k]['capacity'])

    l2 = len(caphalf)//2
    smallcap = caphalf[l2:]
    tallcap = caphalf[:l2]

    l2 = len(chanhalf)//2
    fewchans = chanhalf[l2:]
    manychans = chanhalf[:l2]


    recs = [random.choice(smallcap),
            random.choice(tallcap),
            random.choice(fewchans),
            random.choice(manychans),
            ]

    # Don't hand the results out in a predictable order
    random.shuffle(recs)

    return recs


def suggestchannelsreport(pubkey, graph, config):
    if not nodeiseligible(pubkey, graph):
        log.warning(f'Node was not eligible {pubkey}')
        return False

    log.info(f'Starting channel suggestion report for {pubkey}')

    numcandidates = 20
    preselectionsize = 400 # This should be big
    # TODO I'd rather not use randomness in this function
    random.seed(int(time.time()/(24*3600)))

    nodefilters = config['CandidateFilters']
    graphfilters = config['GraphFilters']

    # Get simplified graph
    g2 = filtergraph(graph, graphfilters)
    if len(g2.nodes) == 0:
        log.error('No relevant nodes in graph')
        return []

    # Select candidates
    t = time.time()
    candidates = selectinitialcandidates(pubkey, g2, nodefilters)
    if len(candidates) == 0:
        log.error('No candidate nodes found')
        return []

    log.info(f'Found {len(candidates)} suggestion candiates')


    # Shuffle candiates to remove any sorting residue
    random.shuffle(candidates)

    log.info(f'Found initial candidates in {time.time()-t:.1f}s')

    t = time.time()

    farnesscores = {}

    myfarness = 1/fastcentrality.closeness(g2, pubkey)
    gcopy = g2.copy()

    # Prevent error in simplified tests
    preselectionsize = min(preselectionsize, len(candidates))

    while len(farnesscores) < preselectionsize:
        c = candidates.pop()
        mfscore = calculatedynamicattractivenessscore(c, myfarness, gcopy, pubkey)
        farnesscores[c] = mfscore

    log.info(f'Found mf scores in {time.time()-t:.1f}s')

    topcandidates = [i[0] for i in
                     sorted(farnesscores.items(),
                     key=lambda n: -n[1])][:numcandidates]

    betweennessdeltas = {}
    mybetweenness = fastcentrality.betweenness(g2, pubkey)
    for k in topcandidates:
        bcdelta = calculatebetweennessdelta(k, gcopy, pubkey, mybetweenness)
        betweennessdeltas[k] = bcdelta

    return betweennessdeltas


