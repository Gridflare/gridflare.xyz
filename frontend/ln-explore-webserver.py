import time
from os import environ
from datetime import datetime
from datetime import timedelta

from quart import Quart, render_template, redirect, url_for, request, send_from_directory
import aiohttp

import queries

app = Quart(__name__)

# Autoreload templates without restarting app
if environ.get('QUART_ENV') == 'development':
    app.jinja_env.auto_reload = True
    app.config['TEMPLATES_AUTO_RELOAD'] = True

# ~ @app.route('/', methods=['GET'])
# ~ async def root():
    # ~ return redirect(url_for('explore_search_form'))


@app.route('/explore', methods=['GET'])
async def explore():
    return redirect(url_for('explore_search_form'))


@app.route('/', methods=['GET'])
@app.route('/explore/search', methods=['GET'])
async def explore_search_form():
    q = request.args.get('q')
    if q is None or len(q) == 0:
        graphstats = await queries.getgraphstats()

        return await render_template('explore-search-form.html',
                                    graphstats=graphstats)

    q = q.strip() # remove enclosing whitespace

    if len(q) == 66: # we have a pubkey
        return redirect(url_for('explore_node', pub_key=q))

    results_per_page = 16
    p = int(request.args.get('p', 1))
    s = (p -1) * results_per_page - 3

    results = await queries.searchnode(q, results_per_page, s)

    if len(results) == 1:
        return redirect(url_for('explore_node', pub_key=results[0]['pub_key']))

    return await render_template('explore-search-results.html',
                                 results=results,q=q, p=p)


@app.route('/explore/node/<string:pub_key>', methods=['GET'])
async def explore_node(pub_key):
    nodedata = await queries.getnodedata(pub_key)

    if nodedata is None:
        return await render_template('message.html',
                returnlink = f'/explore/search',
                message=('Pubkey not found: ' + pub_key
                         )), 404

    del nodedata['_id']
    channel_suggestions = {}
    nodesettings = nodedata.get('nodesettings')

    if nodesettings:
        qualify4suggestions = nodesettings['eligibility']['suggestionpreview']

        nodereports = nodedata.get('nodereports')
        if nodereports:
            channel_suggestions = nodereports['previewsuggestions']

            # Slightly safer to have a fallback
            if 'results' in channel_suggestions:
                suggestionreport = channel_suggestions['results']['suggestions']
            else:
                suggestionreport = []

            # Only the keys are stored, but want aliases too for rendering
            suggestiondata = []
            for k in suggestionreport:
                suggestiondata.append(await queries.getnodeinfo(k))

            channel_suggestions['suggestions'] = suggestiondata

    else:
        qualify4suggestions = False


    return await render_template('explore-node.html',
                                 nodeinfo=nodedata['nodeinfo'],
                                 channel_suggestions=channel_suggestions,
                                 qualify4suggestions=qualify4suggestions,
                                 )

@app.route('/explore/node/<string:pub_key>/channels', methods=['GET'])
async def explore_channels(pub_key):
    nodeinfo = await queries.getnodeinfo(pub_key)

    channels = await queries.getnodechannels(pub_key)

    return await render_template('explore-channels.html',
                             nodeinfo=nodeinfo,
                             channels=channels,
                             )

@app.route('/explore/randomnode', methods=['GET'])
async def explore_random_node():
    nodedata = await queries.getrandomnode()

    return redirect(url_for('explore_node',
                            pub_key=nodedata['pub_key']))

@app.route('/explore/channel/<int:chan_id>', methods=['GET'])
async def explore_channel_details(chan_id):
    channeldata = await queries.getchanneldata(chan_id)

    return await render_template('explore-channel-details.html',
                             channeldata=channeldata,
                             )




@app.route('/report/suggestchannelspreview/', methods=['GET', 'POST'])
async def request_channel_suggestions():
    # ~ j = await request.get_json()
    if not 'pub_key' in request.args:
        errmsg = f'Pub_key not specified in request {request.args}'
        return errmsg, 400, {'X-Status-Reason': errmsg}

    pub_key = request.args.get('pub_key')

    nodedata = await queries.getnodedata(pub_key)
    nodesettings = nodedata.get('nodesettings')

    if nodesettings is None or not nodesettings['eligibility']['suggestionpreview']:
        errmsg = f'Node {pub_key} is ineligible for suggestions'
        return errmsg, 403, {'X-Status-Reason': errmsg}

    # If already pending. don't error, redirect to self
    nodereports = nodedata.get('nodereports')
    if nodereports and 'previewsuggestions' in nodereports:
         if nodereports['previewsuggestions']['status'] in ['pending', 'available']:
            return redirect(f'/explore/node/{pub_key}')

    # Send HTTP request to processor to start processing
    async with aiohttp.ClientSession() as session:
        url = f'http://processor:5000/report/suggestchannelspreview/{pub_key}'
        async with session.post(url) as response:
            status = response.status

    if status != 202:
        errmsg = f'Received unexpected status from processor, please contact @gridflare'
        return errmsg, 500, {'X-Status-Reason': errmsg}

    return await render_template('message.html',
                    returnlink = f'/explore/node/{pub_key}',
                    message=('Your node will now receive daily channel suggestions for 3 days.\n'
                             'The first set should appear on your node\'s page in a few minutes.\n'
                             ))


@app.route('/reports', methods=['GET'])
async def reports():
    return await render_template('reports.html')

@app.route('/about', methods=['GET'])
async def about():
    return await render_template('about.html')

@app.route('/robots.txt')
async def static_from_root():
    return await send_from_directory(app.static_folder, request.path[1:])

@app.route('/sitemap.xml')
async def sitemapxml():
    nodelist = await queries.getallpubkeys(limit=50_000)

    return (await render_template('sitemap.xml', nodes=nodelist),
            200, {'Content-Type': 'application/xml'})


@app.template_filter('utctime')
def timectime(s):
    return time.asctime(time.gmtime(s))

@app.template_filter('dtime')
def timedtime(s):
    if s == 0:
        return ' - '
    d = int(time.time() - s) # Round to int
    p = str(timedelta(seconds=d)).split(':')
    return f'{p[0]}h {p[1]}m'

@app.template_filter('fmtsats')
def fmtsats(sats):
    sats_fmt = '{:,}'.format(int(sats))[-10:]
    btc = int(sats//1e8)
    if btc:
        btc_fmt = '{:,}'.format(btc)
        sats_fmt = btc_fmt + '.' + sats_fmt
    return sats_fmt

@app.context_processor
def inject_now():
    return {'now': datetime.utcnow()}
