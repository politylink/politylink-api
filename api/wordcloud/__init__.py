import orjson
from flask import request
from flask_cors import cross_origin

from api import app
from api.wordcloud.search import search_speech
from api.wordcloud.tfidf import calc_tfidfs, load_minutes_to_term_stats


@app.route('/tf_idf', methods=['POST'])
@cross_origin()
def tfidf_api():
    kwargs = {
        # query target
        'start_date_str': request.json.get('start'),
        'end_date_str': request.json.get('end'),
        'committee': request.json.get('committee'),
        'diet_number': request.json.get('diet'),
        # tfidf param
        'interval': int(request.json.get('interval', 0)),
        'num_items': int(request.json.get('items', 200)),
    }
    tfidfs = calc_tfidfs(**kwargs)
    return orjson.dumps(tfidfs)


@app.route('/search', methods=['POST'])
@cross_origin()
def search_api():
    kwargs = {
        # query target
        'term': request.json.get('term'),
        'start_date_str': request.json.get('start'),
        'end_date_str': request.json.get('end'),
        'committee': request.json.get('committee'),
        # response param
        'num_items': int(request.json.get('items', 3)),
        'fragment_size': int(request.json.get('fragment', 100))
    }
    snippets = search_speech(**kwargs)
    return orjson.dumps(snippets)


@app.route('/load', methods=['POST'])
@cross_origin()
def load_api():
    success = load_minutes_to_term_stats(request.json.get('file'))
    return orjson.dumps(success)
