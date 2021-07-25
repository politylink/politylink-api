import orjson
from flask import request
from flask_cors import cross_origin

from api import app
from api.member.search import search_members


@app.route('/members', methods=['GET'])
@cross_origin()
def get_members_api():
    kwargs = {
        # query param
        'query': request.args.get('q'),
        'groups': request.args.getlist('group', lambda x: int(x)),
        'houses': request.args.getlist('house', lambda x: int(x)),
        # response param
        'page': int(request.args.get('page', 1)),
        'num_items': int(request.args.get('items', 3)),
        'fragment_size': int(request.args.get('fragment', 100))
    }
    app.logger.info(f'search members: {kwargs}')
    response = search_members(**kwargs)
    return orjson.dumps(response)
