import orjson
from flask import request
from flask_cors import cross_origin

from api import app
from api.bill.search import search_bills


@app.route('/bills', methods=['GET'])
@cross_origin()
def get_bills_api():
    kwargs = {
        # query param
        'query': request.args.get('q'),
        # response param
        'num_items': int(request.args.get('items', 3)),
        'fragment_size': int(request.args.get('fragment', 100))
    }
    app.logger.info(kwargs)
    bills = search_bills(**kwargs)
    return orjson.dumps(bills)
