import orjson
from flask import request
from flask_cors import cross_origin
from politylink.graphql.schema import BillCategory

from api import app
from api.bill.search import search_bills


@app.route('/bills', methods=['GET'])
@cross_origin()
def get_bills_api():
    kwargs = {
        # query param
        'query': request.args.get('q'),
        'categories': request.args.getlist('category', lambda x: BillCategory(x.upper())),
        'belonged_to_diets': request.args.getlist('diet', lambda x: int(x)),
        'submitted_diets': request.args.getlist('sdiet', lambda x: int(x)),
        # response param
        'page': int(request.args.get('page', 1)),
        'num_items': int(request.args.get('items', 3)),
        'fragment_size': int(request.args.get('fragment', 100))
    }
    app.logger.info(kwargs)
    response = search_bills(**kwargs)
    return orjson.dumps(response)
