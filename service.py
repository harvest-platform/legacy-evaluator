import json
from flask import Flask, Response, request
import methods


app = Flask(__name__)
host = None
port = None


@app.route('/', methods=['GET'])
def root():
    return '', 204


@app.route('/catalog', methods=['GET'])
def catalog():
    resp = Response(json.dumps(methods.catalog()))
    resp.headers['Content-Type'] = 'application/json'
    return resp


@app.route('/validate', methods=['POST'])
def validate():
    expr = request.get_json()
    error = methods.validate(expr)

    if error:
        return json.dumps({
            'error': error,
        }), 422

    return '', 204


@app.route('/plan', methods=['POST'])
def plan():
    expr = request.get_json()
    plan = methods.plan(expr)

    resp = Response(json.dumps(plan))
    resp.headers['Content-Type'] = 'application/json'

    return resp


@app.route('/idents', methods=['POST'])
def idents():
    expr = request.get_json()
    idents = methods.idents(expr)

    resp = Response(json.dumps(idents))
    resp.headers['Content-Type'] = 'application/json'

    return resp


@app.route('/count', methods=['POST'])
def count():
    expr = request.get_json()
    count = methods.count(expr)

    resp = Response(json.dumps({'count': count}))
    resp.headers['Content-Type'] = 'application/json'

    return resp


if __name__ == '__main__':
    host = '0.0.0.0'
    port = 8100
    debug = True

    app.run(host=host, port=port, threaded=True, debug=debug)
