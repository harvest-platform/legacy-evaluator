"""Harvest 2 Query Evaluator

Usage:
    server.py [--host=<host>] [--port=<port>] [--debug]

Options:
    --help          Show the help.
    --host=<host>   Bind host [default: 127.0.0.1].
    --port=<port>   Bind port [default: 5000].
    --debug         Enable debug mode.

"""

from flask import Flask, Response, request
from .codecs import codecs
from . import methods

app = Flask(__name__)


def decode_request(request):
    codec = codecs.get(request.content_type)
    return codec.decode(request.get_data(cache=False))


def encode_response(content='', status=200):
    accept_type = request.accept_mimetypes.best_match(tuple(codecs))
    codec = codecs[accept_type]
    resp = Response(codec.encode(content),
                    status=status,
                    content_type=accept_type)
    return resp


@app.route('/', methods=['GET'])
def root():
    return '', 204


@app.route('/catalog', methods=['GET'])
def catalog():
    return encode_response(methods.catalog())


@app.route('/validate', methods=['POST'])
def validate():
    expr = decode_request(request)
    _, error = methods.validate(expr)

    if error:
        return encode_response({'error': error}, status=422)

    return encode_response(status=204)


@app.route('/plan', methods=['POST'])
def plan():
    expr = decode_request(request)
    node, error = methods.validate(expr)

    if error:
        return encode_response({'error': error}, status=422)

    plan = methods.plan(expr, node)

    return encode_response(plan)


@app.route('/idents', methods=['POST'])
def idents():
    expr = decode_request(request)
    node, error = methods.validate(expr)

    if error:
        return encode_response({'error': error}, status=422)

    idents = methods.idents(expr, node)

    return encode_response(idents)


@app.route('/count', methods=['POST'])
def count():
    expr = decode_request(request)
    node, error = methods.validate(expr)

    if error:
        return encode_response({'error': error}, status=422)

    count = methods.count(expr, node)

    return encode_response({'count': count})


def serve(host='127.0.0.1', port=5000, debug=False):
    app.run(host=host, port=port, threaded=True, debug=debug)


def main(argv):
    from docopt import docopt

    opts = docopt(__doc__, argv=argv, version='1.0')

    host = opts['--host']
    port = int(opts['--port'])
    debug = opts['--debug']

    serve(host, port, debug)
