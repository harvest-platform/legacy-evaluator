import re
import json
from flask import Flask, Response, request
from avocado.query import oldparsers as parsers
from avocado.models import DataConcept
from avocado.query.operators import registry as operators


app = Flask(__name__)
host = None
port = None

# Operator mapping.
op_map_in = {
    'eq': 'exact',
    '-eq': '-exact',
}

op_map_out = {
    'exact': 'eq',
    '-exact': '-eq',
}

op_set = {'range', 'exact', 'gt', 'lt', 'gte', 'lte', 'in'}

punc_re = re.compile(r'[\/\'",]+')
space_re = re.compile(r'\s+')


@app.route('/', methods=['GET'])
def root():
    return '', 204


@app.route('/catalog', methods=['GET'])
def catalog():
    concepts_query = DataConcept.objects.published().filter(queryable=True)
    concepts = []

    for c in concepts_query:
        params = {}

        for cf in c.concept_fields.select_related('field'):
            f = cf.field

            ops = {}
            for k, _ in f.operators:
                o = operators.get(k)

                if o.lookup not in op_set:
                    continue

                k = op_map_out.get(k, k)
                ops[k] = {
                    'id': o.lookup,
                    'doc': '{} ({})'.format(o.verbose_name, o.short_name),
                    'multiple': hasattr(o, 'join_string'),
                }

            key = '{}.{}.{}'.format(*f.natural_key())

            params[key] = {
                'id': f.id,
                'label': str(cf),
                'type': f.simple_type,
                'doc': f.description,
                'nullable': f.nullable,
                'operators': ops,
            }

        local = space_re.sub('_', punc_re.sub('', c.name.lower()))

        concepts.append({
            'id': c.pk,
            'uri': 'audgendb.{}'.format(local),
            'label': str(c),
            'doc': c.description,
            'keywords': c.keywords,
            'type': None,
            'params': params,
        })

    resp = Response(json.dumps({
        'version': '1.0.0',
        'concept': concepts,
    }))

    resp.headers['Content-Type'] = 'application/json'
    return resp


@app.route('/validate', methods=['POST'])
def validate():
    expr = request.get_json()

    error = None
    context = translate_expr(expr)

    try:
        parsers.datacontext.parse(context)
    except Exception as e:
        error = str(e)

    if error:
        return json.dumps({
            'error': error,
        }), 422

    return '', 204


@app.route('/plan', methods=['POST'])
def plan():
    expr = request.get_json()

    context = translate_expr(expr)
    node = parsers.datacontext.parse(context)

    queryset = node.apply().values_list('pk', flat=True).order_by()
    compiler = queryset.query.get_compiler(queryset.db)
    sql, params = compiler.as_sql()

    resp = Response(json.dumps({
        'plan': {
            'harvest': context,
            'sql': sql,
            'params': params,
        },
    }))

    resp.headers['Content-Type'] = 'application/json'

    return resp


@app.route('/idents', methods=['POST'])
def idents():
    expr = request.get_json()

    context = translate_expr(expr)
    node = parsers.datacontext.parse(context)

    queryset = node.apply().values_list('pk', flat=True).order_by()
    compiler = queryset.query.get_compiler(queryset.db)
    ids = tuple(r[0] for r in compiler.results_iter())

    resp = Response(json.dumps(ids))
    resp.headers['Content-Type'] = 'application/json'

    return resp


@app.route('/count', methods=['POST'])
def count():
    expr = request.get_json()

    context = translate_expr(expr)

    node = parsers.datacontext.parse(context)
    count = node.apply().count()

    resp = Response(json.dumps({
        'count': count,
    }))

    resp.headers['Content-Type'] = 'application/json'

    return resp


def translate_expr(expr):
    return translate_term(expr['term'])


def translate_term(term):
    # Corresponds to a branch.
    if term['type'] == 'branch':
        return {
            'type': term['operator'],
            'children': map(translate_term, term['terms']),
        }

    # Corresponds to a set of query conditions that are ANDed together.
    elif term['type'] == 'leaf':
        # Wrap in a branch node.
        preds = []

        for p in term['params'].values():
            op, val = translate_op(p['operator'], p['value'])

            preds.append({
                'concept': term['id'],
                'field': p['id'],
                'operator': op,
                'value': val,
            })

        return {
            'type': 'and',
            'children': preds,
        }


def translate_op(op, val):
    if op in op_map_in:
        op = op_map_in[op]

    if op == 'range':
        if 'gt' in val and 'lt' in val:
            return op, (val['gt'], val['lt'])

        if 'gt' in val:
            return 'gt', val['gt']

        if 'lt' in val:
            return 'lt', val['lt']

        raise ValueError('invalid range')

    return op, val


if __name__ == '__main__':
    host = '0.0.0.0'
    port = 8100
    debug = True

    app.run(host=host, port=port, threaded=True, debug=debug)
