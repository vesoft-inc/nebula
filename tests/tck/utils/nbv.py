# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#

import re
import ply.lex as lex
import ply.yacc as yacc

if __name__ == "__main__":
    from mmh2 import mmh2
else:
    from tests.tck.utils.mmh2 import mmh2

from nebula2.common.ttypes import (
    Value,
    NullType,
    NMap,
    NList,
    NSet,
    Vertex,
    Tag,
    Edge,
    Path,
    Step,
)

Value.__hash__ = lambda self: self.value.__hash__()
Pattern = type(re.compile(r"^"))

states = (
    ('sstr', 'exclusive'),
    ('dstr', 'exclusive'),
    ('regex', 'exclusive'),
)

tokens = (
    'EMPTY',
    'NULL',
    'NaN',
    'BAD_DATA',
    'BAD_TYPE',
    'OVERFLOW',
    'UNKNOWN_PROP',
    'DIV_BY_ZERO',
    'OUT_OF_RANGE',
    'FLOAT',
    'INT',
    'STRING',
    'BOOLEAN',
    'LABEL',
    'PATTERN',
)

literals = ['(', ')', '[', ']', '{', '}', '<', '>', '@', '-', ':', ',', '/']

t_LABEL = r'[_a-zA-Z][_a-zA-Z0-9]*'

t_ignore = ' \t\n'
t_sstr_dstr_regex_ignore = ''


def t_EMPTY(t):
    r'EMPTY'
    t.value = Value()
    return t


def t_NULL(t):
    r'NULL'
    t.value = Value(nVal=NullType.__NULL__)
    return t


def t_NaN(t):
    r'NaN'
    t.value = Value(nVal=NullType.NaN)
    return t


def t_BAD_DATA(t):
    r'BAD_DATA'
    t.value = Value(nVal=NullType.BAD_DATA)
    return t


def t_BAD_TYPE(t):
    r'BAD_TYPE'
    t.value = Value(nVal=NullType.BAD_TYPE)
    return t


def t_OVERFLOW(t):
    r'OVERFLOW'
    t.value = Value(nVal=NullType.ERR_OVERFLOW)
    return t


def t_UNKNOWN_PROP(t):
    r'UNKNOWN_PROP'
    t.value = Value(nVal=NullType.UNKNOWN_PROP)
    return t


def t_DIV_BY_ZERO(t):
    r'DIV_BY_ZERO'
    t.value = Value(nVal=NullType.DIV_BY_ZERO)
    return t


def t_OUT_OF_RANGE(t):
    r'OUT_OF_RANGE'
    t.value = Value(nVal=NullType.OUT_OF_RANGE)
    return t


def t_FLOAT(t):
    r'-?\d+\.\d+'
    t.value = Value(fVal=float(t.value))
    return t


def t_INT(t):
    r'-?\d+'
    t.value = Value(iVal=int(t.value))
    return t


def t_BOOLEAN(t):
    r'(?i)true|false'
    v = Value()
    if t.value.lower() == 'true':
        v.set_bVal(True)
    else:
        v.set_bVal(False)
    t.value = v
    return t


def t_sstr(t):
    r'\''
    t.lexer.string = ''
    t.lexer.begin('sstr')
    pass


def t_dstr(t):
    r'"'
    t.lexer.string = ''
    t.lexer.begin('dstr')
    pass


def t_regex(t):
    r'/'
    t.lexer.string = ''
    t.lexer.begin('regex')
    pass


def t_sstr_dstr_escape_newline(t):
    r'\\n'
    t.lexer.string += '\n'
    pass


def t_sstr_dstr_escape_tab(t):
    r'\\t'
    t.lexer.string += '\t'
    pass


def t_sstr_dstr_escape_char(t):
    r'\\.'
    t.lexer.string += t.value[1]
    pass


def t_sstr_any(t):
    r'[^\']'
    t.lexer.string += t.value
    pass


def t_dstr_any(t):
    r'[^"]'
    t.lexer.string += t.value
    pass


def t_regex_escape_char(t):
    r'\\/'
    t.lexer.string += t.value[1]
    pass


def t_regex_any(t):
    r'[^/]'
    t.lexer.string += t.value
    pass


def t_regex_PATTERN(t):
    r'/'
    t.value = re.compile(t.lexer.string)
    t.lexer.begin('INITIAL')
    return t


def t_sstr_STRING(t):
    r'\''
    t.value = Value(sVal=t.lexer.string)
    t.lexer.begin('INITIAL')
    return t


def t_dstr_STRING(t):
    r'"'
    t.value = Value(sVal=t.lexer.string)
    t.lexer.begin('INITIAL')
    return t


def t_ANY_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)


def p_expr(p):
    '''
        expr : EMPTY
             | NULL
             | NaN
             | BAD_DATA
             | BAD_TYPE
             | OVERFLOW
             | UNKNOWN_PROP
             | DIV_BY_ZERO
             | OUT_OF_RANGE
             | INT
             | FLOAT
             | BOOLEAN
             | STRING
             | PATTERN
             | list
             | set
             | map
             | vertex
             | edge
             | path
             | function
    '''
    if isinstance(p[1], Value) or isinstance(p[1], Pattern):
        p[0] = p[1]
    elif type(p[1]) in [str, bytes]:
        p[0] = Value(sVal=p[1])
    elif type(p[1]) is int:
        p[0] = Value(iVal=p[1])
    elif type(p[1]) is bool:
        p[0] = Value(bVal=p[1])
    elif type(p[1]) is float:
        p[0] = Value(fVal=p[1])
    else:
        raise ValueError(f"Invalid value type: {type(p[1])}")


def p_list(p):
    '''
        list : '[' list_items ']'
             | '[' ']'
    '''
    l = NList()
    if len(p) == 4:
        l.values = p[2]
    else:
        l.values = []
    p[0] = Value(lVal=l)


def p_set(p):
    '''
        set : '{' list_items '}'
    '''
    s = NSet()
    s.values = set(p[2])
    p[0] = Value(uVal=s)


def p_list_items(p):
    '''
        list_items : expr
                   | list_items ',' expr
    '''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[1].append(p[3])
        p[0] = p[1]


def p_map(p):
    '''
        map : '{' map_items '}'
            | '{' '}'
    '''
    m = NMap()
    if len(p) == 4:
        m.kvs = p[2]
    else:
        m.kvs = {}
    p[0] = Value(mVal=m)


def p_map_items(p):
    '''
        map_items : LABEL ':' expr
                  | STRING ':' expr
                  | map_items ',' LABEL ':' expr
                  | map_items ',' STRING ':' expr
    '''
    if len(p) == 4:
        k = p[1]
        if isinstance(k, Value):
            k = k.get_sVal()
        p[0] = {k: p[3]}
    else:
        k = p[3]
        if isinstance(k, Value):
            k = k.get_sVal()
        p[1][k] = p[5]
        p[0] = p[1]


def p_vid(p):
    '''
        vid : STRING
            | INT
            | function
    '''
    p[0] = p[1]
    if not isinstance(p[0], Value):
        if type(p[0]) in [str, bytes]:
            p[0] = Value(sVal=p[0])
        elif type(p[0]) is int:
            p[0] = Value(iVal=p[0])
        else:
            raise ValueError(f"Invalid vid type: {type(p[0])}")


def p_vertex(p):
    '''
        vertex : '(' tag_list ')'
               | '(' vid tag_list ')'
    '''
    vid = None
    tags = None
    if len(p) == 4:
        tags = p[2]
    else:
        vid = p[2]
        tags = p[3]
    v = Vertex(vid=vid, tags=tags)
    p[0] = Value(vVal=v)


def p_tag_list(p):
    '''
        tag_list :
                 | tag_list tag
    '''
    if len(p) == 3:
        if p[1] is None:
            p[1] = []
        p[1].append(p[2])
        p[0] = p[1]


def p_tag(p):
    '''
        tag : ':' LABEL map
            | ':' LABEL
    '''
    tag = Tag(name=p[2])
    if len(p) == 4:
        tag.props = p[3].get_mVal().kvs
    p[0] = tag


def p_edge_spec(p):
    '''
        edge : '[' edge_rank edge_props ']'
             | '[' ':' LABEL edge_props ']'
             | '[' ':' LABEL edge_rank edge_props ']'
             | '[' vid '-' '>' vid edge_props ']'
             | '[' vid '-' '>' vid edge_rank edge_props ']'
             | '[' ':' LABEL vid '-' '>' vid edge_props ']'
             | '[' ':' LABEL vid '-' '>' vid edge_rank edge_props ']'
             | '[' vid '<' '-' vid edge_props ']'
             | '[' vid '<' '-' vid edge_rank edge_props ']'
             | '[' ':' LABEL vid '<' '-' vid edge_props ']'
             | '[' ':' LABEL vid '<' '-' vid edge_rank edge_props ']'
    '''
    e = Edge()
    name = None
    rank = None
    src = None
    dst = None
    props = None
    etype = None

    if len(p) == 5:
        rank = p[2]
        props = p[3]
    elif len(p) == 6:
        name = p[3]
        props = p[4]
    elif len(p) == 7:
        name = p[3]
        rank = p[4]
        props = p[5]
    elif len(p) == 8:
        src = p[2]
        dst = p[5]
        if p[3] == '<' and p[4] == '-':
            etype = -1
        props = p[6]
    elif len(p) == 9:
        src = p[2]
        dst = p[5]
        if p[3] == '<' and p[4] == '-':
            etype = -1
        rank = p[6]
        props = p[7]
    elif len(p) == 10:
        name = p[3]
        src = p[4]
        dst = p[7]
        if p[5] == '<' and p[6] == '-':
            etype = -1
        props = p[8]
    elif len(p) == 11:
        name = p[3]
        src = p[4]
        dst = p[7]
        if p[5] == '<' and p[6] == '-':
            etype = -1
        rank = p[8]
        props = p[9]

    e.name = name
    e.ranking = rank
    e.src = src
    e.dst = dst
    e.props = props
    # default value of e.type is 1 if etype is None
    e.type = etype

    p[0] = Value(eVal=e)


def p_edge_rank(p):
    '''
        edge_rank : '@' INT
    '''
    p[0] = p[2].get_iVal()


def p_edge_props(p):
    '''
        edge_props :
                   | map
    '''
    if len(p) == 1:
        p[0] = None
    else:
        p[0] = p[1].get_mVal().kvs


def p_path(p):
    '''
        path : '<' vertex steps '>'
             | '<' vertex '>'
    '''
    path = Path()
    path.src = p[2].get_vVal()
    if len(p) == 5:
        path.steps = p[3]
    p[0] = Value(pVal=path)


def p_steps(p):
    '''
        steps : step
              | steps step
    '''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[1].append(p[2])
        p[0] = p[1]


def p_step(p):
    '''
        step : '-' edge '-' '>' vertex
             | '<' '-' edge '-' vertex
             | '-' '-' '>' vertex
             | '<' '-' '-' vertex
    '''
    step = Step()
    if p[1] == '-':
        step.type = 1
    else:
        step.type = -1
    if len(p) == 5:
        v = p[4].get_vVal()
        e = None
    elif p[1] == '-':
        v = p[5].get_vVal()
        e = p[2].get_eVal()
    else:
        v = p[5].get_vVal()
        e = p[3].get_eVal()

    if e is not None:
        step.name = e.name
        step.ranking = e.ranking
        step.props = e.props
    step.dst = v
    p[0] = step


def p_function(p):
    '''
        function : LABEL '(' list_items ')'
    '''
    p[0] = functions[p[1]](*p[3])


def p_error(p):
    print("Syntax error in input!")


lexer = lex.lex()
functions = {}


def murmurhash2(v):
    if isinstance(v, Value):
        v = v.get_sVal()
    if type(v) is str:
        return mmh2(bytes(v, 'utf-8'))
    if type(v) is bytes:
        return mmh2(v)
    raise ValueError(f"Invalid value type: {type(v)}")


def register_function(name, func):
    functions[name] = func


register_function('hash', murmurhash2)


parser = yacc.yacc()


def parse(s):
    return parser.parse(s)


def parse_row(row):
    return [str(parse(x)) for x in row]


if __name__ == '__main__':
    expected = {}
    expected['EMPTY'] = Value()
    expected['NULL'] = Value(nVal=NullType.__NULL__)
    expected['NaN'] = Value(nVal=NullType.NaN)
    expected['BAD_DATA'] = Value(nVal=NullType.BAD_DATA)
    expected['BAD_TYPE'] = Value(nVal=NullType.BAD_TYPE)
    expected['OVERFLOW'] = Value(nVal=NullType.ERR_OVERFLOW)
    expected['UNKNOWN_PROP'] = Value(nVal=NullType.UNKNOWN_PROP)
    expected['DIV_BY_ZERO'] = Value(nVal=NullType.DIV_BY_ZERO)
    expected['OUT_OF_RANGE'] = Value(nVal=NullType.OUT_OF_RANGE)
    expected['123'] = Value(iVal=123)
    expected['-123'] = Value(iVal=-123)
    expected['3.14'] = Value(fVal=3.14)
    expected['-3.14'] = Value(fVal=-3.14)
    expected['true'] = Value(bVal=True)
    expected['True'] = Value(bVal=True)
    expected['false'] = Value(bVal=False)
    expected['fAlse'] = Value(bVal=False)
    expected["'string'"] = Value(sVal="string")
    expected['"string"'] = Value(sVal='string')
    expected['''"string'string'"'''] = Value(sVal="string'string'")
    expected['''/^[_a-z][-_a-z0-9]*$/'''] = re.compile(r'^[_a-z][-_a-z0-9]*$')
    expected['''/\\//'''] = re.compile(r'/')
    expected['''hash("hello")'''] = 2762169579135187400
    expected['''hash("World")'''] = -295471233978816215
    expected['[]'] = Value(lVal=NList([]))
    expected['[{}]'] = Value(lVal=NList([Value(mVal=NMap({}))]))
    expected['[1,2,3]'] = Value(lVal=NList([
        Value(iVal=1),
        Value(iVal=2),
        Value(iVal=3),
    ]))
    expected['{1,2,3}'] = Value(
        uVal=NSet(set([
            Value(iVal=1),
            Value(iVal=2),
            Value(iVal=3),
        ])))
    expected['{}'] = Value(mVal=NMap({}))
    expected['{k1:1,"k2":true}'] = Value(mVal=NMap({
        'k1': Value(iVal=1),
        'k2': Value(bVal=True)
    }))
    expected['()'] = Value(vVal=Vertex())
    expected['("vid")'] = Value(vVal=Vertex(vid='vid'))
    expected['(123)'] = Value(vVal=Vertex(vid=123))
    expected['(-123)'] = Value(vVal=Vertex(vid=-123))
    expected['(hash("vid"))'] = Value(vVal=Vertex(vid=murmurhash2('vid')))
    expected['("vid":t)'] = Value(vVal=Vertex(vid='vid', tags=[Tag(name='t')]))
    expected['("vid":t:t)'] = Value(
        vVal=Vertex(vid='vid', tags=[
            Tag(name='t'),
            Tag(name='t'),
        ]))
    expected['("vid":t{p1:0,p2:" "})'] = Value(vVal=Vertex(
        vid='vid',
        tags=[
            Tag(name='t', props={
                'p1': Value(iVal=0),
                'p2': Value(sVal=' ')
            })
        ]))
    expected['("vid":t1{p1:0,p2:" "}:t2{})'] = Value(vVal=Vertex(
        vid='vid',
        tags=[
            Tag(name='t1', props={
                'p1': Value(iVal=0),
                'p2': Value(sVal=' ')
            }),
            Tag(name='t2', props={})
        ]))
    expected['[:e]'] = Value(eVal=Edge(name='e'))
    expected['[@1]'] = Value(eVal=Edge(ranking=1))
    expected['[@-1]'] = Value(eVal=Edge(ranking=-1))
    expected['["1"->"2"]'] = Value(eVal=Edge(src='1', dst='2'))
    expected['[1->2]'] = Value(eVal=Edge(src=1, dst=2))
    expected['[-1->-2]'] = Value(eVal=Edge(src=-1, dst=-2))
    expected['[hash("1")->hash("2")]'] = Value(eVal=Edge(src=murmurhash2('1'), dst=murmurhash2('2')))
    expected['[:e{}]'] = Value(eVal=Edge(name='e', props={}))
    expected['[:e@123{}]'] = Value(eVal=Edge(name='e', ranking=123, props={}))
    expected['[:e"1"->"2"@123{}]'] = Value(eVal=Edge(
        name='e',
        ranking=123,
        src='1',
        dst='2',
        props={},
    ))
    expected['<()>'] = Value(pVal=Path(src=Vertex()))
    expected['<("vid")>'] = Value(pVal=Path(src=Vertex(vid='vid')))
    expected['<()-->()>'] = Value(pVal=Path(
        src=Vertex(),
        steps=[Step(type=1, dst=Vertex())],
    ))
    expected['<()<--()>'] = Value(pVal=Path(
        src=Vertex(),
        steps=[Step(type=-1, dst=Vertex())],
    ))
    expected['<()-->()-->()>'] = Value(pVal=Path(
        src=Vertex(),
        steps=[
            Step(type=1, dst=Vertex()),
            Step(type=1, dst=Vertex()),
        ],
    ))
    expected['<()-->()<--()>'] = Value(pVal=Path(
        src=Vertex(),
        steps=[
            Step(type=1, dst=Vertex()),
            Step(type=-1, dst=Vertex()),
        ],
    ))
    expected['<("v1")-[:e1]->()<-[:e2]-("v2")>'] = Value(pVal=Path(
        src=Vertex(vid='v1'),
        steps=[
            Step(name='e1', type=1, dst=Vertex()),
            Step(name='e2', type=-1, dst=Vertex(vid='v2'))
        ],
    ))
    for item in expected.items():
        v = parse(item[0])
        assert v is not None, "Failed to parse %s" % item[0]
        assert v == item[1], \
            "Parsed value not as expected, str: %s, expected: %s actual: %s" % (item[0], item[1], v)
