from engine import *
import sqlite3


def main():
    conn = sqlite3.connect('example.db')
    c = conn.cursor()

    '''expr = Projection("voiture", "qsdf", Selection("a", Relation('b')))
    expr = Rename("a", "c", expr)
    expr = Difference(Union(Relation("d"), Relation('f')), expr)
    '''

    proj = Projection(['date', 'trans'], 'stocks')
    select = Selection(Equal('date', 'trans'), proj)
    join = Join('stocks', select)
    rename = Rename('trans', 'transaction', join)
    union = Union(join, 'stocks')
    diff = Difference(union, 'stocks')

    expr = diff
    print(expr.get_attr(c))
    #for g in c.execute('PRAGMA table_info(%s)' % 'stocks'):
    #    print(g[1])

    req = Request(c, expr)
    print(str(req))

    conn.close()


if __name__ == '__main__':
    main()
