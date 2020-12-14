from engine import *


def main():

    proj = Projection(['date', 'trans'], 'stocks')
    select = Selection(Equal('date', 'trans'), proj)
    join = Join('stocks', select)
    rename = Rename('trans', 'transaction', join)
    union = Union(join, 'stocks')
    diff = Difference(union, 'stocks')

    expr = diff
    req = Request('example.db', expr)
    print(req)


if __name__ == '__main__':
    main()
