from engine import *


def main():

    select = Selection(Leq('sal', Const(1000)), 'emp')
    proj = Projection(['ename', 'empno', 'sal', 'job'], select)
    join = Join('stocks', proj)
    rename = Rename('sal', 'lol', proj)
    union = Union(join, 'stocks')
    diff = Difference(union, 'stocks')

    expr = rename
    req = Request('TestTAbles.db', expr)
    print(req.translate())
    req.print_result()


if __name__ == '__main__':
    main()
