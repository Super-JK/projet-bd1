import sqlite3


class Request:
    """docstring for Request"""

    def __init__(self, bdd, expr):

        if not isinstance(bdd, sqlite3.Cursor):
            raise Exception("pas une bdd sql")

        self.bdd = bdd

        if isinstance(expr, Expression):
            self.expr = expr
            self.expr.check(self.bdd)
        elif isinstance(expr, str):
            self.expr = expr
        else:
            raise Exception("not supported")

    def __str__(self):
        return str(self.expr)


class Expression:
    """docstring for Expression"""

    def __init__(self, *arg):
        self.arg = arg

    def __str__(self):
        r = ""
        for test in self.arg:
            r += str(test) + " "
        return r

    def check(self, bdd):
        pass


class Relation(Expression):
    """docstring for Expression"""

    def __init__(self, args):
        self.attr = []
        self.arg = args

    def __str__(self):
        return str(self.arg)

    def check(self, bdd):
        bdd.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name=?''', (self.arg,))
        if bdd.fetchone() is None:
            raise Exception('table not in bdd')

    def get_attr(self, bdd):
        if len(self.attr) == 0:
            for g in bdd.execute('PRAGMA table_info(%s)' % self.arg):
                self.attr.append(g[1])
        return self.attr


class Projection(Expression):
    """docstring for Expression"""

    def __init__(self, atr, expr):
        super().__init__(atr, expr)
        self.atr = atr
        self.expr = expr

        if isinstance(self.expr, str):
            self.expr = Relation(self.expr)
        elif not isinstance(self.expr, Expression):
            raise Exception('pas une expression')

    def __str__(self):
        return "proj( " + str(self.atr) + ", " + str(self.expr) + " )"

    def check(self, bdd):
        self.expr.check(bdd)
        attrs = self.expr.get_attr(bdd)
        for atr in self.atr:
            if atr not in attrs:
                raise Exception("attr not in table")

    def get_attr(self, bdd):
        return list(self.atr)


class Selection(Expression):
    """docstring for Expression"""

    def __init__(self, cond, expr):
        super().__init__(cond, expr)
        self.cond = cond
        self.expr = expr

        if isinstance(self.expr, str):
            self.expr = Relation(self.expr)
        elif not isinstance(self.expr, Expression):
            raise Exception('pas une expression')

        if not isinstance(self.cond, Equal):
            raise Exception('not equal cond')

    def __str__(self):
        return "select(" + str(self.cond) + ", " + str(self.expr) + ")"

    def check(self, bdd):
        self.expr.check(bdd)

        self.cond.check(bdd, self.expr.get_attr(bdd))

    def get_attr(self, bdd):
        return self.expr.get_attr(bdd)


class Equal(Expression):
    """docstring for Expression"""

    def __init__(self, left, right):
        super().__init__(left, right)
        self.left = left
        self.right = right

    def __str__(self):
        return "equ(" + str(self.left) + " = " + str(self.right) + ")"

    def check(self, bdd, attr):
        if self.left not in attr:
            raise Exception('left attr not in table')
        if type(self.right) != Const and self.right not in attr:
            raise Exception('right attr not in table')


class Const:
    """docstring for Expression"""

    def __init__(self, *arg):
        self.arg = arg

    def __str__(self):
        return str(self.arg)


class Join(Expression):
    """docstring for Expression"""

    def __init__(self, left, right):
        super().__init__(left, right)
        self.left = left
        self.right = right

        if isinstance(self.left, str):
            self.left = Relation(self.left)
        elif not isinstance(self.left, Expression):
            raise Exception('pas une expression')

        if isinstance(self.right, str):
            self.right = Relation(self.right)
        elif not isinstance(self.right, Expression):
            raise Exception('pas une expression')

    def __str__(self):
        return "join( " + str(self.left) + " and " + str(self.right) + " )"

    def check(self, bdd):
        self.right.check(bdd)
        self.left.check(bdd)

    def get_attr(self, bdd):
        left_attr = self.left.get_attr(bdd)
        right_attr = self.right.get_attr(bdd)
        return left_attr + list(set(right_attr) - set(left_attr))


class Rename(Expression):
    """docstring for Expression"""

    def __init__(self, old, new, expr):
        super().__init__(old, new, expr)
        self.old = old
        self.new = new
        self.expr = expr

        if isinstance(self.expr, str):
            self.expr = Relation(self.expr)
        elif not isinstance(self.expr, Expression):
            raise Exception('pas une expression')

    def __str__(self):
        return "rename( [" + str(self.old) + " to " + str(self.new) + "], " + str(self.expr) + " )"

    def check(self, bdd):
        self.expr.check(bdd)
        attr = self.expr.get_attr(bdd)
        if self.old not in attr:
            raise Exception('attr not in table')

    def get_attr(self, bdd):
        attr = self.expr.get_attr(bdd)
        i = attr.index(self.old)
        attr[i] = self.new
        return attr


class Union(Expression):
    """docstring for Expression"""

    def __init__(self, left, right):
        super().__init__(left, right)
        self.left = left
        self.right = right

        if isinstance(self.left, str):
            self.left = Relation(self.left)
        elif not isinstance(self.left, Expression):
            raise Exception('pas une expression')

        if isinstance(self.right, str):
            self.right = Relation(self.right)
        elif not isinstance(self.right, Expression):
            raise Exception('pas une expression')

    def __str__(self):
        return "union( " + str(self.left) + " and " + str(self.right) + " )"

    def check(self, bdd):
        self.left.check(bdd)
        self.right.check(bdd)

        if self.left.get_attr(bdd) != self.right.get_attr(bdd):
            raise Exception('attr do not match')

    def get_attr(self, bdd):
        return self.left.get_attr(bdd)


class Difference(Expression):
    """docstring for Expression"""

    def __init__(self, left, right):
        super().__init__(left, right)
        self.left = left
        self.right = right

        if isinstance(self.left, str):
            self.left = Relation(self.left)
        elif not isinstance(self.left, Expression):
            raise Exception('pas une expression')

        if isinstance(self.right, str):
            self.right = Relation(self.right)
        elif not isinstance(self.right, Expression):
            raise Exception('pas une expression')

    def __str__(self):
        return "diff( " + str(self.left) + " - " + str(self.right) + " )"

    def check(self, bdd):
        self.left.check(bdd)
        self.right.check(bdd)

        if self.left.get_attr(bdd) != self.right.get_attr(bdd):
            raise Exception('attr do not match')

    def get_attr(self, bdd):
        return self.left.get_attr(bdd)
