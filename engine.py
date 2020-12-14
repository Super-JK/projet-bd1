import sqlite3


class DataBaseException(Exception):
    """Exception envoyé lors d'un erreur en lien avec la base de données
    """
    pass


class AttributError(Exception):
    """
    Erreur en lien avec les attribut
    """
    pass


class InvalidExpression(Exception):
    """
    Erreur en lien avec une expression
    """
    pass


class DataBaseStructure:
    """classe représentant une structure de base de donnée.
        Il y a un dictionnaire principale qui reprend toute les tables.
        Celui ci est compose d'autre dictionnaire reprenant les champs et leur type.

        La structure peut être générer à partir d'un base de donnée ou directement reprise tel quel.

        Différente méthode sont disponible pour accéder aisément aux information.

        has_table vérifie si une table existe
        has_attribut vérifie si un attribut d'une table existe
        get_attribut renvoie les attribut et type d'une table
    """

    def __init__(self, bdd):
        if type(bdd) == str:
            conn = sqlite3.connect('example.db')
            c = conn.cursor()
            self.tables = {}
            for table in c.execute("SELECT name FROM sqlite_master WHERE type='table'"):
                self.tables[table[0]] = {}

            for table in self.tables:
                for attr in c.execute('PRAGMA table_info(%s)' % table):
                    self.tables[table][attr[1]] = attr[2]
            conn.close()
        elif type(bdd) == dict:
            self.tables = bdd
        else:
            raise TypeError("must be dict or Cursor not '%s' " % type(bdd))

    def has_table(self, table_name):
        return table_name in self.tables

    def hast_attribut(self, table_name, attribut_name):
        if not self.has_table(table_name):
            raise DataBaseException("table '%s' not in database" % table_name)
        return attribut_name in self.tables[table_name]

    def get_attribut(self, table_name):
        return self.tables[table_name]


def nice_print(struct):
    """
    Transforme un dictionnaire d'argument pour être afficher plus lisiblement
    """
    r = ''
    for element in struct:
        r += "\n'%s' %s," % (element, struct[element])
    return r


class Request:
    """Cette classe représente la requête qui sera traduite et/ou exécuter.
     Elle est constitué de l'expression de cette requête ainsi que la base de donnée lié à celle ci.

     La cohérence de cette requête est vérifier et uen erreur est retourner en cas de problème
     """

    def __init__(self, bdd, expr):

        type_ = type(bdd)
        if type_ == DataBaseException:
            self.bdd = bdd
        elif type_ == str or type_ == dict:
            self.bdd = DataBaseStructure(bdd)
        else:
            raise TypeError("must be DataBaseStructure not '%s'" % type(bdd))

        if isinstance(expr, Expression):
            self.expr = expr
            self.expr.check(self.bdd)
        elif isinstance(expr, str):
            self.expr = expr
        else:
            raise TypeError("must be Expression not '%s'" % type(expr))

    def __str__(self):
        return str(self.expr)


class Expression:
    """Classe mère de toutes expressions représente la forme la plus simple de celles ci."""

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
    """Classe représentant une table d'un base de données.

    check vérifie l'existence
    get_attr retourne les attributs de la table
    """

    def __init__(self, args):
        self.attr = []
        self.arg = args

    def __str__(self):
        return str(self.arg)

    def check(self, bdd):
        if not bdd.has_table(self.arg):
            raise DataBaseException("Table '%s' not in the database" % self.arg)

    def get_attr(self, bdd):
        if len(self.attr) == 0:
            self.attr = bdd.get_attribut(self.arg)
        return self.attr


class Projection(Expression):
    """Classe représentant une projection

    check vérifie la cohérence des attributs
    get_attr retourne les attributs de l'expression
    """

    def __init__(self, attr, expr):
        super().__init__(attr, expr)
        self.attr = attr
        self.expr = expr

        if isinstance(self.expr, str):
            self.expr = Relation(self.expr)
        elif not isinstance(self.expr, Expression):
            raise TypeError("must be Expression not '%s'" % type(expr))

    def __str__(self):
        return "proj( " + str(self.attr) + ", " + str(self.expr) + " )"

    def check(self, bdd):
        self.expr.check(bdd)
        expr_attr = self.expr.get_attr(bdd)
        for attr in self.attr:
            if attr not in expr_attr:
                raise InvalidExpression("The expression \n%s\n "
                                        "is invalid because attribut '%s' is not in the schema %s" %
                                        (self.__str__(), str(attr), nice_print(expr_attr)))

    def get_attr(self, bdd):
        attrs = {}
        expr_attr = self.expr.get_attr(bdd)
        for attr in expr_attr:
            if attr in self.attr:
                attrs[attr] = expr_attr[attr]
        return attrs


class Selection(Expression):
    """Classe représentant une sélection

   check vérifie la cohérence des attributs
   get_attr retourne les attributs de l'expression
   """

    def __init__(self, cond, expr):
        super().__init__(cond, expr)
        self.cond = cond
        self.expr = expr

        if isinstance(self.expr, str):
            self.expr = Relation(self.expr)
        elif not isinstance(self.expr, Expression):
            raise TypeError("must be Expression not '%s'" % type(expr))

        if not isinstance(self.cond, Equal):
            raise TypeError("must be Equal not '%s'" % type(expr))

    def __str__(self):
        return "select(" + str(self.cond) + ", " + str(self.expr) + ")"

    def check(self, bdd):
        self.expr.check(bdd)

        self.cond.check(bdd, self.expr.get_attr(bdd))

    def get_attr(self, bdd):
        return self.expr.get_attr(bdd)


class Equal(Expression):
    """Représente une égalité entre attribut ou avec une constante utilisé pour la sélection.
    """

    def __init__(self, left, right):
        super().__init__(left, right)
        self.left = left
        self.right = right

    def __str__(self):
        return "equ(" + str(self.left) + " = " + str(self.right) + ")"

    def check(self, bdd, attr):
        if self.left not in attr:
            raise InvalidExpression("The expression \n%s\n "
                                    "is invalid because attribut '%s' is not in the schema %s" %
                                    (self.__str__(), str(self.left), nice_print(attr)))
        if type(self.right) != Const:
            if self.right not in attr:
                raise InvalidExpression("The expression \n%s\n "
                                        "is invalid because attribut '%s' is not in the schema %s" %
                                        (self.__str__(), str(self.right), nice_print(attr)))
            elif attr[self.left] != attr[self.right]:
                raise AttributError("The expression \n%s\n"
                                    "is invalid because type of attribut\n'%s' %s\nis not in the same as\n'%s' %s" %
                                    (self.__str__(), str(self.left), attr[self.left], str(self.right), attr[self.right]))


class Const:
    """Représente une constante utilisé dans l'expression Equal"""

    def __init__(self, *arg):
        self.arg = arg

    def __str__(self):
        return str(self.arg)


class Join(Expression):
    """Classe représentant une jointure

    check vérifie la cohérence des attributs
    get_attr retourne les attributs de l'expression
    """

    def __init__(self, left, right):
        super().__init__(left, right)
        self.left = left
        self.right = right

        if isinstance(self.left, str):
            self.left = Relation(self.left)
        elif not isinstance(self.left, Expression):
            raise TypeError("must be Expression not '%s'" % type(left))

        if isinstance(self.right, str):
            self.right = Relation(self.right)
        elif not isinstance(self.right, Expression):
            raise TypeError("must be Expression not '%s'" % type(right))

    def __str__(self):
        return "join( " + str(self.left) + " and " + str(self.right) + " )"

    def check(self, bdd):
        self.right.check(bdd)
        self.left.check(bdd)

    def get_attr(self, bdd):
        left_attr = self.left.get_attr(bdd)
        right_attr = self.right.get_attr(bdd)

        for attr in left_attr:
            if attr not in right_attr:
                right_attr[attr] = left_attr[attr]

        return right_attr


class Rename(Expression):
    """Classe représentant un renommage

    check vérifie la cohérence des attributs
    get_attr retourne les attributs de l'expression
    """

    def __init__(self, old, new, expr):
        super().__init__(old, new, expr)
        self.old = old
        self.new = new
        self.expr = expr

        if isinstance(self.expr, str):
            self.expr = Relation(self.expr)
        elif not isinstance(self.expr, Expression):
            raise TypeError("must be Expression not '%s'" % type(expr))

    def __str__(self):
        return "rename( [" + str(self.old) + " to " + str(self.new) + "], " + str(self.expr) + " )"

    def check(self, bdd):
        self.expr.check(bdd)
        attr = self.expr.get_attr(bdd)
        if self.old not in attr:
            raise InvalidExpression("The expression \n%s\n"
                                    "is invalid because attribut\n'%s'\nis not in the schema\n%s" %
                                    (self.__str__(), str(self.old), nice_print(attr)))

    def get_attr(self, bdd):
        attr = self.expr.get_attr(bdd)
        i = attr.index(self.old)
        attr[i] = self.new
        return attr


class Union(Expression):
    """Classe représentant une union

   check vérifie la cohérence des attributs
   get_attr retourne les attributs de l'expression
   """

    def __init__(self, left, right):
        super().__init__(left, right)
        self.left = left
        self.right = right

        if isinstance(self.left, str):
            self.left = Relation(self.left)
        elif not isinstance(self.left, Expression):
            raise TypeError("must be Expression not '%s'" % type(left))

        if isinstance(self.right, str):
            self.right = Relation(self.right)
        elif not isinstance(self.right, Expression):
            raise TypeError("must be Expression not '%s'" % type(right))

    def __str__(self):
        return "union( " + str(self.left) + " and " + str(self.right) + " )"

    def check(self, bdd):
        self.left.check(bdd)
        self.right.check(bdd)
        left_attr = self.left.get_attr(bdd)
        right_attr = self.right.get_attr(bdd)
        if left_attr != right_attr:
            raise InvalidExpression("The expression \n%s\n"
                                    "is invalid because attributs of \n%s\n"
                                    "which are \n%s\n"
                                    "does not match those from \n%s\nwhich are%s" %
                                    (self.__str__(), str(self.left), nice_print(left_attr),
                                     str(self.right), nice_print(right_attr)))

    def get_attr(self, bdd):
        return self.left.get_attr(bdd)


class Difference(Expression):
    """Classe représentant une différence

    check vérifie la cohérence des attributs
    get_attr retourne les attributs de l'expression
    """

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

        left_attr = self.left.get_attr(bdd)
        right_attr = self.right.get_attr(bdd)
        if left_attr != right_attr:
            raise InvalidExpression("The expression \n%s\n"
                                    "is invalid because attributs of \n%s\n"
                                    "which are \n%s\n"
                                    "does not match those from \n%s\nwhich are%s" %
                                    (self.__str__(), str(self.left), nice_print(left_attr),
                                     str(self.right), nice_print(right_attr)))

    def get_attr(self, bdd):
        return self.left.get_attr(bdd)
