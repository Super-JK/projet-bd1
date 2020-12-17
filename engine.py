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

    def __init__(self, db):
        if type(db) == str:
            self.name = db
            conn = sqlite3.connect(db)
            c = conn.cursor()
            self.tables = {}
            for table in c.execute("SELECT name FROM sqlite_master WHERE type='table'"):
                self.tables[table[0]] = {}

            for table in self.tables:
                for attr in c.execute('PRAGMA table_info(%s)' % table):
                    self.tables[table][attr[1]] = attr[2]
            conn.close()
        elif type(db) == dict:
            self.tables = db
        else:
            raise TypeError("must be dict or Cursor not '%s' " % type(db))

    def has_table(self, table_name):
        return table_name in self.tables

    def hast_attribut(self, table_name, attribut_name):
        if not self.has_table(table_name):
            raise DataBaseException("table '%s' not in database" % table_name)
        return attribut_name in self.tables[table_name]

    def get_attribut(self, table_name):
        return self.tables[table_name]

    def get_name(self):
        return self.name


def nice_print(struct):
    """
    Transforme un dictionnaire d'argument pour être afficher plus lisiblement
    """
    r = ''
    for element in struct:
        r += "\n'%s' %s," % (element, struct[element])
    return r


def sql_attribut(attr):
    """
    Retourne la liste des attribut sous un format utilisable par SQL
    """
    r = ''
    if isinstance(attr, dict):
        attr = list(attr)

    i = 0
    while i < len(attr)-1:
        r += attr[i]+', '
        i += 1
    r += attr[i]
    return r


class Request:
    """Classe représentant la requête qui sera traduite et/ou exécuter.
     Elle est constitué de l'expression de cette requête ainsi que la base de donnée lié à celle ci.

     La cohérence de cette requête est vérifier et uen erreur est retourner en cas de problème
     """

    def __init__(self, db, expr):

        type_ = type(db)
        if type_ == DataBaseException:
            self.db = db
        elif type_ == str or type_ == dict:
            self.db = DataBaseStructure(db)
        else:
            raise TypeError("must be DataBaseStructure not '%s'" % type(db))

        if isinstance(expr, Expression):
            self.expr = expr
            self.expr.check(self.db)
        else:
            raise TypeError("must be Expression not '%s'" % type(expr))

    def __str__(self):
        return str(self.expr)

    def translate(self):
        return self.expr.translate()

    def execute(self):
        name = self.db.get_name()
        if name is None:
            raise DataBaseException("no sqlite database provided")
        conn = sqlite3.connect(name)
        c = conn.cursor()
        result = c.execute(self.translate()).fetchall()
        c.close()
        return result

    def create_new_table(self, table_name):
        name = self.db.get_name()
        if name is None:
            raise DataBaseException("no sqlite database provided")
        conn = sqlite3.connect(name)
        c = conn.cursor()
        result = c.execute("CREATE TABLE %s AS %s" % (table_name, self.translate()))
        c.close()
        return result

    def print_result(self):
        tab = self.execute()
        attr = self.expr.get_attr()
        res = ""

        for elem in attr:
            res += "{0:<11.11}| ".format(elem)

        res += '\n{:-<{}s}\n'.format('', len(res))

        for elem in tab:
            for subelem in elem:
                res += "{0:<13}".format(subelem)
            res += '\n'

        print(res)


class Expression:
    """Classe mère de toutes expressions représente la forme la plus simple de celles ci."""

    def __init__(self, *arg):
        self.arg = arg
        self.db = None

    def __str__(self):
        r = ""
        for test in self.arg:
            r += str(test) + " "
        return r

    def check(self, db):
        self.db = db
        pass

    def translate(self):
        pass


class Relation(Expression):
    """Classe représentant une table d'un base de données.

    check vérifie l'existence
    get_attr retourne les attributs de la table
    """

    def __init__(self, args):
        super().__init__(args)
        self.attr = {}
        self.arg = args

    def __str__(self):
        return str(self.arg)

    def check(self, db):
        self.db = db
        if not db.has_table(self.arg):
            raise DataBaseException("Table '%s' not in the database" % self.arg)

    def get_attr(self):
        if len(self.attr) == 0:
            self.attr = self.db.get_attribut(self.arg)
        return self.attr

    def translate(self):
        return str(self.arg)


class Projection(Expression):
    """Classe représentant une projection

    check vérifie la cohérence des attributs
    get_attr retourne les attributs de l'expression
    """

    def __init__(self, attr, expr):
        super().__init__(attr, expr)
        self.attr = attr
        self.expr = expr

        if not isinstance(attr, list):
            self.attr = [attr]

        if isinstance(self.expr, str):
            self.expr = Relation(self.expr)
        elif not isinstance(self.expr, Expression):
            raise TypeError("must be Expression not '%s'" % type(expr))

    def __str__(self):
        return "proj( " + str(self.attr) + ", " + str(self.expr) + " )"

    def check(self, db):
        self.db = db
        self.expr.check(db)
        expr_attr = self.expr.get_attr()
        for attr in self.attr:
            if attr not in expr_attr:
                raise InvalidExpression("The expression \n%s\n "
                                        "is invalid because attribut '%s' is not in the schema %s" %
                                        (self.__str__(), str(attr), nice_print(expr_attr)))

    def translate(self):
        return "SELECT DISTINCT " + sql_attribut(self.attr) + " FROM (" + self.expr.translate() + ")"

    def get_attr(self):
        attrs = {}
        expr_attr = self.expr.get_attr()
        for attr in self.attr:
            if attr in expr_attr:
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

        if not isinstance(self.cond, Condition):
            raise TypeError("must be Equal not '%s'" % type(expr))

    def __str__(self):
        return "select(" + str(self.cond) + ", " + str(self.expr) + ")"

    def check(self, db):
        self.db = db
        self.expr.check(db)

        self.cond.check(self.expr.get_attr())

    def get_attr(self):
        return self.expr.get_attr()

    def translate(self):
        return "SELECT * FROM ("+self.expr.translate()+") WHERE "+self.cond.translate()


class Condition(Expression):
    def __init__(self, left, right):
        super().__init__(left, right)
        self.left = left
        self.right = right

    def check(self, attr):
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
                                    (self.__str__(), str(self.left), attr[self.left],
                                     str(self.right), attr[self.right]))


class Equal(Condition):
    """Représente une égalité entre attribut ou avec une constante utilisé pour la sélection.
    """

    def __str__(self):
        return "equ(" + str(self.left) + " = " + str(self.right) + ")"

    def translate(self):
        return str(self.left)+' = '+str(self.right)


class Neq(Condition):
    """Représente une negation d'égalité entre attribut ou avec une constante utilisé pour la sélection.
    """

    def __str__(self):
        return "neq(" + str(self.left) + " != " + str(self.right) + ")"

    def translate(self):
        return str(self.left)+' != '+str(self.right)


class Gtr(Condition):
    """Représente une comparaison plus grand entre attribut ou avec une constante utilisé pour la sélection.
    """

    def __str__(self):
        return "gtr(" + str(self.left) + " > " + str(self.right) + ")"

    def translate(self):
        return str(self.left)+' > '+str(self.right)


class Geq(Condition):
    """Représente une comparaison plus grand ou égal entre attribut ou avec une constante utilisé pour la sélection.
    """

    def __str__(self):
        return "geq(" + str(self.left) + " >= " + str(self.right) + ")"

    def translate(self):
        return str(self.left)+' >= '+str(self.right)


class Lss(Condition):
    """Représente une comparaison plus petit entre attribut ou avec une constante utilisé pour la sélection.
    """

    def __str__(self):
        return "lss(" + str(self.left) + " < " + str(self.right) + ")"

    def translate(self):
        return str(self.left)+' < '+str(self.right)


class Leq(Condition):
    """Représente une comparaison plus petit ou égal entre attribut ou avec une constante utilisé pour la sélection.
    """

    def __str__(self):
        return "leq(" + str(self.left) + " <= " + str(self.right) + ")"

    def translate(self):
        return str(self.left)+' <= '+str(self.right)


class Const:
    """Représente une constante utilisé dans les expressions Condition"""

    def __init__(self, arg):
        self.arg = arg

    def __str__(self):
        if type(self.arg) == str:
            return "'%s'" % self.arg
        else:
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

    def check(self, db):
        self.db = db
        self.right.check(db)
        self.left.check(db)

    def get_attr(self):
        left_attr = self.left.get_attr()
        right_attr = self.right.get_attr()

        for attr in left_attr:
            if attr not in right_attr:
                right_attr[attr] = left_attr[attr]

        return right_attr

    def translate(self):
        return "SELECT * FROM ("+self.left.translate()+") NATURAL JOIN ("+self.right.translate()+")"


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

    def check(self, db):
        self.db = db
        self.expr.check(db)
        attr = self.expr.get_attr()
        if self.old not in attr:
            raise InvalidExpression("The expression \n%s\n"
                                    "is invalid because attribut\n'%s'\nis not in the schema\n%s" %
                                    (self.__str__(), str(self.old), nice_print(attr)))

    def get_attr(self):
        attr = self.expr.get_attr()
        res = {}
        for elem in attr:
            if elem == self.old:
                res[self.new] = attr[elem]
            else:
                res[elem] = attr[elem]
        return res

    def translate(self):
        attr = self.expr.get_attr()

        r = ""
        for i in attr:
            if i == self.old:
                r += self.old+" as "+self.new+", "
            else:
                r += i+", "
        r = r[0:len(r)-2]
        return "SELECT "+r+" FROM ("+self.expr.translate() + ")"


class DualExpr(Expression):
    """
    Représente   une expression double générique. Classe mère de Union et Difference
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

    def get_attr(self):
        return self.left.get_attr()

    def check(self, db):
        self.db = db
        self.left.check(db)
        self.right.check(db)
        left_attr = self.left.get_attr()
        right_attr = self.right.get_attr()
        if left_attr != right_attr:
            raise InvalidExpression("The expression \n%s\n"
                                    "is invalid because attributs of \n%s\n"
                                    "which are \n%s\n"
                                    "does not match those from \n%s\nwhich are%s" %
                                    (self.__str__(), str(self.left), nice_print(left_attr),
                                     str(self.right), nice_print(right_attr)))

    def translate(self):
        left = self.left.translate()
        right = self.right.translate()

        if isinstance(self.left, Relation):
            left = "SELECT * FROM "+str(self.left)
        if isinstance(self.right, Relation):
            right = "SELECT * FROM "+str(self.right)

        return left, right


class Union(DualExpr):
    """Classe représentant une union
    Hérite de DualExpr (similaire à Difference)
   """

    def __str__(self):
        return "union( " + str(self.left) + " and " + str(self.right) + " )"

    def translate(self):
        r = super().translate()
        return "%s UNION %s" % r


class Difference(DualExpr):
    """Classe représentant une différence
    Hérite de DualExpr (similaire à Union)
    """

    def __str__(self):
        return "diff( " + str(self.left) + " - " + str(self.right) + " )"

    def translate(self):
        r = super().translate()
        return "%s EXCEPT %s" % r
