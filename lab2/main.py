import csv
import random
import json
import sqlparse
from sqlparse.sql import Token
from decimal import Decimal, getcontext
getcontext().prec = 100

def isNotWhitespace(x):
    return len(str(x.ttype).split('.')) < 3 or str(x.ttype).split('.')[2] != 'Whitespace'

def removeSpace(x):
    if type(x) == Token:
        return [x]
    return list(filter(isNotWhitespace, x))

SAMPLE_SIZE = 100000

class Column:

    def __init__(self, name, type):
        self.name = name
        self.type = type
        self.size = 0
        self.none_num = 0
        self.counter = {}
        self.set = set([])
        self.sample = []
        self.sample_idx = 0
        self.min = None
        self.max = None

    def expo(self, tb_name):
        data = {
            'type': self.type,
            'size': self.size,
            'none_num': self.none_num,
            'counter': self.counter,
            'sample': self.sample[:self.sample_idx],
            'min': self.min,
            'max': self.max,
        }
        with open('sample_{}/{}_{}.json'.format(SAMPLE_SIZE, tb_name, self.name), 'w') as f:
            json.dump(data, f)

    def impo(self, tb_name):
        with open('sample_{}/{}_{}.json'.format(SAMPLE_SIZE, tb_name, self.name), 'r') as f:
            data = json.load(f)
        self.type = data['type']
        self.size = data['size']
        self.none_num = data['none_num']
        self.counter = data['counter']
        self.sample = data['sample']
        self.sample.sort()
        self.min = data['min']
        self.max = data['max']

    def add(self, val, line):
        if val == '':
            self.none_num += 1
        else:
            if self.type == 'integer':
                val = int(val)
            if val not in self.set:
                self.set.add(val)
                self.size += 1
                if self.size <= SAMPLE_SIZE:
                    self.counter[val] = 0
                elif self.size == SAMPLE_SIZE + 1:
                    self.counter = {}
            if self.size <= SAMPLE_SIZE:
                self.counter[val] += 1
            if self.sample_idx < len(self.sample) and line == self.sample[self.sample_idx]:
                self.sample[self.sample_idx] = val
                self.sample_idx += 1
            if self.min == None or val < self.min:
                self.min = val 
            if self.max == None or val > self.max:
                self.max = val 


class Table:

    def __init__(self, parsed, impo=False):
        self.name = parsed[4].value
        self.cols = []
        cols = parsed[6]
        for idx, token in enumerate(cols):
            if token.ttype == None and token.value[-1] != ')':
                self.cols += [Column(token.value.split(' ')[-1], cols[idx+2].value)]
        self.size = 0
        if impo:
            for x in self.cols:
                x.impo(self.name)
            self.size = self.cols[0].size
        else:
            with open('imdb/'+self.name+'.csv', "r") as f:
                for _ in f:
                    self.size += 1
            print(self.name, len(self.cols), self.size)
            for x in self.cols:
                x.sample = random.sample(range(self.size), min(SAMPLE_SIZE-1, self.size))
                x.sample.sort()
            with open('imdb/'+self.name+'.csv', "r") as f:
                line = 0
                reader = csv.reader(f)
                for row in reader:
                    for idx, val in enumerate(row):
                        self.cols[idx].add(val, line)
                    line += 1
            for x in self.cols:
                x.expo(self.name)
        self.cols_dict = {}
        for x in self.cols:
            self.cols_dict[x.name] = x

class Database:
    
    def __init__(self, filename, impo=False):
        with open(filename, "r") as f:
            data = f.read()
        self.tables = {}
        for statement in sqlparse.split(data):
            prased = sqlparse.parse(statement)
            if len(prased):
                tb = Table(prased[0], impo)
                self.tables[tb.name] = tb

def findLs(L, val):
    num, pow2 = 0, 1
    while num + pow2 <= len(L):
        if L[num + pow2 - 1] < val:
            num, pow2 = num + pow2, pow2 * 2
        else:
            break
    pow2 = pow2 // 2
    while pow2 > 0:
        if num + pow2 <= len(L) and L[num + pow2 - 1] < val:
            num += pow2
        pow2 = pow2 // 2
    return num

def findGt(L, val):
    num, pow2 = 0, 1
    while num + pow2 <= len(L):
        if L[num + pow2 - 1] <= val:
            num, pow2 = num + pow2, pow2 * 2
        else:
            break
    pow2 = pow2 // 2
    while pow2 > 0:
        if num + pow2 <= len(L) and L[num + pow2 - 1] <= val:
            num += pow2
        pow2 = pow2 // 2
    return len(L) - num

cal_wr = set([])

class Where: # L op R

    def dealSingle(self, prased, fr):
        if prased[0].ttype == None:
            self.R = (fr[prased[0].get_parent_name()], prased[0].get_name())
        elif str(prased[0].ttype) == 'Token.Literal.String.Single':
            self.R = prased[0].value[1:-1]
        elif str(prased[0].ttype) == 'Token.Literal.Number.Integer':
            self.R = int(prased[0].value)
        else:
            print('Unknow Type')
            print(prased[0].value, prased[0].ttype)

    def dealIn(self, prased, _):
        self.R = []
        for token in removeSpace(prased[0][1]):
            if str(token.ttype) == 'Token.Literal.String.Single':
                self.R += [token.value[1:-1]]

    def dealBw(self, prased, _):
        prased = removeSpace(prased)
        if str(prased[0].ttype) == 'Token.Literal.String.Single':
            self.R = (prased[0].value[1:-1], prased[2].value[1:-1])
        elif str(prased[0].ttype) == 'Token.Literal.Number.Integer':
            self.R = (int(prased[0].value), int(prased[2].value))

    deal_dict = {
        '=': dealSingle,
        '!=': dealSingle,
        '<': dealSingle,
        '<=': dealSingle,
        '>': dealSingle,
        '>=': dealSingle,
        'IN': dealIn,
        'BETWEEN': dealBw,
        'LIKE': dealSingle,
        'NOT LIKE': dealSingle,
    }

    def __init__(self, prased, fr):
        if len(prased) == 1:
            prased = removeSpace(prased[0])
        self.L = (fr[prased[0].get_parent_name()], prased[0].get_name())
        # print(self.L)
        self.type = prased[1].value
        self.deal_dict.get(self.type)(self, prased[2:], fr)

    def pEq(self):
        if type(self.R) != tuple:
            col = db.tables[self.L[0]].cols_dict[self.L[1]]
            if col.counter != {}:
                return Decimal(col.counter[str(self.R)]) / db.tables[self.L[0]].size
            else:
                eq = 0
                for x in col.sample:
                    eq += (1 if x == self.R else 0)
                if eq > 0:
                    return Decimal(eq) / db.tables[self.L[0]].size
                else:
                    return Decimal(eq) / col.size
        else:
            if self.L in cal_wr and self.R in cal_wr:
                return Decimal(1)

            col0 = db.tables[self.L[0]].cols_dict[self.L[1]]
            col1 = db.tables[self.R[0]].cols_dict[self.R[1]]
            if col0.counter != {} and col1.counter != {}:
                num = 0
                lst0 = list(col0.counter)
                lst0.sort()
                lst1 = list(col1.counter)
                lst1.sort()
                i0, i1 = 0, 0
                while i0 < len(lst0) and i1 < len(lst1):
                    if lst0[i0] == lst1[i1]:
                        num += col0.counter[lst0[i0]] * col1.counter[lst1[i1]]
                        i0 += 1
                        i1 += 1
                    elif lst0[i0] < lst1[i1]:
                        i0 += 1
                    else:
                        i1 += 1
                poss = Decimal(num) / Decimal(db.tables[self.L[0]].size) / Decimal(db.tables[self.R[0]].size)
            elif col0.counter != {} or col1.counter != {}:
                # print('working 5')
                poss = Decimal(1) # TODO
            else:
                poss = Decimal(1) - self.pLs() - self.pGt()
            
            if self.L in cal_wr:
                poss *= Decimal(db.tables[self.L[0]].size)
            else:
                cal_wr.add(self.L)
            if self.R in cal_wr:
                poss *= Decimal(db.tables[self.R[0]].size)
            else:
                cal_wr.add(self.R)
            
            return poss
    
    def pNe(self):
        return Decimal(1) - self.pEq()
    
    def pLs(self):
        if type(self.R) != tuple:
            col = db.tables[self.L[0]].cols_dict[self.L[1]]
            if col.counter != {}:
                ct = 0
                for val, _ct in col.counter.items():
                    if (int(val) if type(self.R) == int else str(val)) < self.R:
                        ct += _ct
                return Decimal(ct) / db.tables[self.L[0]].size
            else:
                if self.R == col.min:
                    return Decimal(0)
                if self.R == col.max:
                    return Decimal(1) - self.pEq()
                if type(self.R) == str:
                    return (Decimal(findLs(col.sample, self.R)) + Decimal(1) / 2) / Decimal(len(col.sample) + 1)
                if type(self.R) == int:
                    num = findLs(col.sample, self.R)
                    l, r = (col.sample[num-1] if num > 0 else col.min), (col.sample[num] if num < len(col.sample) else col.max)
                    return (Decimal(num) + Decimal(self.R - l) / Decimal(r - l)) / Decimal(len(col.sample) + 1)
        else:
            col0 = db.tables[self.L[0]].cols_dict[self.L[1]]
            col1 = db.tables[self.R[0]].cols_dict[self.R[1]]
            if col0.counter != {} and col1.counter != {}:
                # num = 0
                # lst0 = list(col0.counter)
                # lst0.sort()
                # lst1 = list(col1.counter)
                # lst1.sort()
                # i0, i1, s0, s1 = 0, 0, 0, 0
                # size_R = db.tables[self.R[0]].size
                # while i0 < len(lst0) and i1 < len(lst1):
                #     if lst0[i0] >= lst1[i1]:
                #         s1 += col1.counter[lst1[i1]]
                #         i1 += 1
                #     elif lst0[i0] < lst1[i1]:
                #         num += col0.counter[lst0[i0]] * (size_R - s1)
                #         s0 += col0.counter[lst0[i0]]
                #         i0 += 1
                # return Decimal(num) / Decimal(db.tables[self.L[0]].size) / Decimal(size_R)
                print('working 3')
                return Decimal(1)
            elif col0.counter != {} or col1.counter != {}:
                print('working 4')
                return Decimal(1)
            else:
                num = 0
                lst0 = col0.sample
                lst1 = col1.sample
                i0, i1 = 0, 0
                while i0 < len(lst0) and i1 < len(lst1):
                    if lst0[i0] >= lst1[i1]:
                        i1 += 1
                    else:
                        num += len(lst1) - i1
                        i0 += 1
                return Decimal(num) / len(lst0) / len(lst1)
    
    def pLe(self):
        return Decimal(1) - self.pGt()
    
    def pGt(self):
        if type(self.R) != tuple:
            col = db.tables[self.L[0]].cols_dict[self.L[1]]
            if col.counter != {}:
                ct = 0
                for val, _ct in col.counter.items():
                    if (int(val) if type(self.R) == int else str(val)) > self.R:
                        ct += _ct
                return Decimal(ct) / db.tables[self.L[0]].size
            else:
                if self.R == col.max:
                    return Decimal(0)
                if self.R == col.min:
                    return Decimal(1) - self.pEq()
                if type(self.R) == str:
                    return (Decimal(findGt(col.sample, self.R)) + Decimal(1) / 2) / Decimal(len(col.sample) + 1)
                if type(self.R) == int:
                    num = findGt(col.sample, self.R)
                    l, r = (col.sample[-num-1] if num < len(col.sample) else col.min), (col.sample[-num] if num > 0 else col.max)
                    return (Decimal(num) + Decimal(r - self.R) / Decimal(r - l)) / Decimal(len(col.sample) + 1)
        else:
            col0 = db.tables[self.L[0]].cols_dict[self.L[1]]
            col1 = db.tables[self.R[0]].cols_dict[self.R[1]]
            if col0.counter != {} and col1.counter != {}:
                # num = 0
                # lst0 = list(col0.counter)
                # lst0.sort()
                # lst1 = list(col1.counter)
                # lst1.sort()
                # i0, i1, s0, s1 = 0, 0, 0, 0
                # size_L = db.tables[self.L[0]].size
                # while i0 < len(lst0) and i1 < len(lst1):
                #     if lst0[i0] <= lst1[i1]:
                #         s0 += col0.counter[lst0[i0]]
                #         i0 += 1
                #     elif lst0[i0] > lst1[i1]:
                #         num += (size_L - s0) * col1.counter[lst1[i1]]
                #         s1 += col1.counter[lst1[i1]]
                #         i1 += 1
                # return Decimal(num) / Decimal(db.tables[self.L[0]].size) / Decimal(db.tables[self.R[0]].size)
                print('working 1')
                return Decimal(1)
            elif col0.counter != {} or col1.counter != {}:
                print('working 2')
                return Decimal(1)
            else:
                num = 0
                lst0 = col0.sample
                lst1 = col1.sample
                i0, i1 = 0, 0
                while i0 < len(lst0) and i1 < len(lst1):
                    if lst0[i0] <= lst1[i1]:
                        i0 += 1
                    else:
                        num += len(lst0) - i0
                        i1 += 1
                return Decimal(num) / len(lst0) / len(lst1)
    
    def pGe(self):
        return Decimal(1) - self.pLs()
    
    def pIn(self): # TODO Sample
        return Decimal(1)
    
    def pBw(self):
        _R = self.R
        poss = Decimal(1)
        self.R = _R[0]
        poss -= self.pLs()
        self.R = _R[1]
        poss -= self.pGt()
        return poss
    
    def pLk(self): # TODO
        return Decimal(1)
    
    def pNl(self):
        return Decimal(1) - self.pLk()

    p_dict = {
        '=': pEq,
        '!=': pNe,
        '<': pLs,
        '<=': pLe,
        '>': pGt,
        '>=': pGe,
        'IN': pIn,
        'BETWEEN': pBw,
        'LIKE': pLk,
        'NOT LIKE': pNl,
    }

    def poss(self):
        poss = self.p_dict.get(self.type)(self)
        # print(poss)
        return poss

def run(filename):
    with open(filename, "r") as f:
        data = f.read()
    for statement in sqlparse.split(data):
        prased = sqlparse.parse(statement)
        if len(prased) == 0:
            continue
        fr = {}
        prased = removeSpace(prased[0])
        from_idx = 0
        while prased[from_idx].value != 'FROM':
            from_idx += 1
        for token in prased[from_idx+1:-1]:
            if token.get_name() != None:
                fr[token.get_name().lower()] = token.get_real_name()
            else:
                for _token in removeSpace(token):
                    if _token.ttype == None:
                        fr[_token.get_name().lower()] = _token.get_real_name()

        line_num = 1
        for _, val in fr.items():
            line_num *= db.tables[val].size
        print(line_num)

        wr = []
        lb = 0
        where = removeSpace(prased[-1])
        for idx, clause in enumerate(where):
            if (clause.value == 'AND' and where[idx+1].ttype == None) or (str(clause.ttype) == 'Token.Punctuation'):
                wr += [Where(where[lb+1:idx], fr)]
                lb = idx

        poss = Decimal(1)
        global cal_wr
        cal_wr = set([])
        for w in wr:
            poss *= w.poss()
        print(int(poss * line_num))


# db = Database("imdb/schematext.sql")
db = Database("imdb/schematext.sql", True)

if __name__ == "__main__":
    pass
    # run('input/easy.sql')
    run('input/middle.sql')
    # run('input/hard.sql')
