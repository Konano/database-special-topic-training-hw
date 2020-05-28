import csv
import random
import json
import sqlparse
from sqlparse.sql import Token
from decimal import Decimal, getcontext
import fnmatch
from math import log10, log
getcontext().prec = 100

def isNotWhitespace(x):
    return len(str(x.ttype).split('.')) < 3 or str(x.ttype).split('.')[2] != 'Whitespace'

def removeSpace(x):
    if type(x) == Token:
        return [x]
    return list(filter(isNotWhitespace, x))

SAMPLE_SIZE = 100000
RELATE_FUNC = True

class Column:

    def __init__(self, name, type, tb_name):
        self.tb_name = tb_name
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
        if self.counter != {}:
            self.sample = []
        else:
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
                self.cols += [Column(token.value.split(' ')[-1], cols[idx+2].value, self.name)]
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

class Relate:
    
    def __init__(self, tbl):
        
        with open(f'sample_relate/{tbl}.json', 'r') as f:
            data = json.load(f)
        self.tbl = tbl
        self.col0 = data['col0']
        self.col1 = data['col1']
        self.total = int(data['total'])
        self.csize = data['csize']
        self.count = data['count']

        self._count = {}
        for x in self.count.keys():
            if type(self.count[x]) == list:
                self.count[x] = [int(x) for x in self.count[x]]
                self.count[x].sort()
                self._count[x] = {}
                for y in self.count[x]:
                    if y not in self._count[x]:
                        self._count[x][y] = 1
                    else:
                        self._count[x][y] += 1
            else:
                self._count[x] = [(int(y[0]), y[1]) for y in self.count[x].items()]
                self._count[x].sort()
                cnt = 0
                for i in range(len(self._count[x])):
                    cnt, self._count[x][i] = cnt + self._count[x][i][1], (self._count[x][i][0], cnt)
                self._count[x].append((999999999999, cnt))
                
    
    def cal(self, a, b):

        # print(a.L, a.R, b.L, b.R)
        poss = Decimal(1) / a.p

        if a.L[1] == self.col1 and b.L[1] == self.col0:
            a, b = b, a
        if a.L[1] != self.col0 or b.L[1] != self.col1:
            return b.poss()

        cnt = 0
        at = a.type if a.type != '=' else '=='
        # print(b.type)
        bt = b.type if b.type != '=' else '=='
        for x in self.count.keys():
            if eval(f'{x} {at} {a.R}'):
                if type(self.count[x]) == list:
                    _cnt = 0
                    if bt == '==':
                        _cnt = (self._count[x][b.R] if b.R in self._count[x] else 0)
                    elif bt == '!=':
                        _cnt = len(self.count[x]) - (self._count[x][b.R] if b.R in self._count[x] else 0)
                    elif bt == '<':
                        _cnt = findLs(self.count[x], b.R)
                    elif bt == '<=':
                        _cnt = len(self.count[x]) - findGt(self.count[x], b.R)
                    elif bt == '>':
                        _cnt = findGt(self.count[x], b.R)
                    elif bt == '>=':
                        _cnt = len(self.count[x]) - findLs(self.count[x], b.R)

                    # __cnt = 0
                    # for y in self.count[x]:
                    #     if eval(f'{y} {bt} {b.R}'):
                    #         __cnt += 1
                    # assert(__cnt == _cnt)
                    cnt += int(_cnt / len(self.count[x]) * self.csize[x])
                else:
                    _cnt = 0
                    if bt == '==':
                        _cnt = (self.count[x][str(b.R)] if str(b.R) in self.count[x] else 0)
                    elif bt == '!=':
                        _cnt = self._count[x][-1][1] - (self.count[x][str(b.R)] if str(b.R) in self.count[x] else 0)
                    elif bt == '<':
                        i = findLs(self._count[x], (b.R, 0))
                        _cnt = self._count[x][i][1]
                    elif bt == '<=':
                        i = findLs(self._count[x], (b.R, 0))
                        _cnt = self._count[x][i+(self._count[x][i][0]==b.R)][1]
                    elif bt == '>':
                        i = findLs(self._count[x], (b.R, 0))
                        _cnt = self._count[x][-1][1] - self._count[x][i+(self._count[x][i][0]==b.R)][1]
                    elif bt == '>=':
                        i = findLs(self._count[x], (b.R, 0))
                        _cnt = self._count[x][-1][1] - self._count[x][i][1]

                    # __cnt = 0
                    # for y in self.count[x].keys():
                    #     if eval(f'{y} {bt} {b.R}'):
                    #         __cnt += self.count[x][y]
                    # print(bt, x,  _cnt)
                    # assert(__cnt == _cnt)

                    cnt += _cnt

        # print(cnt, self.total)
        if cnt == 0 and type(self.csize) == dict and self.tbl == 'cast_info':
            cnt = 10
        return poss * Decimal(cnt) / self.total

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

        self.relate = {}
        for x in ['cast_info', 'title', 'movie_companies']:
            self.relate[x] = Relate(x)

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
            self._R = (prased[0].get_parent_name(), prased[0].get_name())
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
        self._L = (prased[0].get_parent_name(), prased[0].get_name())
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
                    return Decimal(eq) / len(col.sample)
                else:
                    return Decimal(1) / col.size # 
        else:
            if self._L in cal_wr and self._R in cal_wr:
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
            elif col0.size == db.tables[self.L[0]].size:
                poss = Decimal(1) / col0.size
            elif col1.size == db.tables[self.R[0]].size:
                poss = Decimal(1) / col1.size
            elif col0.counter != {} or col1.counter != {}:
                # print('working 5')
                poss = Decimal(1) # TODO
            else:
                poss = Decimal(1) - self.pLs() - self.pGt()
            
            cal_wr.add(self._L)
            cal_wr.add(self._R)
            
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
    
    def pIn(self):
        col = db.tables[self.L[0]].cols_dict[self.L[1]]
        if col.counter != {}:
            cnt = 0
            for x in self.R:
                if x in col.counter:
                    cnt += col.counter[x]
            return Decimal(cnt) / db.tables[self.L[0]].size
        else:
            cnt = 0
            for x in col.sample:
                cnt += (1 if x in self.R else 0)
            if cnt > 0:
                return Decimal(cnt) / len(col.sample)
            else:
                return Decimal(len(self.R)) / col.size
    
    def pBw(self):
        _R = self.R
        poss = Decimal(1)
        self.R = _R[0]
        poss -= self.pLs()
        self.R = _R[1]
        poss -= self.pGt()
        return poss
    
    def pLk(self):
        col = db.tables[self.L[0]].cols_dict[self.L[1]]
        if col.counter != {}:
            cnt = 0
            for x in col.counter.keys():
                if fnmatch.fnmatch(x, self.R.replace('%', '*')):
                    cnt += col.counter[x]
            return Decimal(cnt) / db.tables[self.L[0]].size
        else:
            cnt = 0
            for x in col.sample:
                cnt += (1 if fnmatch.fnmatch(x, self.R.replace('%', '*')) else 0)
            if cnt > 0:
                return Decimal(cnt) / len(col.sample)
            else:
                return Decimal(len(self.R)) / col.size
    
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
        self.p = self.p_dict.get(self.type)(self)
        return self.p

def run(level):
    with open(f'input/{level}.sql', "r") as f:
        data = f.read()
    # test = [15]
    # stm = sqlparse.split(data)
    # for _id in test:
    #     statement = stm[_id-1]
    with open(f'output/{level}.txt', "w") as f:
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
            # print(line_num)

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
            for idx, w in enumerate(wr):
                if RELATE_FUNC and idx > 0 and wr[idx-1].L[0] == w.L[0] and type(w.R) != tuple and type(wr[idx-1].R) != tuple and w.L[0] in db.relate:
                    _p = db.relate[w.L[0]].cal(wr[idx-1], w)
                    # print('*', _p)
                else:
                    _p = w.poss()
                    # print(_p)
                poss *= _p
            if RELATE_FUNC == False and int(poss * line_num) == 0:
                while int(poss * line_num) == 0:
                    poss *= len(wr) ** 2
                poss *= len(wr)
            print(int(poss * line_num), file=f)
    print(f'{level} completed.')


# db = Database("imdb/schematext.sql")
db = Database("imdb/schematext.sql", True)
print('init completed.')


if __name__ == "__main__":
    print('start.')
    run('easy')
    run('middle')
    RELATE_FUNC = False
    run('hard')