import csv
import json
import random

tbl = 'cast_info'
col0 = -1
col1 = 1

data = {}
data['col0'] = 'role_id'
data['col1'] = 'person_id'

val0 = []
val1 = []

with open('imdb/'+tbl+'.csv', "r") as f:
    line = 0
    reader = csv.reader(f)
    for row in reader:
        if row[col0] != '' and row[col1] != '':
            val0.append(int(row[col0]))
            val1.append(int(row[col1]))
            line += 1

print('1')

count = {}
for x in list(set(val0)):
    count[x] = []

print('2')

for i in range(line):
    count[val0[i]].append(val1[i])

print('3')

csize = {}

for x in list(set(val0)):
    csize[x] = len(count[x])
    count[x] = random.sample(count[x], 200000)

print('4')

data['total'] = line
data['csize'] = csize
data['count'] = count

with open(f'sample_relate/{tbl}.json', 'w') as f:
    json.dump(data, f)