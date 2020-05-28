def run(level):
    with open(f'output/{level}.txt', 'r') as f:
        s0 = [int(x[:-1]) for x in f.readlines()]

    with open(f'result/{level}.normal', 'r') as f:
        s1 = [int(x[:-1]) for x in f.readlines()]

    ret = [(max(s1[idx], s0[idx])/(min(s1[idx], s0[idx])+1e-1), idx) for idx in range(len(s1))]

    ret.sort(reverse=True)

    with open(f'analyse/{level}.csv', 'w') as f:
        for x, idx in ret:
            print(f'{idx+1}, {s1[idx]}, {s0[idx]}, {x}', file=f)

if __name__ == "__main__":
    run('easy')
    run('middle')
    run('hard')