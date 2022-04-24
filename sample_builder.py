import os
import random

# Generates fake sample data

TYPES = ['deposit', 'withdrawal', 'dispute', 'resolve', 'chargeback']
TYPE_WEIGHTS = [10, 8, 4, 2, 1]
MAX_CLIENTS = 20
MAX_AMOUNT = 1000
MAX_TRANSACTIONS = MAX_CLIENTS * 100

with open('large_sample.csv', 'w') as f:
    f.write('type,client,tx,amount\n')
    for i in range(1, MAX_TRANSACTIONS):
        type = random.choices(TYPES, TYPE_WEIGHTS)[0]
        if type in ('dispute', 'resolve', 'chargeback') and i > 1:
            tx = random.randint(1, i-1)
            amount = 0.0
        else:
            tx = i
            amount = random.uniform(1, MAX_AMOUNT)
        f.write(
            "%s,%d,%d,%.4f\n" % (
                type,
                random.randint(1, MAX_CLIENTS),
                tx,
                amount
            )
        )