import numpy as np

import csv


def read_table(infile, delimiter=","):
  with open(infile) as f:
    return [{k: str(v) for k, v in row.items()} for row in csv.DictReader(f, delimiter=delimiter, skipinitialspace=True)]

def w_num(table,aaa):

    num_a=[]
    for i in range(len(aaa)):
        h_a = set()
        for row in table:
            h_a.add(row[aaa[i]])
        num_a.append(len(h_a))

    return num_a






