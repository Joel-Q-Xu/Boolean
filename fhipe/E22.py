import sys,os,time
import new2 as new1
from pympler import asizeof
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(1, os.path.abspath('..'))

#from charm.toolbox.pairinggroup import ZR
#from fhipe import ipe
import numpy as np
import csv
import sys, os, math, random
import time
from mpmath import mp
import sympy
import hashlib
import mnum


def read_table(infile, delimiter=","):
  with open(infile) as f:
    return [{k: str(v) for k, v in row.items()} for row in csv.DictReader(f, delimiter=delimiter, skipinitialspace=True)]

# infile contains a CSV of a database table
# returns an array of dictionaries where keys are
# the table attributes and values are strings


######################################### EXPERIMENTS ###########################################
# NOTE: you must add a row with column heads to the tables that you will use. one should use the
# attribute names used here: http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf#page=13
def experiment_1( sf,k, iters):
  print("--------------------------- in num="+str(len(k)))
  # table_a_file = "./table_a.in"
  # a="buyer_id"
  # pk_a="buyer_id"
  # x="phone_num"

  table_a_file = "newdata/part2" + sf + "-2.tbl"
  a = ["PARTKEY"]
  pk_a = "PARTKEY"

  x = "CONTAINER"

  #x = "selectivity"

  # table_b_file = "./table_b.in"
  # b = "buyer_id"
  # pk_b = "order_id"
  # y="amount"

  #table_b_file = "mdata/"+sf+"/orders.tbl"
  #b = ["custkey"]
  #pk_b = "orderkey"

  y = "selectivity"
  table_a = read_table(table_a_file, '|')
  l_a=len(table_a)

  #table_b = read_table(table_b_file, '|')
  #x_c = [["addressXSTf4,NCwDVaWNe6tEgvwfmRchLXak","addressIVhzIApeRb ot,c,E",1],["custkey3",0]]
  #y_c = [["selectivity100",1],["orderstatusO",1] ]
  #x_c = [["selectivity"],[["1"]]]
  x_c = [["SIZE"],[k]]
  #x_c = [["selectivity"], [["100"]]]
  #   IVhzIApeRb ot,c,E
  #  [0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1]
  #  [0, 0, 1, 1, 0, 0, 1, 0, 0, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 0, 0, 1, 1, 0, 0]
  y_c = [["selectivity"],[k]]
  aaa = list(table_a[0].keys())
  aaa = aaa[:-1]

  #bbb=list(table_b[0].keys())
  #bbb = bbb[:-1]
  #aaa = ["custkey", "nationkey", "selectivity"]
  #bbb = ["orderkey", "custkey", "orderstatus", "selectivity"]
  # "addressIVhzIApeRb ot,c,E",2

  #aaa = ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment", "selectivity"]
  #bbb = ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment", "selectivity"]

  setup_time=[]
  enc_time=[]
  token_time=[]
  search_time=[]

  for i in range(iters):

    time1 = time.perf_counter()
    o_a = new1.geto(table_a, a, aaa, table_a_file)
    time2 = time.perf_counter()
    setup_time.append(time2 - time1)

    # o_b = new1.geto(table_b, b, bbb, table_b_file)
    time1 = time.perf_counter()
    o_size = asizeof.asizeof(o_a)
    encrypted_table_a = new1.encryptTable(o_a, table_a, pk_a, a, x, aaa, table_a_file)
    # encrypted_table_b = new1.encryptTable(o_b, table_b, pk_b, b, y, bbb, table_b_file)
    edb_size = sys.getsizeof(encrypted_table_a[0][2])
    time2 = time.perf_counter()
    enc_time.append((time2 - time1) / l_a)

    time1 = time.perf_counter()

    # k=1
    k = random.randint(1, 10000000000)

    k_a = new1.encryptQuery(o_a, k, a, aaa, x_c)
    token_size = sys.getsizeof(k_a)
    # k_b = new1.encryptQuery(o_b, k, b, bbb, y_c)
    time2 = time.perf_counter()
    token_time.append(time2 - time1)

    time1 = time.perf_counter()

    hash_table = {}
    tolerance = 10
    for (pk, xx, c_a) in encrypted_table_a:
      d = new1.decrypt(k_a, c_a)
      d = round(d / tolerance) * tolerance
      hash_table[d] = pk

    matches = []
    """
    for (pk, yy, c_b) in encrypted_table_b:
      d = new1.decrypt(k_b, c_b)
      tolerance = 10

                  #.999999999997357
      # tolerance=8911670535002241527
      #time_start = time.time()

      match = new1.find_closest_value_with_tolerance(hash_table, d, tolerance)
      #time_end=time.time()
      #print(time_end-time_start)
      if match:
        matches.append((match, pk))"""
    time2 = time.perf_counter()
    search_time.append((time2 - time1) / l_a)
  a = mnum.ww_num(table_a, aaa)
  #b = mnum.ww_num(table_b, bbb)
  #m = a + b

  print('w_num:' + str(a))
  print(matches)
  print('token time: {}s on average'.format(sum(token_time) / len(token_time)))
  print('Search time: {}s on average'.format(sum(search_time) / (len(search_time))))

  print('num matches: {}'.format(len(matches)))

  print('')
  print('\n\n')
  return sum(token_time) / len(token_time),sum(search_time) / len(search_time)


T1=[]
T2=[]




t1,t2=experiment_1( "28",["1","2","3","4","5"] ,100)
T1.append(t1)
T2.append(t2)

t1,t2=experiment_1( "28",["1","2","3","4","5","6","7","8","9","10"] ,100)
T1.append(t1)
T2.append(t2)

t1,t2=experiment_1( "28",["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15"] ,100)
T1.append(t1)
T2.append(t2)

print(T1)
print(T2)
# MAIN
# experiments to test relationship between overall time and scale factor

'''
experiment_1( "0.01", 3)
experiment_1("0.02", 3)
experiment_1( "0.03", 3)
experiment_1( "0.04", 3)
experiment_1( "0.05", 3)
experiment_1( "0.06", 3)
experiment_1( "0.07", 3)
experiment_1("0.08", 3)
experiment_1("0.09", 3)
experiment_1("0.1", 3)

'''
# experiments to test relationship between overall time and IN_CLAUSE_MAX_SIZE

# experiment_2(1, 20)
# experiment_2(2, 20)
# experiment_2(3, 20)
# experiment_2(5, 20)
# experiment_2(5, 20)
# experiment_2(6, 20)
# experiment_2(7, 20)
# experiment_2(8, 20)
# experiment_2(9, 20)
# experiment_2(10, 20)

