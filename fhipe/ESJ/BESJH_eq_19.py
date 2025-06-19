


import pickle
import sys,os,time
import BESJH as new1
import gc
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(1, os.path.abspath('..'))
from pympler import asizeof
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
def experiment_1( sf, iters):
  print("--------------------------- sf="+sf)
  table_a_file = "mdata/Q19/q_" + sf + "/part.tbl"
  a = ["PARTKEY"]  # 连接属性
  pk_a = "PARTKEY"  # 不用管

  x = "PARTKEY"  # 不用管

  # x = "selectivity"

  table_b_file = "mdata/Q19/q_" + sf + "/lineitem.tbl"
  b = ["PARTKEY"]
  pk_b = "ORDERKEY"
  y = "ORDERKEY"

  # x = "selectivity"

  # table_b_file = "./table_b.in"
  # b = "buyer_id"
  # pk_b = "order_id"
  # y="amount"

  # table_b_file = "mdata/"+sf+"/orders.tbl"
  # b = ["custkey"]
  # pk_b = "orderkey"

  # y = "selectivity"
  table_a = read_table(table_a_file, '|')
  l_a = len(table_a)

  table_b = read_table(table_b_file, '|')
  l_b = len(table_b)
  # x_c = [["addressXSTf4,NCwDVaWNe6tEgvwfmRchLXak","addressIVhzIApeRb ot,c,E",1],["custkey3",0]]
  # y_c = [["selectivity100",1],["orderstatusO",1] ]
  # x_c = [["selectivity"],[["1"]]]
  x_c_1 = [["BRAND"],
           [["Brand#13"]]]
  x_c_1 = [["BRAND", "CONTAINER", "SIZE"],
           [["Brand#13"], ["SM CASE", "SM BOX", "SM PACK", "SM PKG"], ["1", "2", "3", "4", "5"]]]
  x_c_2 = [["BRAND", "CONTAINER", "SIZE"],
           [["Brand#1"], ["MED BAG", "MED BOX", "MED PACK", "MED PKG"],
            ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]]]
  x_c_3 = [["BRAND", "CONTAINER", "SIZE"],
           [["Brand#2"], ["LG CASE", "LG BOX", "LG PACK", "LG PKG"],
           ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"]]]
  x_c = []
  y_c = []
  x_c.append(x_c_1)
  x_c.append(x_c_2)
  x_c.append(x_c_3)

  # x_c = [["selectivity"], [["100"]]]
  #   IVhzIApeRb ot,c,E
  #  [0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1]
  #  [0, 0, 1, 1, 0, 0, 1, 0, 0, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 0, 0, 1, 1, 0, 0]
  y_c_1 = [["SHIPMODE"],[["TRUCK","REG AIR"]]]
  y_c_1 = [["QUANTITY", "SHIPMODE", "SHIPINSTRUCT"],
           [["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"], ["AIR", "AIP REG"], ["DELIVER IN PERSON"]]]
  y_c_2 = [["QUANTITY", "SHIPMODE", "SHIPINSTRUCT"],
          [["11", "12", "13", "14", "15", "16", "17", "18", "19", "20"], ["AIR", "AIP REG"], ["DELIVER IN PERSON"]]]
  y_c_3 = [["QUANTITY", "SHIPMODE", "SHIPINSTRUCT"],
           [["21", "22", "23", "24", "25", "26", "27", "28", "29", "30"], ["AIR", "AIP REG"], ["DELIVER IN PERSON"]]]
  y_c.append(y_c_1)
  y_c.append(y_c_2)
  y_c.append(y_c_3)
  aaa = list(table_a[0].keys())
  aaa = aaa[:-1]


  bbb = list(table_b[0].keys())
  bbb = bbb[:-1]

  setup_time = []
  enc_time = []
  token_time = []
  search_time = []
  setup_time_b = []
  enc_time_b = []
  token_time_b = []
  search_time_b = []

  for i in range(iters):

    time1 = time.perf_counter()
    ll_a ,d_a,seedm_a= new1.getmap(table_a, a, aaa, table_a_file)
    time2 = time.perf_counter()
    setup_time.append(time2 - time1)
    time1 = time.perf_counter()
    ll_b ,d_b,seedm_b= new1.getmap(table_b, b, bbb, table_b_file)
    time2 = time.perf_counter()
    setup_time_b.append(time2 - time1)
    row1 = new1.get_row_sylvester_fast_par(seedm_a, d_a, 1)

    o_size = len(pickle.dumps(aaa))+len(pickle.dumps(seedm_a))+len(pickle.dumps(d_a))
    o_size_b=len(pickle.dumps(bbb))+len(pickle.dumps(seedm_b))+len(pickle.dumps(d_b))
    time1 = time.perf_counter()
    o_a=new1.geto(ll_a,d_a,seedm_a)
    encrypted_table_a = new1.encryptTable(o_a, table_a, pk_a, a, x, aaa, table_a_file)
    time2 = time.perf_counter()
    #edb_size = len(pickle.dumps(encrypted_table_a[0][2])) + len(pickle.dumps(encrypted_table_b[0][2]))

    enc_time.append((time2 - time1))
    time1 = time.perf_counter()

    o_b=new1.geto(ll_b,d_b,seedm_b)


    encrypted_table_b = new1.encryptTable(o_b, table_b, pk_b, b, y, bbb, table_b_file)
    time2 = time.perf_counter()

    enc_time_b.append((time2 - time1))
    edb_size = len(pickle.dumps(encrypted_table_a[0][2]))
    edb_size_b=len(pickle.dumps(encrypted_table_b[0][2]))


    # k=1
    k_a = []
    k_b = []
    matches = []
    k = random.randint(1, 1000000)

    timet1 = 0
    timet11 = 0
    timet2 = 0
    timet22 = 0
    for i in range(len(x_c)):
      print(i)
      time1 = time.perf_counter()

      k_a.append(new1.encryptQuery(seedm_a, k, a, aaa, x_c[i],d_a,d_b))
      time2 = time.perf_counter()
      timet1 += (time2 - time1)
      time1 = time.perf_counter()

      k_b.append(new1.encryptQuery(seedm_b, k, b, bbb, y_c[i],d_b,d_a))
      time2 = time.perf_counter()
      timet11 += (time2 - time1)
      time1 = time.perf_counter()

      """k_a=k_a.astype(np.int64)
      k_b = k_b.astype(np.int64)"""
      token_size = sys.getsizeof(k_a)

      hash_table = {}
      tolerance = 0.1
      for (pk, xx, c_a) in encrypted_table_a:
        d = new1.decrypt(k_a[i], c_a)
        d = round(d / tolerance) * tolerance
        hash_table[d] = pk
      time2 = time.perf_counter()
      timet2 += (time2 - time1)
      time1 = time.perf_counter()

      for (pk, yy, c_b) in encrypted_table_b:
        d = new1.decrypt(k_b[i], c_b)
        d = round(d / tolerance) * tolerance
        match=hash_table.get(d)

        #match = new1.find_closest_value_with_tolerance(hash_table, d, tolerance)
        # time_end=time.time()
        # print(time_end-time_start)
        if match:
          matches.append((match, pk))
      print(matches)
      time2 = time.perf_counter()
      timet22 += (time2 - time1)
    token_time.append(timet1)
    token_time_b.append(timet11)
    search_time.append(timet2)
    search_time_b.append(timet22)

  a = mnum.ww_num(table_a, aaa)
  # b = mnum.ww_num(table_b, bbb)
  # m = a + b
  print('w_num:'+str(a))
  print('setup time: {}s on average'.format(sum(setup_time) / (len(setup_time) )))
  print('enc time: {}s on average'.format(sum(enc_time) / len(enc_time)))
  print('token time: {}s on average'.format(sum(token_time) / len(token_time)))
  print('Search time: {}s on average'.format(sum(search_time) / (len(search_time))))
  print('o_size:'+str(o_size))
  print('c_size:' + str(edb_size))
  print('num matches: {}'.format(len(matches)))

  print('')
  print('\n\n')
  return (sum(setup_time) / len(setup_time), sum(setup_time_b) / len(setup_time_b), sum(enc_time) / len(enc_time),
          sum(enc_time_b) / len(enc_time_b),
          sum(token_time) / len(token_time), sum(token_time_b) / len(token_time_b), sum(search_time) / len(search_time),
          sum(search_time_b) / len(search_time_b), o_size,
          o_size_b, edb_size, edb_size_b)


T1 = []
T11 = []
T2 = []
T22 = []
T3 = []
T33 = []
T4 = []
T44 = []
T5 = []
T55 = []
S1 = []
S11 = []
S2 = []
S22 = []

t1, t11, t2, t22, t3, t33, t4, t44, s1, s11, s2, s22 = experiment_1("20k", 1)
T1.append(t1)
T11.append(t11)
T2.append(t2)
T22.append(t22)
T3.append(t3)
T33.append(t33)
T4.append(t4)
T44.append(t44)

S1.append(s1)
S11.append(s11)
S2.append(s2)
S22.append(s22)
t1, t11, t2, t22, t3, t33, t4, t44, s1, s11, s2, s22 = experiment_1("40k", 1)
T1.append(t1)
T11.append(t11)
T2.append(t2)
T22.append(t22)
T3.append(t3)
T33.append(t33)
T4.append(t4)
T44.append(t44)

S1.append(s1)
S11.append(s11)
S2.append(s2)
S22.append(s22)
t1, t11, t2, t22, t3, t33, t4, t44, s1, s11, s2, s22 = experiment_1("60k", 1)
T1.append(t1)
T11.append(t11)
T2.append(t2)
T22.append(t22)
T3.append(t3)
T33.append(t33)
T4.append(t4)
T44.append(t44)

S1.append(s1)
S11.append(s11)
S2.append(s2)
S22.append(s22)
t1, t11, t2, t22, t3, t33, t4, t44, s1, s11, s2, s22 = experiment_1("80k", 1)
T1.append(t1)
T11.append(t11)
T2.append(t2)
T22.append(t22)
T3.append(t3)
T33.append(t33)
T4.append(t4)
T44.append(t44)
S1.append(s1)
S11.append(s11)
S2.append(s2)
S22.append(s22)
t1, t11, t2, t22, t3, t33, t4, t44, s1, s11, s2, s22 = experiment_1("100k", 1)
T1.append(t1)
T11.append(t11)
T2.append(t2)
T22.append(t22)
T3.append(t3)
T33.append(t33)
T4.append(t4)
T44.append(t44)

S1.append(s1)
S11.append(s11)
S2.append(s2)
S22.append(s22)

print("setup_a")
print(T1)
print("setup_b")
print(T11)
print("enc_a")
print(T2)
print("enc_b")
print(T22)
print("token_a")
print(T3)
print("token_b")
print(T33)
print("search_a")
print(T4)
print("search_b")
print(T44)

print(S1)
print(S11)
print(S2)
print(S22)

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

