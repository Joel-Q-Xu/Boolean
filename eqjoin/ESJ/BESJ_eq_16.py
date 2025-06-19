import sys,os,time
import BESJ as new1
import gc
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
import pickle
from pympler import asizeof
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
  # table_a_file = "./table_a.in"
  # a="buyer_id"
  # pk_a="buyer_id"
  # x="phone_num"

  table_a_file = "mdata/Q16/q_" + sf + "/part.tbl"
  a = ["PARTKEY"]  # 连接属性
  pk_a = "PARTKEY"  # 不用管

  x = "PARTKEY"  # 不用管

  # x = "selectivity"

  table_b_file = "mdata/Q16/q_" + sf + "/partsupp.tbl"
  b = ["PARTKEY"]
  pk_b = "SUPPKEY"
  y = "PARTKEY"


  table_a = read_table(table_a_file, '|')
  l_a=len(table_a)
  print(l_a)
  table_b = read_table(table_b_file, '|')
  l_b=len(table_b)
  print(l_b)
  #x_c = [["addressXSTf4,NCwDVaWNe6tEgvwfmRchLXak","addressIVhzIApeRb ot,c,E",1],["custkey3",0]]
  #y_c = [["selectivity100",1],["orderstatusO",1] ]
  x_c = [["SIZE1", "SIZE2", "SIZE3", "SIZE4", "SIZE5", "SIZE6", "SIZE7", "SIZE8", 1], ["BRANDBrand#13", 0],
         ["TYPEPROMO BURNISHED COPPER", 0]]  # , ["TYPEPROMO BURNISHED COPPER", 0]
  y_c = [["SUPPKEY358", 0]]
  aaa = list(table_a[0].keys())
  aaa = aaa[:-1]

  bbb = list(table_b[0].keys())
  bbb = bbb[:-1]
  # "addressIVhzIApeRb ot,c,E",2
  #aaa = ["custkey", "nationkey", "mktsegment", "selectivity"]

  setup_time = []
  enc_time = []
  token_time = []
  search_time = []
  setup_time_b = []
  enc_time_b = []
  token_time_b = []
  search_time_b = []






  for i in range(iters):

    time1=time.perf_counter()
    ss_a,d_a ,xorf_a,mph_a,seedm_a= new1.getmap(table_a, a, aaa, table_a_file)
    time2 = time.perf_counter()
    setup_time.append(time2 - time1)
    time1 = time.perf_counter()
    ss_b, d_b ,xorf_b,mph_b,seedm_b= new1.getmap(table_b, b, bbb, table_b_file)

    time2 = time.perf_counter()
    setup_time_b.append(time2 - time1)

    #b = mnum.ww_num(table_b, bbb)
    #d_b=new1.next_power_of_two(b+2)
    #d_b=2468
    row1 = new1.get_row_sylvester_fast_par(seedm_a, d_a, 1)
    mph_a.save("mya.mph")
    mph_b.save("myb.mph")
    # Windows: 右键属性 看大小 or 用 Python:

    import os
    size_in_bytes = os.path.getsize("mya.mph")+os.path.getsize("myb.mph")
    o_size = len(pickle.dumps(d_a))+xorf_a.size_in_bytes()+len(pickle.dumps(seedm_a))+os.path.getsize("mya.mph")
    o_size_b=len(pickle.dumps(d_b))+xorf_b.size_in_bytes()+len(pickle.dumps(seedm_b))+ os.path.getsize("myb.mph") # +len(pickle.dumps(o_b))


    time1 = time.perf_counter()

    #o_size = len(pickle.dumps(o_a)) #+len(pickle.dumps(o_b))
    o_a=new1.geto(ss_a,mph_a,seedm_a,d_a)
    encrypted_table_a = new1.encryptTable(o_a, table_a, pk_a, a, x, aaa, table_a_file)
    time2 = time.perf_counter()
    enc_time.append((time2 - time1))  # / (l_a+l_b)

    time1 = time.perf_counter()
    o_b = new1.geto(ss_b, mph_b, seedm_b,d_b)
    encrypted_table_b = new1.encryptTable(o_b, table_b, pk_b, b, y, bbb, table_b_file)
    time2 = time.perf_counter()
    enc_time_b.append((time2 - time1))  # / (l_a+l_b)


    edb_size = len(pickle.dumps(encrypted_table_a[0][2]))
    edb_size_b=len(pickle.dumps(encrypted_table_b[0][2])) #+len(pickle.dumps(encrypted_table_b[0][2]))

    time1=time.perf_counter()

    #k=1
    k = random.randint(1, 10000)
    # k=1
    k_a = new1.encryptQuery(xorf_a,mph_a,seedm_a, k, a, table_a_file, x_c,d_a,d_b)
    time2 = time.perf_counter()
    token_time.append(time2 - time1)

    time1 = time.perf_counter()
    k_b = new1.encryptQuery(xorf_b,mph_b,seedm_b, k, b, table_b_file, y_c,d_b,d_a)
    token_size = sys.getsizeof(k_a)+sys.getsizeof(k_b)
    print("bv nnm,")

    time2=time.perf_counter()
    token_time_b.append(time2-time1)
    k_a = k_a.astype(np.int64)
    k_b = k_b.astype(np.int64)


    time1=time.perf_counter()
    hash_table = {}
    for (pk, xx, c_a) in encrypted_table_a:
      d = new1.decrypt(k_a, c_a)


      hash_table[d] = pk
    time2 = time.perf_counter()
    search_time.append(time2 - time1)
    time1 = time.perf_counter()


    matches = []

    for (pk, yy, c_b) in encrypted_table_b:

      d = new1.decrypt(k_b, c_b)

      match = hash_table.get(d)

      if match:
        matches.append((match, pk))

    time2 = time.perf_counter()
    search_time_b.append(time2 - time1)
    print(len(matches))
    print(matches)



  a = mnum.ww_num(table_a, aaa)
  b = mnum.ww_num(table_b, bbb)
  m = a + b
  print('w_num:'+str(a)+"   "+str(b))
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

t1, t11, t2, t22, t3, t33, t4, t44, s1, s11, s2, s22 = experiment_1("4k", 1)
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
t1, t11, t2, t22, t3, t33, t4, t44, s1, s11, s2, s22 = experiment_1("8k", 1)
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
t1, t11, t2, t22, t3, t33, t4, t44, s1, s11, s2, s22 = experiment_1("12k", 1)
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
t1, t11, t2, t22, t3, t33, t4, t44, s1, s11, s2, s22 = experiment_1("16k", 1)
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

