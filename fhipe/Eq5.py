import sys,os,time
import new5 as new1
import gc
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(1, os.path.abspath('..'))

#from charm.toolbox.pairinggroup import ZR
#from fhipe import ipe
import numpy as np
import csv
import pickle
import sys, os, math, random
import time
from mpmath import mp
import sympy
import hashlib
import mnum
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

  table_b = read_table(table_b_file, '|')
  l_b=len(table_b)
  #x_c = [["addressXSTf4,NCwDVaWNe6tEgvwfmRchLXak","addressIVhzIApeRb ot,c,E",1],["custkey3",0]]
  #y_c = [["selectivity100",1],["orderstatusO",1] ]
  x_c = [["SIZE1", "SIZE2", "SIZE3", "SIZE4", "SIZE5", "SIZE6", "SIZE7", "SIZE8", 1], ["BRANDBrand#13", 0],["TYPEPROMO BURNISHED COPPER", 0]]
  y_c = [["SUPPKEY358", 0]]
  aaa = list(table_a[0].keys())
  aaa = aaa[:-1]

  bbb = list(table_b[0].keys())
  bbb = bbb[:-1]

  w_a = mnum.ww_num(table_a, aaa)
  w_b = mnum.ww_num(table_b, bbb)

  print('w_num:' + str(w_a) + "   " + str(w_b))

  setup_time=[]
  enc_time=[]
  token_time=[]
  search_time=[]






  for i in range(iters):
    o_a={}
    tt_a=[]
    o_b = {}
    tt_b = []

    time1=time.perf_counter()
    o_a_keys_list= new1.geto_n(table_a, a, aaa, table_a_file)
    n_a = new1.get_d(len(o_a_keys_list))
    o_a_values_list = new1.geto_encode(n_a)

    tt_a.append(new1.decimal_to_hadamard_row(o_a_values_list[0], n_a))
    for i in range(len(a)):
      tt_a.append(new1.decimal_to_hadamard_row(o_a_values_list[i + 1], n_a))

    oo_a_keys_list = o_a_keys_list[1 + len(a):]
    oo_a_values_list = o_a_values_list[1 + len(a):]
    oo_a = dict(zip(oo_a_keys_list, oo_a_values_list))
    print(n_a)

    o_b_keys_list= new1.geto_n(table_b, b, bbb, table_b_file)
    n_b = new1.get_d(len(o_b_keys_list))
    o_b_values_list = new1.geto_encode(n_b)
    print(n_b)

    tt_b.append(new1.decimal_to_hadamard_row(o_b_values_list[0], n_b))
    for i in range(len(b)):
      tt_b.append(new1.decimal_to_hadamard_row(o_b_values_list[i + 1], n_b))

    oo_b_keys_list = o_b_keys_list[1 + len(b):]
    oo_b_values_list = o_b_values_list[1 + len(b):]
    oo_b = dict(zip(oo_b_keys_list, oo_b_values_list))
    time2 = time.perf_counter()
    setup_time.append(time2 - time1)
    o_size = len(pickle.dumps(oo_a)) + len(pickle.dumps(asizeof.asizeof(tt_a)))

    #b = mnum.ww_num(table_b, bbb)
    #d_b=new1.next_power_of_two(b+2)




    #o_b,d_b = new1.geto(table_b, b, bbb, table_b_file)
    time1 = time.perf_counter()

    encrypted_table_a = new1.encryptTable(tt_a,oo_a, table_a, pk_a, a, x, aaa, table_a_file)
    encrypted_table_b = new1.encryptTable(tt_b, oo_b, table_b, pk_b, b, x, bbb, table_b_file)
    #encrypted_table_b = new1.encryptTable(o_b, table_b, pk_b, b, y, bbb, table_b_file)

    time2 = time.perf_counter()
    enc_time.append((time2 - time1) ) #/ (l_a+l_b)

    edb_size = len(pickle.dumps(encrypted_table_a[0][2]))

    time1=time.perf_counter()

    #k=1
    k = random.randint(1, 10000)
    # k=1
    k_a = new1.encryptQuery(tt_a,oo_a, k, a, table_a_file, x_c,n_b)
    k_b = new1.encryptQuery(tt_b, oo_b, k, b, table_b_file, y_c, n_a)
    #k_b = new1.encryptQuery(o_b, k, b, table_b_file, y_c,d_a)
    token_size = sys.getsizeof(k_a)+sys.getsizeof(k_b)
    time2=time.perf_counter()
    token_time.append(time2-time1)
    k_a = k_a.astype(np.int64)
    k_b = k_b.astype(np.int64)


    time1=time.perf_counter()
    hash_table = {}
    for (pk, xx, c_a) in encrypted_table_a:
      d = new1.decrypt(k_a, c_a)

      hash_table[d] = pk

    matches = []
    for (pk, yy, c_b) in encrypted_table_b:

      d = new1.decrypt(k_b, c_b)
      match=hash_table.get(d)

      #tolerance = 0.0000000000001

      #match = new1.find_closest_value_with_tolerance(hash_table, d, tolerance)

      if match:
        matches.append((match, pk))

    time2=time.perf_counter()
    search_time.append(time2-time1)
    print(matches)


  a = mnum.ww_num(table_a, aaa)
  b = mnum.ww_num(table_b, bbb)
  m = a + b
  print('w_num:'+str(m))
  print('setup time: {}s on average'.format(sum(setup_time) / (len(setup_time) )))
  print('enc time: {}s on average'.format(sum(enc_time) / len(enc_time)))
  print('token time: {}s on average'.format(sum(token_time) / len(token_time)))
  print('Search time: {}s on average'.format(sum(search_time) / (len(search_time))))
  print('o_size:'+str(o_size))
  print('c_size:' + str(edb_size))
  print('num matches: {}'.format(len(matches)))

  print('')
  print('\n\n')
  return (sum(setup_time) / len(setup_time),sum(enc_time) / len(enc_time),sum(token_time) / len(token_time),sum(search_time) / len(search_time),o_size,
          edb_size)
T1=[]
T2=[]
T3=[]
T4=[]
S1=[]
S2=[]

t1,t2,t3,t4,s1,s2=experiment_1( "4k", 10)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)

t1,t2,t3,t4,s1,s2=experiment_1( "8k", 10)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)

t1,t2,t3,t4,s1,s2=experiment_1( "12k", 10)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)

t1,t2,t3,t4,s1,s2=experiment_1( "16k", 10)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)

t1,t2,t3,t4,s1,s2=experiment_1( "20k", 10)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)

print(T1)
print(T2)
print(T3)
print(T4)
print(S1)
print(S2)

