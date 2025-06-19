import sys,os,time
import new_w as new1
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


def read_table(infile, delimiter=","):
  with open(infile) as f:
    return [{k: str(v) for k, v in row.items()} for row in csv.DictReader(f, delimiter=delimiter, skipinitialspace=True)]

# infile contains a CSV of a database table
# returns an array of dictionaries where keys are
# the table attributes and values are strings


######################################### EXPERIMENTS ###########################################
# NOTE: you must add a row with column heads to the tables that you will use. one should use the
# attribute names used here: http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf#page=13
def experiment_1( sf,h, iters):
  print("--------------------------- hash="+str(h))
  # table_a_file = "./table_a.in"
  # a="buyer_id"
  # pk_a="buyer_id"
  # x="phone_num"

  table_a_file = "mdata/Q16/q_" + sf + "/part.tbl"
  a = ["PARTKEY"]  # 连接属性
  pk_a = "PARTKEY"  # 不用管

  x = "PARTKEY"  # 不用管

  # x = "selectivity"
  table_b_file = "mdata/q_0.04/lineitem.tbl"
  # table_b_file = "mdata/q_" + sf + "/lineitem.tbl"
  b = ["PARTKEY"]
  pk_b = "ORDERKEY"
  y = "ORDERKEY"

  table_a = read_table(table_a_file, '|')
  l_a = len(table_a)
  print(l_a)

  table_b = read_table(table_b_file, '|')
  l_b = len(table_b)
  print(l_b)

  #x_c = [["addressXSTf4,NCwDVaWNe6tEgvwfmRchLXak","addressIVhzIApeRb ot,c,E",1],["custkey3",0]]
  #y_c = [["selectivity100",1],["orderstatusO",1] ]
  #x_c = [["selectivity"],[["1"]]]

  x_c= [["SIZE"],[["1","2","3","4","5"]]]
  #x_c = [["SIZE"],[["1","2","3","4","5","6","7","8","9","10"]]]
  #x_c = [["SIZE"],[["1", "2", "3", "4", "5", "6", "7", "8", "9", "10","11", "12", "13", "14", "15"]]]
  y_c = [["SHIPMODE"],[["AIR","AIP REG"]]]

  #x_c = [["selectivity"],[["100","75"]]]
  #x_c = [["selectivity"], [["100"]]]
  #   IVhzIApeRb ot,c,E
  #  [0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1]
  #  [0, 0, 1, 1, 0, 0, 1, 0, 0, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 0, 0, 1, 1, 0, 0]
  #y_c = [["selectivity"],[["100","75"]]]

  aaa = list(table_a[0].keys())
  aaa = aaa[:-1]

  bbb = list(table_b[0].keys())
  bbb = bbb[:-1]


  # "addressIVhzIApeRb ot,c,E",2

  #aaa = ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment", "selectivity"]
  #bbb = ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment", "selectivity"]

  setup_time=[]
  enc_time=[]
  token_time=[]
  search_time=[]
  num=[]



  time1=time.perf_counter()
  o_a = new1.geto(table_a, a, aaa, table_a_file)
  time2=time.perf_counter()
  setup_time.append(time2 - time1)

  o_b = new1.geto(table_b, b, bbb, table_b_file)
  o_size=len(pickle.dumps(o_a))


  for i in range(iters):
    m="encryptTable"+str(h)
    time1 = time.perf_counter()
    m=getattr(new1,m)
    encrypted_table_a = m(o_a, table_a, pk_a, a, x, aaa, table_a_file)
    time2=time.perf_counter()

    #edb_size = len(pickle.dumps(encrypted_table_a[0][2]))
    edb_size=encrypted_table_a[0][2].nbytes

    enc_time.append(time2-time1) #/l_a
    encrypted_table_b = m(o_b, table_b, pk_b, b, y, bbb, table_b_file)


    #k=1
    time1 = time.perf_counter()
    k = random.randint(1, 10000000000)
    m = "encryptQuery" + str(h)
    m = getattr(new1, m)

    k_a = m(o_a, k, a, aaa, x_c)
    time2 = time.perf_counter()
    token_time.append(time2-time1)

    token_size = sys.getsizeof(k_a)
    k_b = m(o_b, k, b, bbb, y_c)



    time1 = time.perf_counter()
    hash_table = {}
    tolerance = 10
    for (pk, xx, c_a) in encrypted_table_a:
      d = new1.decrypt(k_a, c_a)

      d = round(d / tolerance) * tolerance


      hash_table[d] = pk
    time2 = time.perf_counter()

    matches = []
    for (pk, yy, c_b) in encrypted_table_b:
      d = new1.decrypt(k_b, c_b)
      d = round(d / tolerance) * tolerance


                  #.999999999997357
      # tolerance=8911670535002241527
      #time_start = time.time()

      match =hash_table.get(d)
      #time_end=time.time()
      #print(time_end-time_start)
      if match:
        matches.append((match, pk))

    print(matches)
    print(len(matches))

    search_time.append(time2-time1)
    num.append(len(matches))
  a = mnum.ww_num(table_a, aaa)
  b = mnum.ww_num(table_b, bbb)
  m = a + b
  print('w_num:'+str(m))
  print(num)
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



t1,t2,t3,t4,s1,s2=experiment_1( "4k", 32,100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "4k", 64,100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "4k", 128,100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "4k", 160,100)
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

