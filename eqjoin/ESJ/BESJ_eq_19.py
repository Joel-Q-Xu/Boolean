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

  table_a_file = "mdata/Q19/q_" + sf + "/part.tbl"
  a = ["PARTKEY"]  # 连接属性
  pk_a = "PARTKEY"  # 不用管

  x = "PARTKEY"  # 不用管

  # x = "selectivity"

  table_b_file = "mdata/Q19/q_" + sf + "/lineitem.tbl"
  b = ["PARTKEY"]
  pk_b = "ORDERKEY"
  y = "ORDERKEY"


  table_a = read_table(table_a_file, '|')
  l_a=len(table_a)
  print(l_a)
  table_b = read_table(table_b_file, '|')
  l_b=len(table_b)
  print(l_b)
  #x_c = [["addressXSTf4,NCwDVaWNe6tEgvwfmRchLXak","addressIVhzIApeRb ot,c,E",1],["custkey3",0]]
  #y_c = [["selectivity100",1],["orderstatusO",1] ]
  #x_c_1 = [["BRANDBrand#13",1]]
  x_c_1 = [["BRANDBrand#13",1] ,["CONTAINERSM CASE", "CONTAINERSM BOX", "CONTAINERSM PACK", "CONTAINERSM PKG",1], ["SIZE1", "SIZE2", "SIZE3", "SIZE4", "SIZE5",1]]
  x_c_2 = [["BRANDBrand#1",1], ["CONTAINERMED BAG", "CONTAINERMED BOX", "CONTAINERMED PACK", "CONTAINERMED PKG",1],
            ["SIZE1", "SIZE2", "SIZE3", "SIZE4", "SIZE5", "SIZE6", "SIZE7", "SIZE8", "SIZE9", "SIZE10",1]]
  x_c_3 = [["BRANDBrand#2",1], ["CONTAINERLG CASE", "CONTAINERLG BOX", "CONTAINERLG PACK", "CONTAINERLG PKG",1],
            ["SIZE1", "SIZE2", "SIZE3", "SIZE4", "SIZE5", "SIZE6", "SIZE7", "SIZE8", "SIZE9", "SIZE10", "SIZE11", "SIZE12", "SIZE13", "SIZE14", "SIZE15",1]]
  x_c = []
  y_c = []
  x_c.append(x_c_1)
  x_c.append(x_c_2)
  x_c.append(x_c_3)
  #y_c_1 = [ ["SHIPMODETRUCK", "SHIPMODEREG AIR",1]]
  y_c_1 = [["QUANTITY1", "QUANTITY2", "QUANTITY3", "QUANTITY4", "QUANTITY5", "QUANTITY6", "QUANTITY7", "QUANTITY8", "QUANTITY9", "QUANTITY10",1],
            ["SHIPMODEAIR", "SHIPMODEAIP REG",1], ["SHIPINSTRUCTDELIVER IN PERSON",1]]
  y_c_2 = [["QUANTITY11", "QUANTITY12", "QUANTITY13", "QUANTITY14", "QUANTITY15", "QUANTITY16", "QUANTITY17", "QUANTITY18", "QUANTITY19", "QUANTITY20",1],
            ["SHIPMODEAIR", "SHIPMODEAIP REG",1], ["SHIPINSTRUCTDELIVER IN PERSON",1]]
  y_c_3 = [["QUANTITY21", "QUANTITY22", "QUANTITY23", "QUANTITY24", "QUANTITY25", "QUANTITY26", "QUANTITY27", "QUANTITY28", "QUANTITY29", "QUANTITY30",1],
            ["SHIPMODEAIR", "SHIPMODEAIP REG",1], ["SHIPINSTRUCTDELIVER IN PERSON",1]]
  y_c.append(y_c_1)
  y_c.append(y_c_2)
  y_c.append(y_c_3)
  aaa = list(table_a[0].keys())
  aaa = aaa[:-1]

  bbb = list(table_b[0].keys())
  bbb = bbb[:-1]
  # "addressIVhzIApeRb ot,c,E",2
  #aaa = ["custkey", "nationkey", "mktsegment", "selectivity"]

  setup_time=[]
  enc_time=[]
  token_time=[]
  search_time=[]






  for i in range(iters):

    time1=time.perf_counter()
    ss_a,d_a ,xorf_a,mph_a,seedm_a= new1.getmap(table_a, a, aaa, table_a_file)
    print(d_a)
    ss_b, d_b ,xorf_b,mph_b,seedm_b= new1.getmap(table_b, b, bbb, table_b_file)
    print(d_b)

    time2 = time.perf_counter()
    setup_time.append(time2 - time1)

    #b = mnum.ww_num(table_b, bbb)
    #d_b=new1.next_power_of_two(b+2)
    #d_b=2468
    row1 = new1.get_row_sylvester_fast_par(seedm_a, d_a, 1)
    mph_a.save("mya.mph")
    mph_b.save("myb.mph")
    # Windows: 右键属性 看大小 or 用 Python:

    import os
    size_in_bytes = os.path.getsize("mya.mph") + os.path.getsize("myb.mph")
    o_size = len(pickle.dumps(d_a)) + xorf_a.size_in_bytes() + len(pickle.dumps(seedm_a)) + len(
      pickle.dumps(d_b)) + xorf_b.size_in_bytes() + len(
      pickle.dumps(seedm_b)) + size_in_bytes  # +len(pickle.dumps(o_b))

    time1 = time.perf_counter()
    o_b = new1.geto(ss_b, mph_b, seedm_b, d_b)
    print(d_b)

    #o_size = len(pickle.dumps(o_a)) #+len(pickle.dumps(o_b))
    o_a=new1.geto(ss_a,mph_a,seedm_a,d_a)
    print(d_a)



    encrypted_table_a = new1.encryptTable(o_a, table_a, pk_a, a, x, aaa, table_a_file)
    encrypted_table_b = new1.encryptTable(o_b, table_b, pk_b, b, y, bbb, table_b_file)


    edb_size = len(pickle.dumps(encrypted_table_a[0][2]))+len(pickle.dumps(encrypted_table_b[0][2])) #+len(pickle.dumps(encrypted_table_b[0][2]))



    time2 = time.perf_counter()
    enc_time.append((time2 - time1) ) #/ (l_a+l_b)
    k_a = []
    k_b = []
    matches = []
    k = random.randint(1, 1000000)

    timet1 = 0
    timet2 = 0
    for i in range(len(x_c)):
      print(i)
      time1 = time.perf_counter()

      # k=1
      k = random.randint(1, 10000)
      # k=1
      k_a.append(new1.encryptQuery(xorf_a, mph_a, seedm_a, k, a, table_a_file, x_c[i], d_a, d_b))
      k_b.append( new1.encryptQuery(xorf_b, mph_b, seedm_b, k, b, table_b_file, y_c[i], d_b, d_a))
      token_size = sys.getsizeof(k_a) + sys.getsizeof(k_b)
      print("bv nnm,")

      time2 = time.perf_counter()
      timet1 += (time2 - time1)
      """
      k_a = k_a.astype(np.int64)
      k_b = k_b.astype(np.int64)"""

      time1 = time.perf_counter()
      hash_table = {}
      for (pk, xx, c_a) in encrypted_table_a:
        d = new1.decrypt(k_a[i], c_a)

        hash_table[d] = pk

      matches = []

      for (pk, yy, c_b) in encrypted_table_b:

        d = new1.decrypt(k_b[i], c_b)

        match = hash_table.get(d)

        if match:
          matches.append((match, pk))

      time2 = time.perf_counter()
      timet2 += (time2 - time1)


      print(len(matches))
      print(matches)
    token_time.append(timet1)
    search_time.append(timet2)
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
  return (sum(setup_time) / len(setup_time),sum(enc_time) / len(enc_time),sum(token_time) / len(token_time),sum(search_time) / len(search_time),o_size,
          edb_size)
T1=[]
T2=[]
T3=[]
T4=[]
S1=[]
S2=[]


t1,t2,t3,t4,s1,s2=experiment_1( "20k", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "40k", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "60k", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "80k", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "100k", 1)
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

