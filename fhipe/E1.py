import pickle
import sys,os,time
import new1 as new1
import gc
from pympler import asizeof
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(1, os.path.abspath('..'))

#from charm.toolbox.pairinggroup import ZR
#from fhipe import ipe
import numpy as np
import csv
import sys, os, math, random
import time

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
  # table_a_file = "./table_a.in"
  # a="buyer_id"
  # pk_a="buyer_id"
  # x="phone_num"

  table_a_file = "newdata/part2"+sf+".tbl"
  a = ["PARTKEY"]
  pk_a = "PARTKEY"

  x="CONTAINER"

  #x = "selectivity"

  # table_b_file = "./table_b.in"
  # b = "buyer_id"
  # pk_b = "order_id"
  # y="amount"

  #table_b_file = "mdata/"+sf+"/orders.tbl"

  table_a = read_table(table_a_file, '|')
  l_a=len(table_a)
  print(l_a)

  #table_b = read_table(table_b_file, '|')
  #l_b=len(table_b)
  #x_c = [["addressXSTf4,NCwDVaWNe6tEgvwfmRchLXak","addressIVhzIApeRb ot,c,E",1],["custkey3",0]]
  #y_c = [["selectivity100",1],["orderstatusO",1] ]
  x_c = [["CONTAINERSM CASE   ","CONTAINERSM PKG    ",1],["SIZE1","SIZE2",1]]
  y_c = [["selectivity100","selectivity50",1],["orderstatusO","orderstatusF",1]]
  aaa = list(table_a[0].keys())
  aaa = aaa[:-1]


  #bbb = list(table_b[0].keys())
  #bbb = bbb[:-1]

  #aaa = ["custkey", "address", "nationkey", "mktsegment", "selectivity"]
  bbb = ["orderkey", "custkey", "orderstatus", "selectivity"]
  #aaa = ["custkey", "nationkey", "mktsegment", "selectivity"]


  setup_time=[]
  enc_time=[]
  token_time=[]
  search_time=[]

  for i in range(iters):




    time1=time.perf_counter()
    o_a = new1.geto(table_a, a, aaa, table_a_file)

    time2 = time.perf_counter()
    setup_time.append(time2 - time1)

    #o_b = new1.geto(table_b, b, bbb, table_b_file)




    o_size = len(pickle.dumps(o_a))



    time1 = time.perf_counter()
    """ 
    print(o_size)
    o_a_keys_list = list(o_a.keys())
    o_a_values_list = list(o_a.values())
    o_size = asizeof.asizeof(o_a_values_list) + asizeof.asizeof(o_a_keys_list)
    print(o_size)
    
    o_a = dict(zip(o_a_keys_list, o_a_values_list))
    """

    encrypted_table_a = new1.encryptTable(o_a, table_a, pk_a, a, x, aaa, table_a_file)
    time2 = time.perf_counter()
    #encrypted_table_b = new1.encryptTable(o_b, table_b, pk_b, b, y, bbb, table_b_file)
    #edb_size = len(pickle.dumps(encrypted_table_a[0][2]))
    edb_size = encrypted_table_a[0][2].nbytes

    enc_time.append((time2 - time1) /l_a)
    # print(encrypted_table_a[0][2])



    time1=time.perf_counter()

    #k=1
    k = random.randint(1, 10000000000)
    # k=1
    k_a = new1.encryptQuery(o_a, k, a, table_a_file, x_c)

    time2=time.perf_counter()
    # k_b = new1.encryptQuery(o_b, k, b, table_b_file, y_c)
    token_size = sys.getsizeof(k_a)
    token_time.append(time2-time1)
    """cnt=0
    for i in range(len(k_a)):
        if k_a[i]==0:
            cnt+=1
    print(cnt)"""

    time1=time.perf_counter()



    hash_table = {}
    tolerance = 8
    for (pk, xx, c_a) in encrypted_table_a:


      d = new1.decrypt(k_a, c_a)
      d = round(d / tolerance) * tolerance



      hash_table[d] = pk

    matches = []

    time2=time.perf_counter()
    """
        for (pk, yy, c_b) in encrypted_table_b:
          d_start = time.time()
          d = new1.decrypt(k_b, c_b)
          d_end = time.time()
          tolerance = 1000
          # tolerance=428969812417927922135545435338
          #time_start = time.time()

          match = new1.find_closest_value_with_tolerance(hash_table, d, tolerance)
          #time_end=time.time()
          #print(time_end-time_start)
          if match:
            matches.append((match, pk))
                """
    search_time.append((time2-time1)/l_a)

  a=mnum.ww_num(table_a,aaa)
  #b=mnum.ww_num(table_b,bbb)
  #m=a+b

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
  return (sum(setup_time) / len(setup_time),sum(enc_time) / len(enc_time),sum(token_time) / len(token_time),sum(search_time) / len(search_time),o_size,
          edb_size)
T1=[]
T2=[]
T3=[]
T4=[]
S1=[]
S2=[]
"""t1,t2,t3,t4,s1,s2=experiment_1( "13-1", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
"""
t1,t2,t3,t4,s1,s2=experiment_1( "8-2", 100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)


t1,t2,t3,t4,s1,s2=experiment_1( "8-1", 100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "9-2", 100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "9-1", 100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "10-2", 100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "10-1", 100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "11-2", 100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "11-1", 100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "12-2", 100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "12-1", 100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "13-2", 100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "13-1", 100)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "14-2", 100)
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

