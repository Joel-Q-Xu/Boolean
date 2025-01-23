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

  table_a_file = "mdata/Q16/q_"+sf+"/part.tbl"
  a = ["PARTKEY"]#连接属性
  pk_a = "PARTKEY"#不用管

  x="PARTKEY"#不用管

  #x = "selectivity"

  table_b_file = "mdata/Q16/q_"+sf+"/partsupp.tbl"
  b =["PARTKEY"]
  pk_b = "SUPPKEY"
  y="PARTKEY"

  #table_b_file = "mdata/"+sf+"/orders.tbl"

  table_a = read_table(table_a_file, '|')
  # table_a=table_a[:6]
  l_a=len(table_a)
  print(l_a)

  table_b = read_table(table_b_file, '|')
  # table_b=table_b[:6]
  l_b=len(table_b)
  print(l_b)
  # x_c = [["SIZE1",1]]
  # y_c = [["SUPPKEY2",0]]
  # #x_c = [["addressXSTf4,NCwDVaWNe6tEgvwfmRchLXak","addressIVhzIApeRb ot,c,E",1],["custkey3",0]]
  #y_c = [["selectivity100",1],["orderstatusO",1] ]
  x_c = [["SIZE1","SIZE2","SIZE3","SIZE4","SIZE5","SIZE6","SIZE7","SIZE8",1],["BRANDBrand#13",0],["TYPEPROMO BURNISHED COPPER",0]]
  y_c = [["SUPPKEY358",0]]
  aaa = list(table_a[0].keys())
  aaa = aaa[:-1]


  bbb = list(table_b[0].keys())
  bbb = bbb[:-1]
  w_a = mnum.ww_num(table_a, aaa)
  w_b = mnum.ww_num(table_b, bbb)


  print('w_num:' + str(w_a)+"   "+str(w_b))



  setup_time=[]
  enc_time=[]
  token_time=[]
  search_time=[]
  t1=[]
  t2=[]

  for i in range(iters):




    time1=time.perf_counter()
    o_a = new1.geto(table_a, a, aaa, table_a_file)

    o_b = new1.geto(table_b, b, bbb, table_b_file)



    time2 = time.perf_counter()
    setup_time.append(time2 - time1)




    """keys=o_a.keys()
    keys=list(keys)
    print(type(keys))
    print(type(keys[0]))
    values = o_a.values()
    values = list(values)
    print(type(values))
    print(type(values[0]))
    print(type(values[0][0]))"""

    #o_b = new1.geto(table_b, b, bbb, table_b_file)




    o_size = len(pickle.dumps(o_a))




    """ 
    print(o_size)
    o_a_keys_list = list(o_a.keys())
    o_a_values_list = list(o_a.values())
    o_size = asizeof.asizeof(o_a_values_list) + asizeof.asizeof(o_a_keys_list)
    print(o_size)
    
    o_a = dict(zip(o_a_keys_list, o_a_values_list))
    """
    time1 = time.perf_counter()
    encrypted_table_a = new1.encryptTable(o_a, table_a, pk_a, a, x, aaa, table_a_file)
    encrypted_table_b = new1.encryptTable(o_b, table_b, pk_b, b, y, bbb, table_b_file)
    time2 = time.perf_counter()
    #encrypted_table_b = new1.encryptTable(o_b, table_b, pk_b, b, y, bbb, table_b_file)
    #edb_size = len(pickle.dumps(encrypted_table_a[0][2]))+len(pickle.dumps(encrypted_table_b[0][2]))
    edb_size = encrypted_table_a[0][2].nbytes

    enc_time.append((time2 - time1) ) #/(l_a+l_b)
    # print(encrypted_table_a[0][2])



    time1=time.perf_counter()

    #k=1
    k = random.randint(1, 10000)#10000000000
    # k=1
    k_a = new1.encryptQuery(o_a, k, a, table_a_file, x_c)
    k_b = new1.encryptQuery(o_b, k, b, table_b_file, y_c)

    time2=time.perf_counter()
    # k_b = new1.encryptQuery(o_b, k, b, table_b_file, y_c)
    token_size = sys.getsizeof(k_a)+sys.getsizeof(k_b)
    token_time.append(time2-time1)
    """cnt=0
    for i in range(len(k_a)):
        if k_a[i]==0:
            cnt+=1
    print(cnt)"""

    time1=time.perf_counter()
    hash_table = {}
    tolerance = 8
    tt1=time.perf_counter()
    for (pk, xx, c_a) in encrypted_table_a:
      d = new1.decrypt(k_a, c_a)

      d = round(d / tolerance) * tolerance

      hash_table[d] = pk
    matches = []
    tt2=time.perf_counter()
    tt3=time.perf_counter()
    for (pk, yy, c_b) in encrypted_table_b:
      d = new1.decrypt(k_b, c_b)


      d = round(d / tolerance) * tolerance
      # tolerance=428969812417927922135545435338
      # time_start = time.time()

      #match = new1.find_closest_value_with_tolerance(hash_table, d, tolerance)
      match = hash_table.get(d)

      # time_end=time.time()
      # print(time_end-time_start)
      if match:
        matches.append((match, pk))
    tt4=time.perf_counter()
    print(matches)

    time2=time.perf_counter()
    search_time.append(time2-time1)
    t1.append(tt2-tt1)
    print(t1)
    t2.append(tt4-tt3)
    print(t2)

  a=mnum.ww_num(table_a,aaa)
  b=mnum.ww_num(table_b,bbb)
  m=a+b

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
          edb_size,t1,t2)
T1=[]
T2=[]
T3=[]
T4=[]
S1=[]
S2=[]
tt1=[]
tt2=[]
t1,t2,t3,t4,s1,s2,ttt1,ttt2=experiment_1( "4k", 10)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
tt1.append(ttt1)
tt2.append(ttt2)
t1,t2,t3,t4,s1,s2,ttt1,ttt2=experiment_1( "8k", 10)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
tt1.append(ttt1)
tt2.append(ttt2)
t1,t2,t3,t4,s1,s2,ttt1,ttt2=experiment_1( "12k", 10)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
tt1.append(ttt1)
tt2.append(ttt2)
t1,t2,t3,t4,s1,s2,ttt1,ttt2=experiment_1( "16k", 10)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
tt1.append(ttt1)
tt2.append(ttt2)

print(T1)
print(T2)
print(T3)
print(T4)
print(S1)
print(S2)
print(tt1)
print(tt2)
t1,t2,t3,t4,s1,s2,ttt1,ttt2=experiment_1( "20k", 10)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
tt1.append(ttt1)
tt2.append(ttt2)
"""t1,t2,t3,t4,s1,s2=experiment_1( "8-1", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "9-2", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "9-1", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "10-2", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "10-1", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "11-2", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "11-1", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "12-2", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "12-1", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "13-2", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "13-1", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)
t1,t2,t3,t4,s1,s2=experiment_1( "14-2", 1)
T1.append(t1)
T2.append(t2)
T3.append(t3)
T4.append(t4)
S1.append(s1)
S2.append(s2)"""


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

