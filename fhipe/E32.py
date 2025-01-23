import sys,os,time
import new3 as new1
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
  print("--------------------------- in_num="+str(len(k)-1))
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
  x_c = [k]
  y_c = [k]
  aaa = list(table_a[0].keys())
  aaa = aaa[:-1]

  #bbb = list(table_b[0].keys())
  #bbb = bbb[:-1]
  # "addressIVhzIApeRb ot,c,E",2
  #aaa = ["custkey", "nationkey", "selectivity"]
  #bbb = ["orderkey", "custkey", "orderstatus", "selectivity"]




  #aaa = ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment", "selectivity"]
  #bbb = ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment", "selectivity"]
  setup_time=[]
  enc_time=[]
  token_time=[]
  search_time=[]

  for i in range(iters):

    time1 = time.perf_counter()
    o_a, d_a = new1.geto(table_a, a, aaa, table_a_file)
    time2 = time.perf_counter()
    setup_time.append(time2 - time1)
    #b = mnum.ww_num(table_b, bbb)
    #d_b = new1.next_power_of_two(b + 2)
    d_b=2468
    # o_b,d_b = new1.geto(table_b, b, bbb, table_b_file)
    time1 = time.perf_counter()
    o_size = sys.getsizeof(o_a)

    encrypted_table_a = new1.encryptTable(o_a, table_a, pk_a, a, x, aaa, table_a_file)
    # encrypted_table_b = new1.encryptTable(o_b, table_b, pk_b, b, y, bbb, table_b_file)
    edb_size = sys.getsizeof(encrypted_table_a[0][2])
    time2 = time.perf_counter()
    enc_time.append((time2 - time1) / l_a)

    time1 = time.perf_counter()

    # k=1
    k = random.randint(1, 1)
    # k=1
    k_a = new1.encryptQuery(o_a, k, a, table_a_file, x_c, d_a)
    # k_b = new1.encryptQuery(o_b, k, b, table_b_file, y_c,d_a)
    token_size = sys.getsizeof(k_a)
    time2 = time.perf_counter()
    token_time.append(time2 - time1)
    k_a = k_a.astype(np.int64)


    time1 = time.perf_counter()

    hash_table = {}
    for (pk, xx, c_a) in encrypted_table_a:

      d = new1.decrypt(k_a, c_a)


      hash_table[d] = pk

    matches = []
    """
    for (pk, yy, c_b) in encrypted_table_b:
      d_start = time.perf_counter()
      d = new1.decrypt(k_b, c_b)
      d_end = time.perf_counter()
      tolerance = 0.0000000000001
      # tolerance=428969812417927922135545435338
      #time_start = time.perf_counter()

      match = new1.find_closest_value_with_tolerance(hash_table, d, tolerance)
      #time_end=time.perf_counter()
      #print(time_end-time_start)
      if match:
        matches.append((match, pk))
            """
    time2 = time.perf_counter()
    search_time.append((time2 - time1) / l_a)
  a = mnum.ww_num(table_a, aaa)
  #b = mnum.ww_num(table_b, bbb)
  #m = a + b
  print('w_num:'+str(a))
  #print('w_num:' + str(b))

  print('token time: {}s on average'.format(sum(token_time) / len(token_time)))
  print('Search time: {}s on average'.format(sum(search_time) / (len(search_time))))

  print('num matches: {}'.format(len(matches)))
  print(matches)

  print('')
  print('\n\n')
  return sum(token_time) / len(token_time),sum(search_time) / len(search_time)
T1=[]
T2=[]




t1,t2=experiment_1( "28", ["SIZE1","SIZE2","SIZE3","SIZE4","SIZE5",1],100)
T1.append(t1)
T2.append(t2)

t1,t2=experiment_1( "28", ["SIZE1","SIZE2","SIZE3","SIZE4","SIZE5","SIZE6","SIZE7","SIZE8","SIZE9","SIZE10",1],100)
T1.append(t1)
T2.append(t2)
t1,t2=experiment_1( "28", ["SIZE1","SIZE2","SIZE3","SIZE4","SIZE5","SIZE6","SIZE7","SIZE8","SIZE9","SIZE10","SIZE11","SIZE12","SIZE13","SIZE14","SIZE15",1],100)
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

