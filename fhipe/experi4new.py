import sys,os,time
from fhipe import new1 as new1
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
from pympler import asizeof
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
def experiment_1( sf, iters):
  print("--------------------------- sf="+sf)
  # table_a_file = "./table_a.in"
  # a="buyer_id"
  # pk_a="buyer_id"
  # x="phone_num"

  table_a_file = "data/"+sf+"/customer.tbl"
  a = ["custkey"]
  pk_a = "custkey"

  x="address"

  #x = "selectivity"

  # table_b_file = "./table_b.in"
  # b = "buyer_id"
  # pk_b = "order_id"
  # y="amount"

  table_b_file = "data/"+sf+"/orders.tbl"
  b = ["custkey"]
  pk_b = "orderkey"

  y = "selectivity"
  table_a = read_table(table_a_file, '|')

  table_b = read_table(table_b_file, '|')
  #x_c = [["addressXSTf4,NCwDVaWNe6tEgvwfmRchLXak","addressIVhzIApeRb ot,c,E",1],["custkey3",0]]
  #y_c = [["selectivity100",1],["orderstatusO",1] ]
  x_c = [["addressXSTf4,NCwDVaWNe6tEgvwfmRchLXak",1]]
  y_c = [["selectivity100",1]]
  aaa = ["custkey", "name", "address", "selectivity"]
  bbb = ["orderkey", "custkey", "orderstatus", "totalprice", "selectivity"]
  # "addressIVhzIApeRb ot,c,E",2




  #aaa = ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment", "selectivity"]
  #bbb = ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment", "selectivity"]

  setup_time=[]
  client_time=[]
  server_time=[]






  for i in range(iters):




    time1=time.time()
    o_a = new1.geto(table_a, a, aaa, table_a_file)

    o_b = new1.geto(table_b, b, bbb, table_b_file)


    encrypted_table_a = new1.encryptTable(o_a, table_a, pk_a, a, x, aaa, table_a_file)
    encrypted_table_b = new1.encryptTable(o_b, table_b, pk_b, b, y, bbb, table_b_file)
    time2=time.time()
    setup_time.append(time2-time1)



    time1=time.time()

    #k=1
    k = random.randint(1, 100000000000000000)
    # k=1
    k_a = new1.encryptQuery(o_a, k, a, table_a_file, x_c)
    k_b = new1.encryptQuery(o_b, k, b, table_b_file, y_c)
    time2=time.time()

    client_time.append(time2-time1)


    communication_size1 = sys.getsizeof(k_a)+sys.getsizeof(k_b)



    time1=time.time()



    hash_table = {}
    for (pk, xx, c_a) in encrypted_table_a:

      d_start = time.time()
      d = new1.decrypt(k_a, c_a)
      d_end = time.time()


      hash_table[d] = pk

    matches = []
    for (pk, yy, c_b) in encrypted_table_b:
      d_start = time.time()
      d = new1.decrypt(k_b, c_b)
      d_end = time.time()
      tolerance = 0.000000001
      # tolerance=42896981241792792213554543533824973
      #time_start = time.time()

      match = new1.find_closest_value_with_tolerance(hash_table, d, tolerance)
      #time_end=time.time()
      #print(time_end-time_start)
      if match:
        matches.append((match, pk))
    time2=time.time()
    server_time.append(time2-time1)
    communication_size2 = len(pickle.dumps(matches))
    communication_size = communication_size1 + communication_size2
    print('communication_size:' + str(communication_size) + " bytes")



  print('')
  print('\n\n')
experiment_1( "5", 1)

experiment_1( "10", 1)

experiment_1( "20", 1)

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
