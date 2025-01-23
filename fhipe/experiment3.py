import sys,os,time
from fhipe import icde as ipe
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(1, os.path.abspath('..'))

#from charm.toolbox.pairinggroup import ZR
#from fhipe import ipe
import csv
import timeit
from pympler import asizeof
import pickle
# 创建一个对象


# 获取对象的大小



IN_CLAUSE_MAX_SIZE=1
selectivity_1 = False

# infile contains a CSV of a database table
# returns an array of dictionaries where keys are
# the table attributes and values are strings
def read_table(infile, delimiter=","):
  with open(infile) as f:
    return [{k: str(v) for k, v in row.items()} for row in csv.DictReader(f, delimiter=delimiter, skipinitialspace=True)]

# for a query:
# SELECT * FROM A
# INNER JOIN B ON A.a = B.b
# WHERE A.x IN (x_c) AND B.y IN (y_c)
# such that a is a primary key, and b is the corresponding foreign key
# returns the count of resulting rows



######################################### EXPERIMENTS ###########################################
# NOTE: you must add a row with column heads to the tables that you will use. one should use the
# attribute names used here: http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf#page=13

def experiment_1( sf, iters,  IN_CLAUSE_MAX_SIZE_a,IN_CLAUSE_MAX_SIZE_al,IN_CLAUSE_MAX_SIZE_b,IN_CLAUSE_MAX_SIZE_bl):
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
  x_c = ["XSTf4,NCwDVaWNe6tEgvwfmRchLXak"]
  y_c = ["100"]
  aaa = ["custkey", "name", "address","selectivity"]
  #
  #IN_CLAUSE_MAX_SIZE_a = [5,1,3,1]
  #IN_CLAUSE_MAX_SIZE_al = 14

  bbb = ["orderkey", "custkey","orderstatus", "totalprice", "selectivity"]
  #IN_CLAUSE_MAX_SIZE_b = [5,2,2,1,1]
  #IN_CLAUSE_MAX_SIZE_bl= 16
  #"orderstatus", "totalprice",




  time_start = time.time()
  l_a = len(a) + (IN_CLAUSE_MAX_SIZE_al) + 2
  l_b = len(b) + (IN_CLAUSE_MAX_SIZE_bl) + 2

  (pp_a, msk_a) = ipe.setup(l_a)
  (pp_b, msk_b) = ipe.setup(l_b)
  (detBa, Ba, Bstara, group, g1a, g2a) = msk_a
  (detBb, Bb, Bstarb, group, g1, g2) = msk_b
  msk_a = (detBb, Ba, Bstara, group, g1a, g2a)
  msk_b = (detBa, Bb, Bstarb, group, g1a, g2a)

  encrypted_table_a_size = 0
  encrypted_table_a = ipe.encryptTable(msk_a, table_a, pk_a, a, x, IN_CLAUSE_MAX_SIZE_a, aaa)

  encrypted_table_b = ipe.encryptTable(msk_b, table_b, pk_b, b, y, IN_CLAUSE_MAX_SIZE_b, bbb)
  for i in encrypted_table_a:
      serialized_g1 = group.serialize(i[2][0][1])
      storage_size1 = len(serialized_g1)
      ll = len(i[2][0])

      size = (ll + 1) * storage_size1+len(pickle.dumps(i[0]))+len(pickle.dumps(i[1]))
      encrypted_table_a_size += size

      # encrypted_table_a_serialized = pickle.dumps(i)
      # encrypted_table_a_size+= len(encrypted_table_a_serialized)




  encrypted_table_b_size = 0
  for i in encrypted_table_b:
      erialized_g1 = group.serialize(i[2][0][1])

      storage_size1 = len(serialized_g1)
      ll = len(i[2][0])


      size = (ll+1)*storage_size1+len(pickle.dumps(i[0]))+len(pickle.dumps(i[1]))
      encrypted_table_b_size += size


  EDB_size=encrypted_table_a_size+encrypted_table_b_size


  print('EDB size:'+str(EDB_size))

  print('')
  print('\n\n')



# MAIN
# experiments to test relationship between overall time and scale factor




experiment_1( "5", 1,IN_CLAUSE_MAX_SIZE_a = [2,1,1,1],IN_CLAUSE_MAX_SIZE_al = 9,IN_CLAUSE_MAX_SIZE_b = [2,1,1,1,1],IN_CLAUSE_MAX_SIZE_bl= 11)

experiment_1( "10", 1,IN_CLAUSE_MAX_SIZE_a = [5,1,3,1],IN_CLAUSE_MAX_SIZE_al = 14,IN_CLAUSE_MAX_SIZE_b = [5,2,2,1,1],IN_CLAUSE_MAX_SIZE_bl= 16)

experiment_1( "20", 1,IN_CLAUSE_MAX_SIZE_a = [10,2,6,2],IN_CLAUSE_MAX_SIZE_al = 24,IN_CLAUSE_MAX_SIZE_b = [10,4,4,2,2],IN_CLAUSE_MAX_SIZE_bl= 27)




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

