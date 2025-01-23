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
def hash_based_join(pp_a, msk_a,pp_b, msk_b, encrypted_table_a, encrypted_table_b, len_x,len_y,x_c, y_c,l_a,l_b,aa,bb):
  decryptions = []
  (detBa, B, Bstar, group, g11, g22) = msk_a


  time_start=time.time()
  k = group.random()
  k_a = ipe.encryptQuery(msk_a, k, x_c, len_x,aa,l_a)
  k_b = ipe.encryptQuery(msk_b, k, y_c, len_y,bb,l_b)

  time_end=time.time()
  time1=time_end-time_start

  print((len(k_a)))
  print(len(k_a[0]))
  l1 = len(k_a[0])



  sizea = l1*len(group.serialize(k_a[0][0]))
  l2=len(k_b[0])
  sizeb=l2*len(group.serialize(k_b[0][0]))




  communication_size1 = len(pickle.dumps(pp_a))+sizea+sizeb



  # apply selection first: skip decryption of row if row doesn't satisfy where condition
  time_start=time.time()
  hash_table = {}
  for (pk, x, c_a) in encrypted_table_a:
      d_start = time.time()
      d = ipe.decrypt(pp_a, k_a, c_a)

      # print(d)
      d_end = time.time()
      decryptions.append(d_end - d_start)
      hash_table[d] = pk



  matches = []
  for (pk, y, c_b) in encrypted_table_b:
      d_start = time.time()
      d = ipe.decrypt(pp_b, k_b, c_b)
      # print(d)
      d_end = time.time()
      decryptions.append(d_end - d_start)
      match = hash_table.get(d)
      if match:
          matches.append((match, pk))



  communication_size2 = len(pickle.dumps(matches))
  communication_size=communication_size1+communication_size2
  print('communication_size:' + str(communication_size) + " bytes")


  return (matches, decryptions)


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
# print(encrypted_table_a)

  time_end = time.time()
  len_x = 0
  len_y = 0

  for i in range(aaa.index(x)):
      len_x += IN_CLAUSE_MAX_SIZE_a[i] + 1
  for i in range(bbb.index(y)):
      len_y += IN_CLAUSE_MAX_SIZE_b[i] + 1

  (matches, decryptions) = hash_based_join(pp_a, msk_a, pp_b, msk_b, encrypted_table_a, encrypted_table_b,
                                                       len_x, len_y, x_c, y_c, l_a, l_b, len(a), len(b))
  print(matches)

print('\n\n')
print('\n\n')







experiment_1( "5", 1,IN_CLAUSE_MAX_SIZE_a = [2,1,1,1],IN_CLAUSE_MAX_SIZE_al = 9,IN_CLAUSE_MAX_SIZE_b = [2,1,1,1,1],IN_CLAUSE_MAX_SIZE_bl= 11)

experiment_1( "10", 1,IN_CLAUSE_MAX_SIZE_a = [5,1,3,1],IN_CLAUSE_MAX_SIZE_al = 14,IN_CLAUSE_MAX_SIZE_b = [5,2,2,1,1],IN_CLAUSE_MAX_SIZE_bl= 16)

experiment_1( "20", 1,IN_CLAUSE_MAX_SIZE_a = [10,2,6,2],IN_CLAUSE_MAX_SIZE_al = 24,IN_CLAUSE_MAX_SIZE_b = [10,4,4,2,2],IN_CLAUSE_MAX_SIZE_bl= 27)






# MAIN
# experiments to test relationship between overall time and scale factor




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

