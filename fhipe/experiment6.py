import sys,os,time
import ipe
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
def hash_based_join(pp_a, msk_a,pp_b, msk_b, encrypted_table_a, encrypted_table_b, x_c, y_c):
  decryptions = []
  (detBa, B, Bstar, group, g11, g22) = msk_a
  k = group.random()
  k_a = ipe.encryptQuery(msk_a, k, x_c, 17)
  k_b = ipe.encryptQuery(msk_b, k, y_c, 19)



  l1 = len(k_a[0])

  sizea = l1 * len(group.serialize(k_a[0][0]))
  l2 = len(k_b[0])
  sizeb = l2 * len(group.serialize(k_b[0][0]))

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

  print('communication_size1:' + str(communication_size1) + " bytes")
  print('communication_size2:' + str(communication_size2) + " bytes")


  return (matches, decryptions)


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
  a = "custkey"
  pk_a = "custkey"
  x = "selectivity"

  # table_b_file = "./table_b.in"
  # b = "buyer_id"
  # pk_b = "order_id"
  # y="amount"

  table_b_file = "data/"+sf+"/orders.tbl"
  b = "custkey"
  pk_b = "orderkey"
  y = "selectivity"
  table_a = read_table(table_a_file, '|')

  table_b = read_table(table_b_file, '|')
  x_c = ["100"]
  y_c = ["100"]

  setup_time=[]



  for i in range(iters):
    time_start = time.time()
    (pp_a, msk_a) = ipe.setup(3 + IN_CLAUSE_MAX_SIZE * 9 * 2)
    (pp_b, msk_b) = ipe.setup(3 + IN_CLAUSE_MAX_SIZE * 10 * 2)
    (detBa, Ba, Bstara, group, g1a, g2a) = msk_a
    (detBb, Bb, Bstarb, group, g1, g2) = msk_b
    msk_a = (detBb, Ba, Bstara, group, g1a, g2a)
    msk_b = (detBa, Bb, Bstarb, group, g1a, g2a)
    aaa = ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment", "selectivity"]
    bbb = ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority",
           "comment", "selectivity"]

    encrypted_table_a_size=0
    encrypted_table_a = ipe.encryptTable(msk_a, table_a, pk_a, a, x, IN_CLAUSE_MAX_SIZE,aaa)

    encrypted_table_b = ipe.encryptTable(msk_b, table_b, pk_b, b, y, IN_CLAUSE_MAX_SIZE,bbb)
    # print(encrypted_table_a)

    time_end = time.time()
    setup_time.append(time_end - time_start)


    (matches, decryptions) = hash_based_join(pp_a, msk_a,pp_b, msk_b, encrypted_table_a, encrypted_table_b, x_c, y_c)



  print('num matches: {}'.format(len(matches)))


  print('')
  print('\n\n')



# MAIN
# experiments to test relationship between overall time and scale factor


experiment_1( "0.01", 1)
experiment_1("0.02", 1)
experiment_1( "0.03", 1)
experiment_1( "0.04", 1)
experiment_1( "0.05", 1)
experiment_1( "0.06", 1)
experiment_1( "0.07", 1)
experiment_1("0.08", 1)
experiment_1("0.09", 1)
experiment_1("0.1", 1)


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

