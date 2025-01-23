import sys,os,time
import ipe1
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(1, os.path.abspath('..'))

#from charm.toolbox.pairinggroup import ZR
#from fhipe import ipe
import csv
import timeit
from pympler import asizeof

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

def experiment_1(in_clause_max_size, iters):




  table_a_file = "data/0.01/customer.tbl"

  a ="custkey"
  pk_a = "custkey"
  x = "selectivity"

  # table_b_file = "./table_b.in"
  # b = "buyer_id"
  # pk_b = "order_id"
  # y="amount"

  table_b_file = "data/"+"0.01"+"/orders.tbl"
  b = "custkey"
  pk_b = "orderkey"
  y = "selectivity"
  table_a = read_table(table_a_file, '|')

  table_b = read_table(table_b_file, '|')
  x_c = ["12.5"]
  y_c = ["12.5"]

  setup_time1=[]
  setup_time2=[]
  setup_time3=[]
  setup_time4 = []


  for i in range(iters):
    time_start = time.time()
    (pp_a, msk_a) = ipe1.setup(3 + IN_CLAUSE_MAX_SIZE * 9 * 2)
    (pp_b, msk_b) = ipe1.setup(3 + IN_CLAUSE_MAX_SIZE * 10 * 2)
    (detBa, Ba, Bstara, group, g1a, g2a) = msk_a
    (detBb, Bb, Bstarb, group, g1, g2) = msk_b
    msk_a = (detBb, Ba, Bstara, group, g1a, g2a)
    msk_b = (detBa, Bb, Bstarb, group, g1a, g2a)
    aaa = ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment", "selectivity"]
    bbb = ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority",
           "comment", "selectivity"]
    encrypted_table_a = ipe1.encryptTable(msk_a, table_a, pk_a, a, x, IN_CLAUSE_MAX_SIZE, aaa)

    encrypted_table_b = ipe1.encryptTable(msk_b, table_b, pk_b, b, y, IN_CLAUSE_MAX_SIZE, bbb)
    # print(encrypted_table_a)
    time_end = time.time()
    setup_time1.append(time_end - time_start)
  print('setup time1: {}s on average'.format(sum(setup_time1) / len(setup_time1)))

  for i in range(iters):
    time_start = time.time()
    (pp_a, msk_a) = ipe1.setup(3 + IN_CLAUSE_MAX_SIZE * 9 * 2+1)
    (pp_b, msk_b) = ipe1.setup(3 + IN_CLAUSE_MAX_SIZE * 10 * 2+1)
    (detBa, Ba, Bstara, group, g1a, g2a) = msk_a
    (detBb, Bb, Bstarb, group, g1, g2) = msk_b
    msk_a = (detBb, Ba, Bstara, group, g1a, g2a)
    msk_b = (detBa, Bb, Bstarb, group, g1a, g2a)
    aaa = ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment", "selectivity"]
    bbb = ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority",
           "comment", "selectivity"]
    encrypted_table_a = ipe1.encryptTable2(msk_a, table_a, pk_a, a, x, IN_CLAUSE_MAX_SIZE, aaa)

    encrypted_table_b = ipe1.encryptTable2(msk_b, table_b, pk_b, b, y, IN_CLAUSE_MAX_SIZE, bbb)
    time_end = time.time()
    setup_time2.append(time_end - time_start)
  print('setup time2: {}s on average'.format(sum(setup_time2) / len(setup_time2)))

  for i in range(iters):
    time_start = time.time()
    (pp_a, msk_a) = ipe1.setup(3 + IN_CLAUSE_MAX_SIZE * 9 * 2+2)
    (pp_b, msk_b) = ipe1.setup(3 + IN_CLAUSE_MAX_SIZE * 10 * 2+2)
    (detBa, Ba, Bstara, group, g1a, g2a) = msk_a
    (detBb, Bb, Bstarb, group, g1, g2) = msk_b
    msk_a = (detBb, Ba, Bstara, group, g1a, g2a)
    msk_b = (detBa, Bb, Bstarb, group, g1a, g2a)
    aaa = ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment", "selectivity"]
    bbb = ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority",
           "comment", "selectivity"]
    encrypted_table_a = ipe1.encryptTable3(msk_a, table_a, pk_a, a, x, IN_CLAUSE_MAX_SIZE, aaa)

    encrypted_table_b = ipe1.encryptTable3(msk_b, table_b, pk_b, b, y, IN_CLAUSE_MAX_SIZE, bbb)
    time_end = time.time()
    setup_time3.append(time_end - time_start)
  print('setup time3: {}s on average'.format(sum(setup_time3) / len(setup_time3)))

  for i in range(iters):
    time_start = time.time()
    (pp_a, msk_a) = ipe1.setup(3 + IN_CLAUSE_MAX_SIZE * 9 * 2+3)
    (pp_b, msk_b) = ipe1.setup(3 + IN_CLAUSE_MAX_SIZE * 10 * 2+3)
    (detBa, Ba, Bstara, group, g1a, g2a) = msk_a
    (detBb, Bb, Bstarb, group, g1, g2) = msk_b
    msk_a = (detBb, Ba, Bstara, group, g1a, g2a)
    msk_b = (detBa, Bb, Bstarb, group, g1a, g2a)
    aaa = ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment", "selectivity"]
    bbb = ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority",
           "comment", "selectivity"]
    encrypted_table_a = ipe1.encryptTable4(msk_a, table_a, pk_a, a, x, IN_CLAUSE_MAX_SIZE, aaa)

    encrypted_table_b = ipe1.encryptTable4(msk_b, table_b, pk_b, b, y, IN_CLAUSE_MAX_SIZE, bbb)
    time_end = time.time()
    setup_time4.append(time_end - time_start)
  print('setup time4: {}s on average'.format(sum(setup_time4) / len(setup_time4)))






# MAIN
# experiments to test relationship between overall time and scale factor


experiment_1( 1, 5)



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

