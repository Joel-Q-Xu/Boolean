import sys,os,time
import icde as ipe
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(1, os.path.abspath('..'))

#from charm.toolbox.pairinggroup import ZR
#from fhipe import ipe
import csv
import timeit
from pympler import asizeof
import pickle
# 创建一个对象
import mnum

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
def hash_based_join(pp_a, msk_a, encrypted_table_a,  len_xx,x_c, l_a,aa):
  decryptions = []
  (detBa, B, Bstar, group, g11, g22) = msk_a


  time_start=time.perf_counter()
  k = group.random()
  k_a = ipe.encryptQuery(msk_a,msk_a, k, x_c, len_xx,aa,l_a)
  #k_b = ipe.encryptQuery(msk_b, k, y_c, len_yy,bb,l_b)

  time_end=time.perf_counter()
  time1=(time_end-time_start)
  l1 = len(k_a[0])

  sizea = l1 * len(group.serialize(k_a[0][0]))
  #l2 = len(k_b[0])
  #sizeb = l2 * len(group.serialize(k_b[0][0]))

  communication_size1 = len(pickle.dumps(pp_a)) + sizea

  # apply selection first: skip decryption of row if row doesn't satisfy where condition
  time_start=time.perf_counter()
  hash_table = {}
  for (pk, x, c_a) in encrypted_table_a:
    d_start = time.perf_counter()
    d = ipe.decrypt(pp_a, k_a, c_a)
    d_end = time.perf_counter()
    decryptions.append(d_end - d_start)
    hash_table[d] = pk




  matches = []

  """
  for (pk, y, c_b) in encrypted_table_b:
    d_start = time.perf_counter()
    d = ipe.decrypt(pp_b, k_b, c_b)
    d_end = time.perf_counter()
    decryptions.append(d_end - d_start)
    match = hash_table.get(d)
    if match:
      matches.append((match, pk))"""
  time_end=time.perf_counter()
  time2=time_end-time_start
  return time1,time2,(matches, decryptions),communication_size1

def w_num(table, aaa):

  num_a = []
  for i in range(len(aaa)):
    h_a = set()
    for row in table:
      h_a.add(row[aaa[i]])
    num_a.append(len(h_a))

  return num_a
######################################### EXPERIMENTS ###########################################
# NOTE: you must add a row with column heads to the tables that you will use. one should use the
# attribute names used here: http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf#page=13
def experiment_1( sf,k, iters):
  print("--------------------------- in_num="+str(len(k[0])-1))
  # table_a_file = "./table_a.in"
  # a="buyer_id"
  # pk_a="buyer_id"
  # x="phone_num"

  table_a_file = "newdata/part2" + sf + "-2.tbl"
  a = ["PARTKEY"]
  pk_a = "PARTKEY"

  x = ["SIZE"]
  #x = "selectivity"

  # table_b_file = "./table_b.in"
  # b = "buyer_id"
  # pk_b = "order_id"
  # y="amount"

  """table_b_file = "mdata/"+sf+"/orders.tbl"
  b = ["custkey"]
  pk_b = "orderkey"
  """


  table_a = read_table(table_a_file, '|')
  ll_a=len(table_a)

  #table_b = read_table(table_b_file, '|')
  x_c = k
  y_c = k
  aaa = list(table_a[0].keys())
  aaa = aaa[:-1]

  """bbb = list(table_b[0].keys())
  bbb = bbb[:-1]
  aaa = ["custkey", "name", "address", "selectivity"]
  bbb = ["orderkey", "custkey", "orderstatus", "selectivity"]"""
  #IN_CLAUSE_MAX_SIZE_b = [5,2,2,1,1]
  #IN_CLAUSE_MAX_SIZE_bl= 16
  #"orderstatus", "totalprice",
  IN_CLAUSE_MAX_SIZE_a=w_num(table_a,aaa)
  #IN_CLAUSE_MAX_SIZE_b = w_num(table_b, bbb)
  IN_CLAUSE_MAX_SIZE_al=0
  for i in range(len(IN_CLAUSE_MAX_SIZE_a)):
    IN_CLAUSE_MAX_SIZE_al+=IN_CLAUSE_MAX_SIZE_a[i]
  IN_CLAUSE_MAX_SIZE_al+=len(IN_CLAUSE_MAX_SIZE_a)
  """
  IN_CLAUSE_MAX_SIZE_bl = 0
  for i in range(len(IN_CLAUSE_MAX_SIZE_b)):
    IN_CLAUSE_MAX_SIZE_bl += IN_CLAUSE_MAX_SIZE_b[i]
  IN_CLAUSE_MAX_SIZE_bl += len(IN_CLAUSE_MAX_SIZE_b)
  """



  #aaa = ["custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment", "selectivity"]
  #bbb = ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment", "selectivity"]

  setup_time = []
  enc_time = []
  token_time = []
  search_time = []



  for i in range(iters):


    l_a=len(a)+(IN_CLAUSE_MAX_SIZE_al)+2
    #l_b = len(b) + (IN_CLAUSE_MAX_SIZE_bl) + 2
    #(pp_b, msk_b) = ipe.setup(l_b)
    time_start = time.perf_counter()



    (pp_a, msk_a) = ipe.setup(l_a)
    (detBa, Ba, Bstara, group, g1a, g2a) = msk_a

    time_end = time.perf_counter()
    setup_time.append(time_end - time_start)

    #(detBb, Bb, Bstarb, group, g1, g2) = msk_b
    #detBb=detBa

    #msk_a = (detBb, Ba, Bstara, group, g1a, g2a)
    #msk_b = (detBa, Bb, Bstarb, group, g1a, g2a)


    b_size = sys.getsizeof(msk_a)




    encrypted_table_a_size=0
    time_start=time.perf_counter()
    encrypted_table_a = ipe.encryptTable(msk_a, table_a, pk_a, a, x, IN_CLAUSE_MAX_SIZE_a,aaa)

    #encrypted_table_b = ipe.encryptTable(msk_b, table_b, pk_b, b, y, IN_CLAUSE_MAX_SIZE_b,bbb)
    # print(encrypted_table_a)
    time_end=time.perf_counter()
    enc_time.append((time_end - time_start) / ll_a)
    serialized_g1 = group.serialize(encrypted_table_a[0][2][0][1])
    storage_size1 = len(serialized_g1)
    ll = len(encrypted_table_a[0][2][0])

    size = (ll + 1) * storage_size1 + len(pickle.dumps(encrypted_table_a[0][0])) + len(pickle.dumps(encrypted_table_a[0][1]))
    c_size= size

    len_xx=[]
    len_yy=[]


    for j in range(len(x)):
      len_x = 0
      for i in range(aaa.index(x[j])):
        len_x += IN_CLAUSE_MAX_SIZE_a[i] + 1
      len_xx.append(len_x)

    """
    for j in range(len(y)):
      len_y = 0
      for i in range(bbb.index(y[j])):
        len_y += IN_CLAUSE_MAX_SIZE_b[i] +1
      len_yy.append(len_y)"""
    pp_b=pp_a
    msk_b=msk_a
    encrypted_table_b=encrypted_table_a


    time1,time2,(matches, decryptions),token_size= hash_based_join(pp_a, msk_a,encrypted_table_a, len_xx,x_c,l_a,len(a))
    token_time.append(time1)
    search_time.append(time2/ ll_a)
  print(matches)
  a = mnum.ww_num(table_a, aaa)
  #b = mnum.ww_num(table_b, bbb)
  #m = a + b
  print('w_num:' + str(a))

  print('token time: {}s on average'.format(sum(token_time) / len(token_time)))
  print('Search time: {}s on average'.format(sum(search_time) / (len(search_time))))

  print('num matches: {}'.format(len(matches)))

  print('')
  print('\n\n')
  return sum(token_time) / len(token_time), sum(search_time) / len(search_time),


T1=[]
T2=[]




t1,t2=experiment_1( "28", [["1","2","3","4","5"]],1)
T1.append(t1)
T2.append(t2)

t1,t2=experiment_1( "28", [["1","2","3","4","5","6","7","8","E9","10"]],1)
T1.append(t1)
T2.append(t2)
t1,t2=experiment_1( "28", [["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15"]],1)
T1.append(t1)
T2.append(t2)

print(T1)
print(T2)
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

