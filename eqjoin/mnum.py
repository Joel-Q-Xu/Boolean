import csv


def read_table(infile, delimiter=","):
  with open(infile) as f:
    return [{k: str(v) for k, v in row.items()} for row in csv.DictReader(f, delimiter=delimiter, skipinitialspace=True)]


def ww_num(table, aaa):

  num_a = []

  h_a = []
  for row in table:
    for i in range(len(aaa)):
      s=str(aaa[i])+str(row[aaa[i]])
      h_a.append(s)
  num_a=len(set(h_a))

  return num_a
def test(sf):
  table_a_file = "mdata/" + sf + "/customer.tbl"
  table_a = read_table(table_a_file, '|')
  aaa=list(table_a[0].keys())
  print(len(table_a))
  aaa=aaa[:-1]
  num_a=ww_num(table_a,aaa)



  table_b_file = "mdata/" + sf + "/orders.tbl"
  table_b = read_table(table_b_file, '|')
  bbb = list(table_b[0].keys())
  bbb=bbb[:-1]
  num_b = ww_num(table_b, bbb)
  return num_a,num_b

#print(test("4"))
"""print(test("0.01"))
print(test("0.02"))
print(test("0.03"))
print(test("0.04"))
print(test("0.05"))"""
