import numpy as np
import random
import csv



def changefile(sf):
    table_a_file = "data/" + sf + "/customer.tbl"
    file_modified="mdata/" + sf + "/customer.tbl"
    with open(table_a_file, 'r') as infile, open(file_modified, 'w', newline='') as outfile:
        reader = csv.reader(infile, delimiter='|')
        writer = csv.writer(outfile, delimiter='|')

        for row in reader:
            for i in range(9):
                if row[i] == '':
                    row[i] = random.randint(1,2000)

            # 如果第3列是0，将其替换为5（索引从0开始，第三列为 row[2]）

            writer.writerow(row)

    table_a_file = "data/" + sf + "/orders.tbl"
    file_modified = "mdata/" + sf + "/orders.tbl"
    with open(table_a_file, 'r') as infile, open(file_modified, 'w', newline='') as outfile:
        reader = csv.reader(infile, delimiter='|')
        writer = csv.writer(outfile, delimiter='|')

        for row in reader:
            for i in range(10):
                if row[i] == '':
                    row[i] = random.randint(1,2000)

            # 如果第3列是0，将其替换为5（索引从0开始，第三列为 row[2]）

            writer.writerow(row)

    num_a=[]


    return "qqq"


print(changefile("0.09"))





