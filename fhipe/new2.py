"""
Copyright (c) 2016, Kevin Lewi

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
PERFORMANCE OF THIS SOFTWARE.
"""
import mmh3

"""
Implementation of function-hiding inner product encryption (FHIPE).
"""
import numpy as np
from scipy.linalg import qr,lapack
import csv
import sys, os, math, random
import time
from mpmath import mp
# Path hack
sys.path.insert(0, os.path.abspath('charm'))
sys.path.insert(1, os.path.abspath('../charm'))


import numpy as np

IN_CLAUSE_MAX_SIZE=1
import hashlib
from sympy import Matrix, GramSchmidt
import binascii
from scipy.linalg import null_space



def decrypt(k,ind):
    t=np.dot(k,ind)








    return t

def sha1_binary(input_string):
    # 创建一个新的sha1 hash对象
    # 使用hash对象的update()方法将字符串编码为字节并添加到哈希中
    hash_16=binascii.crc_hqx(input_string.encode('utf-8'),0)


    # 使用digest()方法获取二进制表示的哈希值
    binary_dig = format(hash_16, '016b')
    #binary_dig = bin(hash_16)[2:]


    return binary_dig
def schmidt(ma):
    MA = [Matrix(col) for col in ma]
    gram = GramSchmidt(MA,orthonormal=True)# orthonormal=True表示需要做归一化处理
    gram_numpy = [np.array(gram_vec.tolist()).reshape(-1, 1) for gram_vec in gram]
    # 使用 np.hstack 将这些列向量水平堆叠成一个二维 numpy 数组
    return np.hstack(gram_numpy)

def geto( table, j_a, aaa,table_name):

    l_ja = len(j_a)
    l_a = len(aaa)

    l = l_ja + l_a
    eye_matrix = np.eye(l, dtype=int)
    dict_a = {}
    for i in range(l_ja):
        binary_str_e_vector = ''.join(str(int(bool(e))) for e in eye_matrix[i])
        original_bytes=sha1_binary("join"+j_a[i])
        #binary_str_original = ''.join(format(byte, '08b') for byte in original_bytes)
        binary_string = original_bytes + binary_str_e_vector
        bit_list = [int(bit) for bit in binary_string]
        dict_a["join"+j_a[i]]=bit_list
        # print(x[aaa[i]])

    for i in range(l_a):
        binary_str_e_vector = ''.join(str(int(bool(e))) for e in eye_matrix[l_ja+i])
        original_bytes = sha1_binary(aaa[i])
        #binary_str_original = ''.join(format(byte, '08b') for byte in original_bytes)
        binary_string=original_bytes+binary_str_e_vector
        bit_list = [int(bit) for bit in binary_string]
        dict_a[aaa[i]]=bit_list

    '''
        print(len(binary_str_original))
        print(len(dict_a[h_a[i]]))
        print(dict_a[h_a[i]])
        print(len(binary_str_e_vector))
        print(len(eye_matrix[l_x+1+i]))
    '''
    list_a=[]
    for value in dict_a.values():
        list_a.append(value)

    a = np.array(list_a)
    a=a.T
    q, R = qr(a,mode='economic')
    #print(R)
    #print(q)
    b = q.T
    b=b.astype(np.float32)

    o=np.array(b)




    """print(o)
    print(len(o[0]))
    print(len(o))
"""



    #print([(row[pk], row[x], encryptRow(msk,a, row, max_degree,aaa)) for row in table])

    return o

def encryptTable(o, table, pk, j_a, x,aaa,table_file):
    #print([(row[pk], row[x], encryptRow(msk,a, row, max_degree,aaa)) for row in table])



    return [(row[pk], row[x], encryptRow(o,j_a, row, aaa,table_file)) for row in table]

def encryptRow(o, j_a, row,aaa,table_name):
    # 生成长度为32的全1向量，

    l = len(aaa)
    l_ja = len(j_a)

    K1=0

    s = np.ones(32, dtype=np.int32)

    for i in range(l_ja):
        h_a = int(row[j_a[i]])
        o1 = o[i]
        k1 =K1+ int(h_a)*np.kron(o1, s)

    k2=0
    r = random.randint(1, 1000)
    for i in range(l):
        bit_list = [int(bit) for bit in hash32(row[aaa[i]])]

        bit_list=np.array(bit_list)
        v=bit_list
        o2= o[i + l_ja]
        k2=k2+np.kron(o2,v)
    enc_row=k1+r*k2
    enc_row = enc_row.astype(np.float32)

    return enc_row



def hash32(input_string):
    # 使用mmh3.hash函数计算输入字符串的哈希值
    hash_value = mmh3.hash(input_string)

    # 将哈希值转换为二进制字符串，并确保它是32个字符长
    # 如果哈希值是32位的，那么bin(hash_value)[2:]将直接给出32个字符（或者更少，如果最高位是0）
    # 我们需要填充它以确保它是32个字符长
    binary_dig = bin(hash_value)[3:].zfill(32)



    return binary_dig



def encryptQuery(o, k, j_a,aaa,x_q):
    vector = np.zeros(32,dtype=np.int32)
    vector[0] = 1
    k1=k*np.kron(o[0],vector)
    k2=0

    for i in range(len(x_q[1])):
        index = aaa.index(x_q[0][i])
        oo =o[index + 1]
        a =[]
        r = random.randint(1, 1000)
        for j in range(len(x_q[1][i])):
            bit_list = [int(bit) for bit in hash32(x_q[1][i][j])]

            a.append(bit_list)
        a = np.array(a)

        x=null_space(a)
        x=np.array(x)
        x=x.T
        xx=0
        for i in range(len(x)):
            rr = random.randint(1, 10)
            xx=xx+rr*x[i]


        k2=k2+r*np.kron(oo,xx)

    enc_query=k1+k2


    """for i in range(len(j_a)):
        enc_query += k * kron_product[i]

    for i in range(len(kron_product) - len(j_a)):
        enc_query += float(r) * kron_product[len(j_a) + i]
"""


    return enc_query

"""
    r = random.randint(1, 10000000000000000000000)
    for i in range(len(kron_product[0])):
        kk.append(kron_product[0][i])


    enc_query = 0
    for i in range(len(j_a)):

        enc_query += k * kk[i]



    for i in range(len(kk) - len(j_a)):

        enc_query += float(r) * kk[len(j_a) + i]
"""
    #print(enc_query)









    #for i in range(l_ja):
    #    v+=s

"""
    r = random.randint(1, 10000000000000000000000)

    for i in range(l):

        bit_list = [int(bit) for bit in hash32(row[aaa[i]])]
        bit_list=np.array(bit_list)


        v+=bit_list
    v = np.array(v)
    oo=0
    r = random.randint(1, 10000000000000000000000)
    for i in range(l_ja):
        h_a = hash(row[j_a[i]])
        oo += h_a * o[i]

    for i in range(len(o)-l_ja):
        oo=oo+r*o[i+l_ja]


    kron_product = np.kron(oo, v)




    r = random.randint(1, 10000000000000000000000)
    enc_row = 0
    enc_row=kron_product"""

"""for i in range(l_ja):
        h_a = hash(row[j_a[i]])
        enc_row += h_a * kron_product[i]

    for i in range(len(kron_product) - l_ja):
        enc_row += float(r) * kron_product[l_ja + i]"""







"""
    r = random.randint(1, 10000000000000000000000)
    enc_row=0
    for i in range(l_ja):
        h_a = hash(row[j_a[i]])
        enc_row+=h_a*kron_product[i]

    for i in range(len(kron_product)-l_ja):


        enc_row+=float(r)*kron_product[l_ja+i]"""






def find_closest_value_with_tolerance(d, key, tolerance):
    """
    在给定的字典d中，查找与key最接近（在误差范围内）的键，并返回其对应的值。

    参数:
        d -- 字典，其键可能是浮点数
        key -- 要查找的键（浮点数）
        tolerance -- 允许的误差范围

    返回:
        如果找到匹配的键（在误差范围内），则返回其对应的值；否则返回None。
    """
    closest_key = None
    closest_diff = float('inf')  # 初始化差异为无穷大

    for dict_key in d:
        diff = abs(dict_key - key)
        if diff <= tolerance and diff < closest_diff:
            closest_key = dict_key
            closest_diff = diff

    if closest_key is not None:
        return d[closest_key]
    else:
        return None

