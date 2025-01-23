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

"""
Implementation of function-hiding inner product encryption (FHIPE).
"""
import numpy as np
from scipy.linalg import qr, lapack,hadamard
import csv
import sys, os, math, random
import time
from mpmath import mp

# Path hack
sys.path.insert(0, os.path.abspath('charm'))
sys.path.insert(1, os.path.abspath('../charm'))


import numpy as np

import sympy

IN_CLAUSE_MAX_SIZE = 1
import hashlib
from sympy import Matrix, GramSchmidt
import binascii
from scipy.linalg.blas import sdot




def decrypt(k,ind):
    t = np.dot(k, ind)
    """print(type(k[0]))
    print(type(ind[0]))
    print(len(k))
    print(len(ind))
    time1=time.perf_counter()
    t=np.dot(k,ind)
    time2=time.perf_counter()
    print(time2-time1)
"""


    return t








def sha1_binary(input_string):
    # 创建一个新的sha1 hash对象



    # 使用hash对象的update()方法将字符串编码为字节并添加到哈希中
    hash_16=binascii.crc_hqx(input_string.encode('utf-8'),0)


    # 使用digest()方法获取二进制表示的哈希值
    binary_dig = format(hash_16, '016b')
    #binary_dig = bin(hash_16)[2:]


    return binary_dig


def encryptQuery(o, k, j_a,table_name,x_q,d):
    encry_query=0
    encry_query1 = 0
    encry_query2 = 0
    encry_query3 = 0
    #r=1

    for i in range(len(j_a)):
        time1 = time.time()


        encry_query1+=o[j_a[0]]*k*d
        time2 = time.time()


    for j in range(len(x_q)):


        r = random.randint(1, 1000000)


        encry_query2 -= o[table_name] * r * x_q[j][-1]

        for i in range(len(x_q[j])-1):
            encry_query3 += o[x_q[j][i]] * r




    encry_query=encry_query1+encry_query2+encry_query3

    #encry_query=encry_query.astype(np.int64)

    #for i in range(len(encry_query)):
        #encry_query[i]=encry_query[i].evalf(n=55)

    return encry_query
def schmidt(ma):
    MA = [Matrix(col) for col in ma]
    gram = GramSchmidt(MA,orthonormal=True)# orthonormal=True表示需要做归一化处理
    gram_numpy = [np.array(gram_vec.tolist()).reshape(-1, 1) for gram_vec in gram]
    # 使用 np.hstack 将这些列向量水平堆叠成一个二维 numpy 数组
    return np.hstack(gram_numpy)


def next_power_of_two(n):
    # 如果n已经是2的幂，则直接返回n
    if n & (n - 1) == 0:
        return n
    # n |= n >> 1; n |= n >> 2; n |= n >> 4; ... 不断右移直到n变为全1（对于32位整数）
    # 但在Python中，我们可以直接利用n的二进制表示，通过不断或上自己右移的结果
    # 直到没有更高位的1为止（此时n+1的二进制表示中，最低位的1以上的位全为1）
    # 然后n+1就是我们要找的结果
    n -= 1
    n |= n >> 1
    n |= n >> 2
    n |= n >> 4
    n |= n >> 8
    n |= n >> 16
    n |= n >> 32
    # 如果n是64位的，你可能还需要添加n |= n >> 32
    n += 1
    return n


def next_multiple_of_4(n):
    # Step 1: Integer division by 4
    quotient = n // 4

    # Step 2: Multiply by 4 to get the largest multiple of 4 less than or equal to n
    largest_multiple_leq_n = quotient * 4

    # Step 3: Check if this multiple is equal to n
    # If not, add 4 to get the next multiple of 4
    if largest_multiple_leq_n < n:
        next_multiple = largest_multiple_leq_n + 4
    else:
        # If largest_multiple_leq_n is equal to n (which means n is already a multiple of 4),
        # we can return it directly or add 0 (which doesn't change the value)
        next_multiple = largest_multiple_leq_n

        # Alternatively, you can simplify the above if-else logic with a single line:
    # next_multiple = (quotient + (1 if largest_multiple_leq_n < n else 0)) * 4
    # But the explicit if-else is clearer for understanding.

    return next_multiple
def geto(table, j_a, aaa,table_name):
    h_a = set()
    for row in table:
        l_aaa = len(aaa)
        for i in range(l_aaa):
            h_a.add(aaa[i]+row[aaa[i]])
            #print(aaa[i]+row[aaa[i]])

    h_a=list(h_a)
    l_ja = len(j_a)
    l_a = len(h_a)

    l = l_ja + l_a + 1
    n=next_power_of_two(l)

    h=hadamard(n)
    h = h.astype(np.int8)


    dict_a = {}


    dict_a[table_name] = h[0]
    for i in range(l_ja):

        dict_a[j_a[i]]=h[i+1]


        # print(x[aaa[i]])

    for i in range(l_a):

        dict_a[h_a[i]]=h[1+l_ja+i]





    return dict_a,n

def encryptTable(o, table, pk, j_a, x,aaa,table_file):
    #print([(row[pk], row[x], encryptRow(msk,a, row, max_degree,aaa)) for row in table])



    return [(row[pk], row[x], encryptRow(o,j_a, row, aaa,table_file)) for row in table]

def encryptRow(o, j_a, row,aaa,table_name):
    enc_row1=0
    enc_row2 = 0
    enc_row3 = 0
    enc_row=0
    #r=1
    r = random.randint(1,  1000000)
    l_ja = len(j_a)
    for i in range(l_ja):
        h_a =int(row[j_a[i]])
        #h_a=1
        oo=o[j_a[i]]
        enc_row1=enc_row1+(oo*h_a)
    enc_row2 = enc_row2 + (r * o[table_name])
    l = len(aaa)
    for i in range(l):
        enc_row3=enc_row3+(r*o[aaa[i]+row[aaa[i]]])


    enc_row=enc_row1+enc_row2+enc_row3

    enc_row = enc_row.astype(np.int32)

    return enc_row


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

