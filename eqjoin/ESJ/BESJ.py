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
import bbhash
import hashlib
from pyxorfilter import  Fuse16 as Fuse8
import BF
from numba import njit
from numba import prange,set_num_threads
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
set_num_threads(8)



def decrypt(k,ind):
    t = np.dot(k, ind)


    return t








def encryptQuery(xorf,mph,seedm, k, j_a,table_name,x_q,d,d_d):
    encry_query=0
    encry_query1 = 0
    encry_query2 = 0
    encry_query3 = 0
    #r=1

    dd = np.linalg.norm(get_row_sylvester_fast_par(seedm, d_d, 0), ord=2) **2


    for i in range(len(j_a)):

        encry_query1+=get_row_sylvester_fast_par(seedm, d, mph.lookup(string_to_64bit_int(j_a[0])))*k*dd




    for j in range(len(x_q)):


        r = random.randint(1, 1000000)


        encry_query2 -= get_row_sylvester_fast_par(seedm, d, mph.lookup(string_to_64bit_int(table_name))) * r * x_q[j][-1]

        for i in range(len(x_q[j])-1):

            x=xorf.contains(x_q[j][i])


            if xorf.contains(x_q[j][i]) == True:
            #if ((x_q[j][i]) in bf):
                encry_query3 += get_row_sylvester_fast_par(seedm, d, mph.lookup(string_to_64bit_int(x_q[j][i]))) * r

            """else:
                print("no exist")
            """






    encry_query=encry_query1+encry_query2+encry_query3

    #encry_query=encry_query.astype(np.int64)

    #for i in range(len(encry_query)):
        #encry_query[i]=encry_query[i].evalf(n=55)

    return encry_query


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
def string_to_64bit_int(s):
    h = hashlib.md5(s.encode()).digest()[:8]  # 取前 8 字节（64 位）
    return int.from_bytes(h, byteorder='big')
def get_row_of_sylvester(seed_matrix, n, i):
    """

    Parameters:
    seed_matrix (numpy.ndarray): 2x2 matrix with orthogonal rows
    k (int): The size of the matrix (2^ceil(log2(k)) x 2^ceil(log2(k)))
    i (int): The row index (0-based)

    Returns:
    numpy.ndarray: The i-th row of the Sylvester matrix
    """
    # Validate that n is a power of 2 and at least 2
    if n < 2 or (n & (n - 1)) != 0:
        raise ValueError("n must be a power of 2 (>= 2)")

    row = np.empty(n, dtype=seed_matrix.dtype)
    for j in range(n):
        # Select base seed entry from the 2×2 seed_matrix
        seed_val = seed_matrix[i & 1][j & 1]

        # Compute number of sign flips from recursion levels, using bin().count for popcount
        common = (i >> 1) & (j >> 1)
        flips = bin(common).count('1')

        # Determine sign: flip if odd number of flips
        sign = -1 if (flips & 1) else 1

        row[j] = sign * seed_val

    return row
@njit
def popcount_swar_jit(x: int) -> int:
    # 直接对低 64 位做一次 SWAR，Numba 下 x 会被截断到机器整数
    x = x - ((x >> 1) & 0x5555555555555555)
    x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333)
    x = (x + (x >> 4)) & 0x0F0F0F0F0F0F0F0F
    return (x * 0x0101010101010101) >> 56

@njit(parallel=True)
def get_row_sylvester_fast_par(seed, n, i):
    row = np.empty(n, dtype=seed.dtype)
    i0 = i & 1
    i_shifted = i >> 1
    for j in prange(n):
        sv    = seed[i0, j & 1]
        flips = popcount_swar_jit(i_shifted & (j >> 1))
        row[j] = sv if (flips & 1) == 0 else -sv
    return row

def getmap(table, j_a, aaa,table_name):
    h_a = set()
    for row in table:
        l_aaa = len(aaa)
        for i in range(l_aaa):
            h_a.add(aaa[i]+row[aaa[i]])
            #print(aaa[i]+row[aaa[i]])

    h_a=list(h_a)
    l_ja = len(j_a)
    l_a = len(h_a)
    ss=[]
    ss.append(table_name)
    l = l_ja + l_a + 1

    n = next_power_of_two(l)
    for i in range(l):
        if i<l_ja:
            ss.append(j_a[i])
        else:
            ss.append(h_a[i-l_a])
    sss = [string_to_64bit_int(s) for s in ss]

    mph = bbhash.PyMPHF(sss, len(sss), 8,1)#n/len(sss))
    seed_matrix = np.array([[-6, 3], [3, 6]], dtype=np.int32)  # 2x2 seed matrix
    #k = l  # |W|
    #n = 2 ** (int(np.ceil(np.log2(k))))

    #bf1 = BF.BloomFilter(l, 0.000001)
    #for i in ss:
     #   bf1.add(i)
    xor_filter = Fuse8(l)

    xor_filter.populate(ss.copy())





    return ss,n,xor_filter,mph,seed_matrix
def geto(ss,mph,seed_matrix,n):
    o = {}
    for i in range(len(ss)):
        hashed_key = string_to_64bit_int(ss[i])
        idx = mph.lookup(hashed_key)
        o[ss[i]] = get_row_sylvester_fast_par(seed_matrix, n, idx)
    return o




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
        h_a =hash(row[j_a[i]])
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



