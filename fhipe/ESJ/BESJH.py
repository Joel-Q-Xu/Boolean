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
from scipy.linalg import qr,lapack,hadamard
import csv
import sys, os, math, random
import time
from mpmath import mp
# Path hack
from numba import njit
from numba import prange,set_num_threads
sys.path.insert(0, os.path.abspath('charm'))
sys.path.insert(1, os.path.abspath('../charm'))

import numpy as np
#import bbhash
import hashlib
#from pyxorfilter import Xor8, Xor16, Fuse8, Fuse16
IN_CLAUSE_MAX_SIZE=1

from sympy import Matrix, GramSchmidt
import binascii
from scipy.linalg import null_space

set_num_threads(8)

def decrypt(k,ind):

    t=np.dot(k,ind)



    return t

def hash32(input_string):
    # 使用mmh3.hash函数计算输入字符串的哈希值
    hash_value = mmh3.hash(input_string)

    # 将哈希值转换为二进制字符串，并确保它是32个字符长
    # 如果哈希值是32位的，那么bin(hash_value)[2:]将直接给出32个字符（或者更少，如果最高位是0）
    # 我们需要填充它以确保它是32个字符长
    binary_dig = bin(hash_value)[3:].zfill(32)



    return binary_dig
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
def getmap( table, j_a, aaa,table_name):
    h_a = set()

    l_ja = len(j_a)
    l_a = len(aaa)

    l = l_ja + l_a
    n=next_power_of_two(l)
    seed_matrix = np.array([[-6, 3], [3, 6]], dtype=np.int32)  # 2x2 seed matrix


    return l,n,seed_matrix
def geto(l,n,seed_matrix):
    row = []
    for i in range(l):
        # Get the i-th row directly from 0
        row.append(get_row_sylvester_fast_par(seed_matrix, n, i))
    return row

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
        h_a = hash(row[j_a[i]])
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
    enc_row = enc_row.astype(np.int32)

    return enc_row





def encryptQuery(seedm, k, j_a,aaa,x_q,d,d_d):
    k1=0
    if len(j_a)!=0:
        vector = np.zeros(32, dtype=np.int32)
        vector[0] = 1
        dd = np.linalg.norm(get_row_sylvester_fast_par(seedm, d_d, 0), ord=2) ** 2
        k1 = k * np.kron(get_row_sylvester_fast_par(seedm, d, 0), vector) * dd
    k2=0

    if len(x_q)!=0:
        for i in range(len(x_q[1])):

            index = aaa.index(x_q[0][i])

            oo = get_row_sylvester_fast_par(seedm, d, index + 1)
            a = []
            r = random.randint(1, 1000)
            for j in range(len(x_q[1][i])):
                bit_list = [int(bit) for bit in hash32(x_q[1][i][j])]

                a.append(bit_list)
            a = np.array(a)

            x = null_space(a)
            x = np.array(x)
            x = x.T
            xx = 0
            for i in range(len(x)):
                rr = random.randint(1, 100)
                xx = xx + rr * x[i]

            k2 = k2 + r * np.kron(oo, xx)



    enc_query=k1+k2
    #enc_query=enc_query.astype(np.int64)





    return enc_query



