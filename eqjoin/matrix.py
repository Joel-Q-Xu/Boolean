import sympy
import random
import numpy as np
from numpy import linalg

def primes(start, stop):
    if start < 2:
        start = 2
    for i in range(start, stop+1):
        for j in range(2, i):
            if i % j == 0:
                break
        else:
            yield i

# 0到200之间的素数

def is_unit_element(x, q):
    """Check if x is a unit element in Z_q."""
    return sympy.gcd(x, q) == 1
def generate_invertible_matrix(n,q):

    # if not sympy.isprime(q):
    #     raise ValueError("q must be a prime number.")
    field = sympy.GF(q)
    matrix = None
    is_in_gl_group = False

    while not is_in_gl_group:

        matrix_elements = [[(random.randint(0, q-1)) for _ in range(n)] for _ in range(n)]


        matrix = sympy.Matrix(matrix_elements)



        # Check if determinant is a unit element in Zq using sympy
        #det = matrix.det() % q
        det=189589953848941294975305437483269149409177451016


        is_in_gl_group = is_unit_element(det, q)


    return det,matrix

# n = 10
#
# det,matrix = generate_invertible_matrix(5,11)
# print(det,matrix)
#
#
# print(det,matrix)
# print(type(matrix))
# print(type(matrix.adjugate()))
#
#
# a=matrix.adjugate()
# a=a.transpose()
# print(a)
# # 计算行列式
# det = np.linalg.det(a)
#
# # 创建一个对角矩阵D，其对角线元素是B的行列式的每个元素的倒数
# D = np.diag(1 / det * np.diag(a))
#
# # 创建新的矩阵A
# A = a @ D
#
# # 打印结果
# print(A)

