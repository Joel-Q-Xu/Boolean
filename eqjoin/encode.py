import scipy.linalg
import numpy as np


#----------------------------编码
def hadamard_to_decimal(H):
    # 将 -1 替换为 0
    H_binary = np.where(H == -1, 0, 1)
    # 将每一行的二进制数组转换为十进制整数
    decimal_list = [int(''.join(map(str, row)), 2) for row in H_binary]

    return decimal_list

# ----------------------------解码
def decimal_to_hadamard_row(decimal, n):
    # 将十进制数转换为二进制字符串，长度为 n，前面补 0
    binary_string = format(decimal, f'0{n}b')
    # 将二进制字符串转换为向量：0 变为 -1，1 保持不变
    row = np.array([1 if bit == '1' else -1 for bit in binary_string])
    return row

n = 4096  # 矩阵的大小，必须是 2 的幂次方
H = scipy.linalg.hadamard(n)

encH=hadamard_to_decimal(H)

print("哈达玛矩阵:")
# print(H)

# print("哈达玛矩阵的十进制编码列表:")
# # print(encH)


# 将十进制数还原为哈达玛矩阵的行向量
index = 10
decoded_row = decimal_to_hadamard_row(encH[index], n)
for i in range(len(H[index])):
    if H[index][i]!=decoded_row[i]:
        print("sdfghjkl;")

print(f"\n十进制数 {encH[index]} 解码回的原始行向量:")
print(decoded_row)