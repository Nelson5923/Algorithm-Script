import numpy as np

# Test Data
D = np.array([[7,4,3],
    [4,1,8],
    [6,3,5],
    [8,6,1],
    [8,5,7],
    [7,2,9],
    [5,3,3],
    [9,5,8],
    [7,4,5],
    [8,2,2]], dtype='float64')

n,k = np.shape(D) # Shape of D
mu = np.mean(D, 0) # Mean of each Column

# Center the Data
D = D - np.tile(mu,(n, 1))

# Covariance Matrix
Z = np.dot(D.T, D)/(n-1)

# Eigvalues and Eigenvectors of covariance Matrix
U,V = np.linalg.eigh(Z)

# Rearrange the Eigenvectors and Eigenvalues
U = U[::-1]
for i in range(k):
    V[i,:] = V[i,:][::-1]

v = V[:,:2]  # Picking Principal Component
A = np.dot(D, v) # Dimension Reduction