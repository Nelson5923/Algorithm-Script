# Create sample matrix
A = rbind(c(7,4,3)
, c(4,1,8)
, c(6,3,5)
, c(8,6,1)
, c(8,5,7)
, c(7,2,9)
, c(5,3,3)
, c(9,5,8)
, c(7,4,5)
, c(8,2,2))

# Perform the PCA 
# Scale decide whether standardize the sample with unit variance for PCA
R = prcomp(A, scale = FALSE)

# Choose the number of principal component to retain
plot(R, type = "l")

# Check the proportion of variance
summary(R)

# The eigenvector for PCA
R$rotation

# The principal component for sample
R$x[,1:2]

# The mean or center of original data
R$center

# The sd of original data
R$scale
