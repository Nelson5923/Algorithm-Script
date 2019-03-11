# Create sample matrix
A = rbind(c(2,1,5,4,3,NA)
, c(NA,2,NA,3,5,4)
, c(5,NA,4,1,4,2)
, c(2,3,4,5,NA,NA)
, c(NA,4,1,NA,3,2))

AT <- t(A)

# Item Similarity
calcor <- function(B, r = c(5,NA,4,4,1)){ 
	r = r - mean(r[!is.na(r)])
	r[is.na(r)] <- 0
	C = B - mean(B[!is.na(B)])
	C[is.na(C)] <- 0
	cosine(r,C)
}

# Predict the missing value by Item Similarity # Weight what a User Buy
sim <- apply(AT,1,calcor)
(sim[4] * A[2,4] + sim[5] * A[2,5] + sim[6] * A[3,6]) / (sim[4] + sim[5] + sim[6])
