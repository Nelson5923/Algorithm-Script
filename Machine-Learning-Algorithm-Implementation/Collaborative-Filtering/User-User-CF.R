# Create sample matrix
A = rbind(c(2,1,5,4,3,NA)
, c(NA,2,NA,3,5,4)
, c(5,NA,4,1,4,2)
, c(2,3,4,5,NA,NA)
, c(NA,4,1,NA,3,2))

# User Similarity
calcor <- function(B){ 
	r = c(NA,2,NA,3,5,4)
	r = r - mean(r[!is.na(r)])
	r[is.na(r)] <- 0
	C = B - mean(B[!is.na(B)])
	C[is.na(C)] <- 0
	cosine(r,C)
}

# Predict the missing value by User Similarity
sim <- apply(A,1,calcor)
(sim[1] * A[1,3] + sim[3] * A[3,3]) / (sim[1] + sim[3])
