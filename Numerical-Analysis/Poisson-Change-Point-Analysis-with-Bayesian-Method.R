cc <- rep('NULL', 2) 
cc[2] <- 'integer'
input <- read.table('Assignment3_Q1.txt', header=F, sep="")
y <- input[,2] #extract the column
n <- length(y) - 1    #length of the data - 1
m <- 1000         #length of the chain
p<-rep(0,3*m) # array to store chains
dim(p)<-c(m,3)
a1<-2 # parameter of priors
a2<-2
b1<-2
b2<-2
p[1,1]<-1
p[1,2]<-1
p[1,3]<-as.integer(n*runif(1))+1 #random prior
L <- numeric(n)

for (i in (2:m)) {

	k <- p[i-1,3]
	p[i,1]<-rgamma(1,a1+sum(y[1:k]),k+b1) #lambda
	p[i,2]<-rgamma(1,a2+sum(y)-sum(y[1:k]),n-k+b2) #mu

      for (j in 1:n) { #calculate the probability for each possible k
      	L[j] <- exp((p[i,2] - p[i,1]) * j) *
      	        (p[i,1] / p[i,2])^sum(y[1:j])
      }
      L <- L / sum(L)
	p[i,3] <- sample(1:n, prob=L, size=1) #sample from the pdf

}

burn<- 200
estimate_lambda <- mean(p[(burn+1):m,1])
estimate_mu <- mean(p[(burn+1):m,2])

getmode <- function(v) {
   uniqv <- unique(v)
   uniqv[which.max(tabulate(match(v, uniqv)))]
}

estimate_change_point <- getmode(p[(burn+1):m,3])

cat('Estimated Lambda:', estimate_lambda, '\n')
cat('Estimated Mu:', estimate_mu, '\n')
cat('Estimated Change Point:', estimate_change_point, '\n')

plot(p[,3], type="l", ylab="change point = k")
