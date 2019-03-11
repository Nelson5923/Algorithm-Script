
EM <- function(x, p, u1, u2, var1, var2, max_n = 50){
	n <- 0
	yi <- x
	cat('Initial:', n, p, u1, u2, var1, var2, '\n')
	while(n < max_n){
		wit <- (p * dnorm(yi ,u1, sqrt(var1))) / (p * dnorm(yi ,u1,sqrt(var1)) + (1 - p) * dnorm(yi ,u2,sqrt(var2)))
		p <- sum(wit) / length(yi)
		u1 <- sum(wit * yi) / sum(wit)
		u2 <- sum((1 - wit) * yi) / sum(1 - wit)
		var1 <- sum(wit * (yi - u1)^2) / sum(wit) 
		var2 <- sum((1 - wit) * (yi - u2)^2) / sum(1 - wit)
		n <- n + 1
		cat(n, p, u1, u2, var1, var2, '\n')
	}
	return(c(p, u1, u2, var1, var2))
}

y <- scan(file = "heights_data.txt", what = 0, n = -1, sep = "", skip = 1, quiet = FALSE)

p0 <- 0.5
u1 <- 150
u2 <- 170
var1 <- 99
var2 <- 60
EM(y,p0,u1,u2,var1,var2)

hist(y , 
     main="Height Distribution",         
     xlab="Height",                   
     ylab="Number of Samples")   