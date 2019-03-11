sim_poi <- function(lambda) {
		u <- runif(1)
		x <- 0
		p.x <- exp(-lambda)
		F.x <- p.x
	while (F.x <= u) {
		p.x <- lambda * p.x / (x + 1)
		F.x <- F.x + p.x
		x <- x + 1
	}
	return(x)
}

set.seed(1999)
n <- 5000
g <- rep(0, n)
for (i in 1:n) g[i] <- sim_poi(10)

hist(g, breaks=seq(0,30, length=30), freq=F, xlab="x", ylab="pdf f(x)",
main="theoretical and simulated poisson(10)")

x <- seq(0, max(g))
lines(x, dpois(x, 10))

