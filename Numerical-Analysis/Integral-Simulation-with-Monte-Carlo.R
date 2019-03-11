sim_exp <- function(lambda){
	u <- runif(1)
	x <- -1/(lambda) * log(u)
	y <- x + 3
	return (y)
}

sim_normal <- function() {
	M <- exp(-4.5) / (3 * sqrt(2 * pi) * (1-pnorm(3)))
	g <- function(x) 3 * exp(-3 * (x - 3))
	f <- function(x) exp(-1 * (x^2 / 2)) / (sqrt(2 * pi) * (1-pnorm(3)))
	while (TRUE) {
		u <- runif(1)
		y <- sim_exp(3)
		if (u <= f(y)/M*g(y)) return(y)
	}
}

set.seed(1999)
n <- 5000
g <- rep(0, n)
for (i in 1:n) g[i] <- sim_normal()
x <- sqrt(2 * pi) * (1-pnorm(3)) * sin(g)
cat('Classic Sampling:',mean(x),'\n')

set.seed(1999)
n <- 5000
g <- rep(0, n)
for (i in 1:n) g[i] <- sim_exp(3)
x <- sin(g) * exp(-1 * (g^2 / 2)) / (3 * exp(-3 * (g-3)))
cat('Important Sampling:',mean(x),'\n')





