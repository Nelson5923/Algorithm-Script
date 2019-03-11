poisson <- function(x) {

	xi <- c(-0.30, 0.32, 0.41, 0.62 ,-0.21 ,0.31 ,0.41 ,0.81, 0.50, -0.21, -0.20, -0.7, -0.1, -0.13, 0.96)
	yi <- c(2, 10, 11, 22, 0, 6, 9, 34, 5, 0, 1, 21, 3, 2, 29)
	f <- sum(-1 * exp(x[1] + x[2] * xi) + yi * (x[1] + x[2] * xi) + -1 * log(factorial(yi)))
	f1 <- sum(-1 * exp(x[1] + x[2] * xi) + yi)
	f2 <- sum(xi * -1 * exp(x[1] + x[2] * xi) + (xi * yi))
	f11 <- -1 * sum(exp(x[1] + x[2] * xi))
	f12 <- -1 * sum(xi * exp(x[1] + x[2] * xi))
	f22 <- -1 * sum(xi^2 * exp(x[1] + x[2] * xi))
	G <- c(f1, f2)
	H <- matrix(c(f11, f12, f12, f22), 2, 2)
	return(list(f, G, H))

}

logistic <- function(x) {

	xi <- c(-0.30, 0.32, 0.41, 0.62 ,-0.21 ,0.31 ,0.41 ,0.81, 0.50, -0.21, -0.20, -0.7, -0.1, -0.13, 0.96)
	yi <- c(0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0)
	f <- sum(yi * (x[1] + x[2] * xi)) - sum(log(1+exp(x[1] + x[2] * xi)))
	f1 <- sum(yi) - sum(exp(x[1] + x[2] * xi)/(1+exp(x[1] + x[2] * xi)))
	f2 <- sum(xi * yi) - sum((exp(x[1] + x[2] * xi)/(1+exp(x[1] + x[2] * xi)) * xi)) 
	f11 <- -1 * sum(exp(x[1] + x[2] * xi)/(1+exp(x[1] + x[2] * xi)^2))
	f12 <- -1 * sum((exp(x[1] + x[2] * xi)/(1+exp(x[1] + x[2] * xi)^2)) * xi) 
	f22 <- -1 * sum((exp(x[1] + x[2] * xi)/(1+exp(x[1] + x[2] * xi)^2)) * xi^2) 
	G <- c(f1, f2)
	H <- matrix(c(f11, f12, f12, f22), 2, 2)
	return(list(f, G, H))
}

newton <- function(f3, x0, tol = 1e-9, n.max = 100) {
	
	x <- x0
	n <- 0
	NEW_f3 <- f3(x0)

	while(n < n.max) {
		y <- solve(NEW_f3[[3]], NEW_f3[[2]])
		x <- x - y
		NEW_f3 <- f3(x)
		cat(n, '-->', x, '\n')
		if(max(abs(y)) < tol) break
		n <- n + 1
	}
	if (n == n.max) {
		cat('newton failed to converge\n')
	} 
	else {
		return(x)
	}

}

x0 = c(5,5)
cat('Poisson:', 'From', x0, '-->', newton(poisson ,x0), '\n')

x0 = c(0.5,0.5)
cat('Logistic:', 'From', x0, '-->', newton(logistic ,x0), '\n')



