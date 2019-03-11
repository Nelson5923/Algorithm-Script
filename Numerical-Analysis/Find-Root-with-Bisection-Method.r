func <- function(x) {
  x^3 + 7.8 * x^2 - 28.33 * x - 39.27
}

bisection <- function(f, a, b, n = 1000, tol = 1e-7) {

	if ((f(a) < 0) && (f(b) < 0)) {
		stop('signs of f(a) and f(b) are the same')
	} else if ((f(a) > 0) && (f(b) > 0)) {
		stop('signs of f(a) and f(b) are the same')
	}

	i <- 0
	fa <- f(a)
	while (i < n){
		p <- a + (b - a)/2
		fp <- f(p)
		cat(i, p, '\n')
		if ((fp == 0) || ((b - a)/2 < tol)) 
			return(p)
		i <- i + 1
		if (fa * fp > 0){
			a <- p
			fa <- fp
		}
		else{
			b <- p
		}
	}
	
  	print('Can\'t converge')

}

curve(func, xlim=c(-4,4), col='blue', lwd=1.5, lty=2)
abline(h=0)
abline(v=0)
options(digits=9)
bisection(func, 2, 9)
bisection(func, -2, 0.5)






