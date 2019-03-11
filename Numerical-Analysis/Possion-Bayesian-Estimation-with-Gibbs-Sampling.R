target <- function(lambda){
	n <- 10000
	sum <- 230280
	v <- (log(lambda) * (sum - 1)) + (-n * lambda)      #log of the function
	return (v)
}
poi_sim <-function(){
x = rep(0,10000)
x[1] <- 1
for(i in 2:10000){
	cur_lambda <- x[i-1]
	prop_lambda <- cur_lambda + rnorm(1,mean=0,sd=1)
	if(prop_lambda < 0){
		Accept_Value = 0
	}else{
		Accept_Value = exp(target(prop_lambda) - target(cur_lambda))
	}
	if(runif(1) < Accept_Value){
		x[i] =  prop_lambda     
	} else {
    		x[i] = cur_lambda       
  	}
}
burn <- 2000
x <- x[(burn+1):10000]
return (mean(x))
}

a <- c(0,0)
for(i in 1:30){
a[i] <- poi_sim()
}

x <- rpois(10000,23)
x_l <- length(x)
x_s <- sum(x)

