#Parallel EM Algorithm
Parallel_EM <- function(x, p, u1, u2, var1, var2, cl, core, max_n = 20){
  
	n <- 0
	yi <- x
  	yi_chunks <- split(yi, 1:core)
  	wit <- list(0,0)
  	cat('Initial:', n, p, u1, u2, var1, var2, '\n')
  
  	while(n < max_n){
    
    		#MAP_1
    		MAP_1_rs <- clusterApply(cl,yi_chunks,MAP_1,p,
                             u1,u2,var1,var2)
    
    		MAP_1_f1 <- MAP_1_f2 <-
    		MAP_1_f3 <- MAP_1_f4 <- c(0)
    
    		for(i in 1:core){
      		MAP_1_f1[i]	<- MAP_1_rs[[i]][1]
      		MAP_1_f2[i] <- MAP_1_rs[[i]][2]
      		MAP_1_f3[i]	<- MAP_1_rs[[i]][3]
      		MAP_1_f4[i] <- MAP_1_rs[[i]][4]	
    		}
    
		#TEMP FOR MAP2
		p_old <- p
		u1_old <- u1
		u2_old <- u2

   		#REDUCE_1
    		p <- sum(MAP_1_f1) / length(yi)
    		u1 <- sum(MAP_1_f2) / sum(MAP_1_f1)
    		u2 <- sum(MAP_1_f4) / sum(MAP_1_f3)
    
    		#MAP_2
    		MAP_2_rs <- clusterApply(cl,yi_chunks,MAP_2,
                             p_old,u1_old,u2_old,var1,var2,u1,u2)
    
    		MAP_2_f1 <- MAP_2_f2 <- c(0)
    
    		for(i in 1:core){
      		MAP_2_f1[i] <- MAP_2_rs[[i]][1]
      		MAP_2_f2[i] <- MAP_2_rs[[i]][2]
    		}
    
    		#REDUCE_2
    		var1 <- sum(MAP_2_f1) / sum(MAP_1_f1)
    		var2 <- sum(MAP_2_f2) / sum(MAP_1_f3)
    
    		#Next Iteration
    		n <- n + 1
    		cat(n, p, u1, u2, var1, var2, '\n')
    
  	}
  
  	return(c(p, u1, u2, var1, var2))
  
}

MAP_1 <- function(yi_chunks, p, u1, u2, var1, var2){
  
	wit <- (p * dnorm(yi_chunks ,u1, sqrt(var1))) / 
    	(p * dnorm(yi_chunks ,u1,sqrt(var1)) + 
      (1 - p) * dnorm(yi_chunks ,u2,sqrt(var2)))
  
	f1 <- sum(wit)
  	f2 <- sum(wit * yi_chunks)
  	f3 <- sum(1 - wit)
  	f4 <- sum((1 - wit) * yi_chunks)
  
	return(c(f1,f2,f3,f4))
  
}

MAP_2 <- function(yi_chunks,p_old,u1_old,u2_old
			,var1,var2,u1,u2){
  	
	wit <- (p_old * dnorm(yi_chunks ,u1_old, sqrt(var1))) / 
    	(p_old * dnorm(yi_chunks ,u1_old,sqrt(var1)) + 
      (1 - p_old) * dnorm(yi_chunks ,u2_old,sqrt(var2)))

  	f1 <- sum(wit * (yi_chunks - u1)^2)
  	f2 <- sum((1 - wit) * (yi_chunks - u2)^2)

  	return (c(f1,f2))
  
}

#Declare the Header
library(snow)
library(parallel)

#Empirical Distribution
y <- scan(file = "heights_data.txt", what = 0, n = -1, sep = "", skip = 1, quiet = FALSE)
hist(y , 
     main="Height Distribution",         
     xlab="Height",                   
     ylab="Number of Samples")   

#Initialize the Cluster
cl <- makeCluster(detectCores(), type="SOCK") #Not Thread but Process
core <- length(cl)

#Initial Value
p0 <- 0.3
u1 <- 120
u2 <- 190
var1 <- 99
var2 <- 60

#Driver for Parallel EM Algorithm
run_time <- system.time(result <- Parallel_EM(y, p0, u1, u2, var1, var2, cl,core))
cat('Fraction of Female: ', result[1], '\n')
cat('Mean Height of Female: ', result[2], '\n')
cat('Mean Height of Male: ', result[3], '\n')
cat('Variance of Female Height: ', result[4], '\n')
cat('Variance of Male Height: ', result[5], '\n')
cat('Running Time for Parallel EM Algorithm: ', run_time[3], '\n')
cat('Number of Core Used:' ,core, '\n')

#Close the Resource
stopCluster(cl)
detach("package:snow", unload = TRUE)
detach("package:parallel", unload = TRUE)