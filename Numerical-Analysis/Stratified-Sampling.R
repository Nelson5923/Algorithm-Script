salary_data<-read.table("salary_data.txt", header=T, sep=" ")
set.seed(1989)
s_100 <- salary_data[sample(nrow(salary_data), 100),]
s_100_y <- s_100$Salary
s_100_stratum <- s_100$Age_Indicator
s_100_1 <- s_100_y[s_100_stratum==1]
cat('SD For 1:', sd(s_100_1), '\n')
s_100_2 <- s_100_y[s_100_stratum==2]
cat('SD For 2:', sd(s_100_2), '\n')
s_100_3 <- s_100_y[s_100_stratum==3]
cat('SD For 3:', sd(s_100_3), '\n')

set.seed(1944)
s_1000 <- salary_data[sample(nrow(salary_data), 1000),]
s_1000_y <- s_1000$Salary
s_1000_stratum <- s_1000$Age_Indicator
s_1000_1 <- s_1000_y[s_1000_stratum==1]
cat('Size For 1:', length(s_1000_1), '\n')
s_1000_2 <- s_1000_y[s_1000_stratum==2]
cat('Size For 2:', length(s_1000_2), '\n')
s_1000_3 <- s_1000_y[s_1000_stratum==3]
cat('Size For 3:', length(s_1000_3), '\n')

N1=2000
N2=4000
N3=5000
N=N1+N2+N3

u1 <- mean(s_1000_1)
cat('Sample Mean For 1:', mean(s_1000_1), '\n')
u2 <- mean(s_1000_2)
cat('Sample Mean For 2:', mean(s_1000_2), '\n')
u3 <- mean(s_1000_3)
cat('Sample Mean For 3:', mean(s_1000_3), '\n')
u_est <- (N1*u1 + N2*u2 +N3*u3)/N
cat('Estimated Mean For Population:', u_est, '\n')
u_ture <- mean(salary_data$Salary)
cat('True Mean For Population:', u_ture, '\n')

#Function Testing#
#attach(salary_data)
#table(stratum)
#names(salary_data)
#tapply(y,stratum,mean)
#tapply(y,stratum,var)
#detach(s_100)
#search()






