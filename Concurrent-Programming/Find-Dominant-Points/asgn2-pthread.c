#include<stdio.h>
#include<stdlib.h>
#include<pthread.h>
#include<unistd.h>
#include"util.h"
#include<omp.h>

// If you have referenced to any source code that is not written by you
// You have to cite them here.


int asgn2_pthread(Point * points, Point ** pPermissiblePoints, int number, int dim, int thread_number)
{
	
	
   	int permissiblePointNum = 0;
	int i,j,k;
	int temp1,count;
	thread_number = 4;
	omp_set_num_threads(thread_number);

	Point *permissiblePoints = (Point*)malloc(sizeof(Point) * number);
	int *tag = (int*)malloc(sizeof(int) * number);

	# pragma omp parallel for private(i, j, k, temp1,count) schedule(dynamic)
	for(i = 0; i < number; i++){
		tag[i] = 1;
	}

	#pragma omp parallel for private(i, j, k, temp1,count) schedule(dynamic)
	for(i = 0; i < number ; i++){ //Compare i with j to see if they can prevail each other
		if(tag[i] == 1){
		for(j = i + 1; j < number; j++){ //We don't need to compare repeated point
			if(tag[j] == 1){ //if a point is not prevailed
				temp1 = 0;
				count = 0;

				if(temp1 == 0 && (points[i].values[0] < points[j].values[0])){
					temp1 = 1; //check if i is strictly less than j
				}
				if(points[i].values[0] > points[j].values[0]){
					count++; //count the number of points i is larger than j 
				}

				if(temp1 == 0 && (points[i].values[1] < points[j].values[1])){
					temp1 = 1; //check if i is strictly less than j
				}
				if(points[i].values[1] > points[j].values[1]){
					count++; //count the number of points i is larger than j 
				}

				if(temp1 == 0 && (points[i].values[2] < points[j].values[2])){
					temp1 = 1; //check if i is strictly less than j
				}
				if(points[i].values[2] > points[j].values[2]){
					count++; //count the number of points i is larger than j 
				}
				
				if(!(temp1 == 1 && count > 0)){
		
				for(k = 3; k < dim; k++){
					if(temp1 == 0 && (points[i].values[k] < points[j].values[k])){
						temp1 = 1; //check if i is strictly less than j
					}
					if(points[i].values[k] > points[j].values[k]){
						count++; //count the number of points i is larger than j 
					}
				}

				}

				if ((temp1 == 1) && (count == 0)){ 
				//if there are some value of i is strictly less than j 
				//and all the value of i is smaller or equal than j
					tag[j] = 0; //j is prevailed
				}
				else if (count == dim){ //if all the value of j is larger than i
					tag[i] = 0; //i point is prevailed
					break; //Next i since we can prevail "the points after j" using j
				}
			}
		}
		}
	}

	for(j = 0; j < number; j++){
		if(tag[j] == 1){
			permissiblePoints[permissiblePointNum] = points[j];
			permissiblePointNum++;
		}
	}

	//for(int i = 0; i < permissiblePointNum; i++)
    	//	printPoint(permissiblePoints[i], dim);

	*pPermissiblePoints = permissiblePoints;
	return permissiblePointNum;
}

