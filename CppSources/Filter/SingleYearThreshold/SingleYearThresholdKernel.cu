#pragma once


const int __NUM_THRESHOLD_PER_CYCLE__ = 10;
#define _NUM_THRESHOLD_PER_CYCLE_


__device__ __forceinline__ void _single_year_threshold_investing(
    double *weight, double threshold, int t_idx, double *result,
    double INTEREST, int *INDEX, double *PROFIT, int index_size, int num_cycle,
    int *BOOL_ARG
) {
    double Geo = 0, Har = 0;
    int start, end, count, k, rs_idx;
    double temp, n;
    for (int i=index_size-2; i>0; i--){
        start = INDEX[i];
        end = INDEX[i+1];
        temp = 0.0;
        count = 0;
        for (k=start; k<end; k++){
            if (weight[k] > threshold && BOOL_ARG[k]){
                count++;
                temp += PROFIT[k];
            }
        }

        if (!count){
            Geo += log(INTEREST);
            Har += 1.0 / INTEREST;
        } else {
            temp /= count;
            Geo += log(temp);
            Har += 1.0 / temp;
        }

        if (i <= num_cycle && t_idx+1 >= i){
            rs_idx = num_cycle - i;
            n = index_size - 1 - i;
            result[2*rs_idx] = exp(Geo/n);
            result[2*rs_idx+1] = n / Har;
        }
    }
}


__global__ void single_year_threshold_investing(
    double *weights, double *thresholds, double *results, int num_array, int num_threshold,
    int length, int num_cycle, double INTEREST, int *INDEX, double *PROFIT, int index_size,
    int *BOOL_ARG
) {
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if (index < num_array*num_threshold){
        int ix = index % num_threshold;
        int iy = index / num_threshold;
        _single_year_threshold_investing(
            weights + iy*length,
            thresholds[iy*num_threshold + ix],
            ix / __NUM_THRESHOLD_PER_CYCLE__,
            results + iy*num_threshold*num_cycle*2 + ix*num_cycle*2,
            INTEREST, INDEX, PROFIT, index_size, num_cycle,
            BOOL_ARG
        );
    }
}
