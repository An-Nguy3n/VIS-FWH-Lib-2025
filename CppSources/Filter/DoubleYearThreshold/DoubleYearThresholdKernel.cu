#pragma once
#include "../../Generator/HomoPoly/CUDA/HomoPolyMethod.cu"


#ifndef _NUM_THRESHOLD_PER_CYCLE_
#define _NUM_THRESHOLD_PER_CYCLE_
const int __NUM_THRESHOLD_PER_CYCLE__ = 10;
#endif


__device__ __forceinline__ double max_of_array(double *array, int left, int right, double supremum){
    double max_ = __NEGATIVE_INFINITY__;
    for (int i=left; i<right; i++){
        if (array[i] < supremum && array[i] > max_) max_ = array[i];
    }
    return max_;
}


__device__ __forceinline__ void top_n_of_array(double *array, int left, int right, double *result, int start, int n){
    double supremum = __POSITIVE_INFINITY__;
    for (int i=0; i<n; i++){
        supremum = max_of_array(array, left, right, supremum);
        result[start+i] = supremum;
    }
}


__global__ void fill_thresholds(double *weights, double *thresholds, int *INDEX, int index_length, int num_array, int length){
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    int num_cycle = index_length - 2;
    if (index < num_array*num_cycle){
        int ix = index % num_cycle;
        int iy = index / num_cycle;
        top_n_of_array(weights + iy*length,
                       INDEX[ix+1], INDEX[ix+2],
                       thresholds + iy*__NUM_THRESHOLD_PER_CYCLE__*num_cycle,
                       ix*__NUM_THRESHOLD_PER_CYCLE__, __NUM_THRESHOLD_PER_CYCLE__);
    }
}


__device__ __forceinline__ int binary_symbol_search(int *SYMBOL, int start, int end, int target){
    int left = start, right = end-1;
    int mid;
    while (left <= right){
        mid = left + (right - left) / 2;
        if (SYMBOL[mid] == target) return mid;
        if (SYMBOL[mid] < target) left = mid + 1;
        else right = mid - 1;
    }
    return -1;
}


__device__ __forceinline__ void _double_year_threshold_investing(double *weight, double threshold, int t_idx, double *result,
    double INTEREST, int *INDEX, double *PROFIT, int *SYMBOL, int *BOOL_ARG, int index_size, int num_cycle){
    int reason;
    double Geo2 = 0, Har2 = 0;
    int start, end, end2, count, k, sym, s, rs_idx;
    double temp, n;
    bool check;

    check = false;
    start = INDEX[index_size - 2];
    end = INDEX[index_size - 1];
    for (k=start; k<end; k++){
        if (weight[k] > threshold){
            check = true;
            break;
        }
    }
    reason = !check;

    for (int i=index_size-3; i>0; i--){
        start = INDEX[i];
        end = INDEX[i+1];
        end2 = INDEX[i+2];
        temp = 0;
        count = 0;
        check = false;

        for (k=start; k<end; k++){
            if (weight[k] > threshold){
                check = true;
                if (!BOOL_ARG[k]) continue;

                if (reason){
                    count++;
                    temp += PROFIT[k];
                }
                else {
                    sym = SYMBOL[k];
                    s = binary_symbol_search(SYMBOL, end, end2, sym);
                    if (s != -1 && weight[s] > threshold){
                        count++;
                        temp += PROFIT[k];
                    }
                }
            }
        }

        if (!count){
            Geo2 += log(INTEREST);
            Har2 += 1.0 / INTEREST;
        } else {
            temp /= count;
            Geo2 += log(temp);
            Har2 += 1.0 / temp;
        }

        reason = !check;

        if (i <= num_cycle && t_idx+1 >= i){
            rs_idx = num_cycle - i;
            n = index_size - 2 - i;
            result[2*rs_idx] = exp(Geo2/n);
            result[2*rs_idx+1] = n / Har2;
        }
    }
}


__global__ void double_year_threshold_investing(double *weights, double *thresholds, double *results, int num_array, int num_threshold,
    int length, int num_cycle, double INTEREST, int *INDEX, double *PROFIT, int *SYMBOL, int *BOOL_ARG, int index_size){
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if (index < num_array*num_threshold){
        int ix = index % num_threshold;
        int iy = index / num_threshold;
        _double_year_threshold_investing(
            weights + iy*length,
            thresholds[iy*num_threshold + ix],
            ix / __NUM_THRESHOLD_PER_CYCLE__,
            results + iy*num_threshold*num_cycle*2 + ix*num_cycle*2,
            INTEREST, INDEX, PROFIT, SYMBOL, BOOL_ARG, index_size, num_cycle
        );
    }
}


__global__ void find_best_results(double *results, double *thresholds, double *finals, int num_array, int num_threshold, int num_cycle){
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if (index < 2*num_array*num_cycle){
        int iz = index % 2;
        int ix = (index/2) % num_cycle;
        int iy = (index/2) / num_cycle;

        double *result = results + iy*num_threshold*num_cycle*2;
        double *threshold = thresholds + iy*num_threshold;
        double *final_ = finals + iy*num_cycle*4 + ix*4;

        final_[2*iz] = threshold[0];
        final_[2*iz + 1] = result[2*ix + iz];
        for (int i=1; i<num_threshold; i++){
            if (result[i*num_cycle*2 + 2*ix + iz] > final_[2*iz + 1]){
                final_[2*iz] = threshold[i];
                final_[2*iz + 1] = result[i*num_cycle*2 + 2*ix + iz];
            }
        }
    }
}
