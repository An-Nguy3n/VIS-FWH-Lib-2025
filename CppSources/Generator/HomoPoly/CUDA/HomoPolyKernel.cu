#pragma once
#include <cuda_runtime.h>
#include <device_launch_parameters.h>


__device__ const double __NEGATIVE_INFINITY__ = -1.7976931348623157e+308;
__device__ const double __POSITIVE_INFINITY__ = +1.7976931348623157e+308;


__global__ void cuda_set_array_value(double* array, int length, double value){
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if (index < length) array[index] = value;
};


__global__ void copy_from_operands(double *dest, double *operands, int *arrCpy, int length, int numCpy){
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if (index < numCpy*length){
        int i = index / length;
        int j = index % length;
        dest[index] = operands[arrCpy[i]*length + j];
    }
};


__global__ void update_temp_weight(double *temp_weight_new, double *temp_weight_old, double *operands, int *arrOpr, int length, int numOpr, bool isMul){
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if (index < numOpr*length){
        int i = index / length;
        int j = index % length;
        if (isMul)
            temp_weight_new[index] = temp_weight_old[j] * operands[arrOpr[i]*length + j];
        else
            temp_weight_new[index] = temp_weight_old[j] / operands[arrOpr[i]*length + j];
    }
};


__global__ void update_last_weight(double *last_weight, double *curr_weight, double *temp_weight, int length, int numOpr, bool isAdd){
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if (index < numOpr*length){
        int j = index % length;
        if (isAdd) last_weight[index] = curr_weight[j] + temp_weight[index];
        else last_weight[index] = curr_weight[j] - temp_weight[index];
    }
};


__global__ void update_last_weight_through_operands(double *last_weight, double *curr_weight, double *operands, int *arrOpr, int length, int numOpr, bool isAdd){
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if (index < numOpr*length){
        int i = index / length;
        int j = index % length;
        if (isAdd) last_weight[index] = curr_weight[j] + operands[arrOpr[i]*length + j];
        else last_weight[index] = curr_weight[j] - operands[arrOpr[i]*length + j];
    }
};


__global__ void replace_nan_and_inf(double *array, int length, int numOpr){
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if (index < numOpr*length){
        if (isnan(array[index]) || isinf(array[index]))
            array[index] = __NEGATIVE_INFINITY__;
    }
};
