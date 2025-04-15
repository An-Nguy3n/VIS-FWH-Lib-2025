#pragma once
#include "SingleYearThresholdKernel.cu"
#include "../DoubleYearThreshold/DoubleYearThresholdKernel.cu"
#include "../../Generator/HomoPoly/CUDA_v2/HomoPolyMethod.cu"


class SingleYearThresholdFilter: public Generator {
public:
    double *d_threshold;
    double *d_result;
    double *d_final;
    double *h_final;

    SingleYearThresholdFilter(string config_path);
    ~SingleYearThresholdFilter();

    bool compute_result(bool force_save);
};


SingleYearThresholdFilter::SingleYearThresholdFilter(string config_path)
: Generator(config_path) {
    int num_threshold = __NUM_THRESHOLD_PER_CYCLE__*(index_length - 2);
    cudaMalloc((void**)&d_threshold, 8*(config.storage_size+cols)*num_threshold);
    cudaMalloc((void**)&d_result, 16*(config.storage_size+cols)*num_threshold*config.num_cycle);
    cudaMalloc((void**)&d_final, 32*(config.storage_size+cols)*config.num_cycle);
    cuda_set_array_value<<<2*(config.storage_size+cols)*num_threshold*config.num_cycle/256 + 1, 256>>>(
        d_result, 2*(config.storage_size+cols)*num_threshold*config.num_cycle, 0
    ); cudaDeviceSynchronize();
    h_final = new double[4*(config.storage_size+cols)*config.num_cycle];
}


SingleYearThresholdFilter::~SingleYearThresholdFilter(){
    cudaFree(d_threshold);
    cudaFree(d_result);
    cudaFree(d_final);
    delete[] h_final;
}


bool SingleYearThresholdFilter::compute_result(bool force_save){
    fill_thresholds<<<count_temp_storage*(index_length-2)/256 + 1, 256>>>(
        temp_weight_storage, d_threshold, INDEX, index_length, count_temp_storage, rows
    );
    cudaDeviceSynchronize();

    int num_threshold = __NUM_THRESHOLD_PER_CYCLE__*(index_length - 2);
    single_year_threshold_investing<<<count_temp_storage*num_threshold/256 + 1, 256>>>(
        temp_weight_storage, d_threshold, d_result, count_temp_storage, num_threshold,
        rows, config.num_cycle, config.interest, INDEX, PROFIT, index_length, BOOL_ARG
    );
    cudaDeviceSynchronize();

    find_best_results<<<count_temp_storage*config.num_cycle*2/256+1, 256>>>(
        d_result, d_threshold, d_final, count_temp_storage, num_threshold, config.num_cycle
    );
    cudaDeviceSynchronize();

    //
    cudaMemcpy(h_final, d_final, 32*count_temp_storage*config.num_cycle, cudaMemcpyDeviceToHost);
    return save_result(force_save, h_final);
}
