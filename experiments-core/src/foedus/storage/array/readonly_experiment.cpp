/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
/**
 * @file foedus/storage/array/readonly_experiment.cpp
 * @brief Read-only uniform-random accesses on array storage
 * @author kimurhid
 * @date 2014/04/14
 * @details
 * This is the first experiment to see the speed-of-light, where we use the simplest
 * data structure, array, and run read-only queries. This is supposed to run VERY fast.
 * Actually, we observed more than 300 MQPS in a desktop machine if the payload is small (16 bytes).
 *
 * @section ENVIRONMENTS Environments
 * At least 1GB of available RAM.
 *
 * @section OTHER Other notes
 * No special steps to build/run this expriment. This is self-contained.
 *
 * @section RESULTS Latest Results
 * foedus_results/20140414_kimurhid_array_readonly
 *
 * @todo PAYLOAD/DURATION_MICRO/RECORDS are so far hard-coded constants, not program arguments.
 */
#include <foedus/error_stack.hpp>
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/assorted/uniform_random.hpp>
#include <foedus/debugging/debugging_supports.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/memory/aligned_memory.hpp>
#include <foedus/memory/numa_core_memory.hpp>
#include <foedus/memory/numa_node_memory.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/storage/storage_manager.hpp>
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/xct/xct_manager.hpp>
#include <unistd.h>
#include <sys/mman.h>
#include <google/profiler.h>
#include <atomic>
#include <iostream>
#include <string>
#include <vector>

namespace th = foedus::thread;

foedus::storage::StorageId the_id;
const uint16_t PAYLOAD = 16;  // = 128;
const uint64_t DURATION_MICRO = 10000000;
const uint32_t RECORDS = 1 << 20;
const uint32_t RECORDS_MASK = 0xFFFFF;

class MyTask : public th::ImpersonateTask {
 public:
    foedus::ErrorStack run(th::Thread* context) {
        std::cout << "Ya!" << std::endl;
        foedus::Engine *engine = context->get_engine();
        foedus::storage::array::ArrayStorage *array = NULL;
        CHECK_ERROR(engine->get_storage_manager().create_array(
            context, "aaa", PAYLOAD, RECORDS, &array));
        the_id = array->get_id();
        return foedus::RET_OK;
    }
};

bool start_req = false;
bool stop_req = false;

class MyTask2 : public th::ImpersonateTask {
 public:
    MyTask2() {}
    foedus::ErrorStack run(th::Thread* context) {
        foedus::Engine *engine = context->get_engine();
        CHECK_ERROR(engine->get_xct_manager().begin_xct(context,
            foedus::xct::DIRTY_READ_PREFER_SNAPSHOT));
        foedus::storage::Storage* storage = engine->get_storage_manager().get_storage(the_id);
        foedus::xct::Epoch commit_epoch;

        // pre-calculate random numbers to get rid of random number generation as bottleneck
        random_.set_current_seed(context->get_thread_id());
        CHECK_ERROR(
            context->get_thread_memory()->get_node_memory()->allocate_numa_memory(
                RANDOM_COUNT * sizeof(uint32_t), &numbers_));
        random_.fill_memory(&numbers_);
        const uint32_t *randoms = reinterpret_cast<const uint32_t*>(numbers_.get_block());
        while (!start_req) {
            std::atomic_thread_fence(std::memory_order_acquire);
        }

        foedus::storage::array::ArrayStorage *array
            = dynamic_cast<foedus::storage::array::ArrayStorage*>(storage);
        char buf[PAYLOAD];
        processed_ = 0;
        while (true) {
            uint64_t id = randoms[processed_ & 0xFFFF] & RECORDS_MASK;
            CHECK_ERROR(array->get_record(context, id, buf));
            ++processed_;
            if ((processed_ & 0xFFFF) == 0) {
                CHECK_ERROR(engine->get_xct_manager().precommit_xct(context, &commit_epoch));
                CHECK_ERROR(engine->get_xct_manager().begin_xct(context,
                    foedus::xct::SERIALIZABLE));
                    // foedus::xct::DIRTY_REA   D_PREFER_SNAPSHOT));
                std::atomic_thread_fence(std::memory_order_acquire);
                if (stop_req) {
                    break;
                }
            }
        }

        CHECK_ERROR(engine->get_xct_manager().precommit_xct(context, &commit_epoch));
        numbers_.release_block();
        std::cout << "I'm done! " << context->get_thread_id()
            << ", processed=" << processed_ << std::endl;
        return foedus::RET_OK;
    }

    foedus::memory::AlignedMemory numbers_;
    foedus::assorted::UniformRandom random_;
    uint64_t processed_;
    const uint32_t RANDOM_COUNT = 1 << 19;
    const uint32_t RANDOM_COUNT_MOD = 0x7FFFF;
};

int main(int argc, char **argv) {
    bool profile = false;
    if (argc >= 2 && std::string(argv[1]) == "--profile") {
        profile = true;
        std::cout << "Profiling..." << std::endl;
    }
    foedus::EngineOptions options;
    options.debugging_.debug_log_min_threshold_
        = foedus::debugging::DebuggingOptions::DEBUG_LOG_WARNING;
    std::cout << "options=" << std::endl << options << std::endl;
    const int THREADS = options.thread_.group_count_ * options.thread_.thread_count_per_group_;
    {
        foedus::Engine engine(options);
        COERCE_ERROR(engine.initialize());
        {
            foedus::UninitializeGuard guard(&engine);
            MyTask task;
            th::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
            std::cout << "session: " << session << std::endl;
            std::cout << "session: result=" << session.get_result() << std::endl;

            typedef MyTask2* TaskPtr;
            TaskPtr* task2 = new TaskPtr[THREADS];
            th::ImpersonateSession session2[THREADS];
            for (int i = 0; i < THREADS; ++i) {
                task2[i] = new MyTask2();
                session2[i] = engine.get_thread_pool().impersonate(task2[i]);
                if (!session2[i].is_valid()) {
                    std::cout << "Impersonation failed!" << session2[i].invalid_cause_ << std::endl;
                }
            }
            ::usleep(1000000);
            start_req = true;
            std::atomic_thread_fence(std::memory_order_release);
            if (profile) {
                ::ProfilerStart("readonly_experiment.prof");
            }
            std::cout << "all started!" << std::endl;
            ::usleep(DURATION_MICRO);
            stop_req = true;
            std::atomic_thread_fence(std::memory_order_release);

            uint64_t total = 0;
            std::atomic_thread_fence(std::memory_order_acquire);
            for (int i = 0; i < THREADS; ++i) {
                total += task2[i]->processed_;
            }
            if (profile) {
                ::ProfilerStop();
            }

            for (int i = 0; i < THREADS; ++i) {
                std::cout << "session2: result[" << i << "]="
                    << session2[i].get_result() << std::endl;
                delete task2[i];
            }
            delete[] task2;
            std::cout << "total=" << total << ", MQPS="
                << (static_cast<double>(total)/DURATION_MICRO) << std::endl;
            COERCE_ERROR(engine.uninitialize());
        }
    }

    return 0;
}
