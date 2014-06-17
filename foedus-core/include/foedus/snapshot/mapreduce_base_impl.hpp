/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_MAPREDUCE_BASE_IMPL_HPP_
#define FOEDUS_SNAPSHOT_MAPREDUCE_BASE_IMPL_HPP_
#include <foedus/epoch.hpp>
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/log/fwd.hpp>
#include <foedus/log/log_id.hpp>
#include <foedus/snapshot/fwd.hpp>
#include <foedus/snapshot/snapshot_id.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/thread/stoppable_thread_impl.hpp>
#include <stdint.h>
#include <iosfwd>
#include <string>
namespace foedus {
namespace snapshot {
/**
 * @brief Base class for LogMapper and LogReducer to share common code.
 * @ingroup SNAPSHOT
 * @details
 * The shared parts are:
 * \li init/uninit
 * \li synchronization with gleaner (main thread)
 *
 * @note
 * This is a private implementation-details of \ref SNAPSHOT, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class MapReduceBase : public DefaultInitializable {
 public:
    MapReduceBase(Engine* engine, LogGleaner* parent, uint16_t id, thread::ThreadGroupId numa_node)
        : engine_(engine), parent_(parent), id_(id), numa_node_(numa_node) {}

    ErrorStack  initialize_once() override final;
    ErrorStack  uninitialize_once() override final;

    MapReduceBase() = delete;
    MapReduceBase(const MapReduceBase &other) = delete;
    MapReduceBase& operator=(const MapReduceBase &other) = delete;

    void request_stop() { thread_.request_stop(); }
    void wait_for_stop() { thread_.wait_for_stop(); }

    /** Expects "LogReducer-x", "LogMapper-y" etc. Used only for logging/debugging. */
    virtual std::string to_string() const = 0;

 protected:
    Engine* const                   engine_;
    LogGleaner* const               parent_;
    /** Unique ID of this mapper or reducer. */
    const uint16_t                  id_;
    const thread::ThreadGroupId     numa_node_;

    thread::StoppableThread         thread_;

    /** Implements the specific logics in derived class. Called per epoch. */
    virtual ErrorStack  handle_epoch() = 0;

    /** additional initialization  in derived class called at the beginning of handle() */
    virtual ErrorStack  handle_initialize() = 0;

    /**
     * additional uninitialization in derived class called at the end of handle().
     * This one must be idempotent as we call this again at uninitialize() in case of error-exit.
     */
    virtual ErrorStack  handle_uninitialize() = 0;

    /** Main routine */
    void                handle();
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_MAPREDUCE_BASE_IMPL_HPP_
