/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOG_MANAGER_PIMPL_HPP_
#define FOEDUS_LOG_LOG_MANAGER_PIMPL_HPP_
#include <foedus/epoch.hpp>
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/log/fwd.hpp>
#include <foedus/savepoint/fwd.hpp>
#include <foedus/thread/cond_broadcast_impl.hpp>
#include <foedus/thread/thread_id.hpp>
#include <stdint.h>
#include <atomic>
#include <mutex>
#include <vector>
namespace foedus {
namespace log {
/**
 * @brief Pimpl object of LogManager.
 * @ingroup LOG
 * @details
 * A private pimpl object for LogManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class LogManagerPimpl CXX11_FINAL : public DefaultInitializable {
 public:
    LogManagerPimpl() = delete;
    explicit LogManagerPimpl(Engine* engine) : engine_(engine) {}
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    void        wakeup_loggers();
    ErrorStack  wait_until_durable(Epoch commit_epoch, int64_t wait_microseconds);
    ErrorStack  refresh_global_durable_epoch();
    void        copy_logger_states(savepoint::Savepoint *new_savepoint);

    Epoch       get_durable_global_epoch() const { return durable_global_epoch_.load(); }
    Epoch       get_durable_global_epoch_nonatomic() const {
        return durable_global_epoch_.load(std::memory_order_relaxed);
    }


    Engine* const               engine_;

    thread::ThreadGroupId       groups_;
    uint16_t                    loggers_per_node_;

    /**
     * Log writers.
     */
    std::vector< Logger* >      loggers_;

    /**
     * @brief The durable epoch of the entire engine.
     * @invariant current_global_epoch_ > durable_global_epoch_
     * (we need to advance current epoch to make sure the ex-current epoch is durable)
     * @details
     * This value indicates upto what commit-groups we can return results to client programs.
     * This value is advanced by checking the durable epoch of each logger.
     */
    std::atomic<Epoch>          durable_global_epoch_;

    /** Fired (notify_broadcast) whenever durable_global_epoch_ is advanced. */
    thread::CondBroadcast       durable_global_epoch_advanced_;
    /** Protects durable_global_epoch_advanced_. */
    std::mutex                  durable_global_epoch_advanced_mutex_;

    /** Serializes the thread to take savepoint to advance durable_global_epoch_. */
    std::mutex                  durable_global_epoch_savepoint_mutex_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOG_MANAGER_PIMPL_HPP_
