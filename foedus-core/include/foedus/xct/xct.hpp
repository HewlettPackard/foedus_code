/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_HPP_
#define FOEDUS_XCT_XCT_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/storage/fwd.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/xct/fwd.hpp>
#include <foedus/xct/epoch.hpp>
#include <foedus/xct/xct_id.hpp>
#include <foedus/assert_nd.hpp>
#include <iosfwd>
namespace foedus {
namespace xct {

/**
 * @brief Represents a transaction.
 * @ingroup XCT
 * @details
 * To obtain this object (in other words, to begin a transaction),
 * call XctManager#begin_xct().
 */
class Xct {
 public:
    Xct();

    // No copy
    Xct(const Xct& other) CXX11_FUNC_DELETE;
    Xct& operator=(const Xct& other) CXX11_FUNC_DELETE;

    void initialize(thread::ThreadId thread_id, memory::NumaCoreMemory* core_memory);

    /**
     * Begins the transaction.
     */
    void                activate(IsolationLevel isolation_level) {
        ASSERT_ND(!active_);
        active_ = true;
        isolation_level_ = isolation_level;
        read_set_size_ = 0;
        write_set_size_ = 0;
    }

    /**
     * Closes the transaction.
     */
    void                deactivate() { active_ = false; }

    /** Returns whether the object is an active transaction. */
    bool                is_active() const { return active_; }
    /** Returns the level of isolation for this transaction. */
    IsolationLevel      get_isolation_level() const { return isolation_level_; }
    /** Returns the ID of this transaction, but note that it is not issued until commit time! */
    const XctId&        get_id() const { return id_; }
    uint32_t            get_read_set_size() const { return read_set_size_; }
    uint32_t            get_write_set_size() const { return write_set_size_; }
    XctAccess*          get_read_set()  { return read_set_; }
    WriteXctAccess*     get_write_set() { return write_set_; }

    /**
     * Called while a successful commit to issue a new xct id.
     * @todo advance epoch when wrap around
     */
    void                issue_next_id(const Epoch &epoch)  {
        if (epoch != id_.epoch_) {
            id_.epoch_ = epoch;
            id_.ordinal_and_status_ = 0;
        } else {
            ASSERT_ND(id_.ordinal_and_status_ < 0xFFFF);
            ++id_.ordinal_and_status_;
        }
    }

    /**
     * Add the given record to the read set of this transaction.
     * Inlined in xct_inl.hpp.
     */
    ErrorCode           add_to_read_set(storage::Record* record);
    /**
     * Add the given record to the write set of this transaction.
     * Inlined in xct_inl.hpp.
     */
    ErrorCode           add_to_write_set(storage::Record* record, void* log_entry);

    friend std::ostream& operator<<(std::ostream& o, const Xct& v);

 private:
    /** ID of this transaction, which is issued at commit time. */
    XctId               id_;

    /** Level of isolation for this transaction. */
    IsolationLevel      isolation_level_;

    /** Whether the object is an active transaction. */
    bool                active_;

    XctAccess*          read_set_;
    uint32_t            read_set_size_;
    uint32_t            max_read_set_size_;

    WriteXctAccess*     write_set_;
    uint32_t            write_set_size_;
    uint32_t            max_write_set_size_;
};
}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_HPP_
