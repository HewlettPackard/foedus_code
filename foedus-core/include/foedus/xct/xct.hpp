/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_HPP_
#define FOEDUS_XCT_XCT_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
#include <foedus/storage/fwd.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/xct/fwd.hpp>
#include <foedus/xct/isolation_level.hpp>
#include <foedus/xct/xct_id.hpp>
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

    /**
     * Initializes the transaction information.
     */
    void                activate(thread::Thread* thread);
    /**
     * Deactivates the transaction information and prepare for reuse.
     */
    void                deactivate() { active_ = false; }

    /** Returns whether the object is an active transaction. */
    bool                is_active() const { return active_; }
    /** Returns the level of isolation for this transaction. */
    IsolationLevel      get_isolation_level() const { return isolation_level_; }
    /** Returns the ID of this transaction, but note that it is not issued until commit time! */
    XctId               get_id() const { return id_; }
    uint32_t            get_read_set_size() const { return read_set_size_; }
    uint32_t            get_write_set_size() const { return write_set_size_; }


    ErrorStack          add_to_read_set(storage::Record* record);
    ErrorStack          add_to_write_set(storage::Record* record);

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

    XctAccess*          write_set_;
    uint32_t            write_set_size_;
    uint32_t            max_write_set_size_;

    /** The thread this transaction is running on. */
    thread::Thread*     thread_;
};

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_HPP_
