/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef FOEDUS_XCT_XCT_HPP_
#define FOEDUS_XCT_XCT_HPP_

#include <cstring>
#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_type_invoke.hpp"

// For log verification. Only in debug mode
#ifndef NDEBUG
#include "foedus/log/log_type_invoke.hpp"
#endif  // NDEBUG

#include "foedus/memory/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/xct_access.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace xct {

/**
 * @brief Represents a transaction.
 * @ingroup XCT
 * @details
 * To obtain this object, call Thread#get_current_xct().
 */
class Xct {
 public:
  enum Constants {
    kMaxPointerSets = 1024,
    kMaxPageVersionSets = 1024,
  };

  Xct(Engine* engine, thread::ThreadId thread_id);

  // No copy
  Xct(const Xct& other) CXX11_FUNC_DELETE;
  Xct& operator=(const Xct& other) CXX11_FUNC_DELETE;

  void initialize(memory::NumaCoreMemory* core_memory, uint32_t* mcs_block_current);

  /**
   * Begins the transaction.
   */
  void                activate(IsolationLevel isolation_level) {
    ASSERT_ND(!active_);
    active_ = true;
    isolation_level_ = isolation_level;
    pointer_set_size_ = 0;
    page_version_set_size_ = 0;
    read_set_size_ = 0;
    write_set_size_ = 0;
    lock_free_write_set_size_ = 0;
    *mcs_block_current_ = 0;
    local_work_memory_cur_ = 0;
  }

  /**
   * Closes the transaction.
   */
  void                deactivate() {
    ASSERT_ND(active_);
    active_ = false;
    *mcs_block_current_ = 0;
  }

  uint32_t            get_mcs_block_current() const { return *mcs_block_current_; }
  uint32_t            increment_mcs_block_current() { return ++(*mcs_block_current_); }

  /** Returns whether the object is an active transaction. */
  bool                is_active() const { return active_; }
  /** Returns if this transaction makes no writes. */
  bool                is_read_only() const {
    return write_set_size_ == 0 && lock_free_write_set_size_ == 0;
  }
  /** Returns the level of isolation for this transaction. */
  IsolationLevel      get_isolation_level() const { return isolation_level_; }
  /** Returns the ID of this transaction, but note that it is not issued until commit time! */
  const XctId&        get_id() const { return id_; }
  thread::ThreadId    get_thread_id() const { return thread_id_; }
  uint32_t            get_pointer_set_size() const { return pointer_set_size_; }
  uint32_t            get_page_version_set_size() const { return page_version_set_size_; }
  uint32_t            get_read_set_size() const { return read_set_size_; }
  uint32_t            get_write_set_size() const { return write_set_size_; }
  uint32_t            get_lock_free_write_set_size() const { return lock_free_write_set_size_; }
  const PointerAccess*   get_pointer_set() const { return pointer_set_; }
  const PageVersionAccess*  get_page_version_set() const { return page_version_set_; }
  ReadXctAccess*      get_read_set()  { return read_set_; }
  WriteXctAccess*     get_write_set() { return write_set_; }
  LockFreeWriteXctAccess* get_lock_free_write_set() { return lock_free_write_set_; }


  /**
   * @brief Called while a successful commit of xct to issue a new xct id.
   * @param[in] max_xct_id largest xct_id this transaction depends on.
   * @param[in,out] epoch (in) The \e minimal epoch this transaction has to be in. (out)
   * the epoch this transaction ended up with, which is epoch+1 only when it found ordinal is
   * full for the current epoch.
   * @details
   * This method issues a XctId that satisfies the following properties (see [TU13]).
   * Clarification: "larger" hereby means either a) the epoch is larger or
   * b) the epoch is same and ordinal is larger.
   * \li Larger than the most recent XctId issued for read-write transaction on this thread.
   * \li Larger than every XctId of any record read or written by this transaction.
   * \li In the \e returned(out) epoch (which is same or larger than the given(in) epoch).
   *
   * This method also advancec epoch when ordinal is full for the current epoch.
   * This method never fails.
   */
  void                issue_next_id(XctId max_xct_id, Epoch *epoch);

  /**
   * @brief Add the given page pointer to the pointer set of this transaction.
   * @details
   * You must call this method in the following cases;
   *  \li When following a volatile pointer that might be later swapped with the RCU protocol.
   *  \li When following a snapshot pointer except it is under a snapshot page.
   *
   * To clarify, the first case does not apply to storage types that don't swap volatile pointers.
   * So far, only \ref MASSTREE has such a swapping for root pages. All other storage types
   * thus don't have to take pointer sets for this.
   *
   * The second case doesn't apply to snapshot pointers once we follow a snapshot pointer in the
   * tree because everything is assured to be stable once we follow a snapshot pointer.
   */
  ErrorCode           add_to_pointer_set(
    const storage::VolatilePagePointer* pointer_address,
    storage::VolatilePagePointer observed);

  /**
   * The transaction that has updated the volatile pointer should not abort itself.
   * So, it calls this method to apply the version it installed.
   */
  void                overwrite_to_pointer_set(
    const storage::VolatilePagePointer* pointer_address,
    storage::VolatilePagePointer observed) ALWAYS_INLINE;

  /**
   * @brief Add the given page version to the page version set of this transaction.
   * @details
   * This is similar to pointer set. The difference is that this remembers the PageVersion
   * value we observed when we accessed the page. This can capture many more concurrency
   * issues in the page because PageVersion contains many flags and counters.
   * However, PageVersionAccess can't be used if the page itself might be swapped.
   *
   * Both PointerAccess and PageVersionAccess can be considered as "node set" in [TU2013], but
   * for a little bit different purpose.
   */
  ErrorCode           add_to_page_version_set(
    const storage::PageVersion* version_address,
    storage::PageVersionStatus observed);

  ReadXctAccess*      get_read_access(RwLockableXctId* owner_id_address) ALWAYS_INLINE;
  WriteXctAccess*     get_write_access(RwLockableXctId* owner_id_address) ALWAYS_INLINE;
  WriteXctAccess*     get_empty_write_access(RwLockableXctId* owner_id_address) ALWAYS_INLINE;

  /**
   * @brief Add the given record to the read set of this transaction.
   * @details
   * You must call this method \b BEFORE reading the data, otherwise it violates the
   * commit protocol.
   */
  ErrorCode           add_to_read_set(
    thread::Thread* context,
    storage::StorageId storage_id,
    XctId observed_owner_id,
    RwLockableXctId* owner_id_address,
    bool for_write,
    bool hot_record) ALWAYS_INLINE;
  /** This version always adds to read set regardless of isolation level. */
  ErrorCode           add_to_read_set_force(
    thread::Thread* context,
    storage::StorageId storage_id,
    XctId observed_owner_id,
    RwLockableXctId* owner_id_address) ALWAYS_INLINE;

  /**
   * @brief Add the given record to the write set of this transaction.
   */
  ErrorCode           add_to_write_set(
    thread::Thread* context,
    storage::StorageId storage_id,
    RwLockableXctId* owner_id_address,
    char* payload_address,
    log::RecordLogType* log_entry) ALWAYS_INLINE;

  /**
   * @brief Add the given record to the write set of this transaction.
   */
  ErrorCode           add_to_write_set(
    thread::Thread* context,
    storage::StorageId storage_id,
    storage::Record* record,
    log::RecordLogType* log_entry) ALWAYS_INLINE;

  /**
   * @brief Add a pair of read and write set of this transaction.
   */
  ErrorCode           add_to_read_and_write_set(
    thread::Thread* context,
    storage::StorageId storage_id,
    XctId observed_owner_id,
    RwLockableXctId* owner_id_address,
    char* payload_address,
    log::RecordLogType* log_entry) ALWAYS_INLINE;

  /**
   * @brief Add the given log to the lock-free write set of this transaction.
   */
  ErrorCode           add_to_lock_free_write_set(
    storage::StorageId storage_id,
    log::RecordLogType* log_entry);

  void                remember_previous_xct_id(XctId new_id) {
    ASSERT_ND(id_.before(new_id));
    id_ = new_id;
    ASSERT_ND(id_.get_ordinal() > 0);
    ASSERT_ND(id_.is_valid());
  }

  /**
   * Get a tentative work memory of the specified size from pre-allocated thread-private memory.
   * The local work memory is recycled after the current transaction.
   */
  ErrorCode           acquire_local_work_memory(uint32_t size, void** out, uint32_t alignment = 8) {
    if (size % alignment != 0) {
      size = ((size / alignment) + 1U) * alignment;
    }
    uint64_t begin = local_work_memory_cur_;
    if (begin % alignment != 0) {
      begin = ((begin / alignment) + 1U) * alignment;
    }
    if (UNLIKELY(size + begin > local_work_memory_size_)) {
      return kErrorCodeXctNoMoreLocalWorkMemory;
    }
    local_work_memory_cur_ = size + begin;
    *out = reinterpret_cast<char*>(local_work_memory_) + begin;
    return kErrorCodeOk;
  }

  /**
   * This debug method checks whether the related_read_ and related_write_ fileds in
   * read/write sets are consistent. This method is completely wiped out in release build.
   * @return whether it is consistent. but this method anyway asserts as of finding inconsistency.
   */
  bool assert_related_read_write() const ALWAYS_INLINE;

  friend std::ostream& operator<<(std::ostream& o, const Xct& v);

 private:
  Engine* const engine_;

  /** Thread that owns this transaction. */
  const thread::ThreadId thread_id_;

  /**
   * Most recently issued ID of this transaction. XctID is issued at commit time,
   * so this is "previous" ID unless while or right after commit.
   */
  XctId               id_;

  /** Level of isolation for this transaction. */
  IsolationLevel      isolation_level_;

  /** Whether the object is an active transaction. */
  bool                active_;

  /**
   * How many MCS blocks we allocated in the current thread.
   * reset to 0 at each transaction begin
   * This points to ThreadControlBlock because other SOC might check this value (so far only
   * for sanity check).
   */
  uint32_t*           mcs_block_current_;

  ReadXctAccess*      read_set_;
  uint32_t            read_set_size_;
  uint32_t            max_read_set_size_;

  WriteXctAccess*     write_set_;
  uint32_t            write_set_size_;
  uint32_t            max_write_set_size_;

  LockFreeWriteXctAccess* lock_free_write_set_;
  uint32_t                lock_free_write_set_size_;
  uint32_t                max_lock_free_write_set_size_;

  // @todo we also need a special lock_free read set just for scanning xct on sequential storage.
  // it should check if the biggest XctId the scanner read is still the biggest XctId in the list.
  // we can easily implement it by remembering "safe" page to resume search, or just remembering
  // tail (abort if tail has changed), and then reading all record in the page.
  // as we don't have scanning accesses to sequential storage yet, low priority.

  PointerAccess*      pointer_set_;
  uint32_t            pointer_set_size_;

  PageVersionAccess*  page_version_set_;
  uint32_t            page_version_set_size_;

  void*               local_work_memory_;
  uint64_t            local_work_memory_size_;
  /** This value is reset to zero for each transaction, and always <= local_work_memory_size_ */
  uint64_t            local_work_memory_cur_;
};


inline ErrorCode Xct::add_to_pointer_set(
  const storage::VolatilePagePointer* pointer_address,
  storage::VolatilePagePointer observed) {
  ASSERT_ND(pointer_address);
  if (isolation_level_ != kSerializable) {
    return kErrorCodeOk;
  }

  // TASK(Hideaki) even though pointer set should be small, we don't want sequential search
  // everytime. but insertion sort requires shifting. mmm.
  for (uint32_t i = 0; i < pointer_set_size_; ++i) {
    if (pointer_set_[i].address_ == pointer_address) {
      pointer_set_[i].observed_ = observed;
      return kErrorCodeOk;
    }
  }

  if (UNLIKELY(pointer_set_size_ >= kMaxPointerSets)) {
    return kErrorCodeXctPointerSetOverflow;
  }

  // no need for fence. the observed pointer itself is the only data to verify
  pointer_set_[pointer_set_size_].address_ = pointer_address;
  pointer_set_[pointer_set_size_].observed_ = observed;
  ++pointer_set_size_;
  return kErrorCodeOk;
}

inline void Xct::overwrite_to_pointer_set(
  const storage::VolatilePagePointer* pointer_address,
  storage::VolatilePagePointer observed) {
  ASSERT_ND(pointer_address);
  if (isolation_level_ != kSerializable) {
    return;
  }

  for (uint32_t i = 0; i < pointer_set_size_; ++i) {
    if (pointer_set_[i].address_ == pointer_address) {
      pointer_set_[i].observed_ = observed;
      return;
    }
  }
}

inline ErrorCode Xct::add_to_page_version_set(
  const storage::PageVersion* version_address,
  storage::PageVersionStatus observed) {
  ASSERT_ND(version_address);
  if (isolation_level_ != kSerializable) {
    return kErrorCodeOk;
  } else if (UNLIKELY(page_version_set_size_ >= kMaxPointerSets)) {
    return kErrorCodeXctPageVersionSetOverflow;
  }

  page_version_set_[page_version_set_size_].address_ = version_address;
  page_version_set_[page_version_set_size_].observed_ = observed;
  ++page_version_set_size_;
  return kErrorCodeOk;
}

inline ErrorCode Xct::add_to_read_set(
  thread::Thread* context,
  storage::StorageId storage_id,
  XctId observed_owner_id,
  RwLockableXctId* owner_id_address,
  bool for_write,
  bool hot_record) {
  ASSERT_ND(storage_id != 0);
  ASSERT_ND(owner_id_address);
  ASSERT_ND(observed_owner_id.is_valid());
  if (isolation_level_ != kSerializable) {
    return kErrorCodeOk;
  }

  // Ideally, should S-lock it if this is a hot record and it's not part of an RMW.
  bool s_lock = !for_write && hot_record;

  // First-touch policy: the r/w sets serve as a repo of record temperatures.
  // The temperature of a record doesn't change during the lifetime of a transaction,
  // regardless how the page-header level temperature changes.
  // Now find out the record temperature from reads
  ReadXctAccess* read = get_read_access(owner_id_address);
  if (read) {
    if (read->mcs_block_) {
      // Holding S-lock already, discard hint and return. But the application should
      // avoid callling with for_write=true in this case.
      ASSERT_ND(!for_write);
      return kErrorCodeOk;
    } else {
      // Have to discard hint because we already determined this is a cold record
      s_lock = false;
    }
  }

  // If it's already in the write set, we also have to discard the hint - during
  // the precommit phase we need to take X-lock.
  if (get_write_access(owner_id_address)) {
    s_lock = false;
  }

  read = read_set_ + read_set_size_;
  CHECK_ERROR_CODE(add_to_read_set_force(context, storage_id, observed_owner_id, owner_id_address));

  if (s_lock) {
    // If we need to take an S-lock, we also have to acquire all the X-locks for all
    // writes to prevent deadlock. Sort the write-set here and acquire X-locks for
    // entries with owner_id_address < this record's owner_id_address in order.
    std::sort(write_set_, write_set_ + write_set_size_, WriteXctAccess::compare);
    //LOG(INFO) << context << " write_set_size=" << write_set_size_;
    for (uint32_t i = 0; i < write_set_size_; i++) {
      WriteXctAccess* write = write_set_ + i;
      //LOG(INFO) << context << " write_set " << write;
      if (write->owner_id_address_ > owner_id_address) {
        break;
      } else if (write->mcs_block_ == 0) {
        if (i < write_set_size_ - 1 &&
            write->owner_id_address_ == write_set_[i + 1].owner_id_address_) {
          continue;
        } else {
          write->mcs_block_ =
            context->mcs_acquire_writer_lock(write->owner_id_address_->get_key_lock());
          //LOG(INFO) << context << " X-Locked " << write->owner_id_address_ << " for " << owner_id_address;
        }
      }
    }
    ASSERT_ND(read_set_[read_set_size_ - 1].owner_id_address_ = owner_id_address);
    ASSERT_ND(read_set_[read_set_size_ - 1].mcs_block_ == 0);
    read_set_[read_set_size_ - 1].mcs_block_ =
      context->mcs_acquire_reader_lock(owner_id_address->get_key_lock());
    //LOG(INFO) << context << " S-Locked " << owner_id_address;
  } else {
    ASSERT_ND(read->mcs_block_ == 0);
  }
  return kErrorCodeOk;
}
inline ReadXctAccess* Xct::get_read_access(RwLockableXctId* owner_id_address) {
  ReadXctAccess* ret = NULL;
  // Return the one that's holding the X-lock (if any)
  for (uint32_t i = 0; i < read_set_size_; i++) {
    if (read_set_[i].owner_id_address_ == owner_id_address) {
      ret = read_set_ + i;
      if (ret->mcs_block_) {
        return ret;
      }
    }
  }
  return ret;
}
inline WriteXctAccess* Xct::get_write_access(RwLockableXctId* owner_id_address) {
  WriteXctAccess* ret = NULL;
  // Return the one that's holding the X-lock (if any)
  for (uint32_t i = 0; i < write_set_size_; i++) {
    if (write_set_[i].owner_id_address_ == owner_id_address) {
      ret = write_set_ + i;
      if (ret->mcs_block_) {
        return ret;
      }
    }
  }
  return ret;
}
inline WriteXctAccess* Xct::get_empty_write_access(RwLockableXctId* owner_id_address) {
  // Return the one that's holding the X-lock (if any)
  for (uint32_t i = 0; i < write_set_size_; i++) {
    if (write_set_[i].owner_id_address_ == owner_id_address) {
      if (write_set_[i].log_entry_ == CXX11_NULLPTR) {
        ASSERT_ND(write_set_[i].payload_address_ == CXX11_NULLPTR);
        return write_set_ + i;
      }
    }
  }
  return NULL;
}

// Unconditionally insert a new entry without taking lock.
inline ErrorCode Xct::add_to_read_set_force(
  thread::Thread* context,
  storage::StorageId storage_id,
  XctId observed_owner_id,
  RwLockableXctId* owner_id_address) {
  ASSERT_ND(storage_id != 0);
  ASSERT_ND(owner_id_address);
  if (UNLIKELY(read_set_size_ >= max_read_set_size_)) {
    return kErrorCodeXctReadSetOverflow;
  }
  // The caller should set mcs_block_ after this returns.
  read_set_[read_set_size_].mcs_block_ = 0;
  // if the next-layer bit is ON, the record is not logically a record, so why we are adding
  // it to read-set? we should have already either aborted or retried in this case.
  ASSERT_ND(!observed_owner_id.is_next_layer());
  read_set_[read_set_size_].storage_id_ = storage_id;
  read_set_[read_set_size_].owner_id_address_ = owner_id_address;
  read_set_[read_set_size_].observed_owner_id_ = observed_owner_id;
  read_set_[read_set_size_].related_write_ = CXX11_NULLPTR;
  ++read_set_size_;
  return kErrorCodeOk;
}

inline ErrorCode Xct::add_to_write_set(
  thread::Thread* context,
  storage::StorageId storage_id,
  storage::Record* record,
  log::RecordLogType* log_entry) {
  return add_to_write_set(context, storage_id, &record->owner_id_, record->payload_, log_entry);
}

inline ErrorCode Xct::add_to_write_set(
  thread::Thread* context,
  storage::StorageId storage_id,
  RwLockableXctId* owner_id_address,
  char* payload_address,
  log::RecordLogType* log_entry) {
  ASSERT_ND(storage_id != 0);
  ASSERT_ND(owner_id_address);
  ASSERT_ND(payload_address);
  ASSERT_ND(log_entry);
  // Plain OCC; but we assume that the record isn't S-locked. The application should
  // make sure of that by giving the "xlock" option to add_to_read_set.
#ifndef NDEBUG
  ReadXctAccess* read = get_read_access(owner_id_address);
  if (read) {
    ASSERT_ND(read->mcs_block_ == 0);
  }
  log::invoke_assert_valid(log_entry);
#endif  // NDEBUG
  if (UNLIKELY(write_set_size_ >= max_write_set_size_)) {
    return kErrorCodeXctWriteSetOverflow;
  }
  WriteXctAccess* write = write_set_ + write_set_size_;
  write->mcs_block_ = 0;
  write->write_set_ordinal_ = write_set_size_;
  write->payload_address_ = payload_address;
  write->log_entry_ = log_entry;
  write->storage_id_ = storage_id;
  write->owner_id_address_ = owner_id_address;
  write->related_read_ = CXX11_NULLPTR;
  ++write_set_size_;
  return kErrorCodeOk;
}

inline ErrorCode Xct::add_to_read_and_write_set(
  thread::Thread* context,
  storage::StorageId storage_id,
  XctId observed_owner_id,
  RwLockableXctId* owner_id_address,
  char* payload_address,
  log::RecordLogType* log_entry) {
  ASSERT_ND(observed_owner_id.is_valid());
#ifndef NDEBUG
  ReadXctAccess* r = get_read_access(owner_id_address);
  if (r) {
    ASSERT_ND(r->mcs_block_ == 0);
  }
  log::invoke_assert_valid(log_entry);
#endif  // NDEBUG
  WriteXctAccess* write = write_set_ + write_set_size_;
  CHECK_ERROR_CODE(
    add_to_write_set(context, storage_id, owner_id_address, payload_address, log_entry));
  ASSERT_ND(write->mcs_block_ == 0);
  ReadXctAccess* read = read_set_ + read_set_size_;
  // in this method, we force to add a read set because it's critical to confirm that
  // the physical record we write to is still the one we found.
  CHECK_ERROR_CODE(add_to_read_set_force(context, storage_id, observed_owner_id, owner_id_address));
  ASSERT_ND(read->mcs_block_ == 0);
  ASSERT_ND(read->owner_id_address_ == owner_id_address);
  read->related_write_ = write;
  write->related_read_ = read;
  ASSERT_ND(read->related_write_->related_read_ == read);
  ASSERT_ND(write->related_read_->related_write_ == write);
  ASSERT_ND(write->log_entry_ == log_entry);
  ASSERT_ND(write->owner_id_address_ == owner_id_address);
  return kErrorCodeOk;
}

inline ErrorCode Xct::add_to_lock_free_write_set(
    storage::StorageId storage_id,
  log::RecordLogType* log_entry) {
  ASSERT_ND(storage_id != 0);
  ASSERT_ND(log_entry);
  if (UNLIKELY(lock_free_write_set_size_ >= max_lock_free_write_set_size_)) {
    return kErrorCodeXctWriteSetOverflow;
  }

#ifndef NDEBUG
  log::invoke_assert_valid(log_entry);
#endif  // NDEBUG

  lock_free_write_set_[lock_free_write_set_size_].storage_id_ = storage_id;
  lock_free_write_set_[lock_free_write_set_size_].log_entry_ = log_entry;
  ++lock_free_write_set_size_;
  return kErrorCodeOk;
}

inline bool Xct::assert_related_read_write() const {
#ifndef NDEBUG
  for (uint32_t i = 0; i < write_set_size_; ++i) {
    WriteXctAccess* write = write_set_ + i;
    if (write->related_read_) {
      ASSERT_ND(write->related_read_ >= read_set_);
      uint32_t index = write->related_read_ - read_set_;
      ASSERT_ND(index < read_set_size_);
      ASSERT_ND(write->owner_id_address_ == write->related_read_->owner_id_address_);
      ASSERT_ND(write == write->related_read_->related_write_);
    }
  }

  for (uint32_t i = 0; i < read_set_size_; ++i) {
    ReadXctAccess* read = read_set_ + i;
    if (read->related_write_) {
      ASSERT_ND(read->related_write_ >= write_set_);
      uint32_t index = read->related_write_ - write_set_;
      ASSERT_ND(index < write_set_size_);
      ASSERT_ND(read->owner_id_address_ == read->related_write_->owner_id_address_);
      ASSERT_ND(read == read->related_write_->related_read_);
    }
  }
#endif  // NDEBUG
  return true;
}

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_HPP_
