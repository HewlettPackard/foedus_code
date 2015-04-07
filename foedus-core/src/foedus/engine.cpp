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
#include "foedus/engine.hpp"

#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/engine_pimpl.hpp"

namespace foedus {
Engine::Engine(const EngineOptions& options) : pimpl_(nullptr) {
  pimpl_ = new EnginePimpl(this, options);
}
Engine::Engine(EngineType type, uint64_t master_upid, Eid master_eid, uint16_t soc_id)
  : pimpl_(nullptr) {
  pimpl_ = new EnginePimpl(this, type, master_upid, master_eid, soc_id);
}
Engine::~Engine() {
  delete pimpl_;
}

// simply forward to pimpl object
std::string Engine::describe_short() const { return pimpl_->describe_short(); }
const EngineOptions& Engine::get_options() const    { return pimpl_->options_; }
EngineOptions* Engine::get_nonconst_options()       {
  // this must be used only in child SOC's initialization.
  ASSERT_ND(!is_master());
  return &pimpl_->options_;
}


debugging::DebuggingSupports* Engine::get_debug() const     { return &pimpl_->debug_; }
log::LogManager*        Engine::get_log_manager() const     { return &pimpl_->log_manager_; }
memory::EngineMemory*   Engine::get_memory_manager() const  { return &pimpl_->memory_manager_; }
proc::ProcManager*      Engine::get_proc_manager() const    { return &pimpl_->proc_manager_; }
thread::ThreadPool*     Engine::get_thread_pool() const     { return &pimpl_->thread_pool_; }
savepoint::SavepointManager* Engine::get_savepoint_manager() const {
  return &pimpl_->savepoint_manager_;
}
snapshot::SnapshotManager* Engine::get_snapshot_manager() const {
  return &pimpl_->snapshot_manager_;
}
soc::SocManager* Engine::get_soc_manager() const { return &pimpl_->soc_manager_; }
storage::StorageManager* Engine::get_storage_manager() const { return &pimpl_->storage_manager_; }
xct::XctManager*        Engine::get_xct_manager() const     { return &pimpl_->xct_manager_; }

bool                Engine::is_initialized() const  { return pimpl_->is_initialized(); }
ErrorStack          Engine::initialize()            { return pimpl_->initialize(); }
ErrorStack          Engine::uninitialize()          { return pimpl_->uninitialize(); }

EngineType  Engine::get_type() const        { return pimpl_->type_; }
bool        Engine::is_master() const       { return pimpl_->type_ == kMaster; }
bool        Engine::is_emulated_child() const { return pimpl_->type_ == kChildEmulated; }
bool        Engine::is_forked_child() const { return pimpl_->type_ == kChildForked; }
bool        Engine::is_local_spawned_child() const { return pimpl_->type_ == kChildLocalSpawned; }
bool        Engine::is_remote_spawned_child() const { return pimpl_->type_ == kChildRemoteSpawned; }
soc::SocId  Engine::get_soc_id() const      { return pimpl_->soc_id_; }
soc::SocId  Engine::get_soc_count() const   { return pimpl_->options_.thread_.group_count_; }
soc::Upid   Engine::get_master_upid() const { return pimpl_->master_upid_; }
Eid         Engine::get_master_eid() const  { return pimpl_->master_eid_; }

}  // namespace foedus
