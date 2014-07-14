/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/hash/hash_storage_pimpl.hpp"

#include <glog/logging.h>

#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/storage_manager_pimpl.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_inl.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace hash {

// Defines HashStorage methods so that we can inline implementation calls
bool        HashStorage::is_initialized()   const  { return pimpl_->is_initialized(); }
bool        HashStorage::exists()           const  { return pimpl_->exist_; }
StorageId   HashStorage::get_id()           const  { return pimpl_->metadata_.id_; }
const std::string& HashStorage::get_name()  const  { return pimpl_->metadata_.name_; }
const Metadata* HashStorage::get_metadata() const  { return &pimpl_->metadata_; }
const HashMetadata* HashStorage::get_hash_metadata() const  { return &pimpl_->metadata_; }

ErrorCode HashStorage::get_record(thread::Thread* context, const void *key, uint16_t key_length,
          void *payload, uint16_t payload_offset, uint16_t payload_count) {
  return pimpl_->get_record(context, key, key_length, payload, payload_offset, payload_count);
}

HashStoragePimpl::HashStoragePimpl(Engine* engine, HashStorage* holder,
                   const HashMetadata &metadata, bool create)
  : engine_(engine),
  holder_(holder),
  metadata_(metadata),
  root_page_(nullptr),
  exist_(!create) {
  ASSERT_ND(create || metadata.id_ > 0);
  ASSERT_ND(metadata.name_.size() > 0);
  root_page_pointer_.snapshot_pointer_ = metadata.root_snapshot_page_id_;
  root_page_pointer_.volatile_pointer_.word = 0;
}

ErrorStack HashStoragePimpl::initialize_once() {
  LOG(INFO) << "Initializing an hash-storage " << *holder_ << " exists=" << exist_;
  if (exist_) {
    // initialize root_page_
  }
  return kRetOk;
}

ErrorStack HashStoragePimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing an hash-storage " << *holder_;
  return kRetOk;
}


ErrorStack HashStoragePimpl::create(thread::Thread* context) {
  if (exist_) {
    LOG(ERROR) << "This hash-storage already exists: " << *holder_;
    return ERROR_STACK(kErrorCodeStrAlreadyExists);
  }
  LOG(INFO) << "Newly created an hash-storage " << *holder_;
  exist_ = true;
  engine_->get_storage_manager().get_pimpl()->register_storage(holder_);
  return kRetOk;
}

inline uint64_t compute_hash(const void *key, uint16_t key_length){
  return 0;
}

inline uint8_t compute_tag(const void *key, uint16_t key_length){
  return 0;
}

inline uint8_t get_tag(thread::Thread* context, uint32_t bin, uint32_t x){
  return 0;
}




inline ErrorCode HashStoragePimpl::get_record(thread::Thread* context,
                                              const void *key, uint16_t key_length,
          void *payload, uint16_t payload_offset, uint16_t payload_count) {
  bool exists = false;
  uint64_t bin = compute_hash(key, key_length);
  uint8_t tag = compute_tag(key, key_length);
//   for(uint8_t x=0; x<4; x++){
//     uint8_t tag2 = get_tag(context, bin, x);
//     if(tag == tag2) {
//       if(*key == get_key(context, bin, x)){
//         Record *record = nullptr;
//         CHECK_ERROR_CODE(locate_record(context, bin, x, record)); //Now record is a pointer to the value corresponding to the key
//         exists = true;
//         return context->get_current_xct().read_record(record, payload, payload_offset, payload_count);
//       }
//     }
//   }
//   bin = bin ^ compute_tag_hash(tag);
//   for(uint8_t x=0; x<4; x++){
//     uint8_t tag2 = get_tag(context, bin, x);
//     if(tag == tag2) {
//       if(*key == get_key(context, bin, x)){
//         Record *record = nullptr;
//         CHECK_ERROR_CODE(locate_record(context, bin, x, record)); //Now record is a pointer to the value corresponding to the key
//         exists = true;
//         return context->get_current_xct().read_record(record, payload, payload_offset, payload_count);
//       }
//     }
//   }
  if (!exists) {
    return kErrorCodeStrKeyNotFound;
  }
  return kErrorCodeOk;
}

inline uint16_t get_other_bin(thread::Thread* context, uint16_t bin, uint8_t position){
  return bin ^ get_tag(context, bin, position); //SHOULD BE XORING A HASH, NOT A BIN (THE HASH HAS
  //MORE BITS)
}

ErrorCode insert_to_page(thread::Thread* context, uint16_t bin,
                         uint8_t position_in_bin, const void* key,
                         uint8_t tag, const void* payload,
                         uint16_t payload_count, Record* record) {
  return kErrorCodeOk;
}

inline ErrorCode HashStoragePimpl::write_new_record(thread::Thread* context, uint16_t bin,
                                                    uint8_t position_in_bin, const void* key,
                                                    uint8_t tag, const void* payload, uint16_t payload_count) {
  Record *record=nullptr;
  uint16_t log_length = HashOverwriteLogType::calculate_log_length(0, payload_count); //should involve keylength
  HashOverwriteLogType* log_entry = reinterpret_cast<HashOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length)); //new log of correct log type and leength
  //log_entry->populate(metadata.id_, payload, payload_count); //fill up log //WHERE DO I GET METAEDATA?
  CHECK_ERROR_CODE(insert_to_page(context, bin, position_in_bin, key, tag, payload, payload_count, record));
  //now record is a pointer to the record in the page
  return context->get_current_xct().add_to_write_set(holder_, record, log_entry); //update the write set with log and record
}

ErrorCode get_cuckoo_path(thread::Thread* context, std::vector<uint16_t>* nodes, std::vector<uint16_t>* adjacentnodes, uint16_t depth, uint64_t *place_tracker){
    if(depth > 4) return kErrorCodeStrCuckooTooDeep; //need to define the error code //4 is max depth
       // give place_tracker more bits
    for(uint16_t a = 0; a < nodes -> size(); a++){
      for(uint8_t x = 0; x < 4; x++){
        uint16_t newbin = get_other_bin(context, (*nodes)[a], x);
        adjacentnodes -> push_back(newbin); //stick adjacent nodes in new bins
        for(uint8_t y=0; y < 4; y++){
          if(get_tag(context, newbin, y) == 0){ //If we find an end position in the path
            (*place_tracker) *= 4;
            (*place_tracker) += y; //add on the information for the position used in the final bucket in path
            return kErrorCodeOk;
          }
        }
        (*place_tracker) ++;
      }
    }
    nodes -> resize(0);
    return get_cuckoo_path(context, adjacentnodes, nodes, depth+1, place_tracker);
}

ErrorCode execute_path(thread::Thread* context, uint16_t bin, std::vector<uint16_t> path){ //bin is starting bin in path
//   uint8_t bin_pos = path.back(); // is a number from 0 to 3
//   path.pop_back();
//   uint8_t new_bin_pos = path[path.size()-1]; // is a number from 0 to 3
//   uint16_t newbin = get_other_bin(context, bin, bin_pos);
//   if(path.size() > 1) CHECK_ERROR_CODE(execute_path(context, newbin, place_tracker / 4));
//   (*place_tracker) ++;
//   // TODO(Bill): need to figure out how to actually get payload and payload_count (this basically has to do with page layout I think
//   // If I wanted to make it look good write now, I could just pretend I had functions for them (just like I did for some other things)
//   // But at this point I'm too lazy to do that...
//   // ALSO, DON'T I NEED THE LENGTH OF THE KEY? How could get_key function otherwise if we are using variable length keys...
//   CHECK_ERROR_CODE(write_new_record(context, new_bin, new_bin_pos,
//                                     get_key(context, bin, bin_pos),
//                                     get_tag(context, bin, bin_pos), payload, payload_count));
//   delete_record(context, bin, bin_pos); //function not written yet
  return kErrorCodeOk;
}

ErrorCode HashStoragePimpl::insert_record(thread::Thread* context, const void* key,
                                          uint16_t key_length, const void* payload, uint16_t payload_count) {
  //Do I actually need to add anything to the read set in this function?
  uint64_t bin = compute_hash(key, key_length);
  uint8_t tag = compute_tag(key, key_length);
  for(uint8_t x=0; x<4; x++){
    if(get_tag(context, bin, x) == 0) { //special value of tag that we need to make sure never occurs
      return write_new_record(context, bin, x, key, tag, payload, payload_count); //Needs to be written still
    }
  }
  bin = bin ^ tag;
  for(uint8_t x=0; x<4; x++){
    if(get_tag(context, bin, x) == 0) { //special value of tag that we need to make sure never occurs
      return write_new_record(context, bin, x, key, tag, payload, payload_count); //Needs to be written still
    }
  }
  //In this case we need to build a Cuckoo Chain we should use a bfs that way we can have as few guys in the write set as possible
  //Do we even need to add to the read set guys who we don't use in the chain?
  //For now we'll go with the second bin, even though we should go for the emptier bin in practice
  uint64_t place_tracker=0; //keeps track of how many nodes we've visited -- we can use that to reverse engineer the path to the node
  std::vector<uint16_t> nodes;
  std::vector<uint16_t> adjacentnodes;
  nodes.push_back(bin);
  CHECK_ERROR_CODE(get_cuckoo_path(context, &nodes, &adjacentnodes, 0, &place_tracker));
  //First we want to reverse place_tracker in base 4 (base 4 because we're using 4-way associativity)
  std::vector <uint16_t> path;
  while(place_tracker > 0){
    path.push_back(place_tracker % 4);
    place_tracker /= 4;
  }
  //Now we're ready to execute the path
  CHECK_ERROR_CODE(execute_path(context, bin, path));
  uint16_t positioninbin=0; //TODO
  return write_new_record(context, bin, positioninbin, key, tag, payload, payload_count);
  //TODO(Bill): Keep track of read list in this function (do we need to?) (write list is taken care of already in write_new_record function)
}



}  // namespace hash
}  // namespace storage
}  // namespace foedus
