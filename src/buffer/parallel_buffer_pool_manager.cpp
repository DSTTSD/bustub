//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances 
  num_instances_ = num_instances;
  pool_size_ = pool_size; // instance大小
  start_index_ = 0;
  instances_ = new BufferPoolManager * [num_instances];
  for(uint32_t i = 0; i< num_instances; ++i){
    // uint32_t 
    instances_[i] = new BufferPoolManagerInstance(pool_size, static_cast<uint32_t>(num_instances), i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager(){
  for (size_t i = 0; i < num_instances_; i++) {
    delete instances_[i];
  }
  delete[] instances_;  //释放对象空间
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  auto iter = table_.find(page_id);
  if(iter != table_.end()){
      size_t index = iter -> second;
      return instances_[index];
  }
  return nullptr;
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  auto iter = table_.find(page_id);
  if(iter != table_.end()){
      size_t index = iter -> second;
      return instances_[index]->FetchPage(page_id);
  }
  return nullptr;
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  auto iter = table_.find(page_id);
  if(iter != table_.end()){
      size_t index = iter -> second;
      return instances_[index]->UnpinPage(page_id, is_dirty);
  }
  return false;
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  auto iter = table_.find(page_id);
  if(iter != table_.end()){
      size_t index = iter -> second;
      return instances_[index]->FlushPage(page_id);
  }
  return false;
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  for (size_t i = start_index_; i < start_index_ + num_instances_; i++) {
    // round 一轮
    Page *page =
        dynamic_cast<BufferPoolManagerInstance *>(instances_[i % num_instances_])->NewPage(page_id);
    if (page != nullptr) {
      start_index_ = i % num_instances_;
      int32_t instance_id = static_cast<int32_t> (start_index_);
      table_.emplace(*page_id, instance_id);
      return page;
    }
  }
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  auto iter = table_.find(page_id);
  if(iter != table_.end()){
      size_t index = iter -> second;
      bool res =  instances_[index]->DeletePage(page_id);
      if(res){
        table_.erase(page_id);
        return true;
      }
  }
  return false;
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for(uint32_t i = 0; i< num_instances_; ++i){
    // dynamic_cast<BufferPoolManagerInstance *>()
      instances_[i]->FlushAllPages();
  }
}

}  // namespace bustub
