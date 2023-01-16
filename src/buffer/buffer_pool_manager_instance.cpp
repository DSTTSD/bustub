//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

/**
 * Flushes the target page to disk. 将page写入磁盘；不考虑pin_count
 * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
 * INVALID_PAGE_ID: if a Page object does not contain a physical page, then its page_id must be set to INVALID_PAGE_ID.
 * @return false if the page could not be found in the page table, true otherwise
 */
bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock lock(latch_);
  if(page_id == INVALID_PAGE_ID){
    return false;
  }
  // 1.查找buffer pool是否该页
  auto iter = page_table_.find(page_id);
  if(iter == page_table_.end()){
    return false;
  }
  // 2.该page在table里
  frame_id_t frame_id = iter->second;
  Page * page = & pages_[frame_id];
  // 不管状态，直接写回磁盘, 没有原数据重置（pin_count）
  disk_manager_->WritePage(page->page_id_, page->data_);
  page->is_dirty_ = false;
  return true;
}


void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::scoped_lock lock(latch_);
  for(auto & [page_id, frame_id]: page_table_){
    Page * page = & pages_[frame_id];
    if(page->page_id_ != INVALID_PAGE_ID && page->IsDirty()){
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
  }
}

/**
 * Creates a new page in the buffer pool. 相当于从磁盘中移动一个新建的空page到缓冲池某个位置
 * @param[out] page_id id of created page
 * @return nullptr if no new pages could be created, otherwise pointer to new page
 */
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lock(latch_);
  frame_id_t frame_id = -1;
  // 1 如果所有pages都pinned，没办法添加新的页
  if(!FindVictimPage(& frame_id)){
    return nullptr;
  }
  // 2 找victim
  *page_id = AllocatePage(); // 新的page id
  Page * page = & pages_[frame_id];

  //3 update meta data
  UpdatePage(page, *page_id, frame_id);
  replacer_->Pin(frame_id); // replacer不要有新页
  page->pin_count_ = 1; // 调用的函数会使用（设置1），所以用完需要手动unpin
  return page;
}

/*
从free_list或replacer中得到*frame_id；返回bool类型
（自己补充的函数）
*/
bool BufferPoolManagerInstance::FindVictimPage(frame_id_t *frame_id) {
    // 1. 缓冲池没有满的时候用缓冲池空间
    if(!free_list_.empty()){
      * frame_id = free_list_.front();// 从链表头选取
      free_list_.pop_front();
      return true;
    }
    // 2. 缓冲池满了，lru选取victim
    return replacer_->Victim(frame_id);
}

void BufferPoolManagerInstance::UpdatePage(Page *page, page_id_t new_page_id, frame_id_t new_frame_id){
    // 1. 如果是lru list脏页，需要写回磁盘
    if(page->IsDirty()){
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
    // 2. 更新page table 
    page_table_.erase(page->page_id_); // 删除tabel中frame_id记录
    if(new_page_id != INVALID_PAGE_ID){
      page_table_.emplace(new_page_id, new_frame_id);  
    }
    
    // 3. 重置page的data
    page->ResetMemory();
    page->page_id_ = new_page_id;
}

/**
 * Fetch the requested page from the buffer pool.
 * 如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 * 如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @param page_id id of page to be fetched
 * @return the requested page
 */
Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  // 操作上锁
  std::scoped_lock lock(latch_);
  auto iter = page_table_.find(page_id);
  //1 查找是否在缓存池
  if(iter != page_table_.end()){
    frame_id_t frame_id = iter->second;
    BUSTUB_ASSERT(static_cast<unsigned int> (frame_id) < pool_size_, "frame_id should less than pool size");
    Page * page = &pages_[frame_id];
    replacer_->Pin(frame_id);
    page->pin_count_++; // 更新pin_count
    return page;
  }
  // 如果不在缓存池
  frame_id_t frame_id = -1;
  // 1.2 选取victim page
  if (!FindVictimPage(&frame_id)){
    return nullptr;
  }
  // 2.1 找到page， 做替换
  BUSTUB_ASSERT(static_cast<unsigned int> (frame_id) < pool_size_, "frame_id should less than pool size");
  Page * page = & pages_[frame_id];
  UpdatePage(page, page_id, frame_id); // 2 data置为空，dirty页写入磁盘，然后dirty状态置false
  disk_manager_->ReadPage(page_id, page->data_); // 3 注意，从磁盘文件database file中page_id的位置读取内容到新page->data
  replacer_ ->Pin(frame_id);
  page->pin_count_ = 1; //新的pin_count
  return page;
}

/**
 * Deletes a page from the buffer pool.
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock lock(latch_);
  auto iter = page_table_.find(page_id);
  // 1. 不存在
  if(iter == page_table_.end()){
    return true;
  }
  // 1. 存在page
  frame_id_t frame_id = iter->second;
  Page * page = & pages_[frame_id];
  // 2. 如果别的线程还在用
  if(page->pin_count_ > 0){
    return false;
  }
  // 3：pin_count = 0
  DeallocatePage(page_id);
  UpdatePage(page, INVALID_PAGE_ID, frame_id);
  free_list_.emplace_back(page_id);
  return true;
}

/**
 * Pin与unpin 同lru_replacer相反 这里unpin是移出bufferpool, lru是留在replacer，可以成为victim
 * Unpin the target page from the buffer pool. 取消固定pin_count>0的在缓冲池中的page
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @return false if the page pin count is <= 0 before this call, true otherwise
 */
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { 
    std::scoped_lock lock(latch_);
    //  the is_dirty parameter keeps track of whether a page was modified while it was pinned.
    auto iter = page_table_.find(page_id);
    //1 查找是否在缓存池
    if(iter == page_table_.end()){
      return false;
    }
    // 2 如果存在该页
    frame_id_t frame_id = iter->second;
    Page * page = & pages_[frame_id];
    // 2.1 pincount == 0
    if(page->pin_count_ == 0){ // 不需要再减少pin_page 
      return false;
    }
    // 2.2 pin_count > 0
    // 只有pin_count>0才能进行pin_count--，如果pin_count=0之前就直接返回了
    page->pin_count_--;  // 这里特别注意，只有pin_count减到0的时候才让replacer进行unpin
    if(page -> pin_count_ == 0){
      replacer_->Unpin(frame_id);
    }
    if(is_dirty){
      page->is_dirty_ = true;
    }
    return true; 
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
