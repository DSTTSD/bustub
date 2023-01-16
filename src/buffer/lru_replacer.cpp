//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    max_size = num_pages; // 最大pages
}

LRUReplacer::~LRUReplacer() = default; // 默认析构函数

/**
 * 使用LRU策略删除一个victim frame，这个函数能得到frame_id
 * @param[out] frame_id id of frame that was removed, nullptr if no victim was found
 * @return true if a victim frame was found, false otherwise
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    std::scoped_lock lock(mut); // 上锁
    if(LRUlist.empty()){
        return false;
    }
    *frame_id = LRUlist.back();
    LRUhash.erase(*frame_id);
    LRUlist.pop_back();
    return true; 
}

/**
 * 固定一个frame, 表明它不应该成为victim（即在replacer中移除该frame_id）
 * @param frame_id the id of the frame to pin
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
    std::scoped_lock lock(mut); // 上锁
    // 哈希表找不到frame_id
    if(LRUhash.find(frame_id) == LRUhash.end()){
        return;
    }
    auto iter = LRUhash[frame_id];
    LRUlist.erase(iter);
    LRUhash.erase(frame_id);
}

/**
 * 取消固定一个frame, 表明它可以成为victim（即将该frame_id添加到replacer）
 * @param frame_id the id of the frame to unpin
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::scoped_lock lock(mut); // 上锁
    if(LRUhash.find(frame_id) != LRUhash.end()){
        return; // 有该表
    }
    if (LRUlist.size() == max_size) {
        return;
    }
    LRUlist.push_front(frame_id);
    LRUhash.emplace(frame_id, LRUlist.begin());
}

/** @return replacer中的数量 */
size_t LRUReplacer::Size() { 
    return LRUlist.size(); 
}

}  // namespace bustub
