//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
    // page_id_ = page_id;
    // parent_page_id_ = parent_id;
    // max_size_ = max_size;
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetSize(0); // value的个数
    SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const { 
    int n = GetSize();
    for(int i = 0;i < n;++i){
      if(array_[i].second == value){
          return i;
      }
    }
    return -1; // 找不到返回-1
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array_[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // 查找key所属位置的子树
  int left = 1, right = GetSize()-1; // [1, m-1]
  // 使用type1 模板
  while (left <= right){
    int mid = left + (right - left) / 2;
    if(comparator(KeyAt(mid), key) > 0){ // mid > key
        right = mid - 1;
    }else{
      // mid <= key , 保证最后结束的时候 left位置一定是第一个大于key的
      left = mid + 1;
    }
  }
  int target_index = left;
  assert(target_index >= 1); // 不要<0越界访问
  return ValueAt(target_index - 1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
    // 该函数是产生插入后分裂到根节点位置节点，含有两棵子树
    array_[0].second = old_value;
    array_[1].second = new_value;
    array_[1].first = new_key;
    SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  // 在old_value后面插入{new_key, new_value}
  int insert_index = ValueIndex(old_value) + 1; // old_value 之后一个index
  for(int i = GetSize(); i > insert_index; --i){
    array_[i] = array_[i-1];
  }
  array_[insert_index] = MappingType{new_key, new_value}; 
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 * this page是old_node，recipient page是new_node
 * old_node的右半部分array复制给new_node
 * 并且，将new_node（原old_node的右半部分）的所有孩子结点的父指针更新为指向new_node
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
    int start_index = GetMinSize(); // max / 2 : (0,1,2) start index is 1; (0,1,2,3) start index is 2;
    int move_num = GetSize() - start_index;
    recipient -> CopyNFrom(array_ + start_index, move_num, buffer_pool_manager);
    IncreaseSize(-move_num);
}

/** 从items指向的位置开始，复制size个，到当前调用该函数的page的array尾部（本函数由recipient page调用）
 * 并且，找到调用该函数的page的array中每个value指向的孩子结点，其父指针更新为调用该函数的page id 
Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    std::copy(items, items + size, array_ + GetSize());// 放到当前page后面的空间中
    for(int i = GetSize();i < GetSize() + size; ++i){
      // 将所有新增孩子节点的parent id修改为本页
      Page * child_page = buffer_pool_manager->FetchPage(ValueAt(i)); // 查找对应的孩子页
      BPlusTreePage * child_node = reinterpret_cast<BPlusTreePage * >(child_page -> GetData());
      child_node->SetParentPageId(GetPageId());
      // 修改了child page, 需要进行unpin
      buffer_pool_manager->UnpinPage(child_page->GetPageId(), true); // 引入lru 
    }
    // 增大size空间
    IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
    IncreaseSize(-1); // 一定先减了GetSize不会满
    for(int i=index;i<GetSize(); ++i){
       array_[i] = array_[i+1]; 
    }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() { 
  // 删除最后一个键值对，然后返回值
  SetSize(0);
  return ValueAt(0); 
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
   // 需要合并节点至recipient，那么并入的key应该是middlekey
   SetKeyAt(0, middle_key); 
   recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
    // 将第一个k,v从该page移动到recipient
    SetKeyAt(0, middle_key); // 移到recipient尾部（得修改key，因为原来第一个key是非法的）
    recipient -> CopyLastFrom(array_[0], buffer_pool_manager);
    Remove(0); // 删除第一个k,v
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
   array_[GetSize()] = pair;
   // 修改children的parent_id
   Page *child_page = buffer_pool_manager->FetchPage(ValueAt(GetSize()));
  BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);

   IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
    // 把最后一个插入到recipient第一个
    recipient->SetKeyAt(0, middle_key);
    recipient->CopyFirstFrom(array_[GetSize()-1], buffer_pool_manager);
    IncreaseSize(-1);
    
}
/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
    // 将pair插入到array 第一个
    for(int i = GetSize();i >= 0; --i){
      array_[i] = array_[i-1];
    }
    array_[0] = pair;
    // update parent page id of child page
    Page *child_page = buffer_pool_manager->FetchPage(ValueAt(0));
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
    IncreaseSize(1);
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
