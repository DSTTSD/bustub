/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, Page *page, int index): buffer_pool_manager_(bpm), page_(page), index_(index){
    leaf_ = reinterpret_cast<LeafPage *> (page_ -> GetData());
    page->RLatch(); // 锁住当前页
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
  page_->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::IsEnd() { return leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_->GetSize(); }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return leaf_->GetItem(index_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() { 

    index_++;
    if (index_ == leaf_->GetSize() && leaf_->GetNextPageId() != INVALID_PAGE_ID) {
        // 索引到了叶子page末尾，找下一个页
    Page *next_page = buffer_pool_manager_->FetchPage(leaf_->GetNextPageId());  // pin next leaf page
    next_page->RLatch();

    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);  // unpin current leaf page
    // 下一个页开头
    page_ = next_page;
    leaf_ = reinterpret_cast<LeafPage *>(page_->GetData());  // update leaf page to next page
    index_ = 0;                                              // reset index to zero
    }
    return *this;

 }

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return leaf_->GetPageId() == itr.leaf_->GetPageId() && index_ == itr.index_;  // leaf page和index均相同
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const { return !(*this == itr); }  // 此处可用之前重载的==

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
