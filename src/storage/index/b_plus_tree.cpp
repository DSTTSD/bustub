//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"


namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

/*
  判断操作安不安全
*/

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::IsSafe(N *node, Operation op) {
  // TODO: 边界条件
  if (node->IsRootPage()) {
    // 根节点 
    return (op == Operation::INSERT && node->GetSize() < node->GetMaxSize() - 1) ||
           (op == Operation::DELETE && node->GetSize() > 2);
  }

  if (op == Operation::INSERT) {
    // 
    return node->GetSize() < node->GetMaxSize() - 1; // 所含key少于n-1个， n-1个添加后需要分裂
  }

  if (op == Operation::DELETE) {
    // 此处逻辑需要和coalesce函数对应
    return node->GetSize() > node->GetMinSize();
  }

  // LOG_INFO("IsSafe Thread=%ld", getThreadId());

  return true;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // 查询key在对应leaf page中的value，并将value存入result
  // 1. 先找到leaf page，这里面会调用fetch page
  Page * leaf_page = FindLeafPageByOperation(key, Operation::FIND, transaction).first;

  // 2.对应leafpage之中找到这个key
  LeafPage * leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());  // 记得加上GetData()，跳过page的header

  ValueType value{};// {}调用空构造函数
  bool is_exist = leaf_node->Lookup(key, &value, comparator_);
  
  // 3. page用完后记得unpin page
  auto pid = leaf_page->GetPageId();
  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(pid, false);

  if(!is_exist){
    return false;
  }
  result -> push_back(value);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPages(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }

  // unlock 和 unpin 事务经过的所有parent page
  for (Page *page : *transaction->GetPageSet()) {  // 前面加*是因为page set是shared_ptr类型
    page->WUnlatch();
  }
  transaction->GetPageSet()->clear();  // 清空page set
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockUnpinPages(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  for (Page *page : *transaction->GetPageSet()) {  // 前面加*是因为page set是shared_ptr类型
    auto pid = page->GetPageId();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(pid, false);
  }
  transaction->GetPageSet()->clear();  // 清空page set
}

INDEX_TEMPLATE_ARGUMENTS
std::pair<Page *, bool> BPLUSTREE_TYPE::FindLeafPageByOperation(const KeyType &key, Operation operation,
                                                                Transaction *transaction, bool leftMost,
                                                                bool rightMost) {
  assert(operation == Operation::FIND ? !(leftMost && rightMost) : transaction != nullptr); // 从左到右和从右至左
  // leftmost: 每次找最左边结点；rightmost：每次找最右边结点；
  // transaction不空，意味着需要进行修改操作
  
  root_latch_.lock(); // 锁住根节点, 获取页
  bool is_root_page_id_latched = true;
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (operation == Operation::FIND) {
    // 查找对应页
    page->RLatch(); // 上读锁
    is_root_page_id_latched = false;
    root_latch_.unlock();
  } else {
    // 事物修改上写锁
    page->WLatch();
    if (IsSafe(node, operation)) {
      is_root_page_id_latched = false;
      root_latch_.unlock();
    }
  }

  while (!node->IsLeafPage()) {
    // 一路查找叶子结点
    InternalPage *i_node = reinterpret_cast<InternalPage *>(node);

    page_id_t child_node_page_id;
    if (leftMost) {
      //找最左边
      child_node_page_id = i_node->ValueAt(0);
    } else if (rightMost) {
      // 找最右边
      child_node_page_id = i_node->ValueAt(i_node->GetSize() - 1);
    } else {
      // 找对应区间子树
      child_node_page_id = i_node->Lookup(key, comparator_);
    }

    auto child_page = buffer_pool_manager_->FetchPage(child_node_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData()); // 跳过page的header

    if (operation == Operation::FIND) {
      // 逐步向下“爬”，上锁 & 解锁
      child_page->RLatch();
      auto tmp_pid = page->GetPageId();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(tmp_pid, false); // 后续不需要了，减小page_count
    } else {
      // 修改写锁
      child_page->WLatch();
      transaction->AddIntoPageSet(page); //save  the pages that were latched in the process
      // child node is safe, release all locks on ancestors
      if (IsSafe(child_node, operation)) { // 确定孩子节点完全安全，即使引发
        if (is_root_page_id_latched) {
          // 对于根节点单独判断
          is_root_page_id_latched = false;
          root_latch_.unlock();
        }
        UnlockUnpinPages(transaction); // 向下搜索时,把所有transaction中中间结点unpin 解锁
      }
    }

    page = child_page;
    node = child_node;
  }  
  // 返回根节点是否还是在上锁
  return std::make_pair(page, is_root_page_id_latched); // 对应leaf page
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) { 
  { 
    // 注意新建根节点时要锁住, 使用C++ 11 lock_guard，在该大括号作用域内锁住根节点
    const std::lock_guard<std::mutex> guard(root_latch_);  
    // std::scoped_lock lock{root_latch_};
    if (IsEmpty()) {
      StartNewTree(key, value);
      return true;
    }
  }
  // 插入到对应叶子结点
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // 创建空叶节点L, 同时作为根节点,上级加锁了，这里就不用控制了
  // 1 缓冲池申请一个new page，作为root page
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *root_page = buffer_pool_manager_->NewPage(&new_page_id); 

  if(root_page == nullptr){
    throw std::runtime_error("out of memory");
  }

  // 2 page id 赋给root page id， 
  root_page_id_ = new_page_id;
  UpdateRootPageId(1); // 会在page的header中天加字段Indexname:root_page_id_
  
  // 3 使用leaf page的Insert进行插入
  LeafPage *root_node = reinterpret_cast<LeafPage *>(root_page->GetData());  // 记得加上GetData()
  root_node->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);             // 记得初始化为leaf_max_size
  root_node->Insert(key, value, comparator_);
  // 4 unpin root page
  buffer_pool_manager_->UnpinPage(root_page->GetPageId(), true);  // 注意：这里dirty要置为true！
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // 1 找到需要插入叶子结点的位置
  auto [leaf_page, root_is_latched] = FindLeafPageByOperation(key, Operation::INSERT, transaction);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());  // 注意，记得加上GetData()

  int size = leaf_node->GetSize();
  int new_size = leaf_node -> Insert(key, value, comparator_); //这里 now_size <= newsize <= maxsize

  if(new_size == size){
    // existed
    if(root_is_latched){
      root_latch_.unlock();
    }
    UnlockUnpinPages(transaction); // 把叶子之外的锁住的祖先释放
    // 释放叶子
    auto tmp_pid = leaf_page->GetPageId();
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(tmp_pid, false);
    
    return false;
  }else if(new_size < leaf_node->GetMaxSize()){
    // 可以直接进行，不需要分裂
    if(root_is_latched){
      root_latch_.unlock();
    }
    auto tmp_pid = leaf_page->GetPageId();
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(tmp_pid, true);
    return true;
  }else{
    // new_size == leaf_node->GetMaxSize() 需要分裂
    LeafPage *new_leaf_node = Split(leaf_node);
    bool * pointer_root_is_latched = new bool(root_is_latched);
    InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node, transaction, pointer_root_is_latched);
    // 根节点的latch在这里应该已经释放了 
    assert((*pointer_root_is_latched) == false);
    delete pointer_root_is_latched;
    auto old_tmp_pid = leaf_page->GetPageId();
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(old_tmp_pid, true);      // unpin leaf page
    buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true); 
    return true;
  }
  return false;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  // 1 缓冲区申请一个新页
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);  // 注意new page的pin_count=1，之后记得unpin page
  if (nullptr == new_page) {
    throw std::runtime_error("out of memory");
  }
  // 2 分叶子结点、内部节点情况进行拆分
  N *new_node = reinterpret_cast<N *>(new_page->GetData());  // 记得加上GetData()
  new_node->SetPageType(node->GetPageType());                // DEBUG

  if (node->IsLeafPage()) {  
    // leaf page
    LeafPage *old_leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_leaf_node = reinterpret_cast<LeafPage *>(new_node);
    new_leaf_node->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);  // 注意初始化parent id和max_size
    // old_leaf_node右半部分 移动至 new_leaf_node
    old_leaf_node->MoveHalfTo(new_leaf_node);
    // 更新叶子层的链表，示意如下：
    // 原来：old node ---> next node
    // 最新：old node ---> new node ---> next node
    new_leaf_node->SetNextPageId(old_leaf_node->GetNextPageId());  // 完成连接new node ---> next node
    old_leaf_node->SetNextPageId(new_leaf_node->GetPageId());      // 完成连接old node ---> new node
    new_node = reinterpret_cast<N *>(new_leaf_node);
  } else {  
    // internal page
    InternalPage *old_internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_internal_node = reinterpret_cast<InternalPage *>(new_node);
    new_internal_node->Init(new_page_id, node->GetParentPageId(), internal_max_size_);  // 注意初始化parent id和max_size
    // old_internal_node右半部分 移动至 new_internal_node
    // new_node（原old_node的右半部分）的所有孩子结点的父指针更新为指向new_node
    old_internal_node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
    new_node = reinterpret_cast<N *>(new_internal_node);
  }
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction, bool *root_is_latched) {
  // 1 old_node是根结点，那么整棵树直接升高一层
  // 具体操作是创建一个新结点R当作根结点，其关键字为key，左右孩子结点分别为old_node和new_node
  if (old_node->IsRootPage()) {  
    // old node为根结点
    page_id_t new_page_id = INVALID_PAGE_ID;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);  // 这里应该是NewPage，不是FetchPage！
    root_page_id_ = new_page_id;

    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root_node->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);  // 注意初始化parent page id和max_size
    // 设置孩子
    //修改新的根结点的孩子指针，即array[0].second指向old_node，array[1].second指向new_node；对于array[1].first则赋值为key
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // 修改old_node和new_node的父指针
    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);

    // buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);  // DEBUG
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);  // 修改了new_page->data，所以dirty置为true

    UpdateRootPageId(0);  // 修改HEADER index_name: page_id， update root page id in header page

    // 新的root必定不在transaction的page_set_队列中
    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }
    UnlockPages(transaction);
    return;  // 结束递归
  }else{
    // 2 old_node不是根结点
    
    // a. 父节点未满 先直接插入(key,new_node->page_id)到父结点
    // b. 如果插入后父结点满了，则需要对父结点再进行拆分(Split)，并继续递归
    // 找到old_node的父结点进行操作
    Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId()); 
    InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

    // 将(key,new_node->page_id)插入到父结点中 value==old_node->page_id 的下标之后
    parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

    int size = parent_node->GetSize();
    if(size < parent_node->GetMaxSize()){
      // 没有满直接插入
      if (*root_is_latched) {
        *root_is_latched = false;
        root_latch_.unlock();
      }

      UnlockPages(transaction);  // unlock除了叶子结点以外的所有上锁的祖先节点
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);  // unpin parent page
      return;
    }else{
      // 需要递归操作
      InternalPage * new_parent_node = Split(parent_node);
      InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction, root_is_latched);
      // 保存新的页
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);      // unpin parent page
      buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
   if (IsEmpty()) {
    return;
  }

  // 找到删除的叶子结点位置
  auto [leaf_page, root_is_latched] = FindLeafPageByOperation(key, Operation::DELETE, transaction);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int old_size = leaf_node->GetSize();
  // 在leaf中删除key（如果不存在该key，则size不变）
  int new_size = leaf_node->RemoveAndDeleteRecord(key, comparator_);

  if (new_size == old_size) {
    // 删除失败，没有key
    if (root_is_latched) {
      root_latch_.unlock();
    }
    UnlockUnpinPages(transaction);
    auto tmp_id = leaf_page->GetPageId();
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(tmp_id, false);  // unpin leaf page
    return;
  }else{
    // 删除键成功
    // CoalesceOrRedistribute(合并或者重新分布)
    bool * pointer_root_is_latched = new bool(root_is_latched);
    bool leaf_should_delete = CoalesceOrRedistribute(leaf_node, transaction, pointer_root_is_latched);
    
    assert((*pointer_root_is_latched) == false);
    delete pointer_root_is_latched;

    if (leaf_should_delete) {
      // 对应的是root是leaf size == 1，最后这个leaf page需要删除
      transaction->AddIntoDeletedPageSet(leaf_page->GetPageId());
    }
    // 释放叶节点的锁和控制
    auto tmp_id = leaf_page->GetPageId();
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(tmp_id, true);
    
    // 删除并清空deleted page set
    for (page_id_t page_id : *transaction->GetDeletedPageSet()) {
      buffer_pool_manager_->DeletePage(page_id);
    }
    transaction->GetDeletedPageSet()->clear();
  }

}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction, bool *root_is_latched) {
  // 1 处理根节点
  if (node->IsRootPage()) {
    bool root_should_delete = AdjustRoot(node);

    // DEBUG
    if (*root_is_latched) {
      // LOG_INFO("CoalesceOrRedistribute node is root page root_latch_.unlock()");
      *root_is_latched = false;
      root_latch_.unlock();
    }

    UnlockPages(transaction);
    return root_should_delete;  // NOTE: size of root page can be less than min size
  }

  // 2 内部节点不需要合并或者重新分配
  // 不需要合并或者重分配；不需要删除节点
  if (node->GetSize() >= node->GetMinSize()) { // 非根节点至少有 m / 2
    if (*root_is_latched) {
      // LOG_INFO("CoalesceOrRedistribute node > minsize root_latch_.unlock()");
      *root_is_latched = false;
      root_latch_.unlock();
    }
    UnlockPages(transaction);
    return false;
  }
  // 3 不够，需要合并或者重新分配
  // 先获取node的parent page
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // 获得node在parent的孩子指针(value)的index
  int index = parent->ValueIndex(node->GetPageId()); 

  // 寻找兄弟节点，这里尽量找前一个兄弟
  page_id_t sibling_page_id = parent->ValueAt(index == 0 ? 1 : index - 1);
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  sibling_page->WLatch();  // 上锁

  N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

  // 1 Redistribute 当kv总和能支撑两个Node，那么重新分配即可，不必删除node
  if (node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize()) {
    // 能够保证至少半满
    if (*root_is_latched) {
      // LOG_INFO("CoalesceOrRedistribute before Redistribute root_latch_.unlock()");
      *root_is_latched = false;
      root_latch_.unlock();
    }

    Redistribute(sibling_node, node, index);  // 无返回值

    UnlockPages(transaction);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

    return false;  // node不必被删除
  }else{
    // 2 Coalesce 当sibling和node只能凑成一个Node，那么合并两个结点到sibling，删除node
    bool parent_should_delete =
      Coalesce(&sibling_node, &node, &parent, index, transaction, root_is_latched);  // 返回值是parent是否需要被删除
    assert((*root_is_latched) == false);
    if (parent_should_delete) {
      // 后处理根节点需要被删除
      transaction->AddIntoDeletedPageSet(parent->GetPageId());
    }

    // NOTE: parent unlock is finished in Coalesce
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

    return true; // 在remove中会将多余的叶节点删除
  }
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction, bool *root_is_latched) {
  int key_index = index; //node在parent中对应位置
  if(index == 0){
    // 合并的时候是要往sib中合并，sib选取左边的，因此需要换位置
    std::swap(neighbor_node, node);  // 保证neighbor_node为node的前驱
    key_index = 1;
  }

  // middle_key only used in internal_node->MoveAllTo 覆盖内部节点第一个key
  KeyType middle_key = (*parent)->KeyAt(key_index); // 右边那个key

  if ((*node)->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(*neighbor_node);
    // 全部转移到neighbor
    leaf_node->MoveAllTo(neighbor_leaf_node);
    neighbor_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(*node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(*neighbor_node);
    
    internal_node->MoveAllTo(neighbor_internal_node, middle_key, buffer_pool_manager_);
    // LOG_INFO("Coalesce internal, index=%d, pid=%d neighbor->node", index, (*node)->GetPageId());
  }
  // 删除parent index
  (*parent)->Remove(key_index); 

  return CoalesceOrRedistribute(*parent, transaction, root_is_latched);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
    Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());  // parent of node

  // index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
  // index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
  // 注意更新parent结点的相关kv对

  if (node->IsLeafPage()) {
    // 叶子结点
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if (index == 0) {  
      // move neighbor's first to node's end
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
      parent->SetKeyAt(1, neighbor_leaf_node->KeyAt(0));
    } else {  // neighbor -> node
      // move neighbor's last to node's front
      // LOG_INFO("Redistribute leaf, index=%d, pid=%d neighbor->node", index, node->GetPageId());
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
      parent->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if (index == 0) {  // case: node(left) and neighbor(right)
      // MoveFirstToEndOf do this:
      // 1 set neighbor's first key to parent's second key（详见MoveFirstToEndOf函数）
      // 2 move neighbor's first to node's end
      neighbor_internal_node->MoveFirstToEndOf(internal_node, parent->KeyAt(1), buffer_pool_manager_);
      // set parent's second key to neighbor's "new" first key
      parent->SetKeyAt(1, neighbor_internal_node->KeyAt(0));
    } else {  // case: neighbor(left) and node(right)
      // MoveLastToFrontOf do this:
      // 1 set node's first key to parent's index key（详见MoveLastToFrontOf函数）
      // 2 move neighbor's last to node's front
      neighbor_internal_node->MoveLastToFrontOf(internal_node, parent->KeyAt(index), buffer_pool_manager_);
      // set parent's index key to node's "new" first key
      parent->SetKeyAt(index, internal_node->KeyAt(0));
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // 针对root进行的操作分为两种情况
  // 1 old_root_node是内部结点，且大小为1。表示内部结点其实已经没有key了，
  // 所以要把它的孩子更新成新的根结点
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    // LOG_INFO("AdjustRoot: delete the last element in root page, but root page still has one last child");
    // get child page as new root page
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t child_page_id = internal_node->RemoveAndReturnOnlyChild(); // 删除key， 返回最后一个子树值

    // NOTE: don't need to unpin old_root_node, this operation will be done in CoalesceOrRedistribute function
    // buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);

    // update root page id
    root_page_id_ = child_page_id;
    UpdateRootPageId(0);
    // update parent page id of new root node
    Page *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);

    // if (root_is_latched) {
    //   root_latch_.unlock();
    // }

    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    return true;
  }else if(old_root_node->IsLeafPage() && old_root_node->GetSize() == 0){
    // 2: old_root_node是叶结点，且大小为0。直接更新root page id
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }

  return false; 

}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { 
  Page * leftmost_leaf = FindLeafPageByOperation(KeyType(), Operation::FIND, nullptr, true, false).first;

  return INDEXITERATOR_TYPE(buffer_pool_manager_, leftmost_leaf, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { 
  Page *leaf_page = FindLeafPageByOperation(key, Operation::FIND).first;
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  int index = leaf_node->KeyIndex(key, comparator_); // 找了第一个>=key的元素
  
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() { 
  Page *leaf_page = FindLeafPageByOperation(KeyType(), Operation::FIND, nullptr, false, true).first;
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, leaf_node->GetSize()); 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
