//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  auto page = buffer_pool_manager_->NewPage(&directory_page_id_);
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  dir_page->SetPageId(directory_page_id_);
  dir_page->IncrGlobalDepth();

  page_id_t bucket_page_id_0, bucket_page_id_1;
  buffer_pool_manager_->NewPage(&bucket_page_id_0);
  buffer_pool_manager_->NewPage(&bucket_page_id_1);

  dir_page->SetLocalDepth(0, 1);
  dir_page->SetLocalDepth(1, 1);
  dir_page->SetBucketPageId(0, bucket_page_id_0);
  dir_page->SetBucketPageId(1, bucket_page_id_1);

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id_0, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id_1, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t directory_index = Hash(key) & dir_page->GetGlobalDepthMask();
  return directory_index;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t directory_index = Hash(key) & dir_page->GetGlobalDepthMask();
  page_id_t page_id = dir_page->GetBucketPageId(directory_index);
  return page_id;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  HashTableDirectoryPage * dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE * bucket_page = FetchBucketPage(bucket_page_id);
  auto bucket_page_latch = reinterpret_cast<Page *>(bucket_page);
  bucket_page_latch->RLatch();
  bool success = bucket_page->GetValue(key, comparator_, result);
  bucket_page_latch->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  HashTableDirectoryPage * dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE * bucket_page = FetchBucketPage(bucket_page_id);
  auto bucket_page_latch = reinterpret_cast<Page *>(bucket_page);
  
  bucket_page_latch->RLatch();
  if(bucket_page->IsFull()) {
    bucket_page_latch->RUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.RUnlock();
    return SplitInsert(transaction, key, value);
  }
  bucket_page_latch->RUnlatch();

  bucket_page_latch->WLatch();
  bool success = bucket_page->Insert(key, value, comparator_);
  bucket_page_latch->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  HashTableDirectoryPage * dir_page = FetchDirectoryPage();
  uint32_t dir_index = KeyToDirectoryIndex(key, dir_page);
  page_id_t old_bucket_page_id = KeyToPageId(key, dir_page);

  if(dir_page->GetGlobalDepth() == dir_page->GetLocalDepth(dir_index)) {
    uint32_t num_buckets = dir_page->Size();
    if(num_buckets == DIRECTORY_ARRAY_SIZE) {
      table_latch_.WUnlock();
      return false;
    }
    
    for(uint32_t bucket_index = 0; bucket_index < num_buckets; bucket_index++) {
      uint32_t new_bucket_index = bucket_index + ( 1 << dir_page->GetGlobalDepth() );
      dir_page->SetLocalDepth(new_bucket_index, dir_page->GetLocalDepth(bucket_index));
      dir_page->SetBucketPageId(new_bucket_index, dir_page->GetBucketPageId(bucket_index));
    }
    dir_page->IncrGlobalDepth();
  }


  HASH_TABLE_BUCKET_TYPE * old_bucket_page = FetchBucketPage(old_bucket_page_id);

  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  auto new_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&new_bucket_page_id)->GetData());
  dir_page->IncrLocalDepth(dir_index);

  auto local_mask = dir_page->GetLocalDepthMask(dir_index);

  for(uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if(i!=dir_index&&dir_page->GetBucketPageId(i)==old_bucket_page_id) {
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(dir_index));
      if((local_mask & i) != (local_mask & dir_index))
        dir_page->SetBucketPageId(i, new_bucket_page_id);
    }
  }


  for(uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    auto new_index = KeyToDirectoryIndex(old_bucket_page->KeyAt(i), dir_page);
    if((local_mask & new_index) != (local_mask & dir_index)) {
      new_bucket_page->Insert(old_bucket_page->KeyAt(i), old_bucket_page->ValueAt(i), comparator_);
      old_bucket_page->SetReadable(i, false);
    }
  }


  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(old_bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(new_bucket_page_id, true);
  table_latch_.WUnlock();
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  HashTableDirectoryPage * dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE * bucket_page = FetchBucketPage(bucket_page_id);
  auto bucket_page_latch = reinterpret_cast<Page *>(bucket_page);
  bucket_page_latch->WLatch();
  bool success = bucket_page->Remove(key, value, comparator_);
  bucket_page_latch->WUnlatch();
  table_latch_.RUnlock();

  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);

  if(success && bucket_page->IsEmpty())
    Merge(transaction, key, value);

  return success;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage * dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE * bucket_page = FetchBucketPage(bucket_page_id);

  
  if(!bucket_page->IsEmpty()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.WUnlock();
    return;
  }

  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t sibling_bucket_idx = dir_page->GetSplitImageIndex(bucket_idx);
  page_id_t sibling_page_id = dir_page->GetBucketPageId(sibling_bucket_idx);
  
  if(bucket_page_id != sibling_page_id && dir_page->GetLocalDepth(bucket_idx) == dir_page->GetLocalDepth(sibling_bucket_idx)
                                        && dir_page->GetLocalDepth(bucket_idx) > 0) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->DeletePage(bucket_page_id);

    for(uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
      if(dir_page->GetBucketPageId(i)==bucket_page_id) {
        dir_page->DecrLocalDepth(i);
        dir_page->SetBucketPageId(i, sibling_page_id);
      }
      else if(dir_page->GetBucketPageId(i)==sibling_page_id)
        dir_page->DecrLocalDepth(i);
    }

  }

  while(dir_page->CanShrink() && dir_page->GetGlobalDepth() > 1) {
    dir_page->DecrGlobalDepth();
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
