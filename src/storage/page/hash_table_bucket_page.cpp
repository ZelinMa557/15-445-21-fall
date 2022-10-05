//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"
#include<vector>
#include<iostream>

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  for(size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if(!IsOccupied(bucket_idx))
      break;
    if(IsReadable(bucket_idx) && cmp(KeyAt(bucket_idx), key) == 0)
      result->push_back(ValueAt(bucket_idx));
  }
  if(result->size() > 0)
    return true;
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  size_t bucket_idx = 0;
  for(; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if(!IsOccupied(bucket_idx))
      break;
    if(IsReadable(bucket_idx) && cmp(KeyAt(bucket_idx), key) == 0 && ValueAt(bucket_idx) == value)
      return false;
  }

  if(bucket_idx == BUCKET_ARRAY_SIZE) {
    for(bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
      if(!IsReadable(bucket_idx)) {
        array_[bucket_idx] = std::make_pair(key, value);
        SetReadable(bucket_idx, true);
        return true;
      }
    }
    return false;
  }

  array_[bucket_idx] = std::make_pair(key, value);
  SetOccupied(bucket_idx);
  SetReadable(bucket_idx, true);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  size_t bucket_idx = 0;
  for(; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if(!IsOccupied(bucket_idx))
      break;
    if(IsReadable(bucket_idx) && cmp(KeyAt(bucket_idx), key) == 0 && ValueAt(bucket_idx) == value) {
      SetReadable(bucket_idx, false);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) { SetReadable(bucket_idx, false); }

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  auto num_index = bucket_idx / (8*sizeof(char));
  auto bit_index = bucket_idx % (8*sizeof(char));
  if(occupied_[num_index]>>bit_index & 1)
    return true;
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  auto num_index = bucket_idx / (8*sizeof(char));
  auto bit_index = bucket_idx % (8*sizeof(char));
  occupied_[num_index] = occupied_[num_index] | (1 << bit_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  auto num_index = bucket_idx / (8*sizeof(char));
  auto bit_index = bucket_idx % (8*sizeof(char));
  if(readable_[num_index]>>bit_index & 1)
    return true;
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx, bool set_value) {
  auto num_index = bucket_idx / (8*sizeof(char));
  auto bit_index = bucket_idx % (8*sizeof(char));
  if(set_value)
    readable_[num_index] = readable_[num_index] | (1 << bit_index);
  else
    readable_[num_index] = readable_[num_index] & ~(1 << bit_index); 
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  uint32_t byte_array_size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  for(uint32_t i = 0; i < byte_array_size; i++ ) {
    if (readable_[i] != -1)
      return false;    
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  uint32_t bitmap_size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  for(uint32_t i = 0; i < bitmap_size; i++) {
    if(readable_[i])
      return false;
    if(!occupied_[i])
      break;
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
