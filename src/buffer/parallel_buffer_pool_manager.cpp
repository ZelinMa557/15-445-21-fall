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

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager):
                                                     num_instance(num_instances), 
                                                     pool_size_(pool_size),
                                                     starting_index(0),
                                                     disk_manager_(disk_manager),
                                                     log_manager_(log_manager),
                                                     buffer_pool_managers(num_instances) {
  // Allocate and create individual BufferPoolManagerInstances
  for(size_t i = 0; i < num_instance; i++)
    buffer_pool_managers[i] = new BufferPoolManagerInstance(pool_size_, num_instance, i, disk_manager_, log_manager_);
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for(auto buffer_pool_manager: buffer_pool_managers)
    delete buffer_pool_manager;
};

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return num_instance*pool_size_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return buffer_pool_managers[page_id % num_instance];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager = GetBufferPoolManager(page_id);
  return manager->FetchPage(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager = GetBufferPoolManager(page_id);
  return manager->UnpinPage(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager = GetBufferPoolManager(page_id);
  return manager->FlushPage(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  Page *result = nullptr;
  size_t i = starting_index;
  while(result == nullptr) {
    result = buffer_pool_managers[i]->NewPage(page_id);
    i = (i + 1) % num_instance;
    if(i == starting_index)
      break;
  }
  
  starting_index = (i + 1) % num_instance;
  return result;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager = GetBufferPoolManager(page_id);
  return manager->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for(auto &buffer_pool_manager: buffer_pool_managers)
    buffer_pool_manager->FlushAllPages();
}

}  // namespace bustub
