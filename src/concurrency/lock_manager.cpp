//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }

  if(!CheckShrinking(txn))
    return false;
  
  transaction_table_[txn->GetTransactionId()] = txn;

  if(lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }

  std::unique_lock<std::mutex> lock(latch_);
  auto &req_queue = lock_table_[rid];
  if(req_queue.state == RequestQueueState::WRITE || req_queue.state == RequestQueueState::UPGRADE) {
    PreventDeadLock(txn, req_queue);
    req_queue.cv_.wait(lock, [&req_queue, &txn] () ->bool {
      return txn->GetState() == TransactionState::ABORTED || 
      (req_queue.state != RequestQueueState::WRITE && req_queue.state != RequestQueueState::UPGRADE); 
    });
  }
  
  bool grantable = CheckAborted(txn);
  if(grantable) {
    txn->GetSharedLockSet()->emplace(rid);
    LockRequest req(txn->GetTransactionId(), LockMode::SHARED);
    lock_table_[rid].request_queue_.emplace_back(std::move(req));
    req_queue.state = RequestQueueState::READ;
  }

  return grantable;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  if(!CheckShrinking(txn))
    return false;

  transaction_table_[txn->GetTransactionId()] = txn;

  if(lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }

  auto &req_queue = lock_table_[rid];
  std::unique_lock<std::mutex> lock(latch_);
  if(req_queue.state != RequestQueueState::NOTHING) {
    PreventDeadLock(txn, req_queue);
    req_queue.cv_.wait(lock, [&req_queue, &txn] () ->bool {
      return txn->GetState() == TransactionState::ABORTED || req_queue.state == RequestQueueState::NOTHING; 
    });
  }

  bool grantable = CheckAborted(txn);
  if(grantable) {
    txn->GetExclusiveLockSet()->emplace(rid);
    LockRequest req(txn->GetTransactionId(), LockMode::EXCLUSIVE);
    lock_table_[rid].request_queue_.emplace_back(std::move(req));
    req_queue.state = RequestQueueState::WRITE;
  }

  return grantable;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  if(!CheckShrinking(txn))
    return false;

  transaction_table_[txn->GetTransactionId()] = txn;

  if(lock_table_.find(rid) == lock_table_.end()) {
    return false;
  }


  auto &queue = lock_table_[rid].request_queue_;
  for(auto it = queue.begin(); it != queue.end(); ++it) {
    if(it->txn_id_ == txn->GetTransactionId()){
      queue.erase(it);
      break;
    }
  }
  if(queue.size() == 0)
    lock_table_[rid].state = RequestQueueState::NOTHING;
  
  auto &req_queue = lock_table_[rid];
  std::unique_lock<std::mutex> lock(latch_);
  
  if(req_queue.state != RequestQueueState::NOTHING) {
    PreventDeadLock(txn, req_queue);
    req_queue.cv_.wait(lock, [&req_queue, &txn] () ->bool {
      return txn->GetState() == TransactionState::ABORTED || req_queue.state == RequestQueueState::NOTHING; 
    });
  }

  if(req_queue.state == RequestQueueState::UPGRADE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  bool grantable = CheckAborted(txn);
  if(grantable) {
    req_queue.state = RequestQueueState::UPGRADE;
    txn->GetSharedLockSet()->erase(rid);
    txn->GetExclusiveLockSet()->emplace(rid);
    LockRequest req(txn->GetTransactionId(), LockMode::EXCLUSIVE);
    lock_table_[rid].request_queue_.clear();
    lock_table_[rid].request_queue_.emplace_back(std::move(req));
    req_queue.state = RequestQueueState::WRITE;
  }

  return grantable;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  auto &queue = lock_table_[rid].request_queue_;

  auto it = queue.begin();
  for(; it != queue.end(); ++it) {
    if(it->txn_id_ == txn->GetTransactionId()){
      break;
    }
  }

  if(it == queue.end())
    return false;
  

  if(txn->GetState() == TransactionState::GROWING && !(txn->GetIsolationLevel()==IsolationLevel::READ_COMMITTED && it->lock_mode_==LockMode::SHARED))
    txn->SetState(TransactionState::SHRINKING);
  queue.erase(it);

  if(queue.size() == 0)
    lock_table_[rid].state = RequestQueueState::NOTHING;
  
  lock_table_[rid].cv_.notify_all();
  return true;
}

auto LockManager::CheckShrinking(Transaction *txn) -> bool {
  if(txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  return true;
}

auto LockManager::CheckAborted(Transaction *txn) -> bool {
  if(txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  return true;
}

void LockManager::PreventDeadLock(Transaction *txn , LockRequestQueue &req_queue) {
  auto it = req_queue.request_queue_.begin();
  std::vector<decltype(it)> to_del;
  for(; it != req_queue.request_queue_.end(); ++it) {
    if(it->txn_id_ > txn->GetTransactionId()) {
      to_del.push_back(it);
      transaction_table_[it->txn_id_]->SetState(TransactionState::ABORTED);
    }
  }
  for(const auto &item : to_del)
    req_queue.request_queue_.erase(item);
  
  if(req_queue.request_queue_.size() == 0)
    req_queue.state = RequestQueueState::NOTHING;
  else {
    it = req_queue.request_queue_.begin();
    if(it->lock_mode_ == LockMode::EXCLUSIVE)
      req_queue.state = RequestQueueState::WRITE;
    else req_queue.state = RequestQueueState::READ;
  }
}
}  // namespace bustub
