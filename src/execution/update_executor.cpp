//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)),
      table_indexes(exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_)) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
  
  if(!child_executor_->Next(tuple, rid)) 
    return false;

  Tuple update_tuple = *tuple;
  RID rid_ = *rid;

  bool exist_transaction_ = (exec_ctx_->GetTransaction()!=nullptr);

  if(exist_transaction_) {
      if(exec_ctx_->GetTransaction()->IsSharedLocked(*rid))
        exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), *rid);
      else exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid);
  }

  Tuple updated_tuple = GenerateUpdatedTuple(update_tuple);
  table_info_->table_->UpdateTuple(updated_tuple, rid_, exec_ctx_->GetTransaction());

  if(table_indexes.size() > 0) {
    for(auto index: table_indexes) {
      auto old_key = update_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      auto new_key = updated_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(old_key, rid_, exec_ctx_->GetTransaction());
      index->index_->InsertEntry(new_key, rid_, exec_ctx_->GetTransaction());
      if(exist_transaction_) {
        IndexWriteRecord iwr(rid_, table_info_->oid_, WType::UPDATE, updated_tuple, update_tuple, index->index_oid_, exec_ctx_->GetCatalog());
        exec_ctx_->GetTransaction()->AppendIndexWriteRecord(iwr);
      } 
    }
  }

  return true;
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
