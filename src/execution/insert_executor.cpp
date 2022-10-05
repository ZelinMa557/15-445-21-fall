//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {
        auto table_id = plan_->TableOid();
        table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);
        table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      }

void InsertExecutor::Init() {
    if(!plan_->IsRawInsert())
        child_executor_->Init();
    else
        current_index = 0;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    bool inserted = false;
    Tuple insert_tuple;
    RID rid_ = insert_tuple.GetRid();

    if(plan_->IsRawInsert()) {
        if(current_index != plan_->RawValues().size()) {
            insert_tuple = Tuple(plan_->RawValuesAt(current_index++), &(table_info_->schema_));
            inserted = true;
        }
    }
    else {
        inserted = child_executor_->Next(&insert_tuple, &rid_);
    }

    if(inserted) {
        inserted = table_info_->table_->InsertTuple(insert_tuple, &rid_, exec_ctx_->GetTransaction());
        if(exec_ctx_->GetTransaction()!=nullptr) {
            exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), rid_);
        }
    }
    
    
    if(inserted && table_indexes.size() > 0) {
        for(const auto index: table_indexes) {
            const auto key =
                insert_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
            index->index_->InsertEntry(key, rid_, exec_ctx_->GetTransaction());
            if(exec_ctx_->GetTransaction()!=nullptr) {
                Tuple empty_old_tuple{};
                IndexWriteRecord iwr(rid_, table_info_->oid_, WType::INSERT, insert_tuple, empty_old_tuple, index->index_oid_, exec_ctx_->GetCatalog());
                exec_ctx_->GetTransaction()->AppendIndexWriteRecord(iwr);
            }
        }
    }
    return inserted;
}

}  // namespace bustub
