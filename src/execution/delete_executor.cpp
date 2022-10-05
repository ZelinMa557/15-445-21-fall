//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)),
      table_indexes(exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    if(!child_executor_->Next(tuple, rid)) 
        return false;
    
    Tuple delete_tuple(*tuple);
    RID delete_rid(*rid);

    bool exist_transaction_ = (exec_ctx_->GetTransaction()!=nullptr);

    if(exist_transaction_) {
        if(exec_ctx_->GetTransaction()->IsSharedLocked(*rid))
            exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), delete_rid);
        else exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), delete_rid);
    }


    table_info_->table_->MarkDelete(delete_rid, exec_ctx_->GetTransaction());


    if(table_indexes.size() > 0) {
        for(auto index: table_indexes) {
            auto delete_key = delete_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
            index->index_->DeleteEntry(delete_key, delete_rid, exec_ctx_->GetTransaction());
            if(exist_transaction_) {
                Tuple empty_new_tuple{};
                IndexWriteRecord iwr(delete_rid, table_info_->oid_, WType::DELETE, empty_new_tuple, delete_tuple, index->index_oid_, exec_ctx_->GetCatalog());
                exec_ctx_->GetTransaction()->AppendIndexWriteRecord(iwr);
            } 
        }
    }

    return true;
}

}  // namespace bustub
