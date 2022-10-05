//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <vector>
namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), 
                                                                                           plan_(plan),
                                                                                           table_id(plan_->GetTableOid()),
                                                                                           table_info(exec_ctx_->GetCatalog()->GetTable(table_id)),
                                                                                           iterator(table_info->table_->Begin(exec_ctx_->GetTransaction())),
                                                                                           end(table_info->table_->End()) {

}

void SeqScanExecutor::Init() {
    out_col_id.clear();
    out_col_id.reserve(plan_->OutputSchema()->GetColumnCount());

    for(uint32_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
        auto col_name = plan_->OutputSchema()->GetColumn(i).GetName();
        auto idx = table_info->schema_.GetColIdx(col_name);
        out_col_id.push_back(idx);            
    }

    if(out_col_id.size() == table_info->schema_.GetColumnCount())
        projection = false;
    else
        projection = true;
    
    iterator = table_info->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    for(; iterator != end; ++iterator) {
        *tuple = *iterator;
        if(plan_->GetPredicate()) {
            if(plan_->GetPredicate()->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
                ++iterator;
                if(exec_ctx_->GetTransaction()!=nullptr) {
                    auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
                    switch (iso_level)
                    {
                        case IsolationLevel::READ_UNCOMMITTED: break;
                        case IsolationLevel::READ_COMMITTED:
                        case IsolationLevel::REPEATABLE_READ:
                        exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), iterator->GetRid());
                        break;
                    }
                }
                *rid = tuple->GetRid();

                if(projection) {
                    std::vector<Value> values;
                    values.reserve(out_col_id.size());
                    for(const auto& i : out_col_id)
                        values.push_back(tuple->GetValue(&(table_info->schema_), i));
                    *tuple = Tuple(values, plan_->OutputSchema());
                }

                if(exec_ctx_->GetTransaction()!=nullptr) {
                    auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
                    switch (iso_level)
                    {
                        case IsolationLevel::READ_UNCOMMITTED: break;
                        case IsolationLevel::READ_COMMITTED:
                            exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), iterator->GetRid());
                            break;
                        case IsolationLevel::REPEATABLE_READ: break;
                    }
                }
                return true;
            }
        }

        else {
            ++iterator;
            *rid = tuple->GetRid();
            if(exec_ctx_->GetTransaction()!=nullptr) {
                auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
                switch (iso_level)
                {
                    case IsolationLevel::READ_UNCOMMITTED: break;
                    case IsolationLevel::READ_COMMITTED:
                    case IsolationLevel::REPEATABLE_READ:
                    exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), iterator->GetRid());
                    break;
                }
            }
            if(projection) {
                std::vector<Value> values;
                values.reserve(out_col_id.size());
                for(const auto& i : out_col_id)
                    values.push_back(tuple->GetValue(&(table_info->schema_), i));
                *tuple = Tuple(values, plan_->OutputSchema());
            }

            if(exec_ctx_->GetTransaction()!=nullptr) {
                auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
                switch (iso_level)
                {
                    case IsolationLevel::READ_UNCOMMITTED: break;
                    case IsolationLevel::READ_COMMITTED:
                        exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), iterator->GetRid());
                        break;
                    case IsolationLevel::REPEATABLE_READ: break;
                }
            }
            return true;
        }
    }

    return false;
}

}  // namespace bustub
