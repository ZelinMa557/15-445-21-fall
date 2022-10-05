//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
        left_executor_->Init();
        Tuple tuple;
        RID rid = tuple.GetRid();
        while(left_executor_->Next(&tuple, &rid)) {
            Value val = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, plan_->GetLeftPlan()->OutputSchema());
            HashJoinKey key{val};
            std::vector<Value> vals;
            uint32_t col_num = plan_->GetLeftPlan()->OutputSchema()->GetColumnCount();
            vals.reserve(col_num);

            for(size_t i = 0; i < col_num; i++) {
                vals.emplace_back(tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(), i));
            }

            if(ht_.count(key) > 0)
                ht_[key].emplace_back(std::move(vals));
            else ht_.insert({key, {vals}});
        }
      }

void HashJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();
    cur_idx = -1;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if(cur_idx == -1 || uint32_t(cur_idx) >= ht_[cur_key].size()) {
        RID tmp = right_tuple.GetRid();
        if(!right_executor_->Next(&right_tuple, &tmp))
            return false;
        Value val = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, plan_->GetRightPlan()->OutputSchema());
        HashJoinKey key{val};
        if(ht_.count(key) == 0)
            return Next(tuple, rid);
        cur_key = key;
        cur_idx = 0;
    }

    std::vector<Value> vals;
    uint32_t col_num = GetOutputSchema()->GetColumnCount();
    vals.reserve(col_num);
    for(const auto& col: GetOutputSchema()->GetColumns()) {
        auto expr = reinterpret_cast<const ColumnValueExpression *>(col.GetExpr());
        Value val;
        if(expr->GetTupleIdx()==0)
            val = ht_[cur_key][cur_idx][expr->GetColIdx()];
        else
            val = right_tuple.GetValue(plan_->GetRightPlan()->OutputSchema(), expr->GetColIdx());
        vals.emplace_back(val);
    }

    *tuple = Tuple(vals, GetOutputSchema());
    cur_idx++;
    return true;
}

}  // namespace bustub
