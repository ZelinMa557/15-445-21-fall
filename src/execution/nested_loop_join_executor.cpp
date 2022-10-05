//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <iostream>
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      outer_tuple_() {}

void NestedLoopJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();
    RID tmp;
    left_executor_->Next(&outer_tuple_, &tmp);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    Tuple inner_tuple;
    RID inner_rid = inner_tuple.GetRid();

    while (right_executor_->Next(&inner_tuple, &inner_rid)) {
        bool match;
        if(plan_->Predicate()!=nullptr)
            match = plan_->Predicate()->EvaluateJoin(&outer_tuple_, plan_->GetLeftPlan()->OutputSchema(),
                                            &inner_tuple, plan_->GetRightPlan()->OutputSchema()).GetAs<bool>();
        else match = true;
        if(match) {
            std::vector<Value> values;
            for(const auto& col: GetOutputSchema()->GetColumns()) {
                values.emplace_back(col.GetExpr()->EvaluateJoin(&outer_tuple_, plan_->GetLeftPlan()->OutputSchema(),
                                                                &inner_tuple, plan_->GetRightPlan()->OutputSchema()));
            }
            *tuple = Tuple(values, GetOutputSchema());
            return true;
        }
    }
    
    RID tmp;
    right_executor_->Init();
    bool have_next_outer = left_executor_->Next(&outer_tuple_, &tmp);
    if(have_next_outer)
        return Next(tuple, rid);
    else
        return false;
}

}  // namespace bustub
