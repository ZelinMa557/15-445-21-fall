//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/aggregate_value_expression.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
    child_->Init();
    Tuple tmp_tuple;
    RID tmp_rid = tmp_tuple.GetRid();
    while (child_->Next(&tmp_tuple, &tmp_rid)) {
      auto akey = MakeAggregateKey(&tmp_tuple); 
      auto aval = MakeAggregateValue(&tmp_tuple);
      aht_.InsertCombine(akey, aval);
    }
    aht_iterator_ = aht_.Begin();    
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End())
    return false;
  auto tmp_iterator_ = aht_iterator_;
  ++aht_iterator_;

  std::vector<Value> values;
  values.reserve(GetOutputSchema()->GetUnlinedColumnCount());
  for(const auto &col: GetOutputSchema()->GetColumns()) {
    auto agg_expr = reinterpret_cast<const AggregateValueExpression *>(col.GetExpr());
    values.push_back(agg_expr->EvaluateAggregate(tmp_iterator_.Key().group_bys_, tmp_iterator_.Val().aggregates_));
  }
  *tuple = Tuple(values, GetOutputSchema());
  if(plan_->GetHaving() != nullptr)
    if(!plan_->GetHaving()->EvaluateAggregate(tmp_iterator_.Key().group_bys_, tmp_iterator_.Val().aggregates_).GetAs<bool>())
      return Next(tuple, rid);
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
