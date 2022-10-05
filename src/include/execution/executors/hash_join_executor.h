//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <unordered_map>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
struct HashJoinKey {
  Value key_;
  bool operator==(const HashJoinKey &other) const { return key_.CompareEquals(other.key_) == CmpBool::CmpTrue; }
};
}

namespace std {
template<>
struct hash<bustub::HashJoinKey> {
  size_t operator()(const bustub::HashJoinKey &key) const { return bustub::HashUtil::HashValue(&key.key_); }
};
}

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** Left child executor */
  std::unique_ptr<AbstractExecutor> left_executor_;
  /** Right child executor */
  std::unique_ptr<AbstractExecutor> right_executor_;
  /** Hash table of the executor */
  std::unordered_map<HashJoinKey, std::vector<std::vector<Value>>> ht_;
  /** Key of the current bucket */
  HashJoinKey cur_key;
  /** Index of current bucket */
  int cur_idx;
  /** Current right tuple*/
  Tuple right_tuple;
};

}  // namespace bustub
