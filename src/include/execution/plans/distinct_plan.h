//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_plan.h
//
// Identification: src/include/execution/plans/distinct_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_set>
#include <vector>

#include "execution/plans/abstract_plan.h"
#include "common/util/hash_util.h"

namespace bustub {
struct DistinctKey {
  std::vector<Value> vals;

  auto operator==(const DistinctKey &other) const -> bool {
    if(vals.size() != other.vals.size())
      return false;
    for(size_t i = 0; i < vals.size(); i++) {
      if(vals[i].CompareEquals(other.vals[i]) != CmpBool::CmpTrue)
        return false;
    }
    return true;
  }
};
}// namespace bustub

namespace std {
template<>
struct hash<bustub::DistinctKey> {
  auto operator()(const bustub::DistinctKey &key) const -> std::size_t {
    size_t curr_hash = 0;
    for(auto const &tmp_key : key.vals) {
      if(!tmp_key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&tmp_key));
      }
    }
    return curr_hash;
  }
};
}//namespace std

namespace bustub {

/**
 * Distinct removes duplicate rows from the output of a child node.
 */
class DistinctPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new DistinctPlanNode instance.
   * @param child The child plan from which tuples are obtained
   */
  DistinctPlanNode(const Schema *output_schema, const AbstractPlanNode *child)
      : AbstractPlanNode(output_schema, {child}) {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::Distinct; }

  /** @return The child plan node */
  auto GetChildPlan() const -> const AbstractPlanNode * {
    BUSTUB_ASSERT(GetChildren().size() == 1, "Distinct should have at most one child plan.");
    return GetChildAt(0);
  }
};
}  // namespace bustub
