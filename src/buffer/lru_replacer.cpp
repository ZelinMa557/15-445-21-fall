//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    capacity = num_pages;
    dummy_head = new ListNode(0);
    dummy_tail = new ListNode(0);
    dummy_head->next = dummy_tail;
    dummy_tail->prev = dummy_head;
}

LRUReplacer::~LRUReplacer() {
    ListNode *iter, *deleter;
    iter = deleter = dummy_head;
    while(iter) {
        deleter = iter;
        iter = iter->next;
        delete deleter;
    }
};

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
    lru_lock.lock();
    if(loc_list.size()==0) {
        lru_lock.unlock();
        return false;
    } 
    ListNode *victim_frame = dummy_head->next;
    *frame_id = victim_frame->frame_id;
    victim_frame->next->prev = dummy_head;
    dummy_head->next = victim_frame->next;
    delete victim_frame;
    loc_list.erase(*frame_id);
    lru_lock.unlock();
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    lru_lock.lock();
    if(loc_list.find(frame_id)!=loc_list.end()) {
        ListNode *pin_frame = loc_list[frame_id];
        pin_frame->prev->next = pin_frame->next;
        pin_frame->next->prev = pin_frame->prev;
        delete pin_frame;
        loc_list.erase(frame_id); 
    }
    lru_lock.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    lru_lock.lock();
    if(loc_list.find(frame_id)!=loc_list.end()) {
        lru_lock.unlock();
        return;
    }
    ListNode *new_node = new ListNode(frame_id);
    dummy_tail->prev->next = new_node;
    new_node->prev = dummy_tail->prev;
    new_node->next = dummy_tail;
    dummy_tail->prev = new_node;
    loc_list[frame_id] = new_node;
    lru_lock.unlock();
}

auto LRUReplacer::Size() -> size_t { return loc_list.size(); }

}  // namespace bustub
