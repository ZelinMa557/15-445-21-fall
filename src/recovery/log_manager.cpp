//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_manager.h"

namespace bustub {
/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when timeout or the log buffer is full or buffer
 * pool manager wants to force flush (it only happens when the flushed page has
 * a larger LSN than persistent LSN)
 *
 * This thread runs forever until system shutdown/StopFlushThread
 */
void LogManager::RunFlushThread() {
    if (enable_logging) return;
    enable_logging = true;
    flush_thread_ = new std::thread([&] {
        while(enable_logging) {
            std::unique_lock<std::mutex> latch(latch_);
            cv_.wait_for(latch, log_timeout, [&]{ return need_flush_.load(); });
            if (buffer_offset_ > 0) {
                std::swap(log_buffer_, flush_buffer_);
                memset(log_buffer_, 0, LOG_BUFFER_SIZE);
                disk_manager_->WriteLog(flush_buffer_, buffer_offset_);
                append_cv_.notify_all();
                SetPersistentLSN(GetNextLSN()-1);
            }
            need_flush_ = false;
        }
    });
}

/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
    if (!enable_logging) return;
    enable_logging = false;
    need_flush_ = true;
    flush_thread_->join();
    delete flush_thread_;
}

void LogManager::Flush(bool force) {
    std::unique_lock<std::mutex> lock(latch_);
    if(enable_logging) {
        if (force) {
            need_flush_ = true;
            cv_.notify_one();
            append_cv_.wait(lock, [&] {return !need_flush_.load();});
        }
        else {
            append_cv_.wait(lock);
        }
    }
}
/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
auto LogManager::AppendLogRecord(LogRecord *log_record) -> lsn_t {
    std::unique_lock<std::mutex> lock(latch_);
    if (buffer_offset_ + log_record->GetSize() >= LOG_BUFFER_SIZE) {
        need_flush_ = true;
        cv_.notify_one();
        append_cv_.wait(lock, [&] {
            return buffer_offset_ + log_record->GetSize() < LOG_BUFFER_SIZE;
            });
    }
    

    // First, serialize the record head
    log_record->lsn_ = next_lsn_++;
    write_to_buffer(log_record, 20);

    // Write other info to buffer
    switch (log_record->log_record_type_)
    {
    case LogRecordType::INSERT:
        write_to_buffer(&log_record->GetInsertRID(), sizeof(RID));
        serialize_to_buffer(log_record->GetInsertTuple());
        break;
    
    case LogRecordType::APPLYDELETE:
    case LogRecordType::MARKDELETE:
    case LogRecordType::ROLLBACKDELETE:
        write_to_buffer(&log_record->GetDeleteRID(), sizeof(RID));
        serialize_to_buffer(log_record->GetDeleteTuple());
        break;

    case LogRecordType::UPDATE:
        write_to_buffer(&log_record->GetUpdateRID(), sizeof(RID));
        serialize_to_buffer(log_record->GetOriginalTuple());
        serialize_to_buffer(log_record->GetUpdateTuple());
        break;
    
    case LogRecordType::NEWPAGE:
        write_to_buffer(&log_record->prev_page_id_, sizeof(page_id_t));
        write_to_buffer(&log_record->page_id_, sizeof(page_id_t));
    default:
        break;
    }

    return GetNextLSN()-1;
}

}  // namespace bustub
