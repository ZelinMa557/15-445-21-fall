//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_recovery.cpp
//
// Identification: src/recovery/log_recovery.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_recovery.h"

#include "storage/page/table_page.h"
#include <iostream>
namespace bustub {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
auto LogRecovery::DeserializeLogRecord(const char *data, LogRecord *log_record) -> bool { 
    if (data + LogRecord::HEADER_SIZE > log_buffer_ + LOG_BUFFER_SIZE)
        return false;
    memcpy(&log_record->size_, data, 4);
    memcpy(&log_record->lsn_, data + 4, 4);
    memcpy(&log_record->txn_id_, data + 8, 4);
    memcpy(&log_record->prev_lsn_, data + 12, 4);
    memcpy(&log_record->log_record_type_, data + 16, 4);
    if (log_record->GetSize() + data > log_buffer_ + LOG_BUFFER_SIZE || log_record->GetSize() <= 0)
        return false;
    
    data += LogRecord::HEADER_SIZE;
    switch (log_record->GetLogRecordType())
    {
    case LogRecordType::ABORT:
    case LogRecordType::BEGIN:
    case LogRecordType::COMMIT:
        break;
    
    case LogRecordType::INSERT:
        memcpy(&log_record->insert_rid_, data, sizeof(RID));
        data += sizeof(RID);
        log_record->insert_tuple_.DeserializeFrom(data);
        break;
    
    case LogRecordType::APPLYDELETE:
    case LogRecordType::MARKDELETE:
    case LogRecordType::ROLLBACKDELETE:
        memcpy(&log_record->GetDeleteRID(), data, sizeof(RID));
        data += sizeof(RID);
        log_record->delete_tuple_.DeserializeFrom(data);
        break;
    
    case LogRecordType::UPDATE:
        memcpy(&log_record->GetUpdateRID(), data, sizeof(RID));
        data += sizeof(RID);
        log_record->GetOriginalTuple().DeserializeFrom(data);
        data += log_record->GetOriginalTuple().GetLength();
        log_record->GetUpdateTuple().DeserializeFrom(data);
        break;
    
    case LogRecordType::NEWPAGE:
        memcpy(&log_record->prev_page_id_, data, sizeof(page_id_t));
        memcpy(&log_record->page_id_, data+4, sizeof(page_id_t));
        break;
    default:
        break;
    }
    std::cout<<log_record->ToString()<<std::endl;
    return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
    enable_logging = false;
    offset_ = 0;
    while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_ )) {
        int buffer_offset = 0;
        LogRecord record;
        while (DeserializeLogRecord(log_buffer_ + buffer_offset, &record)) {
            lsn_mapping_[record.GetLSN()] = buffer_offset + offset_;
            active_txn_[record.GetTxnId()] = record.GetLSN();
            buffer_offset += record.GetSize();
            switch (record.log_record_type_)
            {
            case LogRecordType::BEGIN:
                continue;
            case LogRecordType::COMMIT:
            case LogRecordType::ABORT:
                active_txn_.erase(record.txn_id_);
                continue;
            case LogRecordType::NEWPAGE: {
                auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(record.page_id_));
                bool need_redo = record.GetLSN() > page->GetLSN();
                if (need_redo) {
                    page->Init(record.page_id_, PAGE_SIZE, record.prev_page_id_, nullptr, nullptr);
                    page->SetLSN(record.GetLSN());
                    if (record.prev_page_id_ != INVALID_PAGE_ID) {
                        auto prev_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(record.prev_page_id_));
                        bool need_update = prev_page->GetNextPageId() != record.page_id_;
                        prev_page->SetNextPageId(record.page_id_);
                        buffer_pool_manager_->UnpinPage(record.prev_page_id_, need_update);
                    }   
                }
                buffer_pool_manager_->UnpinPage(record.page_id_, need_redo);
                break;
            }
            default:
                break;
            }

            auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(record.page_id_));
            bool need_redo = record.GetLSN() > page->GetLSN();
            if (need_redo) {
                RID rid;
                switch (record.log_record_type_)
                {
                case LogRecordType::INSERT:
                    rid = record.GetInsertRID();
                    page->InsertTuple(record.GetInsertTuple(), &rid, nullptr, nullptr, nullptr);
                    break;
                case LogRecordType::APPLYDELETE:
                    rid = record.GetDeleteRID();
                    page->ApplyDelete(rid, nullptr, nullptr);
                    break;
                case LogRecordType::MARKDELETE:
                    rid = record.GetDeleteRID();
                    page->MarkDelete(rid, nullptr,nullptr, nullptr);
                    break;
                case LogRecordType::ROLLBACKDELETE:
                    rid = record.GetDeleteRID();
                    page->RollbackDelete(rid, nullptr, nullptr);
                    break;
                case LogRecordType::UPDATE:
                    rid = record.GetUpdateRID();
                    page->UpdateTuple(record.GetUpdateTuple(), &record.GetOriginalTuple(), rid, nullptr, nullptr, nullptr);
                    break;
                default:
                    break;
                }
            }
            buffer_pool_manager_->UnpinPage(record.page_id_, need_redo);
        }
        offset_ += buffer_offset;
    }
    
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
    for (const auto &[txn, lsn]: active_txn_) {
        auto lsn_ = lsn;
        LogRecord record;
        while (lsn_ != INVALID_LSN) {
            disk_manager_->ReadLog(log_buffer_, PAGE_SIZE, lsn_mapping_[lsn_]);
            DeserializeLogRecord(log_buffer_, &record);
            assert(record.GetTxnId() == txn);
            switch (record.GetLogRecordType())
            {
            case LogRecordType::BEGIN:
                lsn_ = record.prev_lsn_;
                continue;
            case LogRecordType::ABORT:
            case LogRecordType::COMMIT:assert(false);
            case LogRecordType::NEWPAGE: {
                buffer_pool_manager_->DeletePage(record.page_id_);
                if (record.prev_page_id_ != INVALID_PAGE_ID) {
                    auto prevPage = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(record.prev_page_id_));
                    assert(prevPage != nullptr);
                    assert(prevPage->GetNextPageId() == record.page_id_);
                    prevPage->SetNextPageId(INVALID_PAGE_ID);
                    buffer_pool_manager_->UnpinPage(prevPage->GetPageId(), true);
                }
                lsn_ = record.GetPrevLSN();
                continue;
            }
            default:
                break;
            }

            TablePage * page;
            RID rid;
            switch (record.log_record_type_)
            {
            case LogRecordType::INSERT:
                rid = record.GetInsertRID();
                page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                page->ApplyDelete(rid, nullptr, nullptr);
                break;
            case LogRecordType::APPLYDELETE:
                rid = record.GetDeleteRID();
                page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                page->InsertTuple(record.GetDeleteTuple(), &rid, nullptr, nullptr, nullptr);
                break;
            case LogRecordType::MARKDELETE:
                rid = record.GetDeleteRID();
                page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                page->RollbackDelete(rid, nullptr, nullptr);
                break;
            case LogRecordType::ROLLBACKDELETE:
                rid = record.GetDeleteRID();
                page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                page->MarkDelete(rid, nullptr, nullptr, nullptr);
                break;
            case LogRecordType::UPDATE:
                rid = record.GetUpdateRID();
                page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                page->UpdateTuple(record.GetOriginalTuple(), &record.GetUpdateTuple(), rid, nullptr, nullptr, nullptr);
                break;
            default:
                assert(false);
                break;
            }
            lsn_ = record.prev_lsn_;
        }
        
    }
    active_txn_.clear();
    lsn_mapping_.clear();
}

}  // namespace bustub
