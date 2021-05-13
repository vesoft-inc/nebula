/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/network/NetworkUtils.h"
#include "kvstore/plugins/hbase/HBaseClient.h"
#include <folly/io/async/EventBaseManager.h>
#include <thrift/lib/cpp2/async/RequestChannel.h>
#include <thrift/lib/cpp2/async/ReconnectingRequestChannel.h>
#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>


namespace nebula {
namespace kvstore {

const char* kColumnFamilyName = "cf";

HBaseClient::HBaseClient(const HostAddr& host) {
    clientsMan_ = std::make_shared<thrift::ThriftClientManager<
                                   THBaseServiceAsyncClient>>();
    auto evb = folly::EventBaseManager::get()->getEventBase();
    client_ = clientsMan_->client(host, evb, true);
}


HBaseClient::~HBaseClient() {
}


ResultCode HBaseClient::get(const std::string& tableName,
                            const std::string& rowKey,
                            KVMap& data) {
    TGet tGet;
    tGet.set_row(rowKey);
    std::vector<TColumn> tColumnList;
    TColumn tColumn;
    tColumn.set_family(kColumnFamilyName);
    tColumnList.emplace_back(tColumn);
    tGet.set_columns(tColumnList);

    TResult tResult;
    try {
        client_->sync_get(tResult, tableName, tGet);
        std::vector<TColumnValue> tColumnValueList = tResult.columnValues;
        if (tColumnValueList.size() > 0) {
            for (auto& cv : tColumnValueList) {
                data.emplace(cv.qualifier, cv.value);
            }
            return ResultCode::SUCCEEDED;
        } else {
            return ResultCode::ERR_KEY_NOT_FOUND;
        }
    } catch (const TIOError &ex) {
        LOG(ERROR) << "TIOError: " << ex.message;
        return ResultCode::ERR_IO_ERROR;
    } catch (const apache::thrift::transport::TTransportException &tte) {
        LOG(ERROR) << "TTransportException: " << tte.what();
        return ResultCode::ERR_IO_ERROR;
    }
    return ResultCode::E_UNKNOWN;
}


std::pair<ResultCode, std::vector<Status>> HBaseClient::multiGet(
        const std::string& tableName,
        const std::vector<std::string>& rowKeys,
        std::vector<std::pair<std::string, KVMap>>& dataList) {
    std::vector<TGet> tGetList;
    for (auto& rowKey : rowKeys) {
        TGet tGet;
        tGet.set_row(rowKey);
        std::vector<TColumn> tColumnList;
        TColumn tColumn;
        tColumn.set_family(kColumnFamilyName);
        tColumnList.emplace_back(tColumn);
        tGet.set_columns(tColumnList);
        tGetList.emplace_back(tGet);
    }

    std::vector<TResult> tResultList;
    std::vector<Status> status;
    ResultCode resultCode = ResultCode::SUCCEEDED;
    try {
        client_->sync_getMultiple(tResultList, tableName, tGetList);
        for (auto& tResult : tResultList) {
            std::vector<TColumnValue> tColumnValueList = tResult.columnValues;
            if (tColumnValueList.size() > 0) {
                std::string rowKey = tResult.row;
                KVMap data;
                for (auto& cv : tColumnValueList) {
                    data.emplace(cv.qualifier, cv.value);
                }
                dataList.emplace_back(std::make_pair(rowKey, std::move(data)));
                status.emplace_back(Status::OK());
            } else {
                resultCode = ResultCode::ERR_PARTIAL_RESULT;
                status.emplace_back(Status::KeyNotFound());
            }
        }
        return {resultCode, status};
    } catch (const TIOError &ex) {
        LOG(ERROR) << "TIOError: " << ex.message;
        return {ResultCode::ERR_IO_ERROR, status};
    } catch (const apache::thrift::transport::TTransportException &tte) {
        LOG(ERROR) << "TTransportException: " << tte.what();
        return {ResultCode::ERR_IO_ERROR, status};
    }
    return {ResultCode::E_UNKNOWN, status};
}


ResultCode HBaseClient::put(const std::string& tableName,
                            std::string& rowKey,
                            std::vector<KV>& data) {
    TPut tPut;
    tPut.set_row(rowKey);
    std::vector<TColumnValue> tColumnValueList;
    for (auto& kv : data) {
        TColumnValue tColumnValue;
        tColumnValue.set_family(kColumnFamilyName);
        tColumnValue.set_qualifier(kv.first);
        tColumnValue.set_value(kv.second);
        tColumnValueList.emplace_back(tColumnValue);
    }
    tPut.set_columnValues(tColumnValueList);

    try {
        client_->sync_put(tableName, tPut);
        return ResultCode::SUCCEEDED;
    } catch (const TIOError &ex) {
        LOG(ERROR) << "TIOError: " << ex.message;
        return ResultCode::ERR_IO_ERROR;
    } catch (const apache::thrift::transport::TTransportException &tte) {
        LOG(ERROR) << "TTransportException: " << tte.what();
        return ResultCode::ERR_IO_ERROR;
    }
    return ResultCode::E_UNKNOWN;
}


ResultCode HBaseClient::multiPut(const std::string& tableName,
                                 std::vector<std::pair<std::string, std::vector<KV>>>& dataList) {
    std::vector<TPut> tPutList;
    for (auto& data : dataList) {
        TPut tPut;
        tPut.set_row(data.first);
        auto kvs = data.second;
        std::vector<TColumnValue> tColumnValueList;
        for (auto& kv : kvs) {
            TColumnValue tColumnValue;
            tColumnValue.set_family(kColumnFamilyName);
            tColumnValue.set_qualifier(kv.first);
            tColumnValue.set_value(kv.second);
            tColumnValueList.emplace_back(tColumnValue);
        }
        tPut.set_columnValues(tColumnValueList);
        tPutList.emplace_back(tPut);
    }

    try {
        client_->sync_putMultiple(tableName, tPutList);
        return ResultCode::SUCCEEDED;
    } catch (const TIOError &ex) {
        LOG(ERROR) << "TIOError: " << ex.message;
        return ResultCode::ERR_IO_ERROR;
    } catch (const apache::thrift::transport::TTransportException &tte) {
        LOG(ERROR) << "TTransportException: " << tte.what();
        return ResultCode::ERR_IO_ERROR;
    }
    return ResultCode::E_UNKNOWN;
}


ResultCode HBaseClient::range(const std::string& tableName,
                              const std::string& startRowKey,
                              const std::string& endRowKey,
                              std::vector<std::pair<std::string, KVMap>>& dataList) {
    // TODO(zhangguoqing) This is a simple implementation that get all results immediately,
    // and in the future, it will use HBaseScanIter to improve performance.
    TScan tScan;
    tScan.set_startRow(startRowKey);
    tScan.set_stopRow(endRowKey);
    std::vector<TColumn> tColumnList;
    TColumn tColumn;
    tColumn.set_family(kColumnFamilyName);
    tColumnList.emplace_back(tColumn);
    tScan.set_columns(tColumnList);
    tScan.set_caching(kScanRowNum * kScanRowNum);

    int32_t scannerId = -1;
    try {
        scannerId = client_->sync_openScanner(tableName, tScan);
        while (true) {
            std::vector<TResult> tResultList;
            client_->sync_getScannerRows(tResultList, scannerId, kScanRowNum);
            if (tResultList.size() == 0) break;
            for (auto& tResult : tResultList) {
                std::vector<TColumnValue> tColumnValueList = tResult.columnValues;
                if (tColumnValueList.size() > 0) {
                    std::string rowKey = tResult.row;
                    KVMap data;
                    for (auto& cv : tColumnValueList) {
                        data.emplace(cv.qualifier, cv.value);
                    }
                    dataList.emplace_back(std::make_pair(rowKey, std::move(data)));
                }
            }
            tResultList.clear();
        }
        client_->sync_closeScanner(scannerId);
        return ResultCode::SUCCEEDED;
    } catch (const TIOError &ex) {
        if (scannerId >= 0) client_->sync_closeScanner(scannerId);
        LOG(ERROR) << "TIOError: " << ex.message;
        return ResultCode::ERR_IO_ERROR;
    } catch (const apache::thrift::transport::TTransportException &tte) {
        if (scannerId >= 0) client_->sync_closeScanner(scannerId);
        LOG(ERROR) << "TTransportException: " << tte.what();
        return ResultCode::ERR_IO_ERROR;
    }
    return ResultCode::E_UNKNOWN;
}


ResultCode HBaseClient::remove(const std::string& tableName,
                               const std::string& rowKey) {
    TDelete tDelete;
    tDelete.set_row(rowKey);
    TColumn tColumn;
    tColumn.set_family(kColumnFamilyName);
    std::vector<TColumn> tColumnList;
    tColumnList.emplace_back(tColumn);
    tDelete.set_columns(tColumnList);
    tDelete.set_durability(TDurability::ASYNC_WAL);

    try {
        client_->sync_deleteSingle(tableName, tDelete);
        return ResultCode::SUCCEEDED;
    } catch (const TIOError &ex) {
        LOG(ERROR) << "TIOError: " << ex.message;
        return ResultCode::ERR_IO_ERROR;
    } catch (const apache::thrift::transport::TTransportException &tte) {
        LOG(ERROR) << "TTransportException: " << tte.what();
        return ResultCode::ERR_IO_ERROR;
    }
    return ResultCode::E_UNKNOWN;
}

ResultCode HBaseClient::multiRemove(const std::string& tableName,
                                    std::vector<std::string>& rowKeys) {
    std::vector<TDelete> tDeleteList;
    for (auto& rowKey : rowKeys) {
        TDelete tDelete;
        tDelete.set_row(rowKey);
        TColumn tColumn;
        tColumn.set_family(kColumnFamilyName);
        std::vector<TColumn> tColumnList;
        tColumnList.emplace_back(tColumn);
        tDelete.set_columns(tColumnList);
        tDelete.set_durability(TDurability::ASYNC_WAL);
        tDeleteList.emplace_back(tDelete);
    }

    std::vector<TDelete> tDeleteResultList;
    try {
        client_->sync_deleteMultiple(tDeleteResultList, tableName, tDeleteList);
        return ResultCode::SUCCEEDED;
    } catch (const TIOError &ex) {
        LOG(ERROR) << "TIOError: " << ex.message;
        return ResultCode::ERR_IO_ERROR;
    } catch (const apache::thrift::transport::TTransportException &tte) {
        LOG(ERROR) << "TTransportException: " << tte.what();
        return ResultCode::ERR_IO_ERROR;
    }
    return ResultCode::E_UNKNOWN;
}

}  // namespace kvstore
}  // namespace nebula

