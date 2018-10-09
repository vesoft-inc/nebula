/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "console/CmdProcessor.h"
#include "time/Duration.h"

namespace vesoft {
namespace vgraph {

#define GET_VALUE_WIDTH(VT, FN, FMT) \
    VT val = col.get_ ## FN(); \
    size_t len = folly::stringPrintf(FMT, val).size(); \
    if (widths[idx] < len) { \
        widths[idx] = len; \
        genFmt = true; \
    }

void CmdProcessor::calColumnWidths(
        const cpp2::ExecutionResponse& resp,
        std::vector<size_t>& widths,
        std::vector<std::string>& formats) const {
    widths.clear();
    formats.clear();

    // Check column names first
    if (resp.get_colNames() != nullptr) {
        for (size_t i = 0; i < resp.get_colNames()->size(); i++) {
            widths.emplace_back(resp.get_colNames()->at(i).size());
        }
    }
    if (widths.size() == 0) {
        return;
    }

    std::vector<cpp2::ColumnValue::Type> types(
        widths.size(), cpp2::ColumnValue::Type::__EMPTY__);
    formats.resize(widths.size());
    // Then check data width
    for (auto& row : *(resp.get_data())) {
        size_t idx = 0;
        for (auto& col : row.get_row()) {
            CHECK(types[idx] == cpp2::ColumnValue::Type::__EMPTY__
                  || types[idx] == col.getType());
            bool genFmt = types[idx] == cpp2::ColumnValue::Type::__EMPTY__;
            if (types[idx] == cpp2::ColumnValue::Type::__EMPTY__) {
                types[idx] = col.getType();
            } else {
                CHECK_EQ(types[idx], col.getType());
            }

            switch (col.getType()) {
                case cpp2::ColumnValue::Type::__EMPTY__: {
                    break;
                }
                case cpp2::ColumnValue::Type::boolVal: {
                    // Enough to hold "false"
                    if (widths[idx] < 5UL) {
                        widths[idx] = 5UL;
                        genFmt = true;
                    }
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%%lds |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::intVal: {
                    GET_VALUE_WIDTH(int64_t, intVal, "%ld")
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%%ldld |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::idVal: {
                    // Enough to hold "0x{16 letters}"
                    if (widths[idx] < 18UL) {
                        widths[idx] = 18UL;
                        genFmt = true;
                    }
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%%ldX |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::floatVal: {
                    GET_VALUE_WIDTH(float, floatVal, "%f")
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%%ldf |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::doubleVal: {
                    GET_VALUE_WIDTH(double, doubleVal, "%lf")
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%%ldf |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::strVal: {
                    size_t len = col.get_strVal().size();
                    if (widths[idx] < len) {
                        widths[idx] = len;
                        genFmt = true;
                    }
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%%lds |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::tsVal: {
                    GET_VALUE_WIDTH(int64_t, tsVal, "%ld")
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%%ldld |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::yearVal: {
                    if (widths[idx] < 4UL) {
                        widths[idx] = 4UL;
                        genFmt = true;
                    }
                    if (genFmt) {
                        formats[idx] = folly::stringPrintf(" %%%ldd |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::monthVal: {
                    if (widths[idx] < 7UL) {
                        widths[idx] = 7UL;
                        genFmt = true;
                    }
                    if (genFmt) {
                        formats[idx] = folly::stringPrintf(" %%%ldd/%%02d |",
                                                           widths[idx] - 3);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::dateVal: {
                    if (widths[idx] < 10UL) {
                        widths[idx] = 10UL;
                        genFmt = true;
                    }
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%%ldd/%%02d/%%02d |",
                                                widths[idx] - 6);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::datetimeVal: {
                    if (widths[idx] < 26UL) {
                        widths[idx] = 26UL;
                        genFmt = true;
                    }
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%%ldd/%%02d/%%02d"
                                                " %%02d:%%02d:%%02d"
                                                ".%%03d%%03d |",
                                                widths[idx] - 22);
                    }
                    break;
                }
            }
            ++idx;
        }
    }
}
#undef GET_VALUE_WIDTH


void CmdProcessor::printResult(const cpp2::ExecutionResponse& resp) const {
    std::vector<size_t> widths;
    std::vector<std::string> formats;

    calColumnWidths(resp, widths, formats);

    if (widths.size() == 0) {
        return;
    }

    // Calculate the total width
    int32_t sum = 0;
    for (auto w : widths) {
        sum += w;
    }

    std::string headerLine(sum + 3 * widths.size() + 1, '=');
    std::string rowLine(sum + 3 * widths.size() + 1, '-');
    std::cout << headerLine << "\n|";

    printHeader(resp, widths);
    std::cout << headerLine << "\n";

    printData(resp, rowLine, widths, formats);
}


void CmdProcessor::printHeader(
        const cpp2::ExecutionResponse& resp,
        const std::vector<size_t>& widths) const {
    size_t idx = 0;
    if (resp.get_colNames() == nullptr) {
        return;
    }

    for (auto& cname : (*resp.get_colNames())) {
        std::string fmt = folly::stringPrintf(" %%%lds |", widths[idx]);
        std::cout << folly::stringPrintf(fmt.c_str(), cname.c_str());
    }
    std::cout << "\n";
}


#define PRINT_FIELD_VALUE(...) \
    std::cout << folly::stringPrintf(formats[cIdx].c_str(), __VA_ARGS__)

void CmdProcessor::printData(const cpp2::ExecutionResponse& resp,
                             const std::string& rowLine,
                             const std::vector<size_t>& widths,
                             const std::vector<std::string>& formats) const {
    if (resp.get_data() == nullptr) {
        return;
    }

    for (auto& row : (*resp.get_data())) {
        int32_t cIdx = 0;
        std::cout << "|";
        for (auto& col : row.get_row()) {
            switch (col.getType()) {
                case cpp2::ColumnValue::Type::__EMPTY__: {
                    std::string fmt = folly::stringPrintf(" %%%ldc |", widths[cIdx]);
                    std::cout << folly::stringPrintf(fmt.c_str(), ' ');
                    break;
                }
                case cpp2::ColumnValue::Type::boolVal: {
                    PRINT_FIELD_VALUE(col.get_boolVal() ? "true" : "false");
                    break;
                }
                case cpp2::ColumnValue::Type::intVal: {
                    PRINT_FIELD_VALUE(col.get_intVal());
                    break;
                }
                case cpp2::ColumnValue::Type::idVal: {
                    PRINT_FIELD_VALUE(col.get_idVal());
                    break;
                }
                case cpp2::ColumnValue::Type::floatVal: {
                    PRINT_FIELD_VALUE(col.get_floatVal());
                    break;
                }
                case cpp2::ColumnValue::Type::doubleVal: {
                    PRINT_FIELD_VALUE(col.get_doubleVal());
                    break;
                }
                case cpp2::ColumnValue::Type::strVal: {
                    PRINT_FIELD_VALUE(col.get_strVal().c_str());
                    break;
                }
                case cpp2::ColumnValue::Type::tsVal: {
                    PRINT_FIELD_VALUE(col.get_tsVal());
                    break;
                }
                case cpp2::ColumnValue::Type::yearVal: {
                    PRINT_FIELD_VALUE(col.get_yearVal());
                    break;
                }
                case cpp2::ColumnValue::Type::monthVal: {
                    cpp2::YearMonth month = col.get_monthVal();
                    PRINT_FIELD_VALUE(month.get_year(), month.get_month());
                    break;
                }
                case cpp2::ColumnValue::Type::dateVal: {
                    cpp2::Date date = col.get_dateVal();
                    PRINT_FIELD_VALUE(date.get_year(), date.get_month(), date.get_day());
                    break;
                }
                case cpp2::ColumnValue::Type::datetimeVal: {
                    cpp2::DateTime dt = col.get_datetimeVal();
                    PRINT_FIELD_VALUE(dt.get_year(),
                                      dt.get_month(),
                                      dt.get_day(),
                                      dt.get_hour(),
                                      dt.get_minute(),
                                      dt.get_second(),
                                      dt.get_millisec(),
                                      dt.get_microsec());
                    break;
                }
            }
            ++cIdx;
        }
        std::cout << "\n" << rowLine << "\n";
    }
}
#undef PRINT_FIELD_VALUE


bool CmdProcessor::processClientCmd(folly::StringPiece cmd,
                                    bool& readyToExit) {
    if (cmd == "exit") {
        readyToExit = true;
        return true;
    } else {
        readyToExit = false;
    }

    // TODO(sye) Check for all client commands

    return false;
}


void CmdProcessor::processServerCmd(folly::StringPiece cmd) {
    time::Duration dur;
    cpp2::ExecutionResponse resp;
    cpp2::ResultCode res = client_->execute(cmd, resp);
    if (res == cpp2::ResultCode::SUCCEEDED) {
        // Succeeded
        printResult(resp);
        if (resp.get_data() != nullptr) {
            std::cout << "Got " << resp.get_data()->size()
                      << " rows (Time spent: "
                      << resp.get_latencyInMs() << "/"
                      << dur.elapsedInMSec() << " ms)\n";
        } else {
            std::cout << "Execution succeeded (Time spent: "
                      << resp.get_latencyInMs() << "/"
                      << dur.elapsedInMSec() << " ms)\n";
        }
    } else {
        // TODO(sye) Need to print human-readable error strings
        auto msg = resp.get_errorMsg();
        std::cout << "[ERROR (" << static_cast<int32_t>(res)
                  << ")]: " << (msg != nullptr ? *msg : "")
                  << "\n";
    }
}


bool CmdProcessor::process(folly::StringPiece cmd) {
    bool exit;
    if (processClientCmd(cmd, exit)) {
        return !exit;
    }

    processServerCmd(cmd);

    return true;
}

}  // namespace vgraph
}  // namespace vesoft

