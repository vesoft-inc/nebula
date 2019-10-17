/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "console/CmdProcessor.h"
#include "time/Duration.h"

namespace nebula {
namespace graph {

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
    if (resp.get_column_names() != nullptr) {
        for (size_t i = 0; i < resp.get_column_names()->size(); i++) {
            widths.emplace_back(resp.get_column_names()->at(i).size());
        }
    }
    if (widths.size() == 0) {
        return;
    }

    if (resp.get_rows() == nullptr) {
        return;
    }

    std::vector<cpp2::ColumnValue::Type> types(
        widths.size(), cpp2::ColumnValue::Type::__EMPTY__);
    formats.resize(widths.size());
    // Then check data width
    for (auto& row : *(resp.get_rows())) {
        size_t idx = 0;
        for (auto& col : row.get_columns()) {
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
                case cpp2::ColumnValue::Type::bool_val: {
                    // Enough to hold "false"
                    if (widths[idx] < 5UL) {
                        widths[idx] = 5UL;
                        genFmt = true;
                    }
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%-%lds |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::integer: {
                    GET_VALUE_WIDTH(int64_t, integer, "%ld")
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%-%ldld |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::id: {
                    GET_VALUE_WIDTH(int64_t, id, "%ld");

                    if (genFmt) {
                        formats[idx] = folly::stringPrintf(" %%-%ldld |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::single_precision: {
                    GET_VALUE_WIDTH(float, single_precision, "%f")
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%-%ldf |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::double_precision: {
                    GET_VALUE_WIDTH(double, double_precision, "%lf")
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%-%ldlf |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::str: {
                    size_t len = col.get_str().size();
                    if (widths[idx] < len) {
                        widths[idx] = len;
                        genFmt = true;
                    }
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%-%lds |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::timestamp: {
                    if (widths[idx] < 19) {
                        widths[idx] = 19;
                        genFmt = true;
                    }
                    if (genFmt) {
                        formats[idx] =
                            folly::stringPrintf(" %%%ldd-%%02d-%%02d"
                                                " %%02d:%%02d:%%02d |",
                                                widths[idx] - 15);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::year: {
                    if (widths[idx] < 4UL) {
                        widths[idx] = 4UL;
                        genFmt = true;
                    }
                    if (genFmt) {
                        formats[idx] = folly::stringPrintf(" %%-%ldd |", widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::month: {
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
                case cpp2::ColumnValue::Type::date: {
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
                case cpp2::ColumnValue::Type::datetime: {
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
                case cpp2::ColumnValue::Type::path: {
                    auto pathValue = col.get_path();
                    auto entryList = pathValue.get_entry_list();
                    decltype(entryList.size()) entryIdx = 0;
                    formats.resize(entryList.size(), "");
                    widths.resize(entryList.size(), 0);
                    for (auto &entry : entryList) {
                        if (entry.getType() ==  cpp2::PathEntry::vertex) {
                            auto v = entry.get_vertex();
                            auto id = v.get_id();
                            size_t idLen = folly::stringPrintf("%ld", id).size();
                            if (widths[entryIdx] < idLen) {
                                widths[entryIdx] = idLen;
                                genFmt = true;
                            }
                            if (genFmt) {
                                formats[entryIdx] =
                                        folly::stringPrintf(" %%%ldld", idLen);
                            }
                        }
                        if (entry.getType() == cpp2::PathEntry::edge) {
                            auto e = entry.get_edge();
                            auto type = e.get_type();
                            auto ranking = e.get_ranking();
                            size_t typeLen = folly::stringPrintf("%s", type.c_str()).size();
                            size_t rankingLen = folly::stringPrintf("%ld", ranking).size();
                            size_t len = typeLen + rankingLen + 4;
                            if (widths[entryIdx] < len) {
                                widths[entryIdx] = len;
                                genFmt = true;
                            }
                            if (genFmt) {
                                formats[entryIdx] =
                                    folly::stringPrintf(" <%%%lds,%%%ldld>", typeLen, rankingLen);
                            }
                        }
                        ++entryIdx;
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
    if (resp.get_column_names() == nullptr) {
        return;
    }

    size_t idx = 0;
    for (auto& cname : (*resp.get_column_names())) {
        std::string fmt = folly::stringPrintf(" %%-%lds |", widths[idx++]);
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
    if (resp.get_rows() == nullptr) {
        return;
    }

    for (auto& row : (*resp.get_rows())) {
        int32_t cIdx = 0;
        std::cout << "|";
        for (auto& col : row.get_columns()) {
            switch (col.getType()) {
                case cpp2::ColumnValue::Type::__EMPTY__: {
                    std::string fmt = folly::stringPrintf(" %%-%ldc |", widths[cIdx]);
                    std::cout << folly::stringPrintf(fmt.c_str(), ' ');
                    break;
                }
                case cpp2::ColumnValue::Type::bool_val: {
                    PRINT_FIELD_VALUE(col.get_bool_val() ? "true" : "false");
                    break;
                }
                case cpp2::ColumnValue::Type::integer: {
                    PRINT_FIELD_VALUE(col.get_integer());
                    break;
                }
                case cpp2::ColumnValue::Type::id: {
                    PRINT_FIELD_VALUE(col.get_id());
                    break;
                }
                case cpp2::ColumnValue::Type::single_precision: {
                    PRINT_FIELD_VALUE(col.get_single_precision());
                    break;
                }
                case cpp2::ColumnValue::Type::double_precision: {
                    PRINT_FIELD_VALUE(col.get_double_precision());
                    break;
                }
                case cpp2::ColumnValue::Type::str: {
                    PRINT_FIELD_VALUE(col.get_str().c_str());
                    break;
                }
                case cpp2::ColumnValue::Type::timestamp: {
                    time_t timestamp = col.get_timestamp();
                    struct tm time;
                    if (nullptr == localtime_r(&timestamp, &time)) {
                        time.tm_year = 1970;
                        time.tm_mon = 1;
                        time.tm_mday = 1;
                        time.tm_hour = 0;
                        time.tm_min = 0;
                        time.tm_sec = 0;
                    }
                    PRINT_FIELD_VALUE(time.tm_year + 1900,
                                      time.tm_mon + 1,
                                      time.tm_mday,
                                      time.tm_hour,
                                      time.tm_min,
                                      time.tm_sec);
                    break;
                }
                case cpp2::ColumnValue::Type::year: {
                    PRINT_FIELD_VALUE(col.get_year());
                    break;
                }
                case cpp2::ColumnValue::Type::month: {
                    cpp2::YearMonth month = col.get_month();
                    PRINT_FIELD_VALUE(month.get_year(), month.get_month());
                    break;
                }
                case cpp2::ColumnValue::Type::date: {
                    cpp2::Date date = col.get_date();
                    PRINT_FIELD_VALUE(date.get_year(), date.get_month(), date.get_day());
                    break;
                }
                case cpp2::ColumnValue::Type::datetime: {
                    cpp2::DateTime dt = col.get_datetime();
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
                case cpp2::ColumnValue::Type::path: {
                    auto pathValue = col.get_path();
                    auto entryList = pathValue.get_entry_list();
                    cIdx = 0;
                    for (auto &entry : entryList) {
                        if (entry.getType() == cpp2::PathEntry::vertex) {
                            PRINT_FIELD_VALUE(entry.get_vertex().get_id());
                        }
                        if (entry.getType() == cpp2::PathEntry::edge) {
                            auto e = entry.get_edge();
                            PRINT_FIELD_VALUE(e.get_type().c_str(), e.get_ranking());
                        }
                        ++cIdx;
                    }
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
    normalize(cmd);
    if (cmd == "exit" || cmd== "quit") {
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
    cpp2::ErrorCode res = client_->execute(cmd, resp);
    if (res == cpp2::ErrorCode::SUCCEEDED) {
        // Succeeded
        auto *spaceName = resp.get_space_name();
        if (spaceName && !spaceName->empty()) {
            curSpaceName_ = std::move(*spaceName);
        } else {
            curSpaceName_ = "(none)";
        }
        if (resp.get_rows() && !resp.get_rows()->empty()) {
            printResult(resp);
            std::cout << "Got " << resp.get_rows()->size()
                      << " rows (Time spent: "
                      << resp.get_latency_in_us() << "/"
                      << dur.elapsedInUSec() << " us)\n";
        } else if (resp.get_rows()) {
            std::cout << "Empty set (Time spent: "
                      << resp.get_latency_in_us() << "/"
                      << dur.elapsedInUSec() << " us)\n";
        } else {
            std::cout << "Execution succeeded (Time spent: "
                      << resp.get_latency_in_us() << "/"
                      << dur.elapsedInUSec() << " us)\n";
        }
        std::cout << std::endl;
    } else if (res == cpp2::ErrorCode::E_SYNTAX_ERROR) {
        static const std::regex range("at 1.([0-9]+)-([0-9]+)");
        static const std::regex single("at 1.([0-9]+)");
        std::smatch result;
        auto *msg = resp.get_error_msg();
        auto verbose = *msg;
        std::string headMsg = "syntax error near `";
        auto pos = msg->find("at 1.");
        if (pos != msg->npos) {
            headMsg = msg->substr(0, pos) + "near `";
            headMsg.replace(headMsg.find("SyntaxError:"), sizeof("SyntaxError:"), "");
        }
        if (std::regex_search(*msg, result, range)) {
            auto start = folly::to<size_t>(result[1].str());
            auto end = folly::to<size_t>(result[2].str());
            verbose = headMsg + std::string(&cmd[start-1], end - start + 1) + "'";
        } else if (std::regex_search(*msg, result, single)) {
            auto start = folly::to<size_t>(result[1].str());
            auto end = start + 8;
            end = end > cmd.size() ? cmd.size() : end;
            verbose = headMsg + std::string(&cmd[start-1], end - start + 1) + "'";
        }
        std::cout << "[ERROR (" << static_cast<int32_t>(res)
                  << ")]: " << verbose << "\n";
    } else if (res == cpp2::ErrorCode::E_STATEMENT_EMTPY) {
        return;
    } else {
        // TODO(sye) Need to print human-readable error strings
        auto msg = resp.get_error_msg();
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

void CmdProcessor::normalize(folly::StringPiece &command) {
    command  = folly::trimWhitespace(command);
    while (command.endsWith(";")) {
        command = command.subpiece(0, command.size() - 1);
        command = folly::trimWhitespace(command);
    }
}


const std::string& CmdProcessor::getSpaceName() const {
    return curSpaceName_;
}

}  // namespace graph
}  // namespace nebula
