/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <readline/history.h>
#include "console/CmdProcessor.h"
#include "time/Duration.h"
#include "process/ProcessUtils.h"

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
                    int digits10 = std::numeric_limits<float>::digits10;
                    std::string fmtValue = folly::sformat("%.{}f", digits10);
                    GET_VALUE_WIDTH(float, single_precision, fmtValue.c_str());
                    if (genFmt) {
                        std::string fmt = folly::sformat(" %%-%ld.{}f |", digits10);
                        formats[idx] = folly::stringPrintf(fmt.c_str(), widths[idx]);
                    }
                    break;
                }
                case cpp2::ColumnValue::Type::double_precision: {
                    int digits10 = std::numeric_limits<double>::digits10;
                    const char *fmtValue = folly::sformat("%.{}lf", digits10).c_str();
                    GET_VALUE_WIDTH(double, double_precision, fmtValue);
                    if (genFmt) {
                        std::string fmt = folly::sformat(" %%-%ld.{}lf |", digits10);
                        formats[idx] = folly::stringPrintf(fmt.c_str(), widths[idx]);
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

void CmdProcessor::printTime() const {
    auto now = std::chrono::system_clock::now();
    std::time_t nowTime = std::chrono::system_clock::to_time_t(now);
    std::cout << std::ctime(&nowTime) << std::endl;
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
                      << " rows (Time spent: ";
        } else if (resp.get_rows()) {
            std::cout << "Empty set (Time spent: ";
        } else {
            std::cout << "Execution succeeded (Time spent: ";
        }
        if (resp.get_latency_in_us() < 1000 || dur.elapsedInUSec() < 1000) {
            std::cout << resp.get_latency_in_us() << "/"
                      << dur.elapsedInUSec() << " us)\n";
        } else if (resp.get_latency_in_us() < 1000000 || dur.elapsedInUSec() < 1000000) {
            std::cout << resp.get_latency_in_us() / 1000.0 << "/"
                      << dur.elapsedInUSec() / 1000.0 << " ms)\n";
        } else {
            std::cout << resp.get_latency_in_us() / 1000000.0 << "/"
                      << dur.elapsedInUSec() / 1000000.0 << " s)\n";
        }
        std::cout << std::endl;

        if (resp.__isset.warning_msg) {
            std::cout << "[WARNING]: " << *resp.get_warning_msg() << std::endl;
            std::cout << std::endl;
        }
   } else if (res == cpp2::ErrorCode::E_SYNTAX_ERROR) {
        std::cout << "[ERROR (" << static_cast<int32_t>(res) << ")]: "
                  << (resp.get_error_msg() == nullptr ? "" : *resp.get_error_msg()) << "\n";
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
    SCOPE_EXIT {
        printTime();
    };

    normalize(cmd);

    if (localCommandProcessor_->isLocalCommand(cmd)) {
        return localCommandProcessor_->process(cmd);
    } else if (cmd == "exit" || cmd == "quit" || cmd == "q") {
        return localCommandProcessor_->process(":exit");
    } else if (cmd == "history") {
        return localCommandProcessor_->process(":history");
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


CmdProcessor::LocalCommandProcessor::LocalCommandProcessor() {
    using std::placeholders::_1;
    commands_ = {
        {"exit",        std::bind(&LocalCommandProcessor::doExit, this, _1)},
        {"quit",        std::bind(&LocalCommandProcessor::doExit, this, _1)},
        {"sh",          std::bind(&LocalCommandProcessor::doShell, this, _1)},
        {"bash",        std::bind(&LocalCommandProcessor::doShell, this, _1)},
        {"history",     std::bind(&LocalCommandProcessor::doHistory, this, _1)},
    };
}


bool CmdProcessor::LocalCommandProcessor::doExit(folly::StringPiece) const {
    return false;
}


bool CmdProcessor::LocalCommandProcessor::doShell(folly::StringPiece args) const {
    auto ignored = ::system(args.toString().c_str());
    UNUSED(ignored);
    return true;
}


bool CmdProcessor::LocalCommandProcessor::doHistory(folly::StringPiece) const {
    auto **hists = ::history_list();
    for (auto i = 0; i < ::history_length; i++) {
        fprintf(stdout, "%s\n", hists[i]->line);
    }
    return true;
}


bool CmdProcessor::LocalCommandProcessor::isLocalCommand(folly::StringPiece command) const {
    if (command.empty()) {
        return false;
    }

    if (!command.startsWith(localCommandPrefix_)) {
        return false;
    }

    return true;
}


bool CmdProcessor::LocalCommandProcessor::process(folly::StringPiece command) const {
    folly::StringPiece name;
    folly::StringPiece args;

    command.removePrefix(localCommandPrefix_);

    auto pos = command.find_first_of(" \t");
    if (pos == std::string::npos) {
        name = command;
    } else {
        name = command.subpiece(0, pos);
        args = command.subpiece(pos);
    }

    for (auto &pair : commands_) {
        folly::StringPiece wholeName = pair.first;
        if (wholeName.startsWith(name)) {
            return pair.second(args);
        }
    }

    fprintf(stderr, "No such command: %s\n", name.toString().c_str());

    return true;
}

}  // namespace graph
}  // namespace nebula
