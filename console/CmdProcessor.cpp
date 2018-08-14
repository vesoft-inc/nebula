/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "console/CmdProcessor.h"
#include "time/Duration.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"

namespace vesoft {
namespace vgraph {

#define GET_VALUE_WIDTH(VT, FN, FMT) \
    VT val; \
    if (fieldIt->get ## FN (val) == ResultType::SUCCEEDED) { \
        int32_t len = folly::stringPrintf(FMT, val).size(); \
        if (widths[fieldIdx] < len) { \
            widths[fieldIdx] = len; \
        } \
    }

std::vector<int16_t> CmdProcessor::calColumnWidths(
        const RowSetReader* dataReader) const {
    std::vector<int16_t> widths;

    // Check column names first
    auto* schema = dataReader->schema();
    for (int i = 0; i < schema->getNumFields(0); i++) {
        widths.emplace_back(strlen(schema->getFieldName(i, 0)));
    }

    // TODO Then check data width
    auto rowIt = dataReader->begin();
    while (bool(rowIt)) {
        int32_t fieldIdx = 0;
        auto fieldIt = rowIt->begin();
        while (bool(fieldIt)) {
            switch (schema->getFieldType(fieldIdx, 0)->get_type()) {
                case cpp2::SupportedType::BOOL: {
                    // Enough to hold "false"
                    if (widths[fieldIdx] < 5) {
                        widths[fieldIdx] = 5;
                    }
                    break;
                }
                case cpp2::SupportedType::INT: {
                    GET_VALUE_WIDTH(int64_t, Int, "%d")
                    break;
                }
                case cpp2::SupportedType::FLOAT: {
                    GET_VALUE_WIDTH(float, Float, "%f")
                    break;
                }
                case cpp2::SupportedType::DOUBLE: {
                    GET_VALUE_WIDTH(double, Double, "%f")
                    break;
                }
                case cpp2::SupportedType::STRING: {
                    folly::StringPiece val;
                    if (fieldIt->getString(val) == ResultType::SUCCEEDED) {
                        if (widths[fieldIdx] < val.size()) {
                            widths[fieldIdx] = val.size();
                        }
                    }
                    break;
                }
                case cpp2::SupportedType::VID: {
                    // Enough to hold "0x{16 letters}"
                    if (widths[fieldIdx] < 18) {
                        widths[fieldIdx] = 18;
                    }
                    break;
                }
                default: {
                }
            }
            ++fieldIt;
            ++fieldIdx;
        }
        ++rowIt;
    }

    return std::move(widths);
}
#undef GET_VALUE_WIDTH


int32_t CmdProcessor::printResult(const RowSetReader* dataReader) const {
    std::vector<int16_t> widths = calColumnWidths(dataReader);
    int32_t sum = 0;
    for (auto w : widths) {
        sum += w;
    }

    std::string headerLine(sum + 3 * widths.size() + 1, '=');
    std::string rowLine(sum + 3 * widths.size() + 1, '-');
    std::cout << headerLine << "\n|";

    std::vector<std::string> formats = printHeader(dataReader, widths);
    std::cout << headerLine << "\n";

    int32_t numRows = printData(dataReader, rowLine, formats);
    return numRows;
}


std::vector<std::string> CmdProcessor::printHeader(
        const RowSetReader* dataReader,
        const std::vector<int16_t>& widths) const {
    std::vector<std::string> formats;

    auto* schema = dataReader->schema();
    for (int i = 0; i < schema->getNumFields(0); i++) {
        std::string fmt = folly::stringPrintf(" %%%ds |", widths[i]);
        std::cout << folly::stringPrintf(fmt.c_str(), schema->getFieldName(i, 0));
        switch (schema->getFieldType(i, 0)->get_type()) {
            case cpp2::SupportedType::BOOL: {
                formats.emplace_back(folly::stringPrintf(" %%%ds |", widths[i]));
                break;
            }
            case cpp2::SupportedType::INT: {
                formats.emplace_back(folly::stringPrintf(" %%%dld |", widths[i]));
                break;
            }
            case cpp2::SupportedType::FLOAT: {
                formats.emplace_back(folly::stringPrintf(" %%%df |", widths[i]));
                break;
            }
            case cpp2::SupportedType::DOUBLE: {
                formats.emplace_back(folly::stringPrintf(" %%%df |", widths[i]));
                break;
            }
            case cpp2::SupportedType::STRING: {
                formats.emplace_back(folly::stringPrintf(" %%%ds |", widths[i]));
                break;
            }
            case cpp2::SupportedType::VID: {
                formats.emplace_back(folly::stringPrintf(" %%%sX |", widths[i]));
                break;
            }
            default: {
                formats.emplace_back(" *ERROR* |");
            }
        }
    }
    std::cout << "\n";

    return std::move(formats);
}


#define PRINT_FIELD_VALUE(VT, FN, VAL) \
    VT val; \
    if (fIt->get ## FN (val) == ResultType::SUCCEEDED) { \
        std::cout << folly::stringPrintf(formats[fIdx].c_str(), (VAL)); \
    } else { \
        std::cout << " *BAD* |"; \
    }

int32_t CmdProcessor::printData(const RowSetReader* dataReader,
                                const std::string& rowLine,
                                const std::vector<std::string>& formats) const {
    int32_t numRows = 0;

    auto* schema = dataReader->schema();
    auto rowIt = dataReader->begin();
    while (bool(rowIt)) {
        int32_t fIdx = 0;
        auto fIt = rowIt->begin();
        std::cout << "|";
        while (bool(fIt)) {
            switch (schema->getFieldType(fIdx, 0)->get_type()) {
                case cpp2::SupportedType::BOOL: {
                    PRINT_FIELD_VALUE(bool, Bool, (val ? "true" : "false"))
                    break;
                }
                case cpp2::SupportedType::INT: {
                    PRINT_FIELD_VALUE(int64_t, Int, val)
                    break;
                }
                case cpp2::SupportedType::FLOAT: {
                    PRINT_FIELD_VALUE(float, Float, val)
                    break;
                }
                case cpp2::SupportedType::DOUBLE: {
                    PRINT_FIELD_VALUE(double, Double, val)
                    break;
                }
                case cpp2::SupportedType::STRING: {
                    PRINT_FIELD_VALUE(folly::StringPiece, String, val.toString().c_str())
                    break;
                }
                case cpp2::SupportedType::VID: {
                    PRINT_FIELD_VALUE(int64_t, Vid, val)
                    break;
                }
                default: {
                    std::cout << " *ERROR* |";
                }
            }
            ++fIt;
            ++fIdx;
        }
        std::cout << "\n" << rowLine << "\n";
        ++numRows;
        ++rowIt;
    }

    return numRows;
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
    std::unique_ptr<RowSetReader> dataReader;
    int32_t res = client_->execute(cmd, dataReader);
    if (!res) {
        // Succeeded
        auto* schema = dataReader->schema();
        int32_t numRows = 0;
        if (dataReader->schema()) {
            // Only print when the schema is no empty
            numRows = printResult(dataReader.get());
        }
        std::cout << "Got " << numRows << " rows (Time spent: "
                  << client_->getServerLatency() << "/"
                  << dur.elapsedInMSec() << " ms)\n";
    } else {
        std::cout << "[ERROR (" << res << ")]  "
                  << client_->getErrorStr() << "\n";
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

