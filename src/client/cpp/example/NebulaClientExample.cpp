/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <unistd.h>
#include <thread>
#include <string.h>
#include <iomanip>
#include <glog/logging.h>
#include "nebula/NebulaClient.h"
#include "nebula/ExecutionResponse.h"

void doExecute(nebula::NebulaClient* client, std::string stmt) {
    nebula::ExecutionResponse resp;
    auto code = client->execute(stmt, resp);
    if (code != nebula::kSucceed) {
        std::string error = "Execute cmd:\"" + stmt + "\" failed. errorCode: "
                            + std::to_string(code) + ", errorMsg: " + resp.getErrorMsg();
        throw std::runtime_error(std::move(error));
    }
}

void printResult(nebula::ExecutionResponse &resp) {
    // print column name
    for (auto &name : resp.getColumnNames()) {
        std::cout << std::setw(10) << std::left << name << "|";
    }
    std::cout << std::endl;
    std::cout << "---------------------------------" << std::endl;
    // print column value
    for (auto &row : resp.getRows()) {
        for (auto& col : row.getColumns()) {
            std::cout << std::setw(10) << std::left;
            switch (col.getType()) {
                case nebula::kEmptyType:
                    std::cout << "Empty type";
                    break;
                case nebula::kBoolType:
                    std::cout << col.getBoolValue();
                    break;
                case nebula::kIntType:
                    std::cout << col.getIntValue();
                    break;
                case nebula::kIdType:
                    std::cout << col.getIdValue();
                    break;
                case nebula::kDoubleType:
                    std::cout << col.getDoubleValue();
                    break;
                case nebula::kStringType:
                    std::cout << col.getStrValue();
                    break;
                case nebula::kTimestampType:
                    std::cout << col.getTimestampValue();
                    break;
                case nebula::kPathType:
                default:
                    std::cout << "Unknown type: " << col.getType();
                    break;
            }
            std::cout << "|";
        }
        std::cout << std::endl;
    }
    std::cout << std::endl;
}

int main(int argc, char *argv[]) {
    // must call: init arvs
    nebula::NebulaClient::init(argc, argv);

    google::SetStderrLogging(google::ERROR);

    // must call: init connection pool
    nebula::ConnectionInfo connectionInfo;
    connectionInfo.addr = "127.0.0.1";
    connectionInfo.port = 3699;
    connectionInfo.connectionNum = 2;
    connectionInfo.timeout = 2000;
    std::vector<nebula::ConnectionInfo> connVec(1);
    connVec.emplace_back(connectionInfo);
    connectionInfo.port = 3700;
    connVec.emplace_back(connectionInfo);
    nebula::NebulaClient::initConnectionPool(connVec);

    auto fun = [&] (const std::string &name, const std::string &addr, const uint32_t port) {
        std::this_thread::sleep_for(std::chrono::seconds(atoi(name.c_str())));
        std::cout << name << ": service " << addr << ":" << port << std::endl;
        try {
            nebula::NebulaClient client(addr, port);
            auto code = client.authenticate("user", "password");
            if (code != nebula::kSucceed) {
                std::cout << "Connect " << addr << ":" << port
                          << " failed, code: " << code << std::endl;
                return;
            }

            // async execute cmd: show spaces
            auto cb = [&] (nebula::ExecutionResponse* response, nebula::ErrorCode resultCode) {
                if (resultCode == nebula::kSucceed) {
                    std::cout << "Async do cmd \" SHOW SPACES \" succeed" << std::endl;
                } else {
                    std::cout << "Async do cmd \" SHOW SPACE \" failed" << std::endl;
                }
            };

            // async execute succeed
            client.asyncExecute("SHOW SPACES", cb);

            // async execute failed
            client.asyncExecute("SHOW SPACE", cb);
            sleep(1);

            std::string spaceName = "test" + name;
            nebula::ExecutionResponse resp;

            client.execute("DROP SPACE " + spaceName, resp);
            doExecute(&client, "CREATE SPACE " + spaceName);
            doExecute(&client, "USE " + spaceName);
            std::string cmd = "CREATE TAG person(name string, age int);"
                              "CREATE EDGE like(likeness double);";
            // create schema
            doExecute(&client, cmd);
            sleep(3);
            doExecute(&client, "INSERT VERTEX person(name, age) VALUES 1:(\'Bob\', 10)");
            doExecute(&client, "INSERT VERTEX person(name, age) VALUES 2:(\'Lily\', 9)");
            doExecute(&client, "INSERT VERTEX person(name, age) VALUES 3:(\'Tom\', 10)");
            doExecute(&client, "INSERT EDGE like(likeness) VALUES 1->2:(80.0)");
            doExecute(&client, "INSERT EDGE like(likeness) VALUES 1->3:(70.0)");

            // query
            std::string query = "GO FROM 1 OVER like YIELD $$.person.name as name, "
                                "$$.person.age as age, like.likeness as likeness";
            code = client.execute(query, resp);
            if (code != nebula::kSucceed) {
                std::cout << "Execute cmd:\"" << query << "\" failed" << std::endl;
                return;
            }

            std::cout << "====== thread" << name << " query result ======"
                      << std::endl << std::endl;
            printResult(resp);
        } catch (const std::exception& e) {
            std::cout << "execute error:" << e.what() << std::endl;
            return;
        }
    };

    // Test multi threads
    std::thread thread1(fun, "1", "127.0.0.1", 3699);
    std::thread thread2(fun, "2", "127.0.0.1", 3700);

    sleep(2);

    thread1.join();
    thread2.join();
    return 0;
}
