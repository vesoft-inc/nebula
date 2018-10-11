/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "console/CliManager.h"
#include "client/cpp/GraphDbClient.h"

DEFINE_string(prompt, "vGraph >",
              "Default prompt for the command line interface");


namespace vesoft {
namespace vgraph {

const int32_t kMaxAuthInfoRetries = 3;
const int32_t kMaxUsernameLen = 16;
const int32_t kMaxPasswordLen = 24;
const int32_t kMaxCommandLineLen = 1024;


bool CliManager::connect(const std::string& addr,
                         uint16_t port,
                         const std::string& username,
                         const std::string& password) {
    char user[kMaxUsernameLen + 1];
    char pass[kMaxPasswordLen + 1];

    strncpy(user, username.c_str(), kMaxUsernameLen);
    user[kMaxUsernameLen] = '\0';
    strncpy(pass, password.c_str(), kMaxPasswordLen);
    pass[kMaxPasswordLen] = '\0';

    // Make sure username is not empty
    for (int32_t i = 0; i < kMaxAuthInfoRetries && !strlen(user); i++) {
        // Need to interactively get the username
        std::cout << "Username: ";
        std::cin.getline(user, kMaxUsernameLen);
        user[kMaxUsernameLen] = '\0';
    }
    if (!strlen(user)) {
        std::cout << "Authentication failed: "
                     "Need a valid username to authenticate\n\n";
        return false;
    }

    // Make sure password is not empty
    for (int32_t i = 0; i < kMaxAuthInfoRetries && !strlen(pass); i++) {
        // Need to interactively get the password
        std::cout << "Password: ";
        std::cin.getline(pass, kMaxPasswordLen);
        pass[kMaxPasswordLen] = '\0';
    }
    if (!strlen(pass)) {
        std::cout << "Authentication failed: "
                     "Need a valid password\n\n";
        return false;
    }

    addr_ = addr;
    port_ = port;
    username_ = user;

    auto client = std::make_unique<GraphDbClient>(addr_, port_);
    cpp2::ErrorCode res = client->connect(user, pass);
    if (res == cpp2::ErrorCode::SUCCEEDED) {
        std::cerr << "\nWelcome to vGraph (Version 0.1)\n\n";
        cmdProcessor_ = std::make_unique<CmdProcessor>(std::move(client));
        return true;
    } else {
        // There is an error
        std::cout << "Authentication failed\n";
        return false;
    }
}


void CliManager::batch(const std::string& filename) {
}


void CliManager::loop() {
    char cmd[kMaxCommandLineLen + 1];

    while (true) {
        // Print out prompt
        std::cout << FLAGS_prompt << " ";
        std::cin.getline(cmd, kMaxCommandLineLen);
        cmd[kMaxCommandLineLen] = '\0';

        auto trimmedCmd = folly::trimWhitespace(cmd);
        if (!trimmedCmd.empty() && !cmdProcessor_->process(trimmedCmd)) {
            break;
        }
    }

    std::cout << "\nBye!\n\n";
}

}  // namespace vgraph
}  // namespace vesoft
