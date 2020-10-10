/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/Status.h"
#include <termios.h>
#include <unistd.h>
#include "readline/readline.h"
#include "readline/history.h"
#include "console/CliManager.h"
#include "client/cpp/GraphClient.h"
#include "fs/FileUtils.h"
#include "network/NetworkUtils.h"

DECLARE_string(addr);
DECLARE_int32(port);
DECLARE_string(u);
DECLARE_string(p);
DEFINE_bool(enable_history, false, "Whether to force saving the command history");

namespace nebula {
namespace graph {

CliManager::CliManager() {
    if (!fs::FileUtils::isStdinTTY()) {
        enableHistroy_ = false;
        isInteractive_ = false;
    }

    if (FLAGS_enable_history) {
        enableHistroy_ = true;
    }

    if (enableHistroy_) {
        ::using_history();
    }

    if (isInteractive_) {
        initAutoCompletion();
    }
}


bool CliManager::connect() {
    addr_ = FLAGS_addr;
    port_ = FLAGS_port;
    username_ = FLAGS_u;
    std::string passwd = FLAGS_p;

    IPv4 temp;
    if (!network::NetworkUtils::ipv4ToInt(addr_, temp)) {
        std::cout << "Invalid ip string\n";
        return false;
    }

    auto client = std::make_unique<GraphClient>(addr_, port_);
    cpp2::ErrorCode res = client->connect(username_, passwd);
    if (res == cpp2::ErrorCode::SUCCEEDED) {
#if defined(NEBULA_BUILD_VERSION)
        std::cerr << "\nWelcome to Nebula Graph (Version "
                  << NEBULA_STRINGIFY(NEBULA_BUILD_VERSION) << ")\n\n";
#else
        std::cerr << "\nWelcome to Nebula Graph\n\n";
#endif
        cmdProcessor_ = std::make_unique<CmdProcessor>(std::move(client));
        return true;
    } else {
        // There is an error
        std::cout << "Connection failed\n";
        return false;
    }
}


void CliManager::batch(const std::string& filename) {
    UNUSED(filename);
}


void CliManager::loop() {
    loadHistory();

    while (true) {
        std::string cmd;
        std::string line;
        auto quit = !this->readLine(line, false/*linebreak*/);
        // EOF
        if (quit) {
            break;
        }
        // Empty line
        if (line.empty()) {
            continue;
        }
        // Line break
        while (!quit && !line.empty() && line.back() == '\\') {
            line.resize(line.size() - 1);
            cmd += line;
            quit = !this->readLine(line, true/*linebreak*/);
            continue;
        }
        // EOF
        if (quit) {
            break;
        }
        // Execute the whole command
        cmd += line;
        if (!cmdProcessor_->process(cmd)) {
            break;
        }
    }

    saveHistory();
    fprintf(stderr, "Bye!\n");
}


bool CliManager::readLine(std::string &line, bool linebreak) {
    // Setup the prompt
    std::string prompt;
    static auto color = 0u;
    do {
        if (!isInteractive_) {
            break;
        }
        auto purePrompt = folly::stringPrintf("(%s@nebula) [%s]> ",
                                              username_.c_str(),
                                              cmdProcessor_->getSpaceName().c_str());
        if (linebreak) {
            purePrompt.assign(purePrompt.size() - 3, ' ');
            purePrompt += "-> ";
        } else {
            color++;
        }

        prompt = folly::stringPrintf(
                   "\001"              // RL_PROMPT_START_IGNORE
                   "\033[1;%um"        // color codes start
                   "\002"              // RL_PROMPT_END_IGNORE
                   "%s"                // prompt "(user@host:port) [spaceName]"
                   "\001"              // RL_PROMPT_START_IGNORE
                   "\033[0m"           // restore color code
                   "\002",             // RL_PROMPT_END_IGNORE
                   color % 6 + 31,
                   purePrompt.c_str());
    } while (false);

    // Read one line
    auto *input = ::readline(prompt.c_str());
    auto ok = true;
    do {
        // EOF
        if (input == nullptr) {
            fprintf(stdout, "\n");
            ok = false;
            break;
        }
        // Empty line
        if (input[0] == '\0') {
            line.clear();
            break;
        }
        line = folly::trimWhitespace(input).str();
        if (!line.empty()) {
            // Update command history
            updateHistory(input);
        }
    } while (false);

    ::free(input);

    return ok;
}


void CliManager::updateHistory(const char *line) {
    if (!enableHistroy_) {
        return;
    }
    auto **hists = ::history_list();
    auto i = 0;
    // Search in history
    for (; i < ::history_length; i++) {
        auto *hist = hists[i];
        if (::strcmp(line, hist->line) == 0) {
            break;
        }
    }
    // New command
    if (i == ::history_length) {
        ::add_history(line);
        return;
    }
    // Found in history, make it lastest
    auto *hist = hists[i];
    for (; i < ::history_length - 1; i++) {
        hists[i] = hists[i + 1];
    }
    hists[i] = hist;
}


void CliManager::saveHistory() {
    if (!enableHistroy_) {
        return;
    }
    std::string histfile;
    histfile += ::getenv("HOME");
    histfile += "/.nebula_history";
    auto *file = ::fopen(histfile.c_str(), "w+");
    if (file == nullptr) {
        return;     // fail silently
    }
    auto **hists = ::history_list();
    for (auto i = 0; i < ::history_length; i++) {
        fprintf(file, "%s\n", hists[i]->line);
    }
    ::fflush(file);
    ::fclose(file);
}


void CliManager::loadHistory() {
    if (!enableHistroy_) {
        return;
    }
    std::string histfile;
    histfile += ::getenv("HOME");
    histfile += "/.nebula_history";
    auto *file = ::fopen(histfile.c_str(), "r");
    if (file == nullptr) {
        return;     // fail silently
    }
    char *line = nullptr;
    size_t size = 0;
    ssize_t read = 0;
    while ((read = ::getline(&line, &size, file)) != -1) {
        line[read - 1] = '\0';  // remove the trailing newline
        updateHistory(line);
    }
    ::free(line);
    ::fclose(file);
}


struct StringCaseEqual {
    bool operator()(const std::string &lhs, const std::string &rhs) const {
        return ::strcasecmp(lhs.c_str(), rhs.c_str()) == 0;
    }
};


struct StringCaseHash {
    size_t operator()(const std::string &lhs) const {
        std::string upper;
        upper.resize(lhs.size());
        auto toupper = [] (auto c) { return ::toupper(c); };
        std::transform(lhs.begin(), lhs.end(), upper.begin(), toupper);
        return std::hash<std::string>()(upper);
    }
};

// Primary keywords, like `GO' `CREATE', etc.
static std::vector<std::string> primaryKeywords;

// Keywords along with their sub-keywords, like `SHOW': `TAGS', `SPACES'
static std::unordered_map<std::string, std::vector<std::string>,
                          StringCaseHash, StringCaseEqual> subKeywords;
// Typenames, like `int', `double', `string', etc.
static std::vector<std::string> typeNames;


// To fill the containers above from a json file.
static Status loadCompletions();
static Status parseKeywordsFromJson(const folly::dynamic &json);

// To retrieve matches from within the `primaryKeywords'
static std::vector<std::string>
matchFromPrimaryKeywords(const std::string &text);

// To retrieve matches from within the `subKeywords'
static std::vector<std::string> matchFromSubKeywords(const std::string &text,
                                                     const std::string &primaryKeyword);

// Given a collection of keywords, retrieve matches that prefixed with `text'
static std::vector<std::string> matchFromKeywords(const std::string &text,
                                                  const std::vector<std::string> &keywords);

// To tell if the current `text' is at the start position of a statement.
// If so, we should do completion with primary keywords.
// Otherwise, the primary keyword of the current statement
// will be set, thus we will do completion with its sub keywords.
static bool isStartOfStatement(std::string &primaryKeyword);

// Given the prefix and a collection of keywords, retrieve the longest common prefix
// e.g. given `u' as the prefix and [USE, USER, USERS] as the collection, will return `USE'
static auto longestCommonPrefix(std::string prefix,
                                const std::vector<std::string>& words);

// Callback by realine if an auto completion is triggered
static char** completer(const char *text, int start, int end);


auto longestCommonPrefix(std::string prefix,
                         const std::vector<std::string>& words) {
    if (words.size() == 1) {
        return words[0];
    }

    while (true) {
        char nextChar = 0;
        for (auto &word : words) {
            if (word.size() <= prefix.size()) {
                return word;
            }
            if (nextChar == 0) {
                nextChar = word[prefix.size()];
                continue;
            }
            if (::toupper(nextChar) != ::toupper(word[prefix.size()])) {
                return word.substr(0, prefix.size());
            }
        }
        prefix = words[0].substr(0, prefix.size() + 1);
    }
}


char** completer(const char *text, int start, int end) {
    UNUSED(start);
    UNUSED(end);

    // Dont do filename completion even there is no match.
    ::rl_attempted_completion_over = 1;

    // Dont do completion if in quotes
    if (::rl_completion_quote_character != 0) {
        return nullptr;
    }

    std::vector<std::string> matches;

    std::string primaryKeyword;  // The current primary keyword
    if (isStartOfStatement(primaryKeyword)) {
        matches = matchFromPrimaryKeywords(text);
    } else {
        matches = matchFromSubKeywords(text, primaryKeyword);
    }

    if (matches.empty()) {
        return nullptr;
    }

    char **results = reinterpret_cast<char**>(malloc((2 + matches.size()) * sizeof(char*)));

    // Get the longest common prefix of all matches as the echo back of this completion action
    results[0] = ::strdup(longestCommonPrefix(text, matches).c_str());

    auto i = 1;
    for (auto &word : matches) {
        results[i++] = ::strdup(word.c_str());
    }
    results[i] = nullptr;

    return results;
}


bool isStartOfStatement(std::string &primaryKeyword) {
    // If there is no input
    if (::rl_line_buffer == nullptr || *::rl_line_buffer == '\0') {
        return true;
    }

    std::string line = ::rl_line_buffer;

    auto piece = folly::trimWhitespace(line);
    // If the inputs are all white spaces
    if (piece.empty()) {
        return true;
    }

    // If the inputs are terminated with ';' or '|', i.e. complete statements
    // Additionally, there is an incomplete primary keyword for the next statement
    {
        static const std::regex pattern(R"((\s*\w+[^;|]*[;|]\s*)*(\w+)?)");
        std::smatch result;

        if (std::regex_match(line, result, pattern)) {
            return true;
        }
    }

    // The same to the occasion above, except that the primary keyword is complete
    // This is where sub keywords shall be completed
    {
        static const std::regex pattern(R"((\s*\w+[^;|]*[;|]\s*)*(\w+)[^;|]+)");
        std::smatch result;

        if (std::regex_match(line, result, pattern)) {
            primaryKeyword = result[result.size() - 1].str();
            return false;
        }
    }

    // TODO(dutor) There are still many scenarios we cannot cover with regular expressions.
    // We have to accomplish this with the help of the actual parser.

    return false;
}


std::vector<std::string> matchFromPrimaryKeywords(const std::string &text) {
    return matchFromKeywords(text, primaryKeywords);
}


std::vector<std::string> matchFromSubKeywords(const std::string &text,
                                              const std::string &primaryKeyword) {
    std::vector<std::string> matches = typeNames;
    auto iter = subKeywords.find(primaryKeyword);
    if (iter != subKeywords.end()) {
        matches.insert(matches.end(), iter->second.begin(), iter->second.end());
    }
    return matchFromKeywords(text, matches);
}


std::vector<std::string>
matchFromKeywords(const std::string &text, const std::vector<std::string> &keywords) {
    if (keywords.empty()) {
        return {};
    }

    std::vector<std::string> matches;
    for (auto &word : keywords) {
        if (text.size() > word.size()) {
            continue;
        }
        if (::strncasecmp(text.c_str(), word.c_str(), text.size()) == 0) {
            matches.emplace_back(word);
        }
    }

    return matches;
}


Status loadCompletions() {
    using fs::FileUtils;
    auto dir = FileUtils::readLink("/proc/self/exe").value();
    dir = FileUtils::dirname(dir.c_str()) + "/../share/resources";
    std::string file = dir + "/" + "completion.json";
    auto status = Status::OK();
    int fd = -1;
    do {
        fd = ::open(file.c_str(), O_RDONLY);
        if (fd == -1) {
            status = Status::Error("Failed to open `%s': %s",
                                    file.c_str(), ::strerror(errno));
            break;
        }

        auto len = ::lseek(fd, 0, SEEK_END);
        if (len == 0) {
            status = Status::Error("File `%s' is empty", file.c_str());
            break;
        }

        auto buffer = std::make_unique<char[]>(len + 1);
        ::lseek(fd, 0, SEEK_SET);
        auto ll = ::read(fd, buffer.get(), len);
        UNUSED(ll);
        buffer[len] = '\0';

        std::string content;
        content.assign(buffer.get(), len);

        try {
            status = parseKeywordsFromJson(folly::parseJson(content));
        } catch (const std::exception &e) {
            status = Status::Error("Illegal json `%s': %s", file.c_str(), e.what());
            break;
        }

        if (!status.ok()) {
            break;
        }
    } while (false);

    if (fd != -1) {
        ::close(fd);
    }

    return status;
}


Status parseKeywordsFromJson(const folly::dynamic &json) {
    auto iter = json.find("keywords");
    if (iter == json.items().end()) {
        fprintf(stderr, "completions: no `keywords' found\n");
        return Status::OK();
    }

    for (auto &pair : iter->second.items()) {
        auto &pkw = pair.first;
        primaryKeywords.emplace_back(pkw.asString());
        auto subIter = pair.second.find("sub_keywords");
        if (subIter == pair.second.items().end()) {
            continue;
        }
        if (!subIter->second.isArray()) {
            fprintf(stderr, "sub-keywords for `%s' should be an array\n",
                    pkw.asString().c_str());
            continue;
        }
        for (auto &subKey : subIter->second) {
            if (!subKey.isString()) {
                fprintf(stderr, "keyword name should be of type string\n");
                break;
            }
            subKeywords[pkw.asString()].emplace_back(subKey.asString());
        }
    }

    iter = json.find("typenames");
    if (iter == json.items().end()) {
        fprintf(stderr, "completions: no `typenames' found\n");
        return Status::OK();
    }

    for (auto &tname : iter->second) {
        typeNames.emplace_back(tname.asString());
    }

    return Status::OK();
}


void CliManager::initAutoCompletion() {
    // The completion function
    ::rl_attempted_completion_function = completer;
    // Characters that indicates begin or end of a quote
    ::rl_completer_quote_characters = "\"";
    // Allow conditional parsing of the ~/.inputrc file
    ::rl_readline_name = "nebula-graph";
    auto status = loadCompletions();
    if (!status.ok()) {
        fprintf(stderr, "%s\n", status.toString().c_str());
    }
}

}  // namespace graph
}  // namespace nebula
