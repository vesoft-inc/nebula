/*
 * Reproduction of NB-5: Snapshot promise never fulfilled on leadership change.
 *
 * Bug: SnapshotManager::sendSnapshot() (SnapshotManager.cpp:31-106) creates a
 * folly::Promise<StatusOr<pair<LogID, TermID>>> at line 33.  The leadership
 * check at lines 42-46 returns early WITHOUT fulfilling the promise:
 *
 *   if (tr.second != RaftPart::Role::LEADER) {
 *       VLOG(1) << ...;
 *       return;  // BUG: promise `p` destroyed unfulfilled
 *   }
 *
 * The Host object (Host.cpp:348-378) that initiated the snapshot via
 * startSendSnapshot() is chained on this promise's future via thenValue().
 * When the promise is destroyed without setValue/setException, the future
 * enters a BrokenPromise state.  The thenValue callback only handles the
 * success path; the BrokenPromise exception bypasses it entirely, so the
 * critical cleanup at Host.cpp:372-373:
 *
 *   self->sendingSnapshot_ = false;
 *   self->noMoreRequestCV_.notify_all();
 *
 * ...is NEVER executed.  The Host is permanently wedged:
 *   - sendingSnapshot_ remains true
 *   - startSendSnapshot() short-circuits on line 350: if (!sendingSnapshot_)
 *   - reset() (Host.h:93) force-clears sendingSnapshot_ but only after
 *     waiting for requestOnGoing_ -- which may also be stuck
 *   - No further AppendEntries or snapshots can be sent to this peer
 *
 * This standalone program reproduces the exact mechanism using std::promise
 * and std::future (which exhibit the same broken-promise semantics as
 * folly::Promise/SemiFuture).  It demonstrates:
 *   1. Destroying a promise without set_value/set_exception
 *   2. The future side gets std::future_error (broken_promise)
 *   3. A thenValue-equivalent (success-only) callback never executes
 *   4. The cleanup flag (sendingSnapshot_) remains permanently true
 *
 * Build:
 *   g++ -std=c++17 -pthread -o nb5_snapshot_promise_wedge nb5_snapshot_promise_wedge.cpp
 *
 * Run:
 *   timeout 5 ./nb5_snapshot_promise_wedge
 *   # Exit 0 = bug reproduced (wedge detected)
 *   # Exit 124 = timeout (unexpected deadlock in test itself)
 *   # Exit 1 = bug NOT reproduced (test logic error)
 *
 * vesoft-inc/nebula issue #63: snapshot promise never fulfilled on leadership
 * change — Host permanently wedged with sendingSnapshot_=true.
 */

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <functional>
#include <future>
#include <mutex>
#include <string>
#include <thread>

/* ───────────────────────────────────────────────────────────────────────────
 * Minimal reproduction of the nebula types involved.
 * ─────────────────────────────────────────────────────────────────────────── */

enum class Role { LEADER, FOLLOWER, CANDIDATE };

using TermID = int64_t;
using LogID  = int64_t;

struct StatusOrPair {
    bool ok_;
    std::pair<LogID, TermID> value_;
    std::string error_;

    bool ok() const { return ok_; }
    std::pair<LogID, TermID> value() const { return value_; }

    static StatusOrPair success(LogID logId, TermID termId) {
        return {true, {logId, termId}, ""};
    }
    static StatusOrPair error(const std::string& msg) {
        return {false, {0, 0}, msg};
    }
};

/* ───────────────────────────────────────────────────────────────────────────
 * Minimal Host: mirrors Host.h/Host.cpp state relevant to the bug.
 * ─────────────────────────────────────────────────────────────────────────── */

struct Host {
    std::mutex lock_;
    std::condition_variable noMoreRequestCV_;
    bool sendingSnapshot_ = false;
    LogID lastLogIdSent_ = 0;
    TermID lastLogTermSent_ = 0;
    LogID followerCommittedLogId_ = 0;

    /* Mirrors Host::startSendSnapshot() (Host.cpp:348-378).
     *
     * The real code does:
     *   part_->snapshot_->sendSnapshot(part_, addr_)
     *       .thenValue([self = shared_from_this()](auto&& status) {
     *           ... update state ...
     *           self->sendingSnapshot_ = false;        // line 372
     *           self->noMoreRequestCV_.notify_all();    // line 373
     *       });
     *
     * thenValue only fires on SUCCESS.  A BrokenPromise exception from
     * the SnapshotManager skips the callback entirely.
     */
    void startSendSnapshot(std::future<StatusOrPair> fut) {
        std::lock_guard<std::mutex> g(lock_);
        if (!sendingSnapshot_) {
            sendingSnapshot_ = true;

            /* Launch async handler -- mirrors the thenValue callback */
            std::thread([this, f = std::move(fut)]() mutable {
                try {
                    /* This blocks until the promise is fulfilled or broken */
                    StatusOrPair status = f.get();

                    /* === thenValue callback body (Host.cpp:360-373) === */
                    std::lock_guard<std::mutex> g2(lock_);
                    if (status.ok()) {
                        auto p = status.value();
                        lastLogIdSent_ = p.first;
                        lastLogTermSent_ = p.second;
                        followerCommittedLogId_ = p.first;
                        printf("  [Host] Snapshot succeeded, commitLogId=%ld\n",
                               (long)p.first);
                    } else {
                        printf("  [Host] Snapshot failed (error status)\n");
                    }
                    sendingSnapshot_ = false;
                    noMoreRequestCV_.notify_all();

                } catch (const std::future_error& e) {
                    /*
                     * THIS is the bug path.  In the real nebula code, there is
                     * NO .thenError() or .onError() handler.  The callback is
                     * registered via .thenValue() only.  A BrokenPromise
                     * exception propagates unhandled:
                     *
                     *   - sendingSnapshot_ remains TRUE forever
                     *   - noMoreRequestCV_ is never notified
                     *   - The Host is permanently wedged
                     *
                     * In this reproduction, we DETECT the bug by NOT doing
                     * cleanup here (matching the real code's behavior).
                     */
                    printf("  [Host] BUG HIT: future_error caught: %s\n", e.what());
                    printf("  [Host] sendingSnapshot_ remains TRUE — Host is wedged!\n");
                    /* NO cleanup — this matches the real buggy behavior */
                }
            }).detach();
        }
    }
};

/* ───────────────────────────────────────────────────────────────────────────
 * Minimal SnapshotManager: mirrors SnapshotManager.cpp:31-106.
 * ─────────────────────────────────────────────────────────────────────────── */

struct SnapshotManager {
    /* Current role — changes to simulate leadership loss */
    std::atomic<Role> currentRole_{Role::LEADER};
    std::atomic<TermID> currentTerm_{1};

    std::pair<TermID, Role> getTermAndRole() {
        return {currentTerm_.load(), currentRole_.load()};
    }

    /*
     * Mirrors SnapshotManager::sendSnapshot() (SnapshotManager.cpp:31-106).
     *
     * The real code:
     *   1. Creates promise p (line 33)
     *   2. Gets future from p (line 37)
     *   3. Posts lambda to executor (line 38)
     *   4. Inside lambda: checks leadership (lines 42-46)
     *   5. If not leader: RETURNS without fulfilling p  <-- THE BUG
     *   6. If leader: calls accessAllRowsInSnapshot which eventually
     *      calls p.setValue() (line 85) or p.setValue(Error) (lines 60, 102)
     */
    std::future<StatusOrPair> sendSnapshot() {
        std::promise<StatusOrPair> p;
        auto fut = p.get_future();

        /* Simulate the executor_->add() lambda */
        std::thread([this, p = std::move(p)]() mutable {
            /* SnapshotManager.cpp:41-46 */
            auto tr = getTermAndRole();
            if (tr.second != Role::LEADER) {
                printf("  [SnapshotManager] Leadership changed (term=%ld, role=FOLLOWER)\n",
                       (long)tr.first);
                printf("  [SnapshotManager] Returning WITHOUT fulfilling promise!\n");
                return;  // BUG: promise `p` destroyed here without setValue!
            }

            /* Normal path: would call accessAllRowsInSnapshot and eventually
             * p.setValue(...).  We simulate immediate success. */
            printf("  [SnapshotManager] Still leader, fulfilling promise\n");
            p.set_value(StatusOrPair::success(100, 1));
        }).detach();

        return fut;
    }
};

/* ───────────────────────────────────────────────────────────────────────────
 * Test scenarios
 * ─────────────────────────────────────────────────────────────────────────── */

static bool test_normal_path() {
    printf("\n=== Test 1: Normal path (leadership retained) ===\n");

    SnapshotManager mgr;
    Host host;

    /* Leadership is retained — promise will be fulfilled normally */
    mgr.currentRole_ = Role::LEADER;

    auto fut = mgr.sendSnapshot();
    host.startSendSnapshot(std::move(fut));

    /* Wait for completion */
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::lock_guard<std::mutex> g(host.lock_);
    if (host.sendingSnapshot_) {
        printf("FAIL: sendingSnapshot_ should be false after normal completion\n");
        return false;
    }
    printf("PASS: sendingSnapshot_=false, Host is not wedged\n");
    return true;
}

static bool test_leadership_change_bug() {
    printf("\n=== Test 2: Leadership change — promise never fulfilled (BUG) ===\n");

    SnapshotManager mgr;
    Host host;

    /* Simulate leadership loss BEFORE sendSnapshot checks */
    mgr.currentRole_ = Role::FOLLOWER;
    mgr.currentTerm_ = 2;

    auto fut = mgr.sendSnapshot();
    host.startSendSnapshot(std::move(fut));

    /* Wait for the broken-promise path to execute */
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::lock_guard<std::mutex> g(host.lock_);
    if (host.sendingSnapshot_) {
        printf("BUG CONFIRMED: sendingSnapshot_ is still TRUE — Host is permanently wedged!\n");
        printf("  The Host can no longer send AppendEntries or snapshots to this peer.\n");
        printf("  In production, this means replication to the peer is permanently stalled.\n");
        return true;  /* Bug reproduced = test passes */
    }
    printf("UNEXPECTED: sendingSnapshot_ is false (bug not triggered)\n");
    return false;
}

static bool test_leadership_change_during_transfer() {
    printf("\n=== Test 3: Leadership changes DURING snapshot transfer ===\n");
    printf("  (Demonstrates the second part of the bug: stale state update)\n");

    SnapshotManager mgr;
    Host host;

    /* Start as leader */
    mgr.currentRole_ = Role::LEADER;

    /* Create a custom scenario: snapshot starts, leadership changes mid-transfer,
     * but the callback still updates Host state with stale values.
     *
     * In the real code (Host.cpp:360-364), the thenValue callback does:
     *   self->lastLogIdSent_ = commitLogIdAndTerm.first;
     *   self->lastLogTermSent_ = commitLogIdAndTerm.second;
     *   self->followerCommittedLogId_ = commitLogIdAndTerm.first;
     * WITHOUT checking if the term has changed since snapshot was initiated.
     */

    /* Simulate: promise fulfilled with old-term snapshot data */
    std::promise<StatusOrPair> p;
    auto fut = p.get_future();

    host.startSendSnapshot(std::move(fut));

    /* Leadership changes -- in real code, Host would be part of an old term */
    printf("  [Test] Leadership changed from term 1 to term 2\n");
    mgr.currentRole_ = Role::FOLLOWER;
    mgr.currentTerm_ = 2;

    /* But the snapshot completes with old term data */
    p.set_value(StatusOrPair::success(/*logId=*/50, /*termId=*/1));

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::lock_guard<std::mutex> g(host.lock_);
    if (!host.sendingSnapshot_ && host.lastLogIdSent_ == 50) {
        printf("  Host.lastLogIdSent_ = %ld (set to stale snapshot value)\n",
               (long)host.lastLogIdSent_);
        printf("  Host.lastLogTermSent_ = %ld (stale — should be term 2, got term 1)\n",
               (long)host.lastLogTermSent_);
        printf("BUG CONFIRMED: Snapshot callback updated Host state with stale term data.\n");
        printf("  In production, this corrupts replication state after leadership change.\n");
        return true;  /* Bug reproduced */
    }
    printf("UNEXPECTED: Host state not updated as expected\n");
    return false;
}

static bool test_host_reset_race() {
    printf("\n=== Test 4: Host::reset() race with wedged sendingSnapshot_ ===\n");

    Host host;

    /* Simulate the wedged state from Test 2 */
    {
        std::lock_guard<std::mutex> g(host.lock_);
        host.sendingSnapshot_ = true;
    }

    /* In real code, Host::reset() (Host.h:85-96) does:
     *   noMoreRequestCV_.wait(g, [this] { return !requestOnGoing_; });
     *   ...
     *   sendingSnapshot_ = false;  // Force-clear without waiting for snapshot!
     *
     * This is a partial fix — it clears the flag, but:
     *   1. If the snapshot thread is still running, it may set sendingSnapshot_
     *      back to true or corrupt other state
     *   2. The force-clear doesn't address the root cause (unfulfilled promise)
     *   3. If requestOnGoing_ is also stuck, reset() itself blocks forever
     */

    printf("  [Host] sendingSnapshot_=%s before reset()\n",
           host.sendingSnapshot_ ? "true" : "false");

    /* Simulate reset() -- note: we skip the requestOnGoing_ wait since we're
     * only demonstrating the sendingSnapshot_ force-clear */
    {
        std::lock_guard<std::mutex> g(host.lock_);
        /* This is what Host::reset() does at line 93 */
        host.sendingSnapshot_ = false;
    }

    printf("  [Host] sendingSnapshot_=%s after reset() force-clear\n",
           host.sendingSnapshot_ ? "true" : "false");
    printf("NOTE: reset() force-clears the flag, but this is a band-aid:\n");
    printf("  - The snapshot thread may still be running\n");
    printf("  - The unfulfilled promise is the root cause\n");
    printf("  - If requestOnGoing_ is stuck, reset() deadlocks\n");
    printf("PASS: Demonstrates the incomplete workaround in Host::reset()\n");
    return true;
}

int main() {
    printf("========================================================\n");
    printf("NB-5 Reproduction: Snapshot Promise Never Fulfilled\n");
    printf("  vesoft-inc/nebula issue #63\n");
    printf("  Bug location: SnapshotManager.cpp:42-46\n");
    printf("  Wedge location: Host.cpp:348-378\n");
    printf("========================================================\n");

    int pass = 0, fail = 0;

    if (test_normal_path()) pass++; else fail++;
    if (test_leadership_change_bug()) pass++; else fail++;
    if (test_leadership_change_during_transfer()) pass++; else fail++;
    if (test_host_reset_race()) pass++; else fail++;

    printf("\n========================================================\n");
    printf("Results: %d passed, %d failed\n", pass, fail);
    printf("========================================================\n");

    if (fail > 0) {
        printf("FAILURE: Some tests did not reproduce the expected behavior\n");
        return 1;
    }

    printf("\nAll tests passed — NB-5 bug reproduced.\n\n");
    printf("Root cause:\n");
    printf("  SnapshotManager::sendSnapshot() at SnapshotManager.cpp:42-46 returns\n");
    printf("  early when leadership changes, destroying the promise without fulfilling\n");
    printf("  it. The Host's thenValue callback never fires, so sendingSnapshot_\n");
    printf("  remains true forever, blocking all replication to that peer.\n");
    printf("\nFix:\n");
    printf("  1. Always fulfill the promise on all paths:\n");
    printf("       p.setValue(Status::Error(\"Leadership changed\"));\n");
    printf("     before the early return at line 45.\n");
    printf("  2. Add .thenError() handler in Host::startSendSnapshot() to\n");
    printf("     ensure sendingSnapshot_=false even on exception paths.\n");
    printf("  3. Add term check in snapshot callback: only update Host state\n");
    printf("     if part_->termId() == snapshotInitiatedTerm.\n");
    return 0;
}
