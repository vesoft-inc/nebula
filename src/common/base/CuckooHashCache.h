#ifndef COMMON_BASE_CUCKOOHASHCACHE_H_
#define COMMON_BASE_CUCKOOHASHCACHE_H_

#include <algorithm>
#include <array>
#include <atomic>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "time/WallClock.h"

namespace nebula {

//! The default maximum number of keys per bucket
constexpr size_t DEFAULT_SLOT_PER_BUCKET = 4;
//! The default number of elements in an empty hash table
constexpr size_t DEFAULT_SIZE = (1U << 20) * DEFAULT_SLOT_PER_BUCKET;
constexpr size_t DEFAULT_TTL_SECONDS = 10 * 60;

/**
 * bucket_container manages storage of key-value pairs for the table.
 * It stores the items inline in uninitialized memory, and keeps track of which
 * slots have live data and which do not. It also stores a partial hash for
 * each live key. It is sized by powers of two.
 *
 * @tparam Key type of keys in the table
 * @tparam T type of values in the table
 * @tparam Partial type of partial keys
 * @tparam SLOT_PER_BUCKET number of slots for each bucket in the table
 */
template <class Key, class T, class Partial, std::size_t SLOT_PER_BUCKET>
class bucket_container {
public:
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<const Key, T>;
    using partial_t = Partial;

public:
    /*
     * The bucket type holds SLOT_PER_BUCKET key-value pairs, along with their
     * partial keys and occupancy info. It uses aligned_storage arrays to store
     * the keys and values to allow constructing and destroying key-value pairs
     * in place. The lifetime of bucket data should be managed by the container.
     * It is the user's responsibility to confirm whether the data they are
     * accessing is live or not.
     */
    class bucket {
    public:
        bucket() noexcept : occupied_() {}

        const value_type &kvpair(size_t ind) const {
            return *static_cast<const value_type *>(
                static_cast<const void *>(&values_[ind]));
        }
        value_type &kvpair(size_t ind) {
            return *static_cast<value_type *>(static_cast<void *>(&values_[ind]));
        }

        const key_type &key(size_t ind) const {
            return storage_kvpair(ind).first;
        }

        const mapped_type &mapped(size_t ind) const {
            return storage_kvpair(ind).second;
        }
        mapped_type &mapped(size_t ind) { return storage_kvpair(ind).second; }

        partial_t partial(size_t ind) const { return partials_[ind]; }
        partial_t &partial(size_t ind) { return partials_[ind]; }

        uint32_t expire(size_t ind) const { return expires_[ind]; }
        uint32_t &expire(size_t ind) { return expires_[ind]; }

        bool occupied(size_t ind) const { return occupied_[ind]; }
        bool &occupied(size_t ind) { return occupied_[ind]; }
    private:
        friend class bucket_container;

        using storage_value_type = std::pair<Key, T>;

        const storage_value_type &storage_kvpair(size_t ind) const {
            return *static_cast<const storage_value_type *>(
                static_cast<const void *>(&values_[ind]));
        }
        storage_value_type &storage_kvpair(size_t ind) {
            return *static_cast<storage_value_type *>(
                static_cast<void *>(&values_[ind]));
        }

        std::array<typename std::aligned_storage<sizeof(storage_value_type),
            alignof(storage_value_type)>::type, SLOT_PER_BUCKET> values_;
        std::array<uint32_t, SLOT_PER_BUCKET> expires_;
        std::array<partial_t, SLOT_PER_BUCKET> partials_;
        std::array<bool, SLOT_PER_BUCKET> occupied_;
    };

    bucket_container(size_t hp) : hashpower_(hp), buckets_(size()) { }
    bucket_container(const bucket_container &bc) noexcept = delete;
    bucket_container &operator=(const bucket_container &bc) = delete;
    bucket_container(bucket_container &&bc) noexcept = default;
    bucket_container &operator=(bucket_container &&bc) noexcept = default;
    ~bucket_container() noexcept { clear(); }

    size_t hashpower() const { return hashpower_; }

    void hashpower(size_t val) { hashpower_ = val; }

    size_t size() const { return size_t(1) << hashpower(); }

    bucket &operator[](size_t i) { return buckets_[i]; }
    const bucket &operator[](size_t i) const { return buckets_[i]; }

    // Constructs live data in a bucket
    template <typename K, typename... Args>
    void setKV(size_t ind, size_t slot, partial_t p, uint32_t e, K &&k,
               Args &&... args) {
        bucket &b = buckets_[ind];
        assert(!b.occupied(slot));
        b.partial(slot) = p;
        b.expire(slot) = e;
        new (std::addressof(b.storage_kvpair(slot))) value_type(
            std::piecewise_construct,
            std::forward_as_tuple(std::forward<K>(k)),
            std::forward_as_tuple(std::forward<Args>(args)...));
        // This must occur last, to enforce a strong exception guarantee
        b.occupied(slot) = true;
    }

    using storage_value_type = typename bucket::storage_value_type;

    // Destroys live data in a bucket
    void eraseKV(size_t ind, size_t slot) {
        bucket &b = buckets_[ind];
        assert(b.occupied(slot));
        b.occupied(slot) = false;
        std::addressof(b.storage_kvpair(slot))->~storage_value_type();
    }

    // Destroys all the live data in the buckets. Does not deallocate the bucket
    // memory.
    void clear() noexcept {
        for (size_t i = 0; i < size(); ++i) {
            bucket &b = buckets_[i];
            for (size_t j = 0; j < SLOT_PER_BUCKET; ++j) {
                if (b.occupied(j)) {
                    eraseKV(i, j);
                }
            }
        }
    }

private:
    size_t hashpower_;
    // These buckets are protected by striped locks (external to the
    // BucketContainer), which must be obtained before accessing a bucket.
    std::vector<bucket> buckets_;
};

/**
 * A concurrent hash table
 *
 * @tparam Key type of keys in the table
 * @tparam T type of values in the table
 * @tparam Hash type of hash functor
 * @tparam KeyEqual type of equality comparison functor
 * because the table relies on types that are over-aligned to optimize
 * concurrent cache usage.
 * @tparam SLOT_PER_BUCKET number of slots for each bucket in the table
 */
template <class Key, class T, class Hash = std::hash<Key>,
    class KeyEqual = std::equal_to<Key>,
    std::size_t SLOT_PER_BUCKET = DEFAULT_SLOT_PER_BUCKET>
class CuckoohashCache {
private:
    using partial_t = uint8_t;
    using buckets_t = bucket_container<Key, T, partial_t, SLOT_PER_BUCKET>;

public:
    using key_type = typename buckets_t::key_type;
    using mapped_type = typename buckets_t::mapped_type;
    using value_type = typename buckets_t::value_type;
    using hasher = Hash;
    using key_equal = KeyEqual;

    static constexpr uint16_t slot_per_bucket() { return SLOT_PER_BUCKET; }

    explicit CuckoohashCache(size_t n = DEFAULT_SIZE, size_t ttl = DEFAULT_TTL_SECONDS)
        : buckets_(reserve_calc(n)),
          locks_(std::min(bucket_count(), size_t(kMaxNumLocks)), spinlock()),
          ttl_(ttl) {}

    CuckoohashCache(const CuckoohashCache &other) = delete;
    CuckoohashCache &operator=(const CuckoohashCache &other) = delete;
    CuckoohashCache(CuckoohashCache &&other) noexcept = default;
    CuckoohashCache &operator=(CuckoohashCache &&other) noexcept = default;

    hasher hash_function() const { return hash_fn_; }
    key_equal key_eq() const { return eq_fn_; }
    size_t hashpower() const { return buckets_.hashpower(); }
    size_t bucket_count() const { return buckets_.size(); }
    bool empty() const { return size() == 0; }
    size_t size() const {
        int64_t s = 0;
        for (spinlock &lock : locks_) {
            s += lock.elem_counter();
        }
        assert(s >= 0);
        return static_cast<size_t>(s);
    }

    size_t total() const { return size(); }
    size_t hits() const {
        int64_t s = 0;
        for (spinlock &lock : locks_) {
            s += lock.hit_counter();
        }
        assert(s >= 0);
        return static_cast<size_t>(s);
    }
    size_t evicts() const {
        int64_t s = 0;
        for (spinlock &lock : locks_) {
            s += lock.evict_counter();
        }
        assert(s >= 0);
        return static_cast<size_t>(s);
    }
    size_t capacity() const { return bucket_count() * slot_per_bucket(); }
    double load_factor() const {
        return static_cast<double>(size()) / static_cast<double>(capacity());
    }

    /**
     * Searches the table for @p key, and invokes @p fn on the value. @p fn is
     * not allowed to modify the contents of the value if found.
     *
     * @tparam K type of the key. This can be any type comparable with @c key_type
     * @tparam F type of the functor. It should implement the method
     * <tt>void operator()(const mapped_type&)</tt>.
     * @param key the key to search for
     * @param fn the functor to invoke if the element is found
     * @return true if the key was found and functor invoked, false otherwise
     */
    template <typename K, typename F>
    bool find_fn(const K &key, F fn) {
        const hash_value hv = hashed_key(key);
        const auto b = snapshot_and_lock_two(hv);
        const table_position pos = cuckoo_find(key, hv.partial, b.i1, b.i2);
        if (pos.status == ok) {
            fn(buckets_[pos.index].mapped(pos.slot));
            return true;
        } else {
            return false;
        }
    }

    /**
     * Searches the table for @p key, and invokes @p fn on the value. @p fn is
     * allow to modify the contents of the value if found.
     *
     * @tparam K type of the key. This can be any type comparable with @c key_type
     * @tparam F type of the functor. It should implement the method
     * <tt>void operator()(mapped_type&)</tt>.
     * @param key the key to search for
     * @param fn the functor to invoke if the element is found
     * @return true if the key was found and functor invoked, false otherwise
     */
    template <typename K, typename F>
    bool update_fn(const K &key, F fn) {
        const hash_value hv = hashed_key(key);
        const auto b = snapshot_and_lock_two(hv);
        const table_position pos = cuckoo_find(key, hv.partial, b.i1, b.i2);
        if (pos.status == ok) {
            fn(buckets_[pos.index].mapped(pos.slot));
            return true;
        } else {
            return false;
        }
    }

    /**
     * Searches for @p key in the table, and invokes @p fn on the value if the
     * key is found. The functor can mutate the value, and should return @c true
     * in order to erase the element, and @c false otherwise.
     *
     * @tparam K type of the key
     * @tparam F type of the functor. It should implement the method
     * <tt>bool operator()(mapped_type&)</tt>.
     * @param key the key to possibly erase from the table
     * @param fn the functor to invoke if the element is found
     * @return true if @p key was found and @p fn invoked, false otherwise
     */
    template <typename K, typename F>
    bool erase_fn(const K &key, F fn) {
        const hash_value hv = hashed_key(key);
        const auto b = snapshot_and_lock_two(hv);
        const table_position pos = cuckoo_find(key, hv.partial, b.i1, b.i2);
        if (pos.status == ok) {
            if (fn(buckets_[pos.index].mapped(pos.slot))) {
                del_from_bucket(pos.index, pos.slot);
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Searches for @p key in the table. If the key is found, then @p fn is
     * called on the existing value, and nothing happens to the passed-in key and
     * values. The functor can mutate the value, and should return @c true in
     * order to erase the element, and @c false otherwise. If the key is not
     * found and must be inserted, the pair will be constructed by forwarding the
     * given key and values. If there is no room left in the table, it will be
     * automatically expanded. Expansion may throw exceptions.
     *
     * @tparam K type of the key
     * @tparam F type of the functor. It should implement the method
     * <tt>bool operator()(mapped_type&)</tt>.
     * @tparam Args list of types for the value constructor arguments
     * @param key the key to insert into the table
     * @param fn the functor to invoke if the element is found. If your @p fn
     * needs more data that just the value being modified, consider implementing
     * it as a lambda with captured arguments.
     * @param val a list of constructor arguments with which to create the value
     * @return true if a new key was inserted, false if the key was already in
     * the table
     */
    template <typename K, typename F, typename... Args>
    bool uprase_fn(K &&key, F fn, Args &&... val) {
        hash_value hv = hashed_key(key);
        auto b = snapshot_and_lock_two(hv);
        table_position pos = cuckoo_insert(hv, b, key);
        switch (pos.status) {
            case ok:
                add_to_bucket(pos.index, pos.slot, hv.partial,
                              std::forward<K>(key), std::forward<Args>(val)...);
                break;
            case failure_key_duplicated:
                if (fn(buckets_[pos.index].mapped(pos.slot))) {
                    del_from_bucket(pos.index, pos.slot);
                }
                break;
            case failure_table_full:
                break;
            default:
                assert(false);
        }
        return pos.status == ok;
    }

    /**
     * Equivalent to calling @ref uprase_fn with a functor that modifies the
     * given value and always returns false (meaning the element is not removed).
     * The passed-in functor must implement the method <tt>void
     * operator()(mapped_type&)</tt>.
     */
    template <typename K, typename F, typename... Args>
    bool upsert(K &&key, F fn, Args &&... val) {
        return uprase_fn(
            std::forward<K>(key), [&fn](mapped_type &v) {
                fn(v);
                return false;
            },
            std::forward<Args>(val)...
        );
    }

    /**
   * Copies the value associated with @p key into @p val. Equivalent to
   * calling @ref find_fn with a functor that copies the value into @p val. @c
   * mapped_type must be @c CopyAssignable.
   */
    template <typename K>
    bool find(const K &key, mapped_type &val) {
        return find_fn(key, [&val](const mapped_type &v) mutable { val = v; });
    }

    template <typename K>
    StatusOr<mapped_type> get(const K &key) {
        StatusOr<mapped_type> status = Status::Error();
        find_fn(key, [&status] (const mapped_type &v) mutable {
            status = v;
        });
        return status;
    }

    /**
     * Returns whether or not @p key is in the table. Equivalent to @ref
     * find_fn with a functor that does nothing.
     */
    template <typename K>
    bool contains(const K &key) {
        return find_fn(key, [](const mapped_type &) {});
    }

    /**
     * Updates the value associated with @p key to @p val. Equivalent to
     * calling @ref update_fn with a functor that assigns the existing mapped
     * value to @p val. @c mapped_type must be @c MoveAssignable or @c
     * CopyAssignable.
     */
    template <typename K, typename V>
    bool update(const K &key, V &&val) {
        return update_fn(key, [&val](mapped_type &v) { v = std::forward<V>(val); });
    }

    /**
     * Inserts the key-value pair into the table. Equivalent to calling @ref
     * upsert with a functor that does nothing.
     */
    template <typename K, typename... Args>
    bool insert(K &&key, Args &&... val) {
        return upsert(std::forward<K>(key), [](mapped_type &) {},
                      std::forward<Args>(val)...);
    }

    /**
     * Inserts the key-value pair into the table. If the key is already in the
     * table, assigns the existing mapped value to @p val. Equivalent to
     * calling @ref upsert with a functor that assigns the mapped value to @p
     * val.
     */
    template <typename K, typename V>
    bool insert_or_assign(K &&key, V &&val) {
        return upsert(std::forward<K>(key), [&val](mapped_type &m) { m = val; },
                      std::forward<V>(val));
    }

    /**
     * Erases the key from the table. Equivalent to calling @ref erase_fn with a
     * functor that just returns true.
     */
    template <typename K>
    bool erase(const K &key) {
        return erase_fn(key, [](mapped_type &) { return true; });
    }

    template <typename K>
    bool evict(const K &key) { return erase(key); }

    /**
     * Removes all elements in the table, calling their destructors.
     */
    void clear() {
        buckets_.clear();
        // This will also clear out any data in old_buckets and delete it, if we
        // haven't already.
        for (spinlock &lock : locks_) {
            lock.elem_counter() = 0;
            lock.evict_counter() = 0;
            lock.hit_counter() = 0;
        }
    }

private:
    // true if the key is small and simple, which means using partial keys for
    // lookup would probably slow us down
    static constexpr bool is_simple() {
        return std::is_pod<key_type>::value && sizeof(key_type) <= 8;
    }

    // Contains a hash and partial for a given key. The partial key is used for
    // partial-key cuckoohashing, and for finding the alternate bucket of that a
    // key hashes to.
    struct hash_value {
        size_t hash;
        partial_t partial;
    };

    template <typename K>
    hash_value hashed_key(const K &key) const {
        const size_t hash = hash_function()(key);
        return {hash, partial_key(hash)};
    }

    // hashsize returns the number of buckets corresponding to a given
    // hashpower.
    static inline size_t hashsize(const size_t hp) {
        return size_t(1) << hp;
    }

    // hashmask returns the bitmask for the buckets array corresponding to a
    // given hashpower.
    static inline size_t hashmask(const size_t hp) {
        return hashsize(hp) - 1;
    }

    // The partial key must only depend on the hash value. It cannot change with
    // the hashpower, because, in order for `cuckoo_fast_double` to work
    // properly, the alt_index must only grow by one bit at the top each time we
    // expand the table.
    static partial_t partial_key(const size_t hash) {
        const uint64_t hash_64bit = hash;
        const uint32_t hash_32bit = (static_cast<uint32_t>(hash_64bit) ^
                                     static_cast<uint32_t>(hash_64bit >> 32));
        const uint16_t hash_16bit = (static_cast<uint16_t>(hash_32bit) ^
                                     static_cast<uint16_t>(hash_32bit >> 16));
        const uint8_t hash_8bit = (static_cast<uint8_t>(hash_16bit) ^
                                   static_cast<uint8_t>(hash_16bit >> 8));
        return hash_8bit;
    }

    // index_hash returns the first possible bucket that the given hashed key
    // could be.
    static inline size_t index_hash(const size_t hp, const size_t hv) {
        return hv & hashmask(hp);
    }

    // alt_index returns the other possible bucket that the given hashed key
    // could be. It takes the first possible bucket as a parameter. Note that
    // this function will return the first possible bucket if index is the
    // second possible bucket, so alt_index(ti, partial, alt_index(ti, partial,
    // index_hash(ti, hv))) == index_hash(ti, hv).
    static inline size_t alt_index(const size_t hp, const partial_t partial,
                                   const size_t index) {
        // ensure tag is nonzero for the multiply. 0xc6a4a7935bd1e995 is the
        // hash constant from 64-bit MurmurHash2
        const size_t nonzero_tag = static_cast<size_t>(partial) + 1;
        return (index ^ (nonzero_tag * 0xc6a4a7935bd1e995)) & hashmask(hp);
    }

    // A fast, lightweight spinlock
    //
    // Per-spinlock, we also maintain some metadata about the contents of the
    // table. Storing data per-spinlock avoids false sharing issues when multiple
    // threads need to update this metadata. We store the following information:
    //
    // - elem_counter: A counter indicating how many elements in the table are
    // under this lock. One can compute the size of the table by summing the
    // elem_counter over all locks.
    class alignas(64) spinlock {
    public:
        spinlock() :
            elem_counter_(0),
            evict_counter_(0),
            hit_counter_(0) {
            lock_.clear();
        }

        spinlock(const spinlock &other) noexcept
            : elem_counter_(other.elem_counter()),
              evict_counter_(other.evict_counter()),
              hit_counter_(other.hit_counter()) {
            lock_.clear();
        }

        spinlock &operator=(const spinlock &other) noexcept {
            elem_counter() = other.elem_counter();
            evict_counter() = other.evict_counter();
            return *this;
        }

        void lock() noexcept {
            while (lock_.test_and_set(std::memory_order_acq_rel))
                ;
        }

        void unlock() noexcept { lock_.clear(std::memory_order_release); }

        bool try_lock() noexcept {
            return !lock_.test_and_set(std::memory_order_acq_rel);
        }

        int64_t &elem_counter() noexcept { return elem_counter_; }
        int64_t elem_counter() const noexcept { return elem_counter_; }

        int64_t &evict_counter() noexcept { return evict_counter_; }
        int64_t evict_counter() const noexcept { return evict_counter_; }

        int64_t &hit_counter() noexcept { return hit_counter_; }
        int64_t hit_counter() const noexcept { return hit_counter_; }

    private:
        std::atomic_flag lock_{};
        int64_t elem_counter_;
        int64_t evict_counter_;
        int64_t hit_counter_;
    };

    using locks_t = std::vector<spinlock>;

    // Classes for managing locked buckets. By storing and moving around sets of
    // locked buckets in these classes, we can ensure that they are unlocked
    // properly.
    struct LockDeleter {
        void operator()(spinlock *l) const { l->unlock(); }
    };

    using LockManager = std::unique_ptr<spinlock, LockDeleter>;

    class TwoBuckets {
    public:
        TwoBuckets() {}
        TwoBuckets(locks_t &locks, size_t i1_, size_t i2_)
            : i1(i1_), i2(i2_),
              first_manager_(&locks[lock_ind(i1)]),
              second_manager_((lock_ind(i1) != lock_ind(i2)) ? &locks[lock_ind(i2)] : nullptr) {}

        void unlock() {
            first_manager_.reset();
            second_manager_.reset();
        }

        size_t i1{};
        size_t i2{};
    private:
        LockManager first_manager_, second_manager_;
    };

    LockManager lock_one(size_t i) const {
        const size_t l = lock_ind(i);
        spinlock &lock = locks_[l];
        lock.lock();
        return LockManager(&lock);
    }

    TwoBuckets lock_two(size_t i1, size_t i2) const {
        size_t l1 = lock_ind(i1);
        size_t l2 = lock_ind(i2);
        if (l2 < l1) {
            std::swap(l1, l2);
        }
        locks_[l1].lock();
        if (l2 != l1) {
            locks_[l2].lock();
        }
        return TwoBuckets(locks_, i1, i2);
    }

    // snapshot_and_lock_two loads locks the buckets associated with the given
    // hash value, making sure the hashpower doesn't change before the locks are
    // taken. Thus it ensures that the buckets and locks corresponding to the
    // hash value will stay correct as long as the locks are held. It returns
    // the bucket indices associated with the hash value and the current
    // hashpower.
    TwoBuckets snapshot_and_lock_two(const hash_value &hv) const {
        // Keep the current hashpower and locks we're using to compute the buckets
        const size_t hp = hashpower();
        const size_t i1 = index_hash(hp, hv.hash);
        const size_t i2 = alt_index(hp, hv.partial, i1);
        return lock_two(i1, i2);
    }

    // lock_ind converts an index into buckets to an index into locks.
    static inline size_t lock_ind(const size_t bucket_ind) {
        return bucket_ind & (kMaxNumLocks - 1);
    }

    // The type of the bucket
    using bucket = typename buckets_t::bucket;

    // Status codes for internal functions
    enum cuckoo_status {
        ok,
        failure,
        failure_key_not_found,
        failure_key_duplicated,
        failure_table_full,
    };

    // A composite type for functions that need to return a table position, and
    // a status code.
    struct table_position {
        size_t index;
        size_t slot;
        cuckoo_status status;
    };

    // cuckoo_find searches the table for the given key, returning the position
    // of the element found, or a failure status code if the key wasn't found.
    // It expects the locks to be taken and released outside the function.
    template <typename K>
    table_position cuckoo_find(const K &key, const partial_t partial,
                               const size_t i1, const size_t i2) {
        int slot = try_read_from_bucket(i1, partial, key);
        if (slot != -1) {
            return table_position{i1, static_cast<size_t>(slot), ok};
        }
        slot = try_read_from_bucket(i2, partial, key);
        if (slot != -1) {
            return table_position{i2, static_cast<size_t>(slot), ok};
        }
        return table_position{0, 0, failure_key_not_found};
    }

    // try_read_from_bucket will search the bucket for the given key and return
    // the index of the slot if found, or -1 if not found.
    template <typename K>
    int try_read_from_bucket(const size_t bucket_ind, const partial_t partial, const K &key) {
        bucket &b = buckets_[bucket_ind];
        for (int i = 0; i < static_cast<int>(slot_per_bucket()); ++i) {
            if (try_reclaim(bucket_ind, i)) {
                continue;
            }
            if (!b.occupied(i) || (!is_simple() && partial != b.partial(i))) {
                continue;
            } else if (key_eq()(b.key(i), key)) {
                ++locks_[lock_ind(bucket_ind)].hit_counter();
                return i;
            }
        }
        return -1;
    }

    bool try_reclaim(const size_t bucket_ind, const size_t slot) {
        bucket &b = buckets_[bucket_ind];
        if (b.occupied(slot) &&
            b.expire(slot) <= nebula::time::WallClock::fastNowInSec()) {
            del_from_bucket(bucket_ind, slot);
            return true;
        }
        return false;
    }

    // cuckoo_insert tries to find an empty slot in either of the buckets to
    // insert the given key into. It expects the locks to be taken outside the function. Before inserting, it
    // checks that the key isn't already in the table. cuckoo hashing presents
    // multiple concurrency issues, which are explained in the function. The
    // following return states are possible:
    //
    // ok -- Found an empty slot, locks will be held on both buckets after the
    // function ends, and the position of the empty slot is returned
    //
    // failure_key_duplicated -- Found a duplicate key, locks will be held, and
    // the position of the duplicate key will be returned
    //
    // failure_table_full -- Failed to find an empty slot for the table. Locks
    // are released. No meaningful position is returned.
    template <typename K>
    table_position cuckoo_insert(const hash_value hv, TwoBuckets &b, K &key) {
        int res1, res2;
        if (!try_find_insert_bucket(b.i1, res1, hv.partial, key)) {
            return table_position{b.i1, static_cast<size_t>(res1),
                                  failure_key_duplicated};
        }
        if (!try_find_insert_bucket(b.i2, res2, hv.partial, key)) {
            return table_position{b.i2, static_cast<size_t>(res2),
                                  failure_key_duplicated};
        }
        if (res1 != -1) {
            return table_position{b.i1, static_cast<size_t>(res1), ok};
        }
        if (res2 != -1) {
            return table_position{b.i2, static_cast<size_t>(res2), ok};
        }
        return table_position{0, 0, failure_table_full};
    }

    // add_to_bucket will insert the given key-value pair into the slot. The key
    // and value will be move-constructed into the table, so they are not valid
    // for use afterwards.
    template <typename K, typename... Args>
    void add_to_bucket(const size_t bucket_ind, const size_t slot,
                       const partial_t partial, K &&key, Args &&... val) {
        uint32_t e = nebula::time::WallClock::fastNowInSec() + ttl_;
        buckets_.setKV(bucket_ind, slot, partial, e, std::forward<K>(key),
                       std::forward<Args>(val)...);
        ++locks_[lock_ind(bucket_ind)].elem_counter();
    }

    // try_find_insert_bucket will search the bucket for the given key, and for
    // an empty slot. If the key is found, we store the slot of the key in
    // `slot` and return false. If we find an empty slot, we store its position
    // in `slot` and return true. If no duplicate key is found and no empty slot
    // is found, we store -1 in `slot` and return true.
    template <typename K>
    bool try_find_insert_bucket(const size_t bucket_ind, int &slot,
                                const partial_t partial, const K &key) {
        bucket &b = buckets_[bucket_ind];
        slot = -1;
        for (int i = 0; i < static_cast<int>(slot_per_bucket()); ++i) {
            if (try_reclaim(bucket_ind, i)) {
                slot = i;
            } else if (b.occupied(i)) {
                if (!is_simple() && partial != b.partial(i)) {
                    continue;
                }
                if (key_eq()(b.key(i), key)) {
                    slot = i;
                    ++locks_[lock_ind(bucket_ind)].hit_counter();
                    return false;
                }
            } else {
                slot = i;
            }
        }
        return true;
    }

    // Removes an item from a bucket, decrementing the associated counter as
    // well.
    void del_from_bucket(const size_t bucket_ind, const size_t slot) {
        buckets_.eraseKV(bucket_ind, slot);
        --locks_[lock_ind(bucket_ind)].elem_counter();
        ++locks_[lock_ind(bucket_ind)].evict_counter();
    }

    // reserve_calc takes in a parameter specifying a certain number of slots
    // for a table and returns the smallest hashpower that will hold n elements.
    static size_t reserve_calc(const size_t n) {
        const size_t buckets = (n + slot_per_bucket() - 1) / slot_per_bucket();
        size_t blog2;
        for (blog2 = 0; (size_t(1) << blog2) < buckets; ++blog2)
            ;
        assert(n <= buckets * slot_per_bucket() && buckets <= hashsize(blog2));
        return blog2;
    }

    static constexpr size_t kMaxNumLocks = 1UL << 16;

    // The hash function
    hasher hash_fn_;

    // The equality function
    key_equal eq_fn_;

    // container of buckets. The size or memory location of the buckets cannot be
    // changed unless all the locks are taken on the table. Thus, it is only safe
    // to access the buckets_ container when you have at least one lock held.
    //
    // Marked mutable so that const methods can rehash into this container when
    // necessary.
    mutable buckets_t buckets_;

    // A linked list of all lock containers. We never discard lock containers,
    // since there is currently no mechanism for detecting when all threads are
    // done looking at the memory. The back lock container in this list is
    // designated the "current" one, and is used by all operations taking locks.
    // This container can be modified if either it is empty (which should only
    // occur during construction), or if the modifying thread has taken all the
    // locks on the existing "current" container. In the latter case, a
    // modification must take place before a modification to the hashpower, so
    // that other threads can detect the change and adjust appropriately. Marked
    // mutable so that const methods can access and take locks.
    mutable locks_t locks_;

    size_t ttl_;
};

}

#endif  // COMMON_BASE_CUCKOOHASHCACHE_H_