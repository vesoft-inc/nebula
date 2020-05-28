/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_STRINGUNORDEREDMAP_H_
#define COMMON_BASE_STRINGUNORDEREDMAP_H_

#include <unordered_map>
#include <string>

namespace nebula {

template<typename T>
class StringUnorderedMap {
public:
    using key_type = std::string;
    using mapped_type = T;
    using value_type = std::pair<std::string, T>;
    using size_type = size_t;
    using hasher = folly::SpookyHashV2;
    using reference = value_type&;
    using const_reference = const value_type&;
    // using iterator =
    // using const_iterator =

    /******************************************
     *
     * Constructors and destructor
     *
     *****************************************/
    StringUnorderedMap() = default;
    StringUnorderedMap(const StringUnorderedMap& other);
    StringUnorderedMap(StringUnorderedMap&& other);
    StringUnorderedMap(std::initializer_list<value_type> init);

    template<typename InputIt>
    StringUnorderedMap(InputIt first, InputIt last);

    ~StringUnorderedMap() = default;

    /******************************************
     *
     * Assignmets
     *
     *****************************************/
    StringUnorderedMap& operator=(const StringUnorderedMap& other);
    StringUnorderedMap& operator=(StringUnorderedMap&& other);
    StringUnorderedMap& operator=(StringUnorderedMap&& other) noexcept;
    StringUnorderedMap& operator=(std::initializer_list<value_type> ilist);

    /******************************************
     *
     * Iterators
     *
     *****************************************/
    iterator begin() noexcept;
    const_iterator begin() const noexcept;
    const_iterator cbegin() const noexcept;
    iterator end() noexcept;
    const_iterator end() const noexcept;
    const_iterator cend() const noexcept;

    /******************************************
     *
     * Capacity
     *
     *****************************************/
    bool empty() const noexcept;
    size_type size() const noexcept;
    size_type max_size() const noexcept;

    /******************************************
     *
     * Modifier
     *
     *****************************************/
    void clear() noexcept;

    std::pair<iterator, bool> insert(const value_type& value);
    std::pair<iterator, bool> insert(value_type&& value);
    template<class V>
    std::pair<iterator, bool> insert(V&& value);
    template<class InputIt>
    void insert(InputIt first, InputIt last);
    void insert(std::initializer_list<value_type> ilist);

    template<class V>
    std::pair<iterator, bool> insert_or_assign(const key_type& key, V&& v);
    template<class V>
    std::pair<iterator, bool> insert_or_assign(key_type&& key, V&& v);

    template< class... Args >
    std::pair<iterator, bool> emplace(Args&&... args);

    iterator erase(const_iterator pos);
    iterator erase(const_iterator first, const_iterator last);
    size_type erase(const key_type& key);

    void swap(StringUnorderedMap& other);
    void swap(StringUnorderedMap& other) noexcept

    /******************************************
     *
     * Lookup
     *
     *****************************************/
    T& at(const key_type& key);
    const T& at(const key_type& key) const;

    T& operator[](const key_type& key);
    T& operator[](key_type&& key);

    size_type count(const key_type& key) const;

    iterator find(const key_type& key);
    const_iterator find(const key_type& key) const;

    std::pair<iterator, iterator> equal_range(const key_type& key);
    std::pair<const_iterator, const_iterator> equal_range(const key_type& key) const;

private:
    std::unordered_map<uint64_t, T> valueMap_;
    std::unordered_map<uint64_t, std::string> keyMap_;
};


template<typename T>
bool operator==(const StringUnorderedMap<T>& lhs,
                const StringUnorderedMap<T>& rhs);
template<typename T>
bool operator!=(const StringUnorderedMap<T>& lhs,
                const StringUnorderedMap<T>& rhs);

}  // namespace nebula
#endif  // COMMON_BASE_STRINGUNORDEREDMAP_H_

