# Almost Always Auto

`auto` might make others be a little confused about the deduced types, but it has more advantages:
  * It saves keyboard strikings.
  * It make the variable initialization mandatory.
  * It requires less overhead while refactoring.


# Always Use `override` on Inherited Method

By doing this way, if you happen to write the wrong signature, the compiler would warn you. Otherwise, say that you use `virtual` instead, you are declaring a new virtual method, but not a inherited one.


# Almost Never Use `new` and `delete` Explicitly

Principals to choose between `std::shared_ptr`, `std::unique_ptr` and raw pointer:
  * Whenever you deal with some resource, you MUST sort out its ownership, thoroughly. If not, dont't touch it. If can't, ask the author.
  * Use `std::unique_ptr`
    * When you allocate and use a resource locally.
    * When you own the resource or pass a resource around, and nobody else needs to keep, use or share it.
  * Use `std::shared_ptr`
    * When multiple places need to own a resource at the same time, and the owners have loose dependencies on each other.
  * Use raw pointers
    * You could keep a raw pointer to some resource, only if the resource is owned by some other resource(s) and it would stay availble during the whole time you holding it.
    * We hope we could accomplish the goal that whenever one sees a raw pointer, he has no need to concern about the ownership of the resource. Once not able to, add comments about the ownership wherever it appears.

Remember, remember, all is about ownership.


# What does the accociated lock of conditional variable protect?

Usually, the lock could be used to protect the related data structures from race condition, but it's not designed to. Actually, it protects the **condition**, to be precise, the **condition changing**. For example, if the condition is simply a boolean value or some inherently thread safe data(as we all know, that write or read on a boolean value on x86_64 platform is atomical). This may tempt us to not acquire the lock while we changing it, but this couldn't be more wrong.

BTW. another common mistake we may make while using conditional variable is that, we forgot to check the condition before we fall asleep via `wait`, which will lead us to miss the notify event(actually, we already *missed* it, since it arrived before we acquire the lock). Another pitfall is that we must check on the condition after we are woken up, to prevent the spurious wakeup.

Here are some bad cases:
```c++
AtomicData data;
std::mutex lock;
std::conditional_variable cond;

void thread1() {
    std::unique_lock<std::mutex> guard(lock);
    cond.wait(guard, [&]() { return data.ready(); });
}

void thread2() {
    {
        // Here we change the condition without the `lock' held.
        // If the condition were changed after thread1 had checked on it,
        // and the following notification arrives before thread1 falls asleep,
        // thread1 will never be woken up.

        // Otherwise, if we change the condition with the lock held,
        // only two cases could happen while we changing the condition:
        // 1. thread1 is waiting on the lock. In this case, after we release the lock,
        //    thread1 will see it, no need to wait or to be woken.
        // 2. thread1 has already fallen into sleep(and the lock has been released),
        //    then the following notification would wake it up.
        data.modifySafely();
    }
    cond.notify_one();
}
```

```c++
AtomicData data;
std::mutex lock;
std::conditional_variable cond;

void thread1() {
    std::unique_lock<std::mutex> guard(lock);
    if (!data.ready()) {
        cond.wait(guard);    // when `wait' returns, the condition might not ready.
    }
}

void thread2() {
    {
        std::unique_lock<std::mutex> guard(lock);
        data.modifySafely();
    }
    cond.notify_one();
}
```

```c++
AtomicData data;
std::mutex lock;
std::conditional_variable cond;

void thread1() {
    std::unique_lock<std::mutex> guard(lock);
    cond.wait(guard);    // this is the silliest mistake.
}

void thread2() {
    {
        std::unique_lock<std::mutex> guard(lock);
        data.modifySafely();
    }
    cond.notify_one();
}
```

Here is the right way:

```c++
AtomicData data;
std::mutex lock;
std::conditional_variable cond;

void thread1() {
    std::unique_lock<std::mutex> guard(lock);
    while (!data.ready()) {
        // this loop is exactly the same to cond.wait(guard, [&]() { return data.ready(); });
        cond.wait(guard);
    }
}

void thread2() {
    {
        std::unique_lock<std::mutex> guard(lock);
        data.modifySafely();
    }
    cond.notify_one();  // whether the notification is protected by lock or not does not make much difference.
}
```

# Prefer `using` to `typedef`

The reason to always use `using`, aka. type alias, is simple: `using` is a superset over `typedef`, besides, `using` is more readable, and most importantly, it could be templated.


# Try Hard to Avoid Defining Global Type Aliases

Of course, type alias saves a lot of keyboard strikings and line breaks for us, thus we encourage to use type aliases. But it also introduces problems:
  * It may pollute the naming space.
  * It is hard and annoying for others to reason about the real type.

So our suggestions are:
  * Prefer defining type alias in block scopes to in function scopes, to in class scopes, to in file scopes, to in the global scope.
  * We even prefer giving one type a alias in every compile unit to defining one globally.


# Either Declare a Class `final` or Make the Destructor `virtual`

This is a convention, just comply with it.


# Use `const` Whenever Possible

  * For a parameter of type of reference or pointer , make it `const` whenever it is not being modified.
  * For a readonly member function, make it `const`.


# Pass By `const &` or By Value?

While defining a function, you might have to decide whether to pass parameters by `const &` or by value. Here are the suggestions:
  * When an argument is used in a readonly way, without making a copy from it, use `const &`.
  * When you are about to copy from the argument, AND move is not cheaper than copy, such as some aggregation structures, use `const &`.
  * Like above, but move is cheaper than copy, pass it by value and always move from it inside the function, thus the caller could choose whether to make the copy or not.


# Don't Make a Return Type `const` If It's By Value

It's non-movable and makes nothing better.


# Qualify the Move Constructor and Assignment with `noexcept`

If you want your class to benefit from the move semantics, and your class are being used in STL containers like `std::vector`.
This is because some STL containers utilize `std::move_if_noexcept` under the hood. If the elements' move constructor or assignment are not qualified with `noexcept`, the movement would be decayed to copy in some actions, such as expansion of `std::vector`.

You could verify this behaviour with the following example:

```c++
class X {
public:
    X() {}
    X(X&&) NOEXCEPT { fprintf(stderr, "Move\n"); }
    X(const X&) { fprintf(stderr, "Copy\n"); }
};

int main() {
    std::vector<X> v(4);
    v.emplace_back();
    return 0;
}
```
```bash
$ g++ -std=c++11 main.cpp -DNOEXCEPT=
$ ./a.out
Copy
Copy
Copy
Copy
$ g++ -std=c++11 main.cpp -DNOEXCEPT=noexcept
$ ./a.out
Move
Move
Move
Move
```

Also, since `std::move_if_noexcept` returns a reference of type `T&&` if `T` has a `noexcept` move constructor, or returns `const T&` if not, you could inspect a type through the compiler error messages of following:
```c++
template <typename> class X;
int main() {
    std::string s;
    std::function<void()> f;
    X<decltype<std::move_if_noexcept(T)> x;
    return 0;
}
```

```bash
$ g++ -std=c++11 main.cpp -DT=s
In function 'int main()':
error: aggregate 'X<std::basic_string<char>&&> x' has incomplete type and cannot be defined
     X<decltype(std::move_if_noexcept(s))> x;
$ g++ -std=c++11 main.cpp -DT=f
In function 'int main()':
error: aggregate 'X<const std::function<void()>&> x' has incomplete type and cannot be defined
     X<decltype(std::move_if_noexcept(f))> x;
```

The above example tells that the move constructor of `std::function` is not qualified `noexcept` movable.


# What's `volatile`?

  * It has nothing to do with threading.
  * It only affects the code generation of compiler, i.e. issue a memory fetching instruction every time you read the qualified variable, instead of possibly caching it in registers.


```c
//~ add.c
#ifndef VOLATILE
#define VOLATILE
#endif
VOLATILE long a = 0;
VOLATILE long b = 1;

void add() {
    a += 1;
    b += 1;
    a += 1;
    b += 1;
}
```

```bash
$ cc -O2 -S add.c
$ cat add.s
```
```asm
add:
        movq    b(%rip), %rax
        addq    $2, a(%rip)
        addq    $2, %rax
        movq    %rax, b(%rip)
        ret
```

```bash
$ cc -DVOLATILE=volatile -O2 -S add.c
$ cat add.s
```
```asm
add:
        movq    a(%rip), %rax
        addq    $1, %rax
        movq    %rax, a(%rip)
        movq    b(%rip), %rax
        addq    $1, %rax
        movq    %rax, b(%rip)
        movq    a(%rip), %rax
        addq    $1, %rax
        movq    %rax, a(%rip)
        movq    b(%rip), %rax
        addq    $1, %rax
        movq    %rax, b(%rip)
        ret
```

# Use `do {} while(false)` instead of `goto`

`goto` is good at error handling, and if used with care, it often makes the code more readable than piles of `if`s. But that is the case in C, as for C++, since we are apt to declare objects right before they are actually used and `goto` is forbidden to jump across the initialization point, so we suggest to never use `goto`.

As a workground, a `do while(false)` loop can somewhat do the same job. Follow is a showcase:

```cpp
if (a) {
    ...
    if (b) {
        ...
        if (c) {
            ...
            if (d) {
                ...
            }
        }
    }
}
do {
    if (!a) {
        break;
    }
    ...
    if (!b) {
        break;
    }
    ...
    if (!c) {
        break;
    }
    ...
    if (!d) {
        break;
    }
    ...
} while (false);
```

As a supplement, we encourage you to try to make every single line of code deliberate:
  * Try to make the pair of `if` `else` blocks reside in a single view.
  * Try not to introduce too many indentations, i.e. nesting levels.
  * Try to avoid the needs to break one logical line into multiple. (e.g. long names, function call with too many arguemnts)
