# Summary

Manage the allocate and deallocate of AST object by Arena technology.

# Motivation

The Arena is used to reduce the count of memory allocate and deallocate for many small objects. And AST object is perfect fit this. And construct the objects in continuously block will speed up access according to the locality principle.

# Usage explanation

Currently, we had `ObjPool` to manage AST objects, but it's a simple container which don't change the memory management of AST. It's still manged by the default c++ Allocator.

Now will refactor the `ObjPool` to manage the memory of AST as below:

```c++
class ObjPool {
// ...
    // All AST object must construct from here
    Obj* makeAndAdd(...) {
        void *ptr = arena_.allocateAligned(sizeof(Obj));
        Obj *obj = new (ptr) Obj(...);
        return obj;
    }

    ~ObjPool() {
        for (const auto& obj : objs_) {
            // The new delete function is [](void *p) { reinterpret_cast<T*>(p)->~T() };
            obj->deleteFn();
        }
    }

    Arean arena_;
    List  objs_;
};
```

# Design explanation

The Arena is the technology to pre-allocate large block memory and construct objects on it. It's different from the normal way which allocate then construct one by one. In this way, Arena reduce the count of allocate and deallocate calling.

In Nebula Graph, with the placement new operator of c++, we could construct the object in specific address. So we could construct the object in memory allocated from Arena.

But the stl container members of AST are still managed by c++ default Allocator. For example, a classical Expression definition as below:

```c++
class AttributeExpr {
// ...
    Expression *left;
    std::string right;
};
```

The memory of `AttributeExpr` itself is managed by Arena, but the `std::string` will allocate/deallocate memory by c++ default Allocator although `std::string` itself is managed by Arena.

This trade-off will discussed in next section.

# Rationale and alternatives

The c++ stl container also provide the template argument for custom allocator. But it make container type different. So the same container with different allocator is different type, and need extra conversion.

The polymorphic container override the this only if change all container type to polymorphic. It's too large change to do.

# Drawbacks

# Prior art

# Unresolved questions

The stl container memory management.

# Future possibilities
