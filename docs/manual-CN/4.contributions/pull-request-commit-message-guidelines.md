# PR 和 Commit Message 指南

本文档介绍的 PR 和 Commit Message 指南适用于所有 **Nebula Graph** 仓库。 所有提交至 `master` 分支的 commit 均必须遵循以下准则。

## Commit Message

```bash
<type>(<scope>): <subject> // scope is optional, subject is must

                <body> // optional

                <footer> // optional
```

本指南参照 [AngularJS commit 规则](https://docs.google.com/document/d/1QrDFcIiPjSLDn3EL15IJygNPiHORgU1_OOAqWjiDU5Y/edit)。

- `<Type>` 描述 commit 类型。
- `<subject>` 是 commit 的简短描述。
- 如需添加详细信息，请添加空白行，然后以段落格式进行添加。

### Commit 类型

Type     | Description
---------| ----------------
Feature  | 新功能
Fix      | 修复 bug
Doc      | 文档
Style    | 代码格式
Refactor | 代码重构
Test     | 增加测试
Chore    | 构建过程或辅助工具的变动

## Pull Request

提交 PR 时，请在标题中包含所有更改的详细信息，并确保标题简洁。

PR 标题必需简明概括更改信息。

对于显而易见的简单更改，可不必添加描述。如果 PR 涉及复杂更改，请对更改进行概述。如果 PR 修复了相关 issue，请关联。

### Pull Request 模板

```bash
What changes were proposed in this pull request?

Why are the changes needed?

Does this PR introduce any user-facing change?

How was this patch tested?
```
