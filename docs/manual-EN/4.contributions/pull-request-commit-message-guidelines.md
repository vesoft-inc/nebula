# Pull Request and Commit Message Guidelines

This document describes the commit message and Pull Request style applied to all **Nebula Graph** repositories. Every commit made *directly* to the `master` branch must follow the below guidelines.

## Commit Message

```bash
<type>(<scope>): <subject> // scope is optional, subject is must

                <body> // optional

                <footer> // optional
```

These rules are adopted from the [AngularJS commit convention](https://docs.google.com/document/d/1QrDFcIiPjSLDn3EL15IJygNPiHORgU1_OOAqWjiDU5Y/edit).

- `<Type>` describes the kind of change that this commit is providing.
- `<subject>` is a short description of the change.
- If additional details are required, add a blank line, and then provide explanation and context in paragraph format.

### Commit Types

Type     | Description
---------| ----------------
Feature  | New features
Fix      | Bug fix
Doc      | Documentation changes
Style    | Formatting, missing semi colons, ...
Refactor | Code cleanup
Test     | New tests
Chore    | Maintain

## Pull Request

When you submit a Pull Request, please include enough details about all changes in the title but keep it concise.

The title of a pull request must briefly describe the changes made.

For very simple changes, you can leave the description blank as there’s no need to describe what will be obvious from looking at the diff. For more complex changes, give an overview of the changes. If the PR fixes an issue, make sure to include the GitHub issue-number in the description.

### Pull Request Template

```bash
What changes were proposed in this pull request?

Why are the changes needed?

Does this PR introduce any user-facing change?

How was this patch tested?
```
