---
name: "🐛 Bug Report"
about: I want to report a bug.
title: ''
labels: type/bug
assignees: ''
body:
  - type: checkboxes
    id: no-duplicate-issues
    attributes:
      label: "⚠️ Please verify that this bug has NOT been raised before."
      description: "Search in the issues sections by clicking [HERE](https://github.com/vesoft-inc/nebula/issues?q=). Please check the [FAQ](https://docs.nebula-graph.com.cn/master/20.appendix/0.FAQ/) documentation before raising an issue in case someone has asked the same question that you are asking."
      options:
        - label: "I checked issues and [FAQ](https://docs.nebula-graph.com.cn/master/20.appendix/0.FAQ/) and didn't find similar issue"
          required: true
  - type: textarea
    id: description
    validations:
      required: true
    attributes:
      label: "Description"
      description: "A clear and concise description of what the bug is."
  - type: textarea
    id: environment-and-version
    validations:
      required: true
    attributes:
      label: "Environment"
      description: "A clear and concise description of what the bug is."
      placeholder: |
        * OS: `uname -a`
        * Compiler: `g++ --version` or `clang++ --version`
        * CPU: `lscpu`
        * Commit id (e.g. `a3ffc7d8`)"
  - type: textarea
    id: steps-to-reproduce
    validations:
      required: true
    attributes:
      label: "👟 Reproduction steps"
      description: "How do you trigger this bug? Please walk us through it step by step."
      placeholder: "..."
  - type: textarea
    id: expected-behavior
    validations:
      required: true
    attributes:
      label: "👀 Expected behavior"
      description: "What did you think would happen?"
      placeholder: "..."
  - type: textarea
    id: actual-behavior
    validations:
      required: true
    attributes:
      label: "😓 Actual Behavior"
      description: "What actually happen?"
      placeholder: "..."
  - type: textarea
    id: logs
    attributes:
      label: "📝 Relevant log output & config"
      description: Please copy and paste any relevant log output/ config or traces. This will be automatically formatted into code, so no need for backticks.
      render: shell
    validations:
      required: false
