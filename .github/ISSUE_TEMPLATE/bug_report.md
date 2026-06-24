name: Bug Report
description: Report a bug in HugRS
labels: [bug]
body:
  - type: markdown
    attributes:
      value: |
        感谢你报告 HugRS 的问题 / Thanks for taking the time to report a bug.

  - type: textarea
    id: description
    attributes:
      label: 问题描述 / Bug Description
      description: 发生了什么？期望什么行为？ / What happened? What did you expect?
    validations:
      required: true

  - type: textarea
    id: reproduce
    attributes:
      label: 复现步骤 / Steps to Reproduce
      description: 请提供可复现的最小步骤 / Minimal reproducible steps.
      placeholder: |
        1. Run `cargo run -- serve`
        2. POST `/files/pull` with `{"repo":"..."}`
        3. Observe error
    validations:
      required: true

  - type: textarea
    id: environment
    attributes:
      label: 环境信息 / Environment
      description: 版本、OS、存储后端等 / Version, OS, storage backend, etc.
      placeholder: |
        - Version: `cargo run -- --version`
        - OS: macOS 14 / Ubuntu 24.04
        - Storage: local / S3
    validations:
      required: true

  - type: textarea
    id: logs
    attributes:
      label: 日志 / Logs
      description: 相关日志输出或错误信息 / Relevant logs or error messages.

  - type: textarea
    id: context
    attributes:
      label: 补充信息 / Additional Context
      description: 截图、参考链接等 / Screenshots, references, etc.
