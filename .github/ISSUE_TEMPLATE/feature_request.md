name: Feature Request
description: Suggest an idea for HugRS
labels: [enhancement]
body:
  - type: markdown
    attributes:
      value: |
        感谢你为 HugRS 提出功能建议 / Thanks for suggesting a feature for HugRS.

  - type: textarea
    id: problem
    attributes:
      label: 背景与动机 / Problem & Motivation
      description: 这个功能解决什么问题？你的使用场景是什么？ / What problem does this solve? What's your use case?
    validations:
      required: true

  - type: textarea
    id: proposal
    attributes:
      label: 方案描述 / Proposal
      description: 你期望的解决方案是什么样的？ / What would you like to see?
    validations:
      required: true

  - type: textarea
    id: alternatives
    attributes:
      label: 替代方案 / Alternatives
      description: 是否考虑过其他实现方式？ / Any alternative solutions you've considered?

  - type: textarea
    id: context
    attributes:
      label: 补充信息 / Additional Context
      description: 截图、参考链接等 / Screenshots, references, etc.
