# 文档风格指南

**目的** 本指南旨在指导开发人员编写文档。

## 目标

本指南可帮助开发人员统一文档风格，并提供文档编写帮助。

## 非目标

本指南不是行业标准，只为方便文档格式统一而用，没有绝对的对错。

本指南也将持续更新，更新后之前的文档不会随之更新。我们会努力维护文档风格的一致性，但是部分文档可能与本指南风格不符。如有疑问，请以本指南为准。

## 打破常规

**Nebula Graph** 不会在任何时候强加不适用行文语境的文档格式。但是我们希望保证文档的高质量。本指南旨在提高文档质量，保证行文风格一致，因此，在某些情况下，有必要与我们的指导原则有所不同，以使您的文档更好。

## 行文风格及语气

在文档中，力求对话性，友好和受人尊敬的语气，而不要过于口语化或轻浮；应随意，自然，平易近人，而不是命令或急躁。应使文档读起来像与一个博学多才的朋友谈话，确保文档充分理解开发人员的想法。

### 需要考虑的要点

- 如果您在表达某些内容时遇到麻烦，请退后一步问自己：“我想说什么？”通常，您给自己的答案揭示了您在文档中想表达的意思。
- 如果不确定表述用词，请找同事寻求帮助。
- 大声朗读或仔细阅读文档的某些部分，确认句子是否自然。每个句子的发音都听起来很自然。并不是所有句子都要自然，毕竟这些是书面文件。但是，如果句子很尴尬或令人困惑，请重新组织语言。
- 在句子之间使用过渡。
- 即使在文档行文风格上遇到问题，也请确保文档明确清晰地传达了有效信息，这是重中之重。

## 时态

通常，使用现在时而不是将来时。
读者将来会编写和运行代码并不是使用将来时态的充分理由。请坚持现在时。

## 链接

在编写链接文本时，请使用一个短语描述读者在点链接后将看到的内容。可以采用以下两种形式之一：

- 链接页面的准确标题，大写与标题大写相同。
- 链接页面的描述。

不要在链接内包含：

- “点击此处”
- “本文”
- 文档中不要出现 URL

### 链接标点

如果在链接之前或之后有标点符号，请尽可能将标点符号放在链接标签之外。特别是将引号放在链接标签之外。

## 文档内容

### 注意事项

- 确保读者仅使用键盘即可使用文档，而无需使用鼠标或触控板，即可阅读文档的所有部分（包括选项卡，提交表单的按钮和交互式元素）。

- 不要将颜色，大小，位置或其他视觉提示用作传达信息的主要方式。
  - 如果一定要使用颜色，图标或轮廓线粗细来传达状态，还应提供辅助提示，例如更改文本标签。
  - 请使用标签关联按钮和其他元素的标签而非位置或形状。
- 避免使用不必要的字体格式。
- 如果是为包含特殊功能的产品写文档，请明确记录这些功能。例如，“ gcloud” 命令行工具包括可切换的辅助功能，例如进度条百分比和 ASCII 框渲染。

## 图片

- 请为图片提供文字说明。
- 不要在图像中显示新信息；始终在图像上提供等效的文字说明。
- [Use SVG files](https://developers.google.cn/style/images) 或 PNG 图片。
- 请提供高像素图片 [Provide high-resolution images](https://developers.google.cn/style/images#high-resolution-images)。

## 表格

- 如果表格同时包含行和列标题，使用以下标准标注 [scope](https://www.w3.org/WAI/tutorials/tables/two-headers/)。
- [headers](https://www.w3.org/WAI/tutorials/tables/multi-level/) 属性。

## 形式

- 使用`<label>`元素标记每个输入字段。
- 将标记放在字段之外。
- 当创建用于表单验证的错误消息时，请明确说明出了什么问题以及如何解决。例如：“名称是必填字段。”

## 视频

- 视频需添加文字说明。
- 请确保文字说明可翻译成其他语言。

## 语言及语法

- 推荐使用第二人称
- 请使用主动语态清晰表述动作的发起人。请使用标准的标点符号。

## 格式、标点符号及组织方式

- 文档及章节命名见 [Use sentence case](https://developers.google.cn/style/capitalization)。
- 有序列表见 [Use numbered lists](https://developers.google.cn/style/lists)。
- 其他列表见 [Use bulleted lists](https://developers.google.cn/style/lists)。
- 数据相关见 [Use description lists](https://developers.google.cn/style/lists)。
- 标点符号见 [Use serial commas](https://developers.google.cn/style/commas)。
- [Put code-related text in code font](https://developers.google.cn/style/code-in-text).
- [Put UI elements in bold](https://developers.google.cn/style/ui-elements).
- [Use unambiguous date formatting](https://developers.google.cn/style/dates-times).
