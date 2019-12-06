# Developer Documentation Style Guide

**Key Point:** Use this guide as a style reference for our developer documentation.

## Goals

The guide can help you avoid making decisions
about the same issue over and over, can
provide editorial assistance on structuring
and writing your documentation, and can help
you keep your documentation consistent with
our other documentation.

## Non-Goals

This guide isn't intended to provide an industry documentation standard, nor to compete with other well-known style guides. It's a description of our house style, not a statement that our decisions are objectively correct.

This guide is a living document; it changes over time, and when it changes, we generally don't change previously published documentation to match. We strive for consistency to the extent feasible, but at any given time there are certain to be parts of our documentation that don't match this style guide. When in doubt, follow this guide rather than imitating existing documents.

## Breaking the "Rules"

In most contexts, **Nebula Graph** has no ability nor desire to enforce these guidelines if they're not appropriate to the context. But we hope that you'll join us in striving for high-quality documentation.

Like most style guides, our style guide aims to improve our documentation, especially by improving consistency; therefore, there may be contexts where it makes sense to diverge from our guidelines in order to make your documentation better.

## Style and Authorial Tone

Aim, in your documents, for a voice and tone that's conversational, friendly, and respectful without being overly colloquial or frivolous; a voice that's casual and natural and approachable, not pedantic or pushy. Try to sound like a knowledgeable friend who understands what the developer wants to do.

Don't try to write exactly the way you speak; you probably speak more colloquially and verbosely than you should write, at least for developer documentation. But aim for a conversational tone rather than a formal one.

### Some Techniques and Approaches to Consider

- If you're having trouble expressing something, step back and ask yourself, "What am I trying to say?" Often, the answer you give yourself reveals what you should be saying in the document.
- If you're uncertain about your phrasing or tone, ask a colleague to take a look.
- Try reading parts of your document out loud, or at least mouthing the words. Does it sound natural? Not every sentence has to sound natural when spoken; these are written documents. But if you come across a sentence that's awkward or confusing when spoken, consider whether you can make it more conversational.
- Use transitions between sentences. Phrases like "Though" or "This way" can make paragraphs less stilted. (Then again, sometimes transitions like "However" or "Nonetheless" can make paragraphs more stilted.)
- Even if you're having trouble hitting the right tone, make sure you're communicating useful information in a clear and direct way; that's the most important part.

## Tense

In general, use present tense rather than future tense; in particular, try to avoid using will where possible.

The fact that the reader will be writing and running code in the future isn't a good reason to use future tense. Stick with present tense where. Also avoid the hypothetical future ***would***.

## Links

When you're writing link text, use a phrase that describes what the reader will see after following the link. That can take either of two forms:

- The exact title of the linked-to page, capitalized the same way the title is capitalized.
- A description of the linked-to page, capitalized like ordinary text instead of like a title.

A couple of specific things to not do in link text:

- Don't use the phrase "click here." (It's bad for accessibility and bad for scannability.)
- Similarly, don't use phrases like "this document." (It's easy to read "this" as meaning "the one you're reading now" rather than "the one I'm pointing to.")
- Don't use a URL as link text. Instead, use the page title or a description of the page.

### Punctuation With Links

If you have punctuation immediately before or after a link, put the punctuation outside of the link tags where possible. In particular, put quotation marks outside of link tags.

## Accessible Content

### General dos and Don'ts

- Ensure that readers can reach all parts of the document (including tabs, form-submission buttons, and interactive elements) using only a keyboard, without a mouse or trackpad.
- Don't use color, size, location, or other visual cues as the primary way of communicating information.
  - If you're using color, icon, or outline thickness to convey state, then also provide a secondary cue, such as a change in the text label.
  - Refer to buttons and other elements by their label (or `aria-label`, if they’re visual elements), not by location or shape.
- Avoid unnecessary font formatting. (Screen readers explicitly describe text modifications.)
- If you're documenting a product that includes specialized accessibility features, then explicitly document those features. For example, the `gcloud` command-line tool includes togglable accessibility features such as percentage progress bars and ASCII box rendering.

## Images

- For every image, provide alt text that adequately summarizes the intent of each image.
- Don't present new information in images; always provide an equivalent text explanation with the image.
- [Use SVG files](https://developers.google.cn/style/images) or crushed PNG images.
- [Provide high-resolution images](https://developers.google.cn/style/images#high-resolution-images) when practical.

## Tables

- If your tables include both row and column headings, then mark heading cells with the [scope](https://www.w3.org/WAI/tutorials/tables/two-headers/) attribute.
- If your tables have more than one row containing column headings, then use the [headers](https://www.w3.org/WAI/tutorials/tables/multi-level/) attribute.

## Forms

- Label every input field, using a `<label>` element.
- Place labels outside of fields.
- When you're creating an error message for form validation, clearly state what went wrong and how to fix it. For example: "Name is a required field."

## Videos

- Provide captions.
- Ensure that captions can be translated into major languages.

## Language and Grammar

- Use second person: "you" rather than "we."
- Use active voice; make clear who's performing the action.
Use standard American spelling and punctuation.
Put conditional clauses before instructions, not after.
For usage and spelling of specific words, see the word list.

## Formatting, Punctuation, and Organization

- [Use sentence case](https://developers.google.cn/style/capitalization) for document titles and section headings.
- [Use numbered lists](https://developers.google.cn/style/lists) for sequences.
- [Use bulleted lists](https://developers.google.cn/style/lists) for most other lists.
- [Use description lists](https://developers.google.cn/style/lists) for pairs of related pieces of data.
- [Use serial commas](https://developers.google.cn/style/commas).
- [Put code-related text in code font](https://developers.google.cn/style/code-in-text).
- [Put UI elements in bold](https://developers.google.cn/style/ui-elements).
- [Use unambiguous date formatting](https://developers.google.cn/style/dates-times).
