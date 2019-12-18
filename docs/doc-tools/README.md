# Generate documentation in PDF

This document teaches you how to make documentation in PDF.

## Step One: Merge

Use the provided script `merge-all.py` to merge all the markdown doc files into one.

## Step Two: Generate TOC

Use pandoc to generated TOC (table of content) for the file you just merged. Firstly, you should make sure that pandoc has been installed on your machine. Open your terminal and run the command:

```bash
pandoc -v
```

If pandoc is not installed, please install it first.

**Windows**

```bash
choco install pandoc
```

**macOS**

```bash
brew install pandoc
```

If you have any questions on pandoc installation, please check its document [here](https://pandoc.org/installing.html).

To generate TOC, you should first change directory to the merged file and type the following command:

```bash
pandoc -s --toc merged.md -o merged.md
```

**Note**: The default number of section levels is 3 in the table of contents (which means that level-1, 2, and 3 headings will be listed in the contents), use `--toc-depth=NUMBER` to specify that number.

## Step Three: Generate PDF

You can convert the merged markdown file into PDF and print it out for easy-reading. Use the following command to generate PDF:

```bash
pandoc merged.md  -o merged.pdf
```

**Note:** Make sure [MiKTeX](https://miktex.org/howto/install-miktex) is installed.

Now you've got your PDF documentation and have fun with **Nebula Graph**.
