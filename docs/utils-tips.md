# Bash

Preferences/Profiles/Keys, designate Option as Esc.

  * `Ctrl + a` or `Ctrl + e`, to move the cursor to start or end of the line.
  * `Ctrl + w`, to delete a consecutive non-spaces string from left side of the cursor.
  * `Ctrl + u`, to delete upto the start of the line from the cursor.
  * `Ctrl + y`, to paste from the previous deletion.
  * `Ctrl + h`, equivalent to backspace.
  * `Ctrl + p` or `Ctrl + n`, to traverse the command line history upwards or downwards.
  * `Ctrl + r`, to search by pattern in the command line history.
  * `Ctrl + l`, to clear the screen.
  * `Alt + Backspace`, to delete backwards by word.
  * `Alt + .`, to complete with the last argument of the previous command, like `!$`.
  * `Alt + f` or `Alt + b`, to move the cursor forward or backward by words.

## Useful Commands by Examples

  * `sudo !!` to relaunch the previous command with sudo.
  * `file core.1234`, check filetypes(show the corresponding excutable for a coredump).
  * `echo $((1<<20))`, simple integer calculation.
  * `printf "%x\n" 1024`, simple base conversion.
  * `echo 3 | sudo tee /proc/sys/vm/drop_caches`, or `sudo echo 3 > /proc/sys/vm/drop_caches` results in a no permission error.
  * `diff <(grep key file-a) <(grep key file-b)`, take outputs of commands as a file, without creating temporary files.


## Notes

  * Use `$()` instead of ``, especially in the nested occassion.


# Vim

word: traditional word; WORD: consecutive charactors without spaces.

 * Normal mode: cursor movement, aka. motion.
   * You could prefix every movement command with a number, to do the movement multiple times.
   * `w`, to move forward by one word. (`b` for backwards)
   * `W`, to move forward by one WORD. (`B` for backwards)
   * `e`, to move forward by one word, but locate the cursor at the end charactor.
   * `E`, to move forward by one WORD, but locate the cursor at the end charactor.
   * `*`, to search the next matched word with the current one. (`#` for backward search)
   * `n` or `N`, to repeat the `*` or `#` searching.
   * `H`, to move the cursor to the top of the screen, `M` for middle, `L` for the bottom.
   * `gg`, to go back to the first line of this document, `G` to the last line.
   * `0`, to go to the first charactor of the current line, `$` to the last
   * `^`, to go to the first non-space charactor of the current line, `g_` to the last.
   * `zz`, to make the current line at the middle of the screen, `zt` for top, `zb` for bottom.
   * `<ctrl>-d` or `<ctrl>-u`, to scroll down or up by half page/screen.
   * `<ctrl>-f` or `<ctrl>-b`, to scroll forward or backward by one page/screen.
 * Normal mode: edit
   * `yy`, to yank(copy) the current line.
   * `dd`, to yank and delete current line.
   * `p`, to paste after.
   * `P`,to paste before.
   * `x', to yank and delete the current charactor.
   * `X`, to yank and delete the previous charactor.
   * `ddp`, to exchange current line with next line.
   * `xp`, to exchange current charactor with the next charactor.
   * `r`*x*, to replace the current charactor with a charactor *x*.
   * *N*`<ctrl>-a` or *N*`<ctrl>-x`, to increase or decrease the current number by *N*, applied to decimal, hexadecimal, and octal numbers.
   * `==`, to indent the current line.
   * `>>`, to increase indentation.
   * `<<`, to decrease indentation.
   * `u`, to undo the last edit.
   * `<ctrl>-r`, to redo the last undo.
   * `.`, to repeat the last edit.
 * Enter edit mode
   * `i`, to insert in front of the cursor.
   * `a`, to append back to the cursor.
   * `I`, to insert at the first non-space position of the current line.
   * `A`, to append at the end of this line.
   * `cc`, to clear the whole line and enter the edit mode.
   * `C`, to clear upto the end of the line from the cursor and enter the edit mode.
   * `cw`, to clear a word and enter the edit mode.
   * `o`, to open a newline after the current line.
   * `O`, to open a newline before the current line.
 * Edit with text objects
   * Text Objects
     * `a` for a whole object, `i` for inner the object.
     * `ab` or `a(`, a block surrounded by `()`.
     * `aB` or `a{`, a block surrounded by `{}`.
     * `a[` or `a]`, a block surrounded by `[]`.
     * `a<` or `a>`, a block surrounded by `<>`.
     * `a'`, a single quoted block.
     * `a"`, a double quoted block.
   * `dab`, to delete a `()` block.
   * `ciB`, to change text inner the `{}` block.
   * `=iB`, auto indent text inside the `{}` block.
   * `yaB`, to yank the whole `{}` block.
 * Edit mode
   * `<ctrl>-w`, to delete word backwards.
   * `<ctrl>-t`, to increase indentation.
   * `<ctrl>-d`, to decrease indentation.
   * `<ctrl>-n`, auto complete the current word, navigate candidates with `<ctrl>-n` and `<ctrl-p>`.
   * `<esc>` or `<ctrl>-[`, to exit from edit mode.
 * Select mode
   * `v`, to select freely.
   * `<shift>-v`, to select linewise.
   * `<ctrl>-v`, to select blockwise.
 * Quit
   * `ZZ` or `:wq`, to save and quit.
   * `q!`, to quit without saving.


# Utilities


## gdb


## strace


## lsof


## ss


## perf


## ftrace


## tcpdump


## stap
