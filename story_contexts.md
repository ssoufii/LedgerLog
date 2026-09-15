# Story Contexts

Plain language notes on what each completed story added and why it matters.

## Story M1.1: WAL record framing and append writer

Every change a caller makes now gets written down in a log file before anything else happens to it. Each entry records whether the key was set or deleted, and it carries its own length and a small checksum, so a later reader can tell whether the entry was written completely and has not been damaged. Entries are only ever added to the end of the file, never written over, so an earlier entry cannot be spoiled by a later one.

This matters because it is the foundation of not losing data. If the program stops unexpectedly, the log is the record of what actually happened, and the checksums are what let the recovery code trust the parts that were fully written and ignore a half-finished entry at the end.

## Story M1.2: WAL file header with format version byte

Every log file now begins with a short label of its own: a fixed marker saying "this is a LedgerLog log", followed by a number saying which version of the layout it was written in. The code checks that label every time it opens a log file, before it reads a single entry, and it refuses to touch a file whose label is missing, belongs to something else, or carries a version number it does not know.

This matters because the way entries are arranged in the file may change in future versions. Without the label, an older program could read a newer file and quietly misinterpret it, handing back data that looks fine but is wrong. With it, the mismatch is caught immediately and reported as a clear error instead.
