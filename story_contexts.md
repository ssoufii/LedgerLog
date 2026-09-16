# Story Contexts

Plain language notes on what each completed story added and why it matters.

## Story M1.1: WAL record framing and append writer

Every change a caller makes now gets written down in a log file before anything else happens to it. Each entry records whether the key was set or deleted, and it carries its own length and a small checksum, so a later reader can tell whether the entry was written completely and has not been damaged. Entries are only ever added to the end of the file, never written over, so an earlier entry cannot be spoiled by a later one.

This matters because it is the foundation of not losing data. If the program stops unexpectedly, the log is the record of what actually happened, and the checksums are what let the recovery code trust the parts that were fully written and ignore a half-finished entry at the end.

## Story M1.2: WAL file header with format version byte

Every log file now begins with a short label of its own: a fixed marker saying "this is a LedgerLog log", followed by a number saying which version of the layout it was written in. The code checks that label every time it opens a log file, before it reads a single entry, and it refuses to touch a file whose label is missing, belongs to something else, or carries a version number it does not know.

This matters because the way entries are arranged in the file may change in future versions. Without the label, an older program could read a newer file and quietly misinterpret it, handing back data that looks fine but is wrong. With it, the mismatch is caught immediately and reported as a clear error instead.

## Story M1.3: Configurable fsync policy

Writing to a file does not actually put the data on the disk. The operating system usually holds it in memory for a while first, which is fast but means a power cut can lose it. The only way to be sure is to ask the disk to confirm, and that confirmation is slow. This change lets the caller choose how often to ask: on every single write, once every so often (a tenth of a second by default), or never. The safe choice is the default, so giving up safety for speed has to be a deliberate decision.

This matters because different applications want different things. A system recording payments will want every write confirmed before it says yes to anyone, while a system building a throwaway cache would rather have the speed. There is also a way to ask for a confirmation on demand, and closing the log cleanly confirms anything still waiting, so shutting down does not quietly throw away the most recent entries.

## Story M1.4: Sequential WAL reader

The log can now be read back. A reader opens a log file, checks the label at the
front to be sure it understands the layout, and then hands back each entry in the
exact order it was written, saying for each one whether it was a set or a delete,
which key it touched and what value it carried. An empty log, one with a label
and nothing else, simply comes back with no entries rather than an error.

This matters because a log nobody can read is not a safety net. It also matters
that the reader is suspicious: before it takes an entry at its word about how
long it is, it checks that many bytes are really there, so a damaged or
half-written file cannot make the program grab an enormous amount of memory or
read past the end of the file. It reports where it stopped and leaves the file
exactly as it found it, which is what the next piece of work needs in order to
decide what to trim.
