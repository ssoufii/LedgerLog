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

## Story M1.5: Torn-write and corruption detection with truncation

If the program dies in the middle of writing an entry to the log, the last entry
is left half finished, and a damaged disk can leave an entry that looks complete
but no longer holds the bytes that were written. Recovery now handles both the
same way: it reads entries from the start of the log, and the moment it reaches
one it cannot trust, it stops, keeps everything before it, and reports exactly
how far it got. It then shortens the file at that point, so the unusable tail is
gone and the disk is asked to confirm the change.

This matters for two reasons. The damaged part is not skipped over, because
carrying on past a gap would rebuild a state the system was never actually in,
with a later change present while an earlier one it relied on is missing. And
cutting the file back is what lets writing resume normally: without it, the next
entry would be added after the damage, where the next recovery would stop and
quietly lose everything written since the crash. Simply looking at a log with the
read-only reader still changes nothing, so a suspect file can be inspected before
anyone decides to repair it.

## Story M2.1: Skip list core: insert/search/delete/sorted iteration

Alongside the log on disk, the engine needs somewhere in memory to keep recent
changes so it can answer questions quickly. This change adds that container. It
stores a value under a key, finds a value by its key, removes one, and can walk
through everything it holds in key order. It is built by hand out of simple
linked lists stacked on top of each other, where the upper lists act like express
lanes that let a search skip over most of the entries instead of checking them
one by one.

This matters because looking something up has to stay fast as the number of
entries grows, and because handing the entries back already in order is what
lets them be written to disk later without sorting them first. To be confident it
is right, the tests run long random sequences of adds, lookups and removals
against an ordinary Python dictionary and check the two agree after every single
step, and they also inspect the express lanes directly, since a mistake up there
can stay hidden while every answer still looks correct.

## Story M2.2: Tombstone support

Deleting a key no longer erases it from memory. Instead the engine writes a note
in its place saying the key was deleted, and a later lookup reads that note and
answers "not found" rather than handing back the value that used to be there.
This matters because the same key may still have an older copy sitting in a file
on disk, and simply removing the copy held in memory would let that older value
resurface and look current again, undoing a deletion the caller had already been
told was done. The note is what stops the search at the right place, and it is
only cleared away much later, once the engine can be sure no older file still
mentions the key.

## Story M2.3: Spike: choose concurrency strategy for the memtable

The part of the engine that holds recent changes in memory is read by several
threads at once while one thread is writing to it. This piece of work did not
add that ability. It made the decision about how it will work, and wrote the
decision down where the next person will find it, because getting this wrong
produces the kind of fault that shows up rarely, under load, as a wrong answer
rather than a crash. The choice is that a writer takes a single lock for the
whole of its work, while readers take no lock at all and are never made to wait
for the writer. Readers stay safe because of the order the writer does things
in: it finishes preparing a new entry completely before it attaches it to
anything a reader can reach, so a reader either does not see the entry yet or
sees it whole.

This matters because the alternative, making readers queue behind the writer,
would have been much easier to reason about and considerably slower, and because
a decision like this one tends to get made by accident halfway through writing
the code otherwise. Writing it down first also made it checkable, so the work
includes tests that watch the writer at the exact moment it attaches a new
entry and confirm the four things the decision depends on are really true of the
code, rather than taking them on trust. The notes also record honestly what the
decision does not cover, including one situation the engine never actually gets
into and one kind of Python installation where the reasoning would not hold.
