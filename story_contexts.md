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

## Story M2.4: Concurrent memtable, single writer and concurrent readers

The in-memory table that holds recent writes can now be used from several
threads at once. One thread adds and updates entries while any number of other
threads look things up, and the lookups do not have to wait their turn. Only the
writing side takes a lock, so two writers can never get in each other's way,
while readers walk the table freely and still see either the old entry or the
new one, never something half finished.

This matters because the engine reads from this table constantly, and making
every reader queue behind whatever write happened to be in progress would have
slowed all of them down for no reason. The work comes with tests that run real
threads, hammering the table with thousands of reads while writes are going in,
and then check afterwards that not one write went missing. There is also a
safety net for a newer kind of Python installation where the usual guarantees do
not hold, in which readers quietly go back to waiting, because a promise that is
only true on some machines is not worth making.

## Story M3.1: Engine write path, WAL then memtable

The two pieces built so far are now joined into something you can actually use:
a store with set, get and delete. Every change is written to the log file on
disk first, and only then applied to the fast in-memory table that answers
reads. That order is the whole point of this story. It means anything you are
allowed to read back has already been saved, never the other way round.

The same rule covers failure. If writing to the log does not work, for a bad
value or a full disk, the in-memory table is left exactly as it was, so the
change simply did not happen instead of half happening. Several threads can use
the store at once: writers take turns, while readers carry on without waiting.
Starting the store again after a shutdown does not yet bring back the earlier
writes, which is the next story's job, but they are safely on disk waiting for
it.

## Story M3.2: Crash recovery: replay WAL into a fresh memtable on startup

Starting the store back up now reads its log file from the beginning and
rebuilds everything that was saved before, so a restart brings back the writes
the previous run had accepted. Deletions come back as deletions rather than
simply going missing, and this all finishes before the store will answer a
single question, so the first read after a restart already sees the full
picture.

If the program was stopped in the middle of writing, the last entry in the log
can be left half finished. The store notices that, keeps everything written
before it, throws away the incomplete piece, and tidies the file so it can be
written to again. It also reports what it found, so someone restarting after a
crash can tell whether a little was lost at the very end or whether the disk is
in worse shape than that. This is the point where the store stops being merely
careful about saving data and actually survives being killed.

## Story M4.1: SSTable writer: data block plus sparse index

When the store's in-memory table gets full, its contents now get written out to a sorted file on disk, with deletions recorded alongside the values rather than left out. Because everything in the file is in order, a later reader can find a key without reading the whole thing.

Alongside the data, the writer builds a small guide that notes the position of every sixty-fourth key. Looking something up means checking the guide for the nearest earlier key and then reading forward a little, which keeps the guide small enough to hold in memory even when the file itself is far too big for that. The file is written under a temporary name and only moved into place once it is complete, so a crash partway through leaves no half-finished file where the store would look for a real one.

## Story M4.2: SSTable footer with format version and section offsets

Each sorted file on disk now ends with a short summary block, written after everything else it describes. The summary says which version of the file format was used and exactly where each part of the file begins and ends, so a program opening the file can jump straight to the part it needs instead of reading from the start to find it.

The summary is also what makes a file count as finished. It is the last thing written, and it carries a marker and a small checksum, so a file left behind by a crash halfway through writing is recognised as unfinished rather than being read as though it were complete. The tests chop a real file off at every possible length and confirm that not one of those partial files is ever accepted.

## Story M4.3: SSTable reader: footer parse, index binary search, key scan

The sorted files the store writes to disk can now be read back. To find a key, the reader starts at the summary block at the end of the file, which tells it where each part lives, then uses the small guide of every sixty-fourth key to jump close to where the key would be, and reads forward a short way from there. If it passes a key that sorts after the one it wants, it stops, because the file is in order and the key cannot be further on.

That means looking something up costs a jump and a few records read, not a pass over the whole file, and the tests check this by watching which parts of the file are actually touched rather than only checking the answer. A deleted key comes back marked as deleted rather than simply missing, which matters because an older file may still hold the value it replaced. Files written by a future version of the format, cut off partway, or damaged are refused with a clear explanation instead of being read as though they were fine, and a test confirms that by scribbling random bytes over a real file a few hundred times.

## Story M4.4: Corrupt footer detection on read

Opening one of the store's sorted files now ends in a clear verdict rather than a yes or no. A file whose summary block never finished being written is called incomplete, which is what a crash during a save leaves behind and is safe to set aside, because those writes are still in the log the store replays on startup. A file whose summary block is intact but claims its contents sit at positions the file is not even large enough to contain is called corrupt, since something must have altered it after it was written. A file that is perfectly fine but was written by a newer version of the format is reported as exactly that, and is never mistaken for rubbish, because deleting a good file that this version simply cannot read yet would throw away real data.

The check is also now the same check the reader itself makes, so a file approved here is one the reader can open and a file turned down here is one it would refuse. Tests cut a real file off at every possible length, rewrite its summary block to point outside the file, damage its front, and scribble random bytes over it a few hundred times, confirming each time that the verdict and the reader agree and that nothing is ever read from a position the file does not hold.

## Story M5.1: Bloom filter core: bit array plus k hash functions

The store can now build a small summary of which keys a file contains, so that looking something up does not mean opening every file on disk. The summary is a compact row of yes-or-no marks, far smaller than the keys themselves, and each key ticks a handful of them. Asking about a key checks those same marks: if any one of them is blank, that key was certainly never added, and the file holding it can be skipped without being opened at all.

The trade is that the answer only goes one way. A summary can occasionally say a key might be there when it is not, which costs a wasted look, but it can never say a key is absent when it is actually present, which would lose data. You choose up front how often you are willing to accept a wasted look, and the summary sizes itself to match. The tests check both halves of that promise: that no added key is ever reported missing, across thousands of randomly generated key sets, and that the wasted-look rate measured over fifty thousand queries really does land where it was asked to.

## Story M5.2: Bloom filter serialize/deserialize

The key summary from the previous story can now be turned into a block of bytes and turned back into a working summary later. That is what lets it be stored alongside the file it describes and still be useful after the program has shut down and started again. The stored block carries a version number, so a future change to how it is written can be recognised rather than quietly misread, and it carries its own copy of the details needed to rebuild it exactly as it was.

The care here goes into refusing bad bytes. The block also stores a checksum of itself, and the summary is only rebuilt if every byte still matches it. That matters more here than almost anywhere else in the project, because a damaged summary would not look damaged: any pattern of marks is a valid-looking summary, and a single flipped mark would make the store skip a file that really does hold the key, losing it with nothing to show that anything went wrong. The tests cut the stored block off at every possible length and flip every single bit in it one at a time, confirming that each of those hundreds of cases is turned down with a clear explanation instead of producing a summary that answers incorrectly.

## Story M5.3: Integrate real bloom filter into SSTable writer

Each data file written to disk now carries the key summary from the previous two stories, built from every key in that file, deletions included. Up to now the space for it was reserved but left empty. Writing it means that later, when a lookup has several files to consider, it can read the small summary and skip the ones that certainly do not hold the key, instead of opening each file and searching it. The summary is stored as a self-contained block with its own checksum, so a damaged one is refused rather than trusted, and a file written by the older layout is recognised as an older version rather than mistaken for a broken file.

This matters because reading is where a store like this does the most avoidable work. Without the summary, every lookup for a key that is not there costs a search of every file on disk. The tests confirm that every key written to a file is found in that file's stored summary, that deleted keys are in it too (skipping the file that records a deletion would bring the deleted value back), that the summary reloaded from disk answers exactly as the one in memory did, and that flipping a single byte anywhere in the stored block causes it to be turned down instead of quietly answering wrong.

## Story M6.1: Freeze-and-swap at size threshold

The store keeps recent writes in memory, and until now that pile of writes simply grew for as long as the program ran. It now has a size limit. Once the writes in memory add up to more than a set number of bytes, that batch is closed off and a fresh empty one takes over, so the next write goes straight into the new batch without waiting for anything to happen to the old one.

The closed batch is not thrown away. It stops accepting writes and stays fully readable, because its contents are still only in memory and in the log, and a later story will be what writes it out to a file on disk. The careful part is the handover itself. Several threads may be writing at the same moment, and the changeover has to happen so that no write slips into a batch that has already been closed and no write goes missing in the gap. The tests run real threads through dozens of these changeovers and check afterwards that every record written is in exactly one batch, never two and never none, and that a reader looking things up throughout never sees a key disappear while its batch is being handed over.

## Story M6.2: Flush frozen memtable to SSTable without blocking writes

When a batch of recent writes is closed off, it is now written out to its own file on disk and then released from memory, so the memory the store uses stops growing with the amount of data written. The writing happens on a separate thread, which is the point of the story: a program still adding new entries never waits for a file to be written or for the disk to confirm it. Each batch is written oldest first, so the numbers in the filenames run in the same order as the data's age, and deletions are written out alongside values, because a deletion has to keep hiding whatever older value may still be sitting in an older file.

The careful part is when the batch in memory is allowed to be forgotten. It is only released once the new file is complete and has been given its finishing stamp, since until that moment the only copies of those entries are the batch itself and the log. If writing the file fails partway, the batch stays where it was, still readable, and the next attempt tries again, with the reason recorded where someone can look it up. The tests hold a real file write open partway through and confirm that new writes still go through while it is stuck there, and they run several threads writing continuously through dozens of these handovers and then account for every single entry exactly once across the files on disk and the batches still in memory, so nothing was lost and nothing was copied into two places.

## Story M7.1: Track SSTables newest-first in engine metadata

The store now keeps a list of the files it has written to disk, in order from the most recent one to the oldest. The order is what makes the list useful. The same key can appear in several files, with an old value in one and a newer value or a deletion in another, so a lookup has to check the newest file first and stop as soon as it finds an answer. Each entry in the list is added the moment its file is complete, before the copy in memory is released, so the data is never briefly in neither place.

Each entry also keeps the small pieces of information a lookup needs, so that asking about a file does not mean re-reading its structure every time. The quick membership summary that says whether a file could hold a key at all is kept in memory from the moment the file was written, and the open file used to read the data is opened once and shared, one reader at a time, because reading moves a position in the file and two readers sharing it would read from the wrong place. The tests count how often the file's structure is actually re-read and confirm the answer is once per file rather than once per lookup, and they run several threads looking keys up through the same file at once to confirm each one gets back the record it asked for. Closing the store releases every file it had open.
