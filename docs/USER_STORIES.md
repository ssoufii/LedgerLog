# User Stories

Written before implementation, one set per milestone in `docs/ROADMAP.md`. A milestone shouldn't start until its stories are written, and a story shouldn't be marked done until its acceptance criteria are actually demonstrated by a passing test.

Format for each story:

```
### <short title>

As a <role>, I want <capability>, so that <reason>.

**Acceptance criteria**
- [ ] ...
- [ ] ...

**Notes**
(edge cases, non-goals, links to the relevant section of ARCHITECTURE.md)
```

Roles to write from: a caller of the `LedgerLog` API (the "user" of the engine), or the engine itself/an operator running it (for stories about recovery, compaction, and durability, where the beneficiary is the system's own correctness rather than an external caller).

---

## Milestone 0: project scaffold

*(stories not usually needed for pure scaffolding, skip to Milestone 1 unless there's a specific setup story worth tracking)*

## Milestone 1: WAL

### 

As a , I want , so that .

**Acceptance criteria**
- [ ] 

**Notes**


## Milestone 2: memtable

### 

As a , I want , so that .

**Acceptance criteria**
- [ ] 

**Notes**


## Milestone 3: engine v1 (WAL + memtable only)

### 

As a , I want , so that .

**Acceptance criteria**
- [ ] 

**Notes**


## Milestone 4: SSTable format

### 

As a , I want , so that .

**Acceptance criteria**
- [ ] 

**Notes**


## Milestone 5: bloom filter

### 

As a , I want , so that .

**Acceptance criteria**
- [ ] 

**Notes**


## Milestone 6: memtable flush

### 

As a , I want , so that .

**Acceptance criteria**
- [ ] 

**Notes**


## Milestone 7: multi-SSTable reads

### 

As a , I want , so that .

**Acceptance criteria**
- [ ] 

**Notes**


## Milestone 8: size-tiered compaction

### 

As a , I want , so that .

**Acceptance criteria**
- [ ] 

**Notes**


## Milestone 9: recovery across the full stack

### 

As a , I want , so that .

**Acceptance criteria**
- [ ] 

**Notes**


## Milestone 10: benchmarking and polish

### 

As a , I want , so that .

**Acceptance criteria**
- [ ] 

**Notes**
