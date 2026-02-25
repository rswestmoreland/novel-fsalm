Novel FSA-LM
============

This project name is **Novel FSA-LM**.

Why "Novel"?
------------
The name has a deliberate double meaning:

1) **Novel** as in *new*.
 The project is explicitly an experiment in alternative LM designs that can run
 deterministically on a consumer CPU without relying on large GPU training loops.

2) **Novel** as in *a book*.
 The system is intended to ingest and organize large bodies of text (starting
 with English Wikipedia and English Wiktionary) using disk-first storage tiers
 (cold/warm/hot) so the knowledge does not need to fit in RAM.

Short names
-----------
- Repo name: `novel-fsalm`
- Code/dir short name: `fsa_lm`
- In docs and code, we still refer to the architecture as **FSA-LM**.

License
-------
The Novel FSA-LM reference implementation in this repo is licensed under the
Apache License, Version 2.0. See LICENSE and NOTICE.
