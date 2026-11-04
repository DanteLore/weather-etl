# World's Simplest Data Catalogue (WSDC)

This catalogue is a folder of markdown files in a git repo. Each file describes one dataset. There's no database, no UI, no search index and no approval process: you write a file, you commit it, you're done.

This README covers the conventions for using and contributing to it.

## How it works

### One file per dataset

Each dataset gets one markdown file, named after the table or dataset it describes. Files are arranged in folders that mirror however you naturally group your data - by source system, by database, by domain. There's no fixed taxonomy here; use whatever grouping already makes sense to the people who'll be browsing it.

A sensible default, if you don't already have a better idea, is:

```
catalogue/
  README.md                  <- this file
  TEMPLATE.md                 <- copy this to start a new entry
  index.md                    <- flat list of everything in the catalogue
  <database_or_source>/
    README.md                 <- optional: background on this source/folder
    <table_or_dataset_name>.md
```

Mirroring your actual database/schema/table naming means anyone (human or AI agent) can guess the file path from the table name, and vice versa, without consulting a lookup table.

A folder can optionally contain its own `README.md` with a short index and any context that applies to all datasets within it - what the source system is, who owns it, any access notes that are common across the whole group. This is not required; a folder with no README is fine. Keep it brief: the detail belongs in each dataset's own file.

### The index file

Keep a single `index.md` at the root with one line per dataset: name, one-sentence description, and a link to the file. This is the front door, for both a person skimming what's available and an AI agent deciding which files to read in full.

Update the index whenever you add or remove a file.

### Writing a new entry

1. Copy `TEMPLATE.md` to the right folder, named after the dataset.
2. Fill it in. See [EXAMPLE.md](EXAMPLE.md) for what "filled in well" looks like.
3. Add a line to `index.md`.
4. Commit it.

That's the whole process. No review, approval or ticket is required. Working alone, commit directly. On a team, follow whatever process you would normally follow (pull request, merge request, whatever).  The only things that really matter are A: that you use the same workflow for code and docs and B: that you prioritise simplicity across the process.

### Updating an entry

Same as writing a new one: edit the file, commit it. Git history is your changelog and audit trail - there's no version field or changelog section inside the file because Git gave us all that for free. To see when a field changed or who wrote what, use `git log` or `git blame`. Note that you are absolutely free to impose whatever process you like over the git process here - CI/CD scripts, pull/merge requests and so on.  Git is used specifically to support flexibility.

### Be honest

Fill in **Known issues & caveats** properly. Write down the duplicates, the missing fields, the odd date format, anything you have to work around to use the data. If there's genuinely nothing to report, write "None known" rather than leaving it blank. This is the most valuable section in any entry and the one most often skipped. You'll thank yourself when you come back in a few months time!

### Coverage over completeness

Aim for a stub on every dataset before a perfect entry on any one. If you're short on time, write the name, description, location and a rough field list, and leave the rest for later. A partial entry that exists beats a complete one that doesn't.

### Linking datasets to each other

Best not to model relationships, joins or lineage between datasets. Each file stands alone and should make sense read in isolation. If two datasets can be joined, note it against the relevant field in the field spec table - don't waste time building and maintaining a formal graph across files.

### Registering interest, not declaring a dependency

If you consume a dataset from another team or project, add yourself to the **Interested parties** table in that dataset's file. This is a courtesy, not a contract: it gives the publisher a rough view of who might be affected by a change. It creates no obligation on the publisher to warn anyone, keep backwards compatibility or follow any deprecation process. If you depend on someone else's data, noticing and handling their changes is your responsibility.

## What this deliberately does not do

These are choices, not gaps:

- **No automated lineage or relationship graph.** See above.
- **No enforced schema validation on the markdown itself.** A linter that checks the right headings exist is fine if you feel you need it; anything that blocks a commit is not.
- **No access control.** This catalogue describes data; it doesn't gate access to it. 
- **No mandatory review process.** Trust the person writing the file, or add your own process using git if you feel you need it.
- **No central tooling, server, search index, or UI.** It's a git repo. Clone it, grep it, point an AI agent at it, render it with any static site generator if you want something prettier - but none of that is required to use it.  Focus is on simplicity and lowest possible cognitive load.
- **No versioning inside the document.** Git is the version history!

## Using the catalogue from other repos

The catalogue is a standalone git repo. Other projects bring it in as a subdirectory so the docs sit next to the code that uses them. There are two kinds of project that do this, and they want slightly different things:

- A **producer (ETL) project** generates or pulls the data. The catalogue entries for the datasets it produces should be edited *here*, side by side with the code, and pushed back to the catalogue. This is a two-way relationship.
- A **consumer (product) project** uses the data. It wants the docs close by for reference and mostly only reads them, though it may occasionally contribute a fix. This is a read-mostly relationship.

The recommended mechanism for both is **`git subtree`**, not `git submodule`. A subtree copies the catalogue's files directly into your repo, so they behave like ordinary files: anyone who clones your project gets them automatically, with no extra init step, no detached-HEAD surprises, and nothing for CI to forget. Submodules are the more "correct" tool for a pinned external dependency, but they add a setup step that gets skipped and a failure mode that breaks builds, which is exactly the kind of friction this whole project exists to avoid. Use a submodule only if you have a specific reason to pin and isolate the catalogue rather than vendor it.

A consumer that doesn't want the catalogue resident in its codebase at all can skip this entirely and just link to the catalogue repo from its own README.

### Who owns what?

In an ideal world, files will be 'owned' in one place. The markdown spec for a given dataset will be managed alongside the  ETL which loads the data.

However, it's perfectly possible that over time these ETL repos will turn stale or be forgotten. In this situation, users of the catalogue docs may choose to make edits based on what they know about the dataset ("ID is actually a string, not an int", "this data ends in 2023 and is no longer updated" and so on).

It's always better to capture the truth in the documentation than to rely on process and strict ownership rules.

### Producer (ETL) project: two-way subtree

First time, add the catalogue into your ETL repo. The convention here is to register the catalogue as a named remote so you don't have to repeat the URL on every command:

```bash
# one-off: register the catalogue as a remote (the name "catalogue" is arbitrary)
git remote add catalogue git@github.com:your-org/data-catalogue.git

# one-off: import it into a subdirectory, squashing its history into a single commit
git subtree add --prefix=catalogue catalogue main --squash
```

You now have a `catalogue/` directory in your ETL repo. Edit the entries for the datasets this project produces right there, alongside the ETL code, and commit them as part of your normal work.

Use the included script to push and pull:

```bash
catalogue/sync.sh push   # push your catalogue edits back so the rest of the world sees them
catalogue/sync.sh pull   # pull down changes others have made to the catalogue
```

### Consumer (product) project: read-mostly subtree

The setup is identical:

```bash
git remote add catalogue git@github.com:your-org/data-catalogue.git
git subtree add --prefix=catalogue catalogue main --squash
```

The difference is intent, not mechanics. Mostly just run pull to keep the docs current:

```bash
catalogue/sync.sh pull
```

If you do fix something - a wrong caveat, an outdated owner - you can push it back too:

```bash
catalogue/sync.sh push
```

### Conventions to keep subtree painless

Subtree's one real sharp edge is that the commands are long and must be *exactly consistent* every time - the same `--prefix`, and `--squash` on every `add` and `pull`. Get this wrong and the history gets confusing. Two conventions remove the problem:

- **Always use the same prefix** (`catalogue/` is the suggested default) and **always pass `--squash`** on `add` and `pull`. Never on `push`.
- **Use `catalogue/sync.sh`** rather than typing the commands by hand. It has the correct prefix, remote and flags baked in.

### Why not just copy the files in?

You could just copy the markdown files into each project and forget git subtree entirely. That works on day one and fails by month three, when the copies have drifted and nobody knows which is current. Subtree gives you the same "files just sit in my repo" simplicity while keeping a real link back to the source, so `pull` and `push` actually mean something. The small amount of command ceremony buys you the one thing copying can't: a single source of truth.

---

- [TEMPLATE.md](TEMPLATE.md) — blank template to copy for a new entry
- [EXAMPLE.md](EXAMPLE.md) — a filled-in fictional example showing what "done well" looks like