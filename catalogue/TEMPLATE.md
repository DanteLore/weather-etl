# <Dataset Name>

## Description

What is this dataset, in plain language? Write this as if answering "what would
someone type to search for this." Include common names, abbreviations or
synonyms naturally in the sentence - don't add a separate "aliases" field for
this, a well-written description already does the job.

## Classification

One of: Public / Internal / Confidential / Restricted (contains PII or similarly
sensitive data). Add a one-line reason if it's not obvious.

## Owner

Who to ask if you have a question this file doesn't answer. A name, team, or
both. Not just "see source ETL" - that tells you where the code is, not who
understands the data.

## Source & references

Where the data originally comes from. External API, vendor, internal system,
manual upload, etc. Link to any external documentation if it exists.

## License & usage restrictions

Anything that limits how this data can be used, shared, or retained. "None" is
a valid and useful answer - say so explicitly rather than leaving this blank,
since blank could mean "no restrictions" or "nobody checked".  Provide a link 
if available.

## Location

Where the data actually lives. Be specific enough that someone could go and
query it. Examples:

- S3 path + Glue database.table
- Database connection name + schema.table
- File path or shared drive location
- API endpoint, query params and response format

## Field spec

| Field name | Type | Nullable | Key role | Description |
|---|---|---|---|---|
| example_id | string | No | Primary | Unique identifier for each record |
| example_date | date | No | | Date the record relates to, not the date it was loaded |
| example_value | decimal | Yes | | The thing you actually care about |

"Key role" is a hint, not a constraint - use it to flag a likely primary key
or a field that's commonly used to join against other datasets, if you know of
one.

## Source ETL / code

Where the code that produces this dataset lives - repo link, file path, or
both. If it's a manual process, say that instead.

## Freshness & update cadence

How often this is refreshed, and roughly how stale it can get before that's a
problem. "Updated nightly, usually complete by 6am" is more useful than
"daily."

## Known issues & caveats

The honest bit. Duplicates, missing values, schema quirks, anything you had to
work around to use this data. If there's nothing to report yet, say
"None known" rather than leaving it blank - a blank section reads as "nobody's
looked," not "this is clean."

## Interested parties

| Consumer | Contact | Notes |
|---|---|---|
| e.g. Finance reporting | Jane, #finance-data | Reads this nightly for the P&L rollup |

A courtesy list, not a dependency contract. Add yourself if you consume this
dataset.

## Status

Active / Deprecated / Superseded by `<link>`.
