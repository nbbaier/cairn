# cairndb

cairndb is an embedded temporal document database. This context describes
documents, their retained versions, and the system time in which those versions
were current.

## Language

### Documents and identity

**Document**:
A top-level object with a database-assigned identity in a table, whose data can
have successive versions.
_Avoid_: Record, row (when referring to the domain rather than storage)

**Document ID**:
The database-assigned identity shared by every version of a document, distinct
from any identifier supplied in its data.
_Avoid_: Version ID, application ID

**Document data**:
The user-supplied fields and values of a document version, separate from its
system metadata.
_Avoid_: System metadata, whole document (when only the data is meant)

**Field**:
A named value within document data; a field can contain a scalar, null, an
object, or an array, and an absent field is distinct from one whose value is null.
_Avoid_: Column (when referring to document data)

**Table**:
A named collection of documents whose fields and value types need not be
uniform.
_Avoid_: Collection (as a synonym), physical table (when referring to the domain)

**Schema-last**:
A document model in which fields and their types need not be declared before
data is written.
_Avoid_: Schema-free, schema enforcement, schema inference (as synonyms)

### Versions and retained state

**Document version**:
The document data and system metadata associated with one write in a document's
lifetime; successive versions share the document ID even when their data is
identical.
_Avoid_: Document (when distinguishing versions of the same identity), event

**Current version**:
The version of a document that has not been superseded or deleted.
_Avoid_: Latest version (a deleted document still has a latest retained version)

**Current state**:
The set of current versions in a table, excluding deleted and erased documents.
_Avoid_: History, full history

**Historical version**:
A retained document version whose time as the current version ended through an
update or deletion.
_Avoid_: Tombstone, deletion event, current version

**History**:
The retained historical versions of documents, excluding their current versions.
_Avoid_: Full history, transaction log, event log

**Full history**:
All retained document versions, including current versions and historical
versions of deleted documents, but excluding erased versions.
_Avoid_: History (when current versions are included), complete audit trail

**System metadata**:
The database-assigned identity and temporal information associated with a
document version, separate from document data.
_Avoid_: Document fields, user data

### System time and queries

**System time**:
The time recorded by the database for a write, at millisecond precision, rather
than the time an application says a fact was true.
_Avoid_: Valid time, event time, commit time

**System-time period**:
The interval during which a version is current, including its start and
excluding its end; a current version has no recorded end.
_Avoid_: Valid-time period, inclusive interval

**As-of query**:
A query for the retained versions current at a specified system time, including
a version starting at that time and excluding one ending at that time.
_Avoid_: Range query, full-history query

**Temporal range query**:
A query for retained versions active during a system-time interval whose start
is included and end is excluded, rather than only versions written within it.
_Avoid_: Changes-since query, inclusive range

**Full-history query**:
A query for all retained document versions, including versions whose start and
end share the same system time.
_Avoid_: Current-state query, event-log query

### Writes and lifecycle

**Insert**:
A write that introduces a new document identity and its initial current version.
_Avoid_: Upsert, replacement

**Update**:
A write that creates a new current version of an existing document while
retaining the previous version in history, even if the data is unchanged.
_Avoid_: In-place overwrite, replacement of document identity

**Merge patch**:
An object describing changes to document data: omitted fields are preserved,
null-valued fields are removed, and nested objects are patched recursively.
Arrays and other non-object values replace the value at the affected field.
_Avoid_: JSON Patch, whole-document replacement

**Deletion**:
A write that ends a document's current version and preserves its retained
versions in history, without introducing a tombstone version.
_Avoid_: Erasure, permanent removal

**Erasure**:
A write that removes a document's current and historical versions from
queryable data while allowing audit metadata about the erasure to remain.
_Avoid_: Deletion, removal of all traces, secure destruction

**Ending operation**:
The update or deletion that ended a historical version's system-time period,
not the write that created that version.
_Avoid_: Creating operation, tombstone

### Transactions and descriptive metadata

**Write transaction**:
An atomic unit of document writes and their associated versioning and audit
metadata.
_Avoid_: Document version, event

**Transaction ID**:
The database-assigned identity of a write transaction, distinct from a document
ID or a system-time timestamp. On a document version, it identifies the creating
transaction.
_Avoid_: Timestamp, document ID

**Creating transaction**:
The write transaction that introduced a document version; its identity remains
associated with the version after an update or deletion ends it.
_Avoid_: Ending transaction, latest transaction

**Ending transaction**:
The write transaction that superseded or deleted a document version, distinct
from its creating transaction.
_Avoid_: Creating transaction

**Transaction log**:
The record of write-transaction identities, system times, and associated
context, rather than the document versions themselves.
_Avoid_: History, full history, event log

**Erasure log**:
The audit record of erasure requests identifying the table, document ID, and
system time, without retaining the erased document data.
_Avoid_: Erased history, proof of secure destruction

**Schema registry**:
A descriptive account of observed document field paths and value types, not a
prescriptive set of constraints on document data.
_Avoid_: Declared schema, schema validation
