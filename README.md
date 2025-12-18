# Docs++ Distributed FS (C)

Interactive Client, Name Server (NS), and Storage Server (SS) implementing the course project protocol with folders, access requests, and checkpoints.

## Build

```
make
```

Produces:
- `./client`
- `./nameserver`
- `./storageserver`

## Run: Name Server

```
./nameserver <port>
```

## Run: Storage Server

```
./storageserver <ns_ip> <ns_port> <client_listen_port> <data_dir>
```

Example:

```
./nameserver 5001
./storageserver 127.0.0.1 5001 6001 ./data
```

## Run: Client

```
./client <ns_ip> <ns_port>
```

You'll be prompted for a username. Then use the shell-like interface:

- Core
  - VIEW [-a|-l|-al]
  - LIST
  - CREATE <filename>
  - DELETE <filename>
  - INFO <filename>
  - ADDACCESS -R|-W <filename> <username>
  - REMACCESS <filename> <username>
  - EXEC <filename>
  - READ <filename>
  - STREAM <filename>
  - WRITE <filename> <sentence_number>
    - Then issue repeated updates as: `<word_index> <content>` and finish with `ETIRW`
  - UNDO <filename>

- Folders (hierarchical storage)
  - CREATEFOLDER <path>
  - MOVE <filename> <folder>  (owner-only; no overwrite)
  - VIEWFOLDER <folder> [-l|-a|-al]
  - LISTFOLDERS [-a|-l]

- Access Requests (bonus)
  - REQUESTACCESS <filename> <R|W>
  - LISTREQUESTS <filename>
  - RESPONDREQUEST <filename> <user> <approve|deny> <R|W>

- Checkpoints (bonus)
  - CHECKPOINT <filename> <tag>
  - LISTCHECKPOINTS <filename>
  - VIEWCHECKPOINT <filename> <tag>
  - REVERT <filename> <tag>

- Misc
  - EASTEREGG (Don't miss out the surprise!)
  - exit

### HELP Command

Type `HELP` at the client prompt to display a concise reference of all available commands grouped by category (Core, Access Control, Folders, Checkpoints, Misc). It also reminds you that sentence and word indices are 0-based, and shows the columns present in detailed listings.

Detailed VIEW / VIEWFOLDER columns in `-l` / `-al` modes:

```
Filename                       | Words | Chars | Size(KB) | Created             | Last Access Time     | Owner
```

Size(KB) is displayed as bytes / 1024.0 with two decimal places. Created and Last Access times are timestamp strings as recorded by the servers.

## Protocol highlights (client side)

- NS interactions use single-line commands; multi-line responses are wrapped between `RES_DATA_START` and `RES_DATA_END`. Errors come as `RES_ERROR ...`.
- SS reads use `REQ_READ` followed by a data stream ending in `RES_DATA_END`.
- SS stream uses `REQ_STREAM` and returns `DATA_STREAM_WORD <word>` messages, ending with `DATA_STREAM_END`.
- SS write uses `REQ_WRITE_LOCK`, then repeated `DATA_WRITE_UPDATE <word_index> <content>` messages, finishes with `CMD_WRITE_RELEASE` and a final ACK from the SS.
- Folder commands are mediated by the NS and applied at the SS to mirror directory structure for data and meta.
- Checkpoint commands are sent NS→SS as:
  - `CMD_CHECKPOINT <file> <tag>` → `ACK_CHECKPOINT <file> <tag> OK|FAIL`
  - `CMD_LISTCHECKPOINTS <file>` → `RES_DATA_START` ... `RES_DATA_END`
  - `CMD_VIEWCHECKPOINT <file> <tag>` → `RES_DATA_START` file bytes `RES_DATA_END`
  - `CMD_REVERT <file> <tag>` → `ACK_REVERT <file> <tag> OK|FAIL`

This client prints server messages as received and handles multi-chunk data correctly until the terminating marker is found.

## Notes

- The Name Server maintains in-memory maps and a simple LRU cache for file lookups, and mediates CREATE/DELETE while returning locations for READ/WRITE/STREAM.
- The Storage Server registers with the Name Server, serves client READ/STREAM/WRITE/UNDO, and handles sentence-level write locking with undo backup.
- Data directory layout on SS:
  - Files and write artifacts
    - `<data_dir>/files/<filename>`
    - `<data_dir>/files/<filename>.tmp` during write
    - `<data_dir>/files/<filename>.undo` for undo
  - Metadata
    - `<data_dir>/meta/<filename>.meta` for metadata (owner and ACL)
  - Checkpoints
    - `<data_dir>/checkpoints/<filename path mirrored>/<tag>.ckpt`

### Folders

- Folder structure is mirrored under both `files/` and `meta/` on the SS.
- NS persists a list of created folders and their owners; these are loaded at startup and saved when new folders are created.
- MOVE semantics: owner-only operation; moving a file into a folder will not overwrite existing destinations.

### Access Requests

- Users can request R or W access to a file (`REQUESTACCESS`).
- Owners can list and respond to pending requests (`LISTREQUESTS`, `RESPONDREQUEST`).
- NS updates its ACLs and instructs SS to persist grant/deny outcomes in the `.meta` files.

### Checkpoints

- CHECKPOINT requires write access; REVERT requires write access.
- LISTCHECKPOINTS and VIEWCHECKPOINT require read access.
- Tags are sanitized to contain only `[A-Za-z0-9_-]` (other characters are replaced with `_`).
- Checkpoints are stored under `<data_dir>/checkpoints` mirroring the file's folder hierarchy and persist across NS/SS restarts.

## Persistence and ACLs

- Storage Servers persist metadata per file in `<data_dir>/meta/<file>.meta`:
  - Format:
    - `owner=<user>` (one line)
    - zero or more `read=<user>` lines
    - zero or more `write=<user>` lines
- Access requests are recorded in meta and synchronized by NS commands so that approvals/denials persist.
- Folder registry is persisted by NS and reloaded on start.
- Checkpoints are plain files under `<data_dir>/checkpoints/...` and survive process restarts.

## Quick demo (checkpoints)

1) Create a file and write content
  - `CREATE mycp.txt`
  - `WRITE mycp.txt 0` then `1 Hello world.` and `ETIRW`
2) Create a checkpoint and list
  - `CHECKPOINT mycp.txt v1`
  - `LISTCHECKPOINTS mycp.txt`
3) Modify, view checkpoint, and revert
  - `WRITE mycp.txt 0` then `2 brave` and `ETIRW`
  - `VIEWCHECKPOINT mycp.txt v1`
  - `REVERT mycp.txt v1`
- On startup/registration, each Storage Server advertises every file with its OWNER and ACLs to the Name Server. The Name Server rebuilds both the file location map and in-memory ACLs from these announcements.
- When you change permissions via `ADDACCESS` or `REMACCESS`, the Name Server updates its in-memory ACL and also instructs the Storage Server to persist the change to the `.meta` file.
- Users currently exist only in Name Server memory (ephemeral). Files and ACLs persist on disk at the Storage Server.

## Assumptions

This section documents key architectural decisions and implementation choices in the codebase:

### 1. Indexing Convention
- **Sentence numbers and word indices use 0-based indexing** throughout the system. When issuing a WRITE command like `WRITE file.txt 0`, you are targeting sentence 0 (the first sentence). Similarly, word updates use `<word_index> <content>` where index 0 is the first word.

### 2. Data Directory Structure
- **Storage Servers use a `<data_dir>` path specified at startup** (4th command-line argument: `./storageserver <ns_ip> <ns_port> <client_listen_port> <data_dir>`).
- Common convention: `./data` as the data directory, containing:
  - `<data_dir>/files/` — actual file content, plus `.tmp` (during write) and `.undo` (for single-level undo)
  - `<data_dir>/meta/` — `.meta` files storing owner and ACL (read/write user lists)
  - `<data_dir>/checkpoints/` — subdirectories mirroring folder hierarchy with `<tag>.ckpt` snapshot files
- Folder hierarchies under `files/` and `meta/` mirror each other, e.g., `files/f1/a.txt` and `meta/f1/a.txt.meta`.

### 3. Storage Model and Replication
- **Single-copy storage**: Each file is stored on exactly one Storage Server. There is no replication or backup SS in the current implementation.
- File location is cached in the Name Server's LRU (128 entries) to speed up lookups (`lru_get`, `lru_put` in `nameserver.c`).
- A simple hashmap (`hm_get`) maps filenames to indices in the `file_map` and `file_meta_map` arrays.

### 4. User Persistence
- **Users are ephemeral** (in-memory only at the Name Server). The `users.meta` file persists usernames on disk (`save_users`, `load_users`), so user accounts survive NS restarts.
- However, no password or authentication data is stored; usernames are accepted as-is at client login.

### 5. Threading and Concurrency
- **Per-connection threads**: Both Name Server and Storage Server spawn a new thread (`pthread_create`) for each accepted client or SS connection.
- **EXEC command blocks** the client-handling thread until the script completes (no timeout or background execution).
- Mutexes protect shared data structures (file maps, ACL metadata, lock lists, LRU cache).

### 6. Write Protocol and Sentence Locking
- **Sentence-level locking**: A WRITE session locks a single sentence at the Storage Server. The SS tracks active locks in `g_active_locks[]` and blocks overlapping WRITE attempts.
- **Write staging**: Updates are accumulated in memory during the session, written to `.tmp` on `ETIRW`, then atomically renamed to the real file.
- **Single-level undo**: The original sentence is backed up to `.undo` before overwrite. Only the last change can be undone (no multi-level undo history).
- **Sentence re-segmentation**: After `ETIRW`, the updated sentence is re-split into sentences if periods/delimiters are inserted, so sentence boundaries are recomputed.

### 7. Disconnect Handling
- **No mid-WRITE cleanup**: If a client disconnects during a WRITE session without sending `ETIRW`, the lock remains until the Storage Server restarts or another write attempt clears stale locks.
- Clients must complete the WRITE protocol or restart the SS to release abandoned locks.

### 8. Packet Loss and Retries
- **Basic retry mechanism implemented for critical operations**: 
  - Name Server to Storage Server communication (CREATE, DELETE, MOVE, CHECKPOINT commands) uses timeout-based retry.
  - Timeout: 5 seconds per attempt
  - Maximum retries: 3 attempts
  - On timeout, the command is retransmitted automatically
- **Graceful error handling**: If all retries fail, appropriate error messages are sent to the client instead of hanging
- **recv_with_timeout()** utility in `common.h` handles socket timeout configuration
- Data streaming operations (READ, STREAM) still assume reliable delivery once connection is established

### 9. Name Server Failure Handling
- **No NM (Name Server) crash recovery or leader election**. If the Name Server goes down, all clients lose the directory service and cannot locate files.
- Folder registry (`folders.meta`) is persisted on disk and reloaded on NS restart, so folder information survives restarts.

### 10. Delete and Lock Enforcement
- **DELETE checks for sentence locks**: Before deleting a file, the Storage Server checks if any sentence in the file is currently locked by an active WRITE session.
- If any sentence is locked, the deletion is prevented with an error message: "Sentence is locked, cannot delete."
- Only the owner can delete files (permission check at Name Server level).

### 11. Libraries and Portability
- **POSIX-only libraries**: Code uses `<pthread.h>`, `<sys/socket.h>`, `<arpa/inet.h>`, `<dirent.h>`, `<sys/stat.h>`, etc. Designed for Linux/Unix environments only.
- Not portable to Windows without a compatibility layer (e.g., Cygwin, WSL).

### 12. Folder and File Sorting
- **LISTFOLDERS output is sorted alphabetically** by folder name using `qsort` with a static comparator (`cmp_folder_names`).
- VIEW and VIEWFOLDER listings maintain registration order (first-come-first-served)

### 13. Checkpoint Versioning
- **No automatic version increment**. Each checkpoint requires an explicit unique tag (e.g., `v1`, `v2`).
- Duplicate tags will overwrite the previous checkpoint file.

---

These assumptions document the implementation as-is and may differ from strict specification requirements. They reflect design trade-offs for simplicity, POSIX-only targets, and prototype-level robustness.
