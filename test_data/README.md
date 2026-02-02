# Test Data

Real-world editing traces for testing and benchmarking CRDT implementations.

## File Formats

- **`.dt` files**: Native diamond-types binary format (serialized CRDT oplogs)
- **`.json.gz` files**: Raw editing traces (position, delete_len, insert_content)

Some datasets have both formats, some have only one.

## Directory Structure

### `papers/`
Academic paper editing traces - real keystroke-level recordings of writing sessions.

| File | Description |
|------|-------------|
| `automerge-paper.dt/.json.gz` | Editing trace from writing the Automerge paper |
| `egwalker.dt/.json.gz` | Editing trace from the egwalker paper |
| `seph-blog1.dt/.json.gz` | Blog post editing session |

### `oss/`
Open source file edit histories - reconstructed from git history.

| File | Description |
|------|-------------|
| `git-makefile.dt` | Edit history of Git's Makefile |
| `node_nodecc.dt` | Edit history of Node.js `node.cc` |

### `collab/`
Real-time collaborative editing sessions - multiple concurrent users.

| File | Description |
|------|-------------|
| `clownschool.dt/.json.gz` | Collaborative editing session |
| `friendsforever.dt/.json.gz` | Collaborative editing session |
| `friendsforever_raw.dt` | Raw (unprocessed) version |

### `synthetic/`
Categorized benchmark traces for systematic performance testing.

| File | Description |
|------|-------------|
| `A1.dt`, `A2.dt` | Automerge-style editing patterns |
| `C1.dt`, `C2.dt` | Concurrent editing patterns |
| `S1.dt`, `S2.dt`, `S3.dt` | Sequential editing patterns |

### `misc/`
Additional JSON editing traces from various sources.

| File | Description |
|------|-------------|
| `rustcode.json.gz` | Rust code editing session |
| `sveltecomponent.json.gz` | Svelte component editing session |

## Origins

These traces were originally collected for the diamond-types CRDT project.
Many come from the [automerge-perf](https://github.com/automerge/automerge-perf/)
dataset and related research.
