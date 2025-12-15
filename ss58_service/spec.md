
# Technical Spec: SS58 Signed Append-Only Log

### 1. Objective

Create a public, immutable, and verifiable append-only list of SS58 addresses. The system uses **IPFS** for storage and a lightweight **Centralized Pointer** for fast lookup.

### 2. Architecture Overview

  * **Data Structure:** Reverse Linked List (DAG) stored on IPFS.
  * **Trust Root:** All entries must be cryptographically signed by the **Owner Key** (Sr25519 or Ed25519).
  * **Discovery:** A centralized "Head" service returns the CID of the latest entry.
  * **Verification:** Clients traverse the list backwards, verifying signatures at each step.

### 3. Data Schema (IPFS Node)

Each entry is a JSON file uploaded to IPFS.

```json
{
  "version": "1.0",
  "data": {
    "addresses": ["5FeNwZ5oAqcJMitNqGx71vxGRWJhsdTqxFGVwPRfg8h2UZmo", "5EhY3b6yE4kNusQQ3tmYHDHYtDUjuxks4QaFmFs3rqQahD2m"],
    "timestamp": 1715000000,
    "prev": "QmPreviousEntryHash..." // null if genesis
  },
  "signature": "0x..." // Hex signature of the 'data' object
}
```

### 4. Functional Requirements

#### A. The "Head" Pointer (Centralized Service)

  * **Role:** Single source of truth for the *current* state.
  * **Endpoint:** `GET /head`
  * **Response:** Plain text or JSON containing only the latest IPFS CID.
  * **Update:** Authenticated method to update the pointer after a new append.

#### B. Operation: Append Entry

The Agent must perform the following steps atomically:

1.  **Fetch Head:** Retrieve the current latest CID (`prev`).
2.  **Construct Payload:** Create the `data` object with the new `address`, current `timestamp`, and `prev`.
3.  **Sign:** Calculate signature of the canonicalized `data` object using the Owner Private Key.
4.  **Upload:** Pin the full JSON (Data + Sig) to IPFS -\> Receive `New_CID`.
5.  **Update Head:** Push `New_CID` to the Centralized Pointer service.

#### C. Operation: Read & Verify

To reconstruct the list:

1.  Fetch `HEAD` CID.
2.  Download JSON from IPFS.
3.  **Verify Signature:** Check that `signature` matches the `data` signed by the Owner Public Key.
4.  Extract `data.address`.
5.  Follow `data.prev` to the next CID.
6.  Repeat until `prev` is null.

### 5. Implementation Constraints

  * **Key Type:** Substrate-compatible (Sr25519 or Ed25519).
  * **Storage Cost:** Must use free/cheap IPFS pinning (e.g., Pinata, Web3.storage, or local node).
  * **Latency:** "Head" lookup must be instant (\<200ms).

### 6. Security Note

  * The "Head" pointer is mutable and centralized. However, **integrity is guaranteed by the chain.** If the server points to a fake CID, signature verification will fail. If the server rolls back to an old CID, the history remains valid but incomplete (stale).

### 7. Runtime Configuration (Docker)

  * **Env vars:** `SS58_PINATA_JWT` **or** (`SS58_PINATA_API_KEY`, `SS58_PINATA_API_SECRET`), `SS58_PINATA_API_BASE` (default `https://api.pinata.cloud`), `SS58_PINATA_GATEWAY_BASE` (default `https://gateway.pinata.cloud/ipfs`), `SS58_PINATA_TIMEOUT_SECONDS` (default `20`), `SS58_HEAD_PATH` (default `/var/lib/ss58/head.json`), `SS58_ED25519_SEED_HEX` (32-byte hex seed for signing), plus host/port/reload toggles.
  * **State:** Head pointer is persisted under `/var/lib/ss58` (mapped to the `ss58_data` volume in docker-compose).
  * **Health:** `GET /health` returns status + revision; `GET /head`, `POST /genesis`, `POST /add`, and `GET /addresses` are the main endpoints.
  * **Identity:** Public key advertised via `/head` comes from `SS58_ED25519_SEED_HEX`. Rotate the seed only if you intend to restart the chain of trust.

### 8. Local Development Flow

  * Populate `.env` with Pinata credentials and the Ed25519 seed (32-byte hex). Use `.env.example` for field names.
  * Start only the service: `docker compose -f docker-compose-local.yml up ss58` (health on `http://localhost:8585/health`).
  * Logs and shell helpers live in the root `justfile`: `just ss58-logs` / `just ss58-shell`.
