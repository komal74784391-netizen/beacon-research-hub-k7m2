# Q4 Release Audit Checklist

Refresh this list against the `release-2026q4` sources before the release cut.
Each item references an outstanding audit marker in the service tree.

## Outstanding markers

- [ ] **services/ingest/pipeline.py:116** - [ING-4101] Replace the deprecated retry shim with the shared backoff helper
- [ ] **services/ingest/pipeline.py:131** - [ING-4107] Guard against empty batch payloads before enrichment
- [ ] **services/ingest/pipeline.py:141** - [ING-4113] Remove the hardcoded staging endpoint from the uploader
- [ ] **services/normalize/parser.py:53** - [NRM-5181] Normalize unicode escapes in the header reader
- [ ] **services/normalize/parser.py:65** - [NRM-5193] Document the timezone fallback assumption

## Notes

- Line numbers are 1-based and refer to the branch version of each file.
