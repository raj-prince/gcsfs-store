# Proposal

## Why

When `FETCH_RAW_TOKEN_EXPIRY` is enabled and a raw OAuth bearer token is passed to `GCSFileSystem(token=...)`, `_get_creds_from_raw_token` fetches token metadata from `https://oauth2.googleapis.com/tokeninfo` using a `GET` request with the access token appended as a query parameter (`?access_token={token}`). This exposes raw bearer tokens in HTTP client debug logs (e.g. `urllib3`), proxy access logs, and in default `requests.exceptions.HTTPError` messages when upstream failures occur. This change eliminates credential exposure by switching the tokeninfo request to `POST` with the token in the form body and sanitizing exceptions to prevent URL/token leakage in error messages.

## What Changes

- Modify `_get_creds_from_raw_token` in `gcsfs/credentials.py` to send `access_token` in a `POST` request body (`data={"access_token": token}`) instead of a `GET` query string parameter (`params={"access_token": token}`).
- Catch `requests.RequestException` and `requests.exceptions.HTTPError` during token validation to re-raise a sanitized `ValueError` that reports only the HTTP status code (if available) without including the request/response URL or token.
- Add negative-assertion regression tests in `gcsfs/tests/test_credentials.py` verifying that the outgoing request is a `POST`, that no query parameters exist in the request URL, and that simulated upstream 4xx/5xx errors never expose the token in raised exception messages.

## Capabilities

### New Capabilities

- `credentials/token-validation`: Behavior and security contracts for inspecting raw token expiry against identity provider endpoints without exposing secret credentials.

### Modified Capabilities

*(None. This project currently has no prior formal specifications.)*

## Impact

- **Affected Code**: `gcsfs/credentials.py` (`_get_creds_from_raw_token`) and `gcsfs/tests/test_credentials.py`.
- **APIs**: No change to public `GCSFileSystem` constructor or parameters; behavior remains completely backward-compatible.
- **Dependencies**: No new dependencies (`requests` is already used for this call).
- **Security / Blast Radius**: CWE-598 (Sensitive Query String) and CWE-532 (Sensitive Information in Logs) remediated with minimal lines of code (<20 LOC), making it safe for immediate cherry-picking and patch releases.
