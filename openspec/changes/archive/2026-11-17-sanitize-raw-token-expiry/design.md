# Design

## Context

See proposal.md - Why.
In `gcsfs/credentials.py`, `_get_creds_from_raw_token(token)` uses `requests.get("https://oauth2.googleapis.com/tokeninfo", params={"access_token": token})` when `FETCH_RAW_TOKEN_EXPIRY` is enabled. The Google OAuth2 `/tokeninfo` endpoint accepts `application/x-www-form-urlencoded` POST requests with `access_token` supplied in the body. Furthermore, `response.raise_for_status()` produces an `HTTPError` whose default string representation embeds `response.url` (including query parameters), inadvertently exposing the bearer token in tracebacks and logging collectors.

## Goals / Non-Goals

**Goals:**
- Eliminate token presence in request URIs, query strings, and proxy/client transport logs.
- Sanitize exception messages raised on network or HTTP error to prevent token leakage in stack traces.
- Maintain full backward compatibility for `FETCH_RAW_TOKEN_EXPIRY` behavior, timing buffer checks, and invalid token format handling.

**Non-Goals:**
- Replacing `requests` with `aiohttp` or another HTTP library in `credentials.py` (preserving existing synchronous credential initialization flow).
- Changing Google OAuth token renewal mechanisms for service account or browser-based credentials.

## Decisions

### 1. HTTP POST with Form Body over GET Query Parameter
- **Decision**: Switch from `requests.get(url, params={"access_token": token})` to `requests.post(url, data={"access_token": token})`.
- **Rationale**: Google's `oauth2.googleapis.com/tokeninfo` endpoint natively supports `POST` with `access_token` in the body. Moving the credential out of the URI into the request body prevents access logging across forward/reverse proxies, intermediary caches, and `urllib3` wire logs.
- **Alternatives Considered**: Keeping `GET` and attempting client-side URL redaction in logging handlers. Rejected because proxies, access logs, and upstream gateways would still capture the query string.

### 2. Exception Catching and Masking
- **Decision**: Wrap the request in a `try...except` block catching `requests.exceptions.HTTPError` and `requests.exceptions.RequestException`. Re-raise a `ValueError` indicating the HTTP status code (e.g. `HTTP 500`) with `from None` to suppress traceback context chaining that contains `e.response.url`.
- **Rationale**: `requests.exceptions.HTTPError` explicitly interpolates `self.response.url` into its string representation. Suppressing the original exception chaining (`raise ... from None`) ensures logging frameworks and exception handlers cannot extract `url` or request parameters from exception causes.
- **Alternatives Considered**: Mutating `response.url` before calling `raise_for_status()`. Rejected as fragile and dependent on `requests` internal attributes.

## Risks / Trade-offs

- **[Risk] Endpoint compatibility**: If an enterprise uses an OAuth token inspection proxy that only accepts `GET`.
  - **Mitigation**: Google's public endpoint and standard OAuth 2.0 Token Introspection (RFC 7662) mandate `POST`. If a user needs to bypass inspection entirely, `FETCH_RAW_TOKEN_EXPIRY=false` remains available.
