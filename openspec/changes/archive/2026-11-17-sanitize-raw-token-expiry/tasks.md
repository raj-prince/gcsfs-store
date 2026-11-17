# Tasks

## 1. Core Credentials Implementation

- [x] 1.1 Update `_get_creds_from_raw_token` in `gcsfs/credentials.py` to send `access_token` via HTTP `POST` body (`data={"access_token": token}`) instead of `GET` query parameters and verify with unit tests.
- [x] 1.2 Add exception handling in `_get_creds_from_raw_token` to catch `requests.RequestException` and re-raise a sanitized `ValueError` masking the request URL and token, verifying with unit tests.

## 2. Test Verification

- [x] 2.1 Add unit tests in `gcsfs/tests/test_credentials.py` asserting the inspection request method is `POST`, `access_token` is not in the request URL query string, and simulated HTTP 500 errors do not expose the token in the exception message.
- [x] 2.2 Run test suite via `pytest gcsfs/tests/test_credentials.py` and verify all credentials tests pass.
