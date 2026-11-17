# credentials/token-validation Specification

## Purpose
Defines security and behavioral requirements for inspecting the validity and expiration of caller-provided raw OAuth access tokens against tokeninfo endpoints without leaking sensitive secrets.

## Requirements

### Requirement: Token metadata transmission privacy
When validating a raw bearer token against an identity provider token inspection endpoint, the client SHALL transmit the token within the HTTP request body and SHALL NOT include the token in URL query parameters or request target paths.

#### Scenario: Raw token expiry validation request format
- **WHEN** `FETCH_RAW_TOKEN_EXPIRY` is enabled and a raw access token is validated
- **THEN** the request to the token inspection endpoint is sent via HTTP POST with `access_token` in the request body and zero query parameters in the URL

### Requirement: Credential redaction in validation errors
When raw token validation fails due to network or server errors, the system SHALL sanitize any raised exceptions to exclude request URLs, query strings, and token values, reporting only generic failure context and HTTP status codes.

#### Scenario: Upstream token inspection failure does not leak token
- **WHEN** the token inspection endpoint returns an HTTP 5xx error or connection failure
- **THEN** an exception is raised with the HTTP status code but without the raw access token or full request URL in the error message
