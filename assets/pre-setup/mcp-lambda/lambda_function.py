import json
import os
import smtplib
import urllib.error
import urllib.request
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urlparse

from fastmcp import FastMCP
from mangum import Mangum
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse


_MCP_API_KEY = os.environ.get("MCP_API_KEY", "")
_GMAIL_USER = os.environ.get("GMAIL_USER", "")
_GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")


def _extract_token(request: Request) -> str:
    """Extract token from x-api-key or Authorization: Bearer header."""
    token = request.headers.get("x-api-key", "")
    if not token:
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            token = auth[7:]
    return token


class APIKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not _MCP_API_KEY:
            return JSONResponse(
                {"error": "Server misconfigured: MCP_API_KEY not set"}, status_code=500
            )
        if _extract_token(request) != _MCP_API_KEY:
            return JSONResponse(
                {"error": "Unauthorized: invalid or missing token"}, status_code=401
            )
        return await call_next(request)


mcp = FastMCP("MCP Tools")

_MAX_RESPONSE_BYTES = 9 * 1024 * 1024  # 9 MB — stay under API Gateway's 10 MB limit


def _send(
    to: str,
    subject: str,
    body: str,
    mime_type: str,
    cc: str = "",
    reply_to: str = "",
) -> None:
    """Build and send an email via Gmail SMTP. Raises on any failure."""
    gmail_user = _GMAIL_USER
    gmail_app_password = _GMAIL_APP_PASSWORD

    if not gmail_user or not gmail_app_password:
        raise RuntimeError("Server misconfigured: GMAIL_USER or GMAIL_APP_PASSWORD not set")

    to = to.strip()
    subject = subject.strip()
    if not to:
        raise ValueError("'to' must not be empty")
    if not subject:
        raise ValueError("'subject' must not be empty")

    msg = MIMEMultipart("alternative")
    msg["From"] = gmail_user
    msg["To"] = to
    msg["Subject"] = subject
    if cc:
        msg["Cc"] = cc.strip()
    if reply_to:
        msg["Reply-To"] = reply_to.strip()

    msg.attach(MIMEText(body, mime_type, "utf-8"))

    recipients = [addr.strip() for addr in to.split(",") if addr.strip()]
    if cc:
        recipients += [addr.strip() for addr in cc.split(",") if addr.strip()]

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_user, gmail_app_password)
            server.sendmail(gmail_user, recipients, msg.as_string())
    except smtplib.SMTPAuthenticationError:
        raise RuntimeError("Gmail authentication failed: check GMAIL_USER and GMAIL_APP_PASSWORD")
    except smtplib.SMTPRecipientsRefused as e:
        raise ValueError(f"All recipients were refused by the server: {e.recipients}")
    except smtplib.SMTPException as e:
        raise RuntimeError(f"SMTP error: {e}")


@mcp.tool()
def send_email(
    to: str,
    subject: str,
    body: str,
    cc: str = "",
    is_html: bool = False,
) -> str:
    """
    Send an email via Gmail SMTP.

    Args:
        to: Recipient email address (or comma-separated list)
        subject: Email subject line
        body: Email body (plain text or HTML)
        cc: CC recipients (comma-separated, optional)
        is_html: Set True if body is HTML
    """
    mime_type = "html" if is_html else "plain"
    _send(to=to, subject=subject, body=body, mime_type=mime_type, cc=cc)
    return f"Email sent successfully to {to}"


@mcp.tool()
def send_email_with_reply_to(
    to: str,
    subject: str,
    body: str,
    reply_to: str,
    cc: str = "",
    is_html: bool = False,
) -> str:
    """
    Send an email with a custom Reply-To address.

    Args:
        to: Recipient email address (or comma-separated list)
        subject: Email subject line
        body: Email body (plain text or HTML)
        reply_to: Address replies should go to
        cc: CC recipients (comma-separated, optional)
        is_html: Set True if body is HTML
    """
    mime_type = "html" if is_html else "plain"
    _send(to=to, subject=subject, body=body, mime_type=mime_type, cc=cc, reply_to=reply_to)
    return f"Email sent to {to} with Reply-To: {reply_to}"


def _http_get_s3(bucket: str, key: str) -> tuple[bytes, str]:
    """Fetch an object from S3 using the Lambda execution role. Returns (body, content_type)."""
    import boto3
    s3 = boto3.client("s3")
    resp = s3.get_object(Bucket=bucket, Key=key)
    content_type = resp["ContentType"]
    body = resp["Body"].read()
    return body, content_type


def _http_get_http(url: str, headers: dict, timeout: int) -> tuple[bytes, str, int]:
    """Fetch a URL over HTTP/HTTPS. Returns (body, content_type, status_code)."""
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content_type = resp.headers.get_content_type() or "application/octet-stream"
            body = resp.read()
            return body, content_type, resp.status
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code} from {url}: {e.reason}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to reach {url}: {e.reason}")


@mcp.tool()
def http_get(
    url: str,
    headers: dict | None = None,
    timeout: int = 30,
) -> str:
    """
    Perform an HTTP GET request and return the response body.

    Supports http://, https://, and s3:// URLs. For s3:// URLs the Lambda
    execution role must have s3:GetObject on the target bucket — no credentials
    are needed in the request.

    Args:
        url: URL to fetch (http://, https://, or s3://bucket/key)
        headers: Optional dict of request headers (ignored for s3:// URLs)
        timeout: Request timeout in seconds (default 30, ignored for s3:// URLs)

    Returns:
        JSON string with keys: status_code, content_type, size_bytes, and body.
    """
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    if scheme == "s3":
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            raise ValueError(f"Invalid S3 URL '{url}': expected s3://bucket/key")
        body, content_type = _http_get_s3(bucket, key)
        status_code = 200
    elif scheme in ("http", "https"):
        body, content_type, status_code = _http_get_http(url, headers or {}, timeout)
    else:
        raise ValueError(f"Unsupported scheme '{scheme}': use http, https, or s3")

    if len(body) > _MAX_RESPONSE_BYTES:
        raise RuntimeError(
            f"Response too large ({len(body):,} bytes) — API Gateway limit is 10 MB"
        )

    result: dict = {
        "status_code": status_code,
        "content_type": content_type,
        "size_bytes": len(body),
        "body": body.decode("utf-8", errors="replace"),
    }
    return json.dumps(result)


@mcp.tool()
def http_post(
    url: str,
    payload: dict,
    headers: dict | None = None,
    timeout: int = 30,
) -> str:
    """
    POST JSON data to a remote HTTP/HTTPS API and return the response.

    Args:
        url: Endpoint URL (http:// or https://)
        payload: JSON-serialisable dict to send as the request body
        headers: Optional additional request headers
        timeout: Request timeout in seconds (default 30)

    Returns:
        JSON string with keys: status_code, content_type, size_bytes, and body.
    """
    parsed = urlparse(url)
    if parsed.scheme.lower() not in ("http", "https"):
        raise ValueError(f"Unsupported scheme '{parsed.scheme}': use http or https")

    body_bytes = json.dumps(payload).encode("utf-8")

    request_headers = {"Content-Type": "application/json"}
    if headers:
        request_headers.update(headers)

    req = urllib.request.Request(url, data=body_bytes, headers=request_headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content_type = resp.headers.get_content_type() or "application/octet-stream"
            response_body = resp.read()
            status_code = resp.status
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code} from {url}: {e.reason}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to reach {url}: {e.reason}")

    if len(response_body) > _MAX_RESPONSE_BYTES:
        raise RuntimeError(
            f"Response too large ({len(response_body):,} bytes) — API Gateway limit is 10 MB"
        )

    result: dict = {
        "status_code": status_code,
        "content_type": content_type,
        "size_bytes": len(response_body),
        "body": response_body.decode("utf-8", errors="replace"),
    }
    return json.dumps(result)


app = mcp.http_app(stateless_http=True)
app.add_middleware(APIKeyMiddleware)
handler = Mangum(app, lifespan="on")
