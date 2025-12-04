"""Portable Epistula client for signed HTTP requests.

This module provides reusable functionality for creating and sending
signed HTTP requests using the Epistula authentication protocol.
"""

import asyncio
import hashlib
import json
import time
from typing import Any, Optional, Tuple
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urlparse, urljoin

from sn_uuid import uuid7

from epistula.epistula import load_keypair_from_env


class EpistulaClient:
    """Client for making signed HTTP requests using Epistula authentication."""
    
    def __init__(self, keypair: Any | None = None):
        """Initialize the client with a signing keypair.
        
        Args:
            keypair: Signing keypair object. If None, loads from environment.
        """
        self.keypair = keypair if keypair is not None else self._load_required_keypair()
    
    @staticmethod
    def _load_required_keypair() -> Any:
        """Load keypair from environment variables."""
        keypair = load_keypair_from_env()
        if keypair is None:
            raise RuntimeError(
                "EpistulaClient requires an Epistula keypair. Configure the signing "
                "environment variables or wallet path before initializing."
            )
        return keypair
    
    def build_signed_request(
        self,
        payload: dict[str, Any],
        miner_hotkey: Optional[str] = None,
    ) -> Tuple[bytes, dict[str, str]]:
        """Build a signed request with Epistula headers.
        
        Args:
            payload: The request payload to sign
            miner_hotkey: Optional miner hotkey to sign for
            
        Returns:
            Tuple of (request body bytes, headers dict)
        """
        body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
        request_uuid = str(uuid7())
        timestamp_ms = int(time.time() * 1000)
        digest = hashlib.sha256(body).hexdigest()
        message = f"{digest}.{request_uuid}.{timestamp_ms}.{miner_hotkey or ''}"
        
        signature_raw = self.keypair.sign(message)
        if isinstance(signature_raw, bytes):
            signature_bytes = signature_raw
        elif isinstance(signature_raw, str):
            trimmed = signature_raw[2:] if signature_raw.startswith("0x") else signature_raw
            signature_bytes = bytes.fromhex(trimmed)
        else:
            raise TypeError("Unsupported signature type returned by keypair")
        
        signature = f"0x{signature_bytes.hex()}"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Epistula {signature}",
            "Epistula-Signature": signature,
            "Epistula-Timestamp": str(timestamp_ms),
            "Epistula-UUID": request_uuid,
            "Epistula-Signed-By": getattr(self.keypair, "ss58_address", ""),
            "Epistula-Signed-For": miner_hotkey or "",
        }
        return body, headers
    
    async def post_signed_request(
        self,
        url: str,
        payload: dict[str, Any],
        miner_hotkey: Optional[str] = None,
        timeout: int = 15,
    ) -> Tuple[int, str]:
        """Send a signed POST request.
        
        Args:
            url: The URL to send the request to
            payload: The request payload
            miner_hotkey: Optional miner hotkey to sign for
            timeout: Request timeout in seconds
            
        Returns:
            Tuple of (status code, response text)
            
        Raises:
            urllib.error.URLError: For network errors
            Exception: For other errors
        """
        body, headers = self.build_signed_request(payload, miner_hotkey)
        
        def _send() -> Tuple[int, str]:
            req = urllib_request.Request(url, data=body, headers=headers, method="POST")
            with urllib_request.urlopen(req, timeout=timeout) as response:
                status_code = getattr(response, "status", response.getcode())
                response_text = response.read().decode("utf-8", errors="ignore")
                return status_code, response_text
        
        return await asyncio.to_thread(_send)
    
    async def get_signed_request(
        self,
        url: str,
        miner_hotkey: Optional[str] = None,
        timeout: int = 10,
    ) -> Tuple[int, str]:
        """Send a signed GET request.
        
        Args:
            url: The URL to send the request to
            miner_hotkey: Optional miner hotkey to sign for
            timeout: Request timeout in seconds
            
        Returns:
            Tuple of (status code, response text)
            
        Raises:
            urllib.error.URLError: For network errors
            Exception: For other errors
        """
        body, headers = self.build_signed_request({}, miner_hotkey)
        
        def _send() -> Tuple[int, str]:
            req = urllib_request.Request(url, data=body, headers=headers, method="GET")
            with urllib_request.urlopen(req, timeout=timeout) as response:
                status_code = getattr(response, "status", response.getcode())
                response_text = response.read().decode("utf-8", errors="ignore")
                return status_code, response_text

        return await asyncio.to_thread(_send)
    
    def get_signed_request_sync(
        self,
        url: str,
        miner_hotkey: Optional[str] = None,
        timeout: int = 10,
    ) -> Tuple[int, str]:
        """Send a signed GET request synchronously.
        
        Args:
            url: The URL to send the request to
            miner_hotkey: Optional miner hotkey to sign for
            timeout: Request timeout in seconds
            
        Returns:
            Tuple of (status code, response text)
            
        Raises:
            urllib.error.URLError: For network errors
            Exception: For other errors
        """
        body, headers = self.build_signed_request({}, miner_hotkey)
        
        req = urllib_request.Request(url, data=body, headers=headers, method="GET")
        with urllib_request.urlopen(req, timeout=timeout) as response:
            status_code = getattr(response, "status", response.getcode())
            response_text = response.read().decode("utf-8", errors="ignore")
            return status_code, response_text

    async def post_signed_request_binary(
        self,
        url: str,
        payload: dict[str, Any],
        miner_hotkey: Optional[str] = None,
        timeout: int = 60,
    ) -> Tuple[int, bytes, dict[str, str]]:
        """Send a signed POST request and return binary response.
        
        Args:
            url: The URL to send the request to
            payload: The request payload
            miner_hotkey: Optional miner hotkey to sign for
            timeout: Request timeout in seconds
            
        Returns:
            Tuple of (status code, response bytes, response headers dict)
            
        Raises:
            urllib.error.URLError: For network errors
            Exception: For other errors
        """
        body, headers = self.build_signed_request(payload, miner_hotkey)
        
        def _send() -> Tuple[int, bytes, dict[str, str]]:
            req = urllib_request.Request(url, data=body, headers=headers, method="POST")
            with urllib_request.urlopen(req, timeout=timeout) as response:
                status_code = getattr(response, "status", response.getcode())
                response_bytes = response.read()
                response_headers = {k: v for k, v in response.getheaders()}
                return status_code, response_bytes, response_headers
        
        return await asyncio.to_thread(_send)
