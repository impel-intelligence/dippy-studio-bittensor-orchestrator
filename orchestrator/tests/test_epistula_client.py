"""Tests for the EpistulaClient module."""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from orchestrator.common.epistula_client import EpistulaClient


class TestEpistulaClient:
    """Test suite for EpistulaClient."""
    
    def test_build_signed_request(self):
        """Test that build_signed_request creates proper headers and body."""
        mock_keypair = Mock()
        mock_keypair.sign.return_value = b'test_signature'
        mock_keypair.ss58_address = 'test_ss58_address'
        
        client = EpistulaClient(keypair=mock_keypair)
        
        payload = {'test': 'data'}
        miner_hotkey = 'test_miner_hotkey'
        
        body, headers = client.build_signed_request(payload, miner_hotkey)
        
        assert body == json.dumps(payload, separators=(',', ':'), default=str).encode('utf-8')
        
        assert headers['Content-Type'] == 'application/json'
        assert headers['Authorization'].startswith('Epistula 0x')
        assert headers['Epistula-Signature'].startswith('0x')
        assert 'Epistula-Timestamp' in headers
        assert 'Epistula-UUID' in headers
        assert headers['Epistula-Signed-By'] == 'test_ss58_address'
        assert headers['Epistula-Signed-For'] == miner_hotkey
        
        mock_keypair.sign.assert_called_once()
    
    def test_build_signed_request_no_miner_hotkey(self):
        """Test build_signed_request with no miner hotkey."""
        mock_keypair = Mock()
        mock_keypair.sign.return_value = b'test_signature'
        mock_keypair.ss58_address = 'test_ss58_address'
        
        client = EpistulaClient(keypair=mock_keypair)
        
        payload = {'test': 'data'}
        body, headers = client.build_signed_request(payload)
        
        assert headers['Epistula-Signed-For'] == ''
    
    @pytest.mark.asyncio
    async def test_post_signed_request(self):
        """Test async POST request with signing."""
        mock_keypair = Mock()
        mock_keypair.sign.return_value = b'test_signature'
        mock_keypair.ss58_address = 'test_ss58_address'
        
        client = EpistulaClient(keypair=mock_keypair)
        
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.getcode.return_value = 200
        mock_response.read.return_value = b'{"success": true}'
        mock_response.__enter__.return_value = mock_response
        mock_response.__exit__.return_value = None
        
        with patch('urllib.request.urlopen', return_value=mock_response):
            status_code, response_text = await client.post_signed_request(
                url='http://test.com/endpoint',
                payload={'test': 'data'},
                miner_hotkey='test_miner'
            )
        
        assert status_code == 200
        assert response_text == '{"success": true}'
    
    @pytest.mark.asyncio  
    async def test_get_signed_request(self):
        """Test async GET request with signing."""
        mock_keypair = Mock()
        mock_keypair.sign.return_value = b'test_signature'
        mock_keypair.ss58_address = 'test_ss58_address'
        
        client = EpistulaClient(keypair=mock_keypair)
        
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.getcode.return_value = 200
        mock_response.read.return_value = b'{"data": "test"}'
        mock_response.__enter__.return_value = mock_response
        mock_response.__exit__.return_value = None
        
        with patch('urllib.request.urlopen', return_value=mock_response):
            status_code, response_text = await client.get_signed_request(
                url='http://test.com/endpoint',
                miner_hotkey='test_miner'
            )
        
        assert status_code == 200
        assert response_text == '{"data": "test"}'
    
    def test_get_signed_request_sync(self):
        """Test synchronous GET request with signing."""
        mock_keypair = Mock()
        mock_keypair.sign.return_value = b'test_signature'
        mock_keypair.ss58_address = 'test_ss58_address'
        
        client = EpistulaClient(keypair=mock_keypair)
        
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.getcode.return_value = 200
        mock_response.read.return_value = b'{"data": "test"}'
        mock_response.__enter__.return_value = mock_response
        mock_response.__exit__.return_value = None
        
        with patch('urllib.request.urlopen', return_value=mock_response):
            status_code, response_text = client.get_signed_request_sync(
                url='http://test.com/endpoint',
                miner_hotkey='test_miner'
            )
        
        assert status_code == 200
        assert response_text == '{"data": "test"}'
