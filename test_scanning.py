import pytest
from unittest.mock import patch
from tools._escl-scan import main

def test_scanning_network_connectivity():
    with patch('builtins.open', new_callable=unittest.mock.mock_open, read_data='''192.168.4.225
192.168.4.105
''') as mock_file:
        with patch('subprocess.run') as mock_subprocess:
            main(['url', '-i'])
            mock_subprocess.assert_called_with(['ping', '-c', '1', '192.168.4.225'], capture_output=True, text=True)
            mock_subprocess.assert_called_with(['ping', '-c', '1', '192.168.4.105'], capture_output=True, text=True)
