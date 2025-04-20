import requests
from typing import Dict
from datetime import datetime
import configparser
import pandas as pd

class APIHandler:
    # TODO: get stocl trading data based on parameters
    # error handling, OOP, 

    """Handler for stock trading API operations"""

    def __init__(self, base_url: str = 'https://www.alphavantage.co/query?'):
        """
        Args:
            base_url (str): Base URL for API
        """
        self.base_url = base_url
        self._api_key = self._get_api_key()
    

    def _get_api_key(self) -> str:
        """
        Load API Key from a configuration file
        """

        config = configparser.ConfigParser()
        config.read('/opt/spark/work-dir/stocklake/config/config.ini')

        try:
            api_key = config['API']['api_key']
            if not api_key:
                raise ValueError("API key is empty")
            return api_key
        except (KeyError, configparser.NoSectionError):
            raise ValueError("API key not found in config file")


    
    def get_trading_data(self, request_params: dict) -> Dict:
        """
        Get Trading data based on parameters

        Args:
            request_params: for the API request

        Returns:
            resonse.json(): JSON response from API

        Raises:
            ValueError: If params are invalid
            RequestException: If API request fails
        """

        try:
            # Construct parameters string
            params = ''
            for key, value in request_params.items():
                params += f'{key}={value}&'

            # Build complete URL
            url = self.base_url + params + f'apikey={self._api_key}'


            # Make API request
            response = requests.get(url)
            response.raise_for_status()

            return response.json()
        
        except requests.RequestException as e:
            raise RuntimeError(f"API request failed: {str(e)}")
