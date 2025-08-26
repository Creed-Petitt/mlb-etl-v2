#!/usr/bin/env python3

import requests
import logging
import os
import time
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class FanDuelClient:
    
    def __init__(self):
        self.session = requests.Session()
        
        # Browser-like headers to satisfy CloudFront/PerimeterX
        self.session.headers.update({
            "accept": "application/json",
            "accept-language": "en-US,en;q=0.9",
            "accept-encoding": "gzip, deflate, br, zstd",
            "origin": "https://sportsbook.fanduel.com",
            "referer": "https://sportsbook.fanduel.com/",
            "sec-ch-ua": '"Not;A=Brand";v="99", "Microsoft Edge";v="139", "Chromium";v="139"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": os.getenv(
                "FANDUEL_USER_AGENT",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0"
            ),
            "x-sportsbook-region": "NJ"
        })
        
        # Required PerimeterX context token
        px_context = os.getenv("FANDUEL_PX_CONTEXT")
        if px_context:
            self.session.headers["x-px-context"] = px_context
        else:
            logger.warning("No FANDUEL_PX_CONTEXT token found in environment")
        
        # Base URLs
        self.mlb_page_url = (
            "https://api.sportsbook.fanduel.com/sbapi/content-managed-page"
            "?page=CUSTOM&customPageId=mlb&pbHorizontal=false&_ak=FhMFpcPWXMeyZxOx"
            "&timezone=America%2FNew_York"
        )
        
        self.prices_url = (
            "https://smp.nj.sportsbook.fanduel.com/api/sports/fixedodds/readonly/v1/getMarketPrices"
        )
    
    def fetch_mlb_page(self) -> Optional[Dict[str, Any]]:
        """
        Fetch the complete MLB page with all markets and events
        """
        try:
            logger.info("Fetching FanDuel MLB page data...")
            
            response = self.session.get(self.mlb_page_url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Quick validation
            attachments = data.get('attachments', {})
            markets = attachments.get('markets', {})
            events = attachments.get('events', {})
            
            logger.info(f"Successfully fetched FanDuel data: {len(markets)} markets, {len(events)} events")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching FanDuel MLB page: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching FanDuel data: {e}")
            return None
    
    def fetch_market_prices(self, market_ids: List[str], include_history: bool = False) -> Optional[List[Dict]]:
        """
        Fetch current prices for specific markets
        
        Args:
            market_ids: List of FanDuel market IDs
            include_history: Whether to include price history
        """
        if not market_ids:
            return []
        
        try:
            logger.info(f"Fetching prices for {len(market_ids)} markets...")
            
            # FanDuel has a limit on how many markets per request
            # Split into batches if needed
            batch_size = 50
            all_prices = []
            
            for i in range(0, len(market_ids), batch_size):
                batch = market_ids[i:i + batch_size]
                
                payload = {
                    "marketIds": batch
                }
                
                params = {
                    "priceHistory": "1" if include_history else "0"
                }
                
                response = self.session.post(
                    self.prices_url,
                    json=payload,
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                
                prices = response.json()
                all_prices.extend(prices)
                
                # Small delay between batches to be respectful
                if i + batch_size < len(market_ids):
                    time.sleep(0.5)
            
            logger.info(f"Successfully fetched prices for {len(all_prices)} markets")
            return all_prices
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching market prices: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching prices: {e}")
            return None
    
    def close(self):
        if self.session:
            self.session.close()