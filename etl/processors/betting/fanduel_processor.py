#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal

from models import (
    FanDuelBook, FanDuelEvent, FanDuelMarket, 
    FanDuelRunner, FanDuelPrice, get_session
)

logger = logging.getLogger(__name__)

class FanDuelProcessor:
    
    def __init__(self, session=None):
        self.session = session or get_session()
        self.owns_session = session is None
        
        # Get or create FanDuel book
        self.book = self.session.query(FanDuelBook).filter_by(name="FanDuel").first()
        if not self.book:
            self.book = FanDuelBook(name="FanDuel")
            self.session.add(self.book)
            self.session.commit()
        
        # Stats tracking
        self.stats = {
            'events_processed': 0,
            'markets_processed': 0,
            'runners_processed': 0,
            'prices_processed': 0,
            'futures_processed': 0,
            'props_processed': 0,
            'game_lines_processed': 0,
            'errors': 0
        }
    
    def process_mlb_page(self, page_data: Dict[str, Any]) -> Dict:
        """Process complete MLB page data"""
        
        attachments = page_data.get('attachments', {})
        events = attachments.get('events', {})
        markets = attachments.get('markets', {})
        
        logger.info(f"Processing {len(events)} events and {len(markets)} markets")
        
        # Process events first
        self._process_events(events)
        
        # Process markets with their runners
        market_ids = self._process_markets(markets, events)
        
        # Return market IDs for price fetching
        return {
            'market_ids': market_ids,
            'stats': self.stats.copy()
        }
    
    def _process_events(self, events: Dict[str, Any]):
        """Process and store events"""
        
        for event_id, event_data in events.items():
            try:
                # Extract event info
                book_event_id = str(event_data.get('eventId'))
                
                # Check if event exists
                existing = self.session.query(FanDuelEvent).filter_by(
                    book_id=self.book.id,
                    book_event_id=book_event_id
                ).first()
                
                # Parse open date
                open_date = None
                open_date_str = event_data.get('openDate')
                if open_date_str:
                    try:
                        open_date = datetime.fromisoformat(open_date_str.replace('Z', '+00:00'))
                    except:
                        pass
                
                if existing:
                    # Update existing event
                    existing.event_name = event_data.get('name')
                    existing.open_date = open_date
                    existing.status = 'OPEN' if not event_data.get('isSuspended') else 'SUSPENDED'
                    existing.updated_at = datetime.now()
                else:
                    # Create new event
                    event = FanDuelEvent(
                        book_id=self.book.id,
                        book_event_id=book_event_id,
                        competition_id=str(event_data.get('competitionId', '')),
                        event_type_id=str(event_data.get('eventTypeId', '')),
                        country_code='US',
                        event_name=event_data.get('name'),
                        market_group='MLB',
                        open_date=open_date,
                        status='OPEN' if not event_data.get('isSuspended') else 'SUSPENDED'
                    )
                    self.session.add(event)
                
                self.stats['events_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing event {event_id}: {e}")
                self.stats['errors'] += 1
    
    def _process_markets(self, markets: Dict[str, Any], events: Dict[str, Any]) -> List[str]:
        """Process and store markets with runners"""
        
        market_ids = []
        
        for market_id, market_data in markets.items():
            try:
                book_market_id = str(market_id)
                market_ids.append(book_market_id)
                
                # Get associated event
                event_id = market_data.get('eventId')
                event = None
                if event_id:
                    event = self.session.query(FanDuelEvent).filter_by(
                        book_id=self.book.id,
                        book_event_id=str(event_id)
                    ).first()
                
                # Check if market exists
                existing = self.session.query(FanDuelMarket).filter_by(
                    book_market_id=book_market_id
                ).first()
                
                # Categorize market
                market_type = market_data.get('marketType', '')
                market_category, market_key = self._categorize_market(market_type, market_data)
                
                # Update stats based on category
                if market_category == 'future':
                    self.stats['futures_processed'] += 1
                elif market_category == 'player_prop':
                    self.stats['props_processed'] += 1
                elif market_category == 'game':
                    self.stats['game_lines_processed'] += 1
                
                if existing:
                    # Update existing market
                    existing.market_name = market_data.get('marketName')
                    existing.status = 'OPEN' if not market_data.get('isSuspended') else 'SUSPENDED'
                    existing.in_play = market_data.get('inPlay', False)
                    existing.market_category = market_category
                    existing.market_key = market_key
                    existing.updated_at = datetime.now()
                else:
                    # Create new market
                    market = FanDuelMarket(
                        book_id=self.book.id,
                        event_id=event.id if event else None,
                        book_market_id=book_market_id,
                        market_type=market_type,
                        market_name=market_data.get('marketName'),
                        market_level='AVB_EVENT',
                        in_play=market_data.get('inPlay', False),
                        sgm_market=market_data.get('sgmMarket', False),
                        status='OPEN' if not market_data.get('isSuspended') else 'SUSPENDED',
                        market_category=market_category,
                        market_key=market_key
                    )
                    self.session.add(market)
                    self.session.flush()  # Get market.id for runners
                    existing = market
                
                # Process runners for this market
                self._process_runners(existing, market_data.get('runners', []))
                
                self.stats['markets_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing market {market_id}: {e}")
                self.stats['errors'] += 1
        
        return market_ids
    
    def _process_runners(self, market: FanDuelMarket, runners: List[Dict]):
        """Process runners (selections) for a market"""
        
        for runner_data in runners:
            try:
                selection_id = str(runner_data.get('selectionId'))
                
                # Check if runner exists
                existing = self.session.query(FanDuelRunner).filter_by(
                    market_id=market.id,
                    selection_id=selection_id
                ).first()
                
                # Extract handicap
                handicap = None
                if runner_data.get('handicap') is not None:
                    try:
                        handicap = Decimal(str(runner_data['handicap']))
                    except:
                        pass
                
                if existing:
                    # Update existing runner
                    existing.runner_name = runner_data.get('runnerName')
                    existing.handicap = handicap
                    existing.runner_status = runner_data.get('runnerStatus', 'OPEN')
                    existing.sort_priority = runner_data.get('sortPriority')
                    existing.updated_at = datetime.now()
                else:
                    # Create new runner
                    runner = FanDuelRunner(
                        market_id=market.id,
                        selection_id=selection_id,
                        runner_name=runner_data.get('runnerName'),
                        handicap=handicap,
                        is_player=self._is_player_runner(market.market_type, runner_data),
                        runner_status=runner_data.get('runnerStatus', 'OPEN'),
                        sort_priority=runner_data.get('sortPriority')
                    )
                    self.session.add(runner)
                
                self.stats['runners_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing runner: {e}")
                self.stats['errors'] += 1
    
    def process_market_prices(self, prices_data: List[Dict]) -> int:
        """Process and store market prices"""
        
        if not prices_data:
            return 0
        
        count = 0
        
        for market_price in prices_data:
            try:
                market_id = str(market_price.get('marketId'))
                
                # Find market in database
                market = self.session.query(FanDuelMarket).filter_by(
                    book_market_id=market_id
                ).first()
                
                if not market:
                    continue
                
                # Process each runner's price
                for runner_price in market_price.get('runners', []):
                    selection_id = str(runner_price.get('selectionId'))
                    
                    # Find runner
                    runner = self.session.query(FanDuelRunner).filter_by(
                        market_id=market.id,
                        selection_id=selection_id
                    ).first()
                    
                    # Extract odds
                    win_runner_odds = runner_price.get('winRunnerOdds', {})
                    
                    # Create price record
                    price = FanDuelPrice(
                        market_id=market.id,
                        runner_id=runner.id if runner else None,
                        selection_id=selection_id,
                        american_odds=win_runner_odds.get('americanDisplayOdds', {}).get('americanOdds'),
                        decimal_odds=Decimal(str(win_runner_odds.get('decimalDisplayOdds', {}).get('decimalOdds', 0))) if win_runner_odds.get('decimalDisplayOdds') else None,
                        fractional_numerator=win_runner_odds.get('fractionalDisplayOdds', {}).get('numerator'),
                        fractional_denominator=win_runner_odds.get('fractionalDisplayOdds', {}).get('denominator'),
                        true_decimal_odds=Decimal(str(win_runner_odds.get('trueOdds', {}).get('decimalOdds', {}).get('decimalOdds', 0))) if win_runner_odds.get('trueOdds') else None,
                        line=runner.handicap if runner else None,
                        in_play=market_price.get('inplay', False)
                    )
                    self.session.add(price)
                    count += 1
                    
            except Exception as e:
                logger.error(f"Error processing price: {e}")
                self.stats['errors'] += 1
        
        self.stats['prices_processed'] += count
        return count
    
    def _categorize_market(self, market_type: str, market_data: Dict) -> Tuple[str, str]:
        """Categorize market into type and key"""
        
        market_name = market_data.get('marketName', '')
        
        # Futures
        if any(x in market_type for x in ['DIVISION_WINNER', 'LEAGUE_WINNER', 'OUTRIGHT', 'CY_YOUNG', 'MVP', 'ROY', 'WORLD_SERIES']):
            return 'future', market_type.lower()
        
        # Player props
        if 'PLAYER_' in market_type or 'PITCHER_' in market_type or 'TO_RECORD' in market_type:
            return 'player_prop', market_type.lower()
        
        # First innings
        if '1ST_HALF' in market_type or 'FIRST_5' in market_name:
            return 'game', f"f5_{market_type.lower()}"
        
        # Standard game markets
        if market_type in ['MATCH_BETTING', 'MATCH_HANDICAP_(2-WAY)', 'TOTAL_POINTS_(OVER/UNDER)']:
            return 'game', market_type.lower()
        
        # Default
        return 'other', market_type.lower()
    
    def _is_player_runner(self, market_type: str, runner_data: Dict) -> bool:
        """Determine if runner is a player"""
        return 'PLAYER_' in market_type or 'PITCHER_' in market_type or 'TO_RECORD' in market_type
    
    def commit_changes(self):
        """Commit all changes to database"""
        try:
            self.session.commit()
            logger.info("Successfully committed all changes")
            return True
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error committing changes: {e}")
            return False
    
    def get_stats(self) -> Dict:
        """Get processing statistics"""
        return self.stats.copy()
    
    def close(self):
        """Close session if we own it"""
        if self.owns_session and self.session:
            self.session.close()