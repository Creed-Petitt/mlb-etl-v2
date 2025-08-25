#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from sqlalchemy import and_

from models import Game, EspnOdds, get_session

logger = logging.getLogger(__name__)

class ESPNBettingProcessor:
    
    def __init__(self, session=None):
        self.session = session or get_session()
        self.owns_session = session is None
        
        # Stats tracking
        self.stats = {
            'games_processed': 0,
            'games_matched': 0,
            'games_unmatched': 0,
            'odds_extracted': 0,
            'odds_stored': 0
        }
        
        # Team abbreviation mappings (ESPN → Our Database)
        self.team_mappings = {
            'OAK': 'ATH',  # Oakland Athletics
            'CHW': 'CWS',  # Chicago White Sox
            'ARI': 'AZ',   # Arizona Diamondbacks
            'WSH': 'WSH',  # Washington Nationals (same)
            'SFG': 'SF',   # San Francisco Giants
            'SDP': 'SD',   # San Diego Padres
            'TBR': 'TB',   # Tampa Bay Rays
            'KCR': 'KC',   # Kansas City Royals
            'PHI': 'PHI',  # Philadelphia (same)
            'DET': 'DET',  # Detroit (same)
            'MIL': 'MIL',  # Milwaukee (same)
            'LAD': 'LAD',  # Los Angeles Dodgers (same)
            'STL': 'STL'   # St Louis Cardinals (same)
        }
    
    def process_espn_response(self, espn_data: Dict) -> List[Dict]:
        games_with_odds = []
        
        try:
            sports = espn_data.get('sports', [])
            if not sports:
                logger.warning("No sports data found in ESPN response")
                return games_with_odds
            
            events = sports[0].get('leagues', [{}])[0].get('events', [])
            logger.info(f"Processing {len(events)} games from ESPN")
            
            for event in events:
                game_data = self._extract_game_data(event)
                if game_data:
                    # Match with database game
                    matched_game = self._match_game_with_database(game_data)
                    if matched_game:
                        game_data['game_pk'] = matched_game.game_pk
                        game_data['matched'] = True
                        self.stats['games_matched'] += 1
                    else:
                        game_data['matched'] = False
                        self.stats['games_unmatched'] += 1
                    
                    games_with_odds.append(game_data)
                    self.stats['games_processed'] += 1
            
            logger.info(f"Processed {len(games_with_odds)} games with odds data")
            return games_with_odds
            
        except Exception as e:
            logger.error(f"Error processing ESPN response: {e}")
            return games_with_odds
    
    def _get_game_status(self, event: Dict) -> Optional[str]:
        try:
            status = event.get('status')
            if isinstance(status, dict):
                return status.get('type', {}).get('state')
            elif isinstance(status, str):
                return status
            return None
        except:
            return None
    
    def _extract_game_data(self, event: Dict) -> Optional[Dict]:
        try:
            # Basic game info
            game_data = {
                'espn_game_id': event.get('id'),
                'game_date': None,
                'start_time': None,
                'venue_name': event.get('location'),
                'game_status': self._get_game_status(event),
                'home_team_id': None,
                'home_team_name': None,
                'home_team_abbreviation': None,
                'away_team_id': None,
                'away_team_name': None,
                'away_team_abbreviation': None,
                'odds': []
            }
            
            # Parse date
            date_str = event.get('date', '')
            if date_str:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                game_data['game_date'] = dt.date()
                game_data['start_time'] = dt
            
            # Extract team info
            competitors = event.get('competitors', [])
            for competitor in competitors:
                team_id = competitor.get('id')
                team_name = competitor.get('displayName')
                team_abbr = competitor.get('abbreviation')
                
                if competitor.get('homeAway') == 'home':
                    game_data['home_team_id'] = team_id
                    game_data['home_team_name'] = team_name
                    game_data['home_team_abbreviation'] = team_abbr
                else:
                    game_data['away_team_id'] = team_id
                    game_data['away_team_name'] = team_name
                    game_data['away_team_abbreviation'] = team_abbr
            
            # Extract odds data
            odds_data = event.get('odds', {})
            if odds_data and odds_data.get('provider', {}).get('name') == 'ESPN BET':
                self._extract_odds_data(odds_data, game_data)
            
            return game_data
            
        except Exception as e:
            logger.error(f"Error extracting game data for event {event.get('id', 'unknown')}: {e}")
            return None
    
    def _extract_odds_data(self, odds_data: Dict, game_data: Dict):
        try:
            espn_game_id = game_data['espn_game_id']
            
            # Extract moneyline odds
            moneyline = odds_data.get('moneyline', {})
            if moneyline:
                # Home moneyline
                home_ml = moneyline.get('home', {})
                if home_ml:
                    game_data['odds'].append({
                        'espn_game_id': espn_game_id,
                        'bet_type': 'moneyline',
                        'bet_side': 'home',
                        'open_odds': home_ml.get('open', {}).get('odds'),
                        'close_odds': home_ml.get('close', {}).get('odds'),
                        'final_odds': home_ml.get('current', {}).get('odds'),
                        'outcome': home_ml.get('close', {}).get('outcome'),
                        'provider_name': 'ESPN BET'
                    })
                    self.stats['odds_extracted'] += 1
                
                # Away moneyline
                away_ml = moneyline.get('away', {})
                if away_ml:
                    game_data['odds'].append({
                        'espn_game_id': espn_game_id,
                        'bet_type': 'moneyline',
                        'bet_side': 'away',
                        'open_odds': away_ml.get('open', {}).get('odds'),
                        'close_odds': away_ml.get('close', {}).get('odds'),
                        'final_odds': away_ml.get('current', {}).get('odds'),
                        'outcome': away_ml.get('close', {}).get('outcome'),
                        'provider_name': 'ESPN BET'
                    })
                    self.stats['odds_extracted'] += 1
            
            # Extract runline odds (pointSpread in ESPN data)
            runline = odds_data.get('pointSpread', {})
            if runline:
                # Home runline
                home_rl = runline.get('home', {})
                if home_rl:
                    game_data['odds'].append({
                        'espn_game_id': espn_game_id,
                        'bet_type': 'runline',
                        'bet_side': 'home',
                        'open_line': home_rl.get('open', {}).get('line'),
                        'open_odds': home_rl.get('open', {}).get('odds'),
                        'close_line': home_rl.get('close', {}).get('line'),
                        'close_odds': home_rl.get('close', {}).get('odds'),
                        'final_line': home_rl.get('current', {}).get('line'),
                        'final_odds': home_rl.get('current', {}).get('odds'),
                        'outcome': home_rl.get('close', {}).get('outcome'),
                        'provider_name': 'ESPN BET'
                    })
                    self.stats['odds_extracted'] += 1
                
                # Away runline
                away_rl = runline.get('away', {})
                if away_rl:
                    game_data['odds'].append({
                        'espn_game_id': espn_game_id,
                        'bet_type': 'runline',
                        'bet_side': 'away',
                        'open_line': away_rl.get('open', {}).get('line'),
                        'open_odds': away_rl.get('open', {}).get('odds'),
                        'close_line': away_rl.get('close', {}).get('line'),
                        'close_odds': away_rl.get('close', {}).get('odds'),
                        'final_line': away_rl.get('current', {}).get('line'),
                        'final_odds': away_rl.get('current', {}).get('odds'),
                        'outcome': away_rl.get('close', {}).get('outcome'),
                        'provider_name': 'ESPN BET'
                    })
                    self.stats['odds_extracted'] += 1
            
            # Extract total (over/under) odds
            total = odds_data.get('total', {})
            if total:
                # Over
                over = total.get('over', {})
                if over:
                    game_data['odds'].append({
                        'espn_game_id': espn_game_id,
                        'bet_type': 'total',
                        'bet_side': 'over',
                        'open_line': over.get('open', {}).get('line'),
                        'open_odds': over.get('open', {}).get('odds'),
                        'close_line': over.get('close', {}).get('line'),
                        'close_odds': over.get('close', {}).get('odds'),
                        'final_line': over.get('current', {}).get('line'),
                        'final_odds': over.get('current', {}).get('odds'),
                        'outcome': over.get('close', {}).get('outcome'),
                        'provider_name': 'ESPN BET'
                    })
                    self.stats['odds_extracted'] += 1
                
                # Under
                under = total.get('under', {})
                if under:
                    game_data['odds'].append({
                        'espn_game_id': espn_game_id,
                        'bet_type': 'total',
                        'bet_side': 'under',
                        'open_line': under.get('open', {}).get('line'),
                        'open_odds': under.get('open', {}).get('odds'),
                        'close_line': under.get('close', {}).get('line'),
                        'close_odds': under.get('close', {}).get('odds'),
                        'final_line': under.get('current', {}).get('line'),
                        'final_odds': under.get('current', {}).get('odds'),
                        'outcome': under.get('close', {}).get('outcome'),
                        'provider_name': 'ESPN BET'
                    })
                    self.stats['odds_extracted'] += 1
            
        except Exception as e:
            logger.error(f"Error extracting odds data: {e}")
    
    def _match_game_with_database(self, game_data: Dict) -> Optional[Game]:
        try:
            if not game_data['game_date']:
                return None
            
            # Normalize team abbreviations
            home_abbr = self._normalize_team_abbr(game_data['home_team_abbreviation'])
            away_abbr = self._normalize_team_abbr(game_data['away_team_abbreviation'])
            
            # Try to match by date and team abbreviations
            matched_game = self.session.query(Game).filter(
                and_(
                    Game.official_date == game_data['game_date'],
                    Game.home_team_abbreviation == home_abbr,
                    Game.away_team_abbreviation == away_abbr
                )
            ).first()
            
            if matched_game:
                logger.debug(f"Matched ESPN game {game_data['espn_game_id']} with DB game {matched_game.game_pk}")
            else:
                logger.warning(f"No match for ESPN game: {away_abbr} @ {home_abbr} on {game_data['game_date']}")
            
            return matched_game
            
        except Exception as e:
            logger.error(f"Error matching game with database: {e}")
            return None
    
    def _normalize_team_abbr(self, abbr: str) -> str:
        if not abbr:
            return abbr
        return self.team_mappings.get(abbr, abbr)
    
    def store_odds_records(self, games_with_odds: List[Dict]) -> Tuple[int, int]:
        created = 0
        updated = 0
        
        for game_data in games_with_odds:
            if not game_data.get('matched') or not game_data.get('game_pk'):
                continue
            
            game_pk = game_data['game_pk']
            
            for odds_data in game_data['odds']:
                try:
                    # Check if odds record exists
                    existing_odds = self.session.query(EspnOdds).filter_by(
                        game_pk=game_pk,
                        bet_type=odds_data['bet_type'],
                        bet_side=odds_data['bet_side']
                    ).first()
                    
                    if existing_odds:
                        # Update existing record
                        for key, value in odds_data.items():
                            if key not in ['espn_game_id'] and hasattr(existing_odds, key):
                                setattr(existing_odds, key, value)
                        existing_odds.updated_at = datetime.now()
                        updated += 1
                    else:
                        # Create new record
                        new_odds = EspnOdds(
                            game_pk=game_pk,
                            espn_game_id=odds_data.get('espn_game_id'),
                            bet_type=odds_data['bet_type'],
                            bet_side=odds_data['bet_side'],
                            open_line=str(odds_data.get('open_line')) if odds_data.get('open_line') else None,
                            open_odds=str(odds_data.get('open_odds')) if odds_data.get('open_odds') else None,
                            close_line=str(odds_data.get('close_line')) if odds_data.get('close_line') else None,
                            close_odds=str(odds_data.get('close_odds')) if odds_data.get('close_odds') else None,
                            final_line=str(odds_data.get('final_line')) if odds_data.get('final_line') else None,
                            final_odds=str(odds_data.get('final_odds')) if odds_data.get('final_odds') else None,
                            outcome=odds_data.get('outcome'),
                            provider_name=odds_data.get('provider_name', 'ESPN BET'),
                            created_at=datetime.now(),
                            updated_at=datetime.now()
                        )
                        self.session.add(new_odds)
                        created += 1
                    
                    self.stats['odds_stored'] += 1
                    
                except Exception as e:
                    logger.error(f"Error storing odds record: {e}")
                    continue
        
        try:
            self.session.commit()
            return created, updated
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error committing odds records: {e}")
            return 0, 0
    
    def get_stats(self) -> Dict:
        return self.stats.copy()
    
    def close(self):
        if self.owns_session and self.session:
            self.session.close()