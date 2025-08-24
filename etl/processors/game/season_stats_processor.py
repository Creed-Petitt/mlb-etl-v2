#!/usr/bin/env python3

import logging
from datetime import datetime

from models import Game, PlayerSeasonStats, TeamSeasonStats, get_session

logger = logging.getLogger(__name__)

class SeasonStatsProcessor:
    """Handles player season stats and team season records"""
    
    def __init__(self, session=None):
        self.session = session or get_session()
        self.owns_session = session is None
        self.stats = {
            'player_season_stats_loaded': 0,
            'team_season_stats_loaded': 0
        }
        
    def process_season_stats(self, game_data, game_pk):
        """
        Process season stats ONLY if this is the most recent game for each team
        Returns True if successful, False otherwise
        """
        try:
            # Check if this game should update season stats
            home_team_id = game_data.get('team_home_id')
            away_team_id = game_data.get('team_away_id')
            
            if not home_team_id or not away_team_id:
                logger.warning("Missing team IDs, skipping season stats")
                return True
            
            # Only process if this is the most recent Final game for each team
            should_update_home = self._should_update_team_stats(home_team_id, game_pk)
            should_update_away = self._should_update_team_stats(away_team_id, game_pk)
            
            if should_update_home:
                logger.info(f"Updating season stats for home team {home_team_id} (most recent game)")
                self._process_team_season_stats(game_data, 'home')
                self._process_player_season_stats(game_data, 'home')
            else:
                logger.debug(f"Skipping home team {home_team_id} season stats (not most recent)")
                
            if should_update_away:
                logger.info(f"Updating season stats for away team {away_team_id} (most recent game)")
                self._process_team_season_stats(game_data, 'away')
                self._process_player_season_stats(game_data, 'away')
            else:
                logger.debug(f"Skipping away team {away_team_id} season stats (not most recent)")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing season stats: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def _should_update_team_stats(self, team_id, current_game_pk):
        """
        Check if this is the most recent Final game for the team
        Only update season stats on the team's most recent completed game
        """
        try:
            # Find the most recent Final game for this team
            most_recent_game = self.session.query(Game).filter(
                ((Game.home_team_id == team_id) | (Game.away_team_id == team_id)),
                Game.status_detailed.in_(['Final', 'F'])
            ).order_by(Game.official_date.desc(), Game.game_pk.desc()).first()
            
            if most_recent_game and most_recent_game.game_pk == current_game_pk:
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking if should update team {team_id} stats: {e}")
            return False
    
    def _process_team_season_stats(self, game_data, team_type):
        """Process team season statistics from team_data"""
        try:
            # Get team data from boxscore
            boxscore = game_data.get('boxscore', {})
            teams = boxscore.get('teams', {})
            team_data = teams.get(team_type, {})
            
            # Get team info
            team_info = team_data.get('team', {})
            
            # Get team ID from top level if not in team_info
            if team_type == 'home':
                team_id = game_data.get('team_home_id')
            else:
                team_id = game_data.get('team_away_id')
            
            if not team_id or team_id == 0:
                logger.warning(f"No valid team ID found for {team_type} team")
                return
            
            # Extract team info
            season = 2025  # Current season
            team_name = team_data.get('name', '')
            abbreviation = team_data.get('abbreviation', '')
            club_name = team_data.get('clubName', '')
            location_name = team_data.get('locationName', '')
            
            # League/Division info
            league_info = team_data.get('league', {})
            division_info = team_data.get('division', {})
            
            # Season record from team_data.record or seasonStats
            record = team_data.get('record', {})
            
            # Check if team season stats already exist
            existing_stats = self.session.query(TeamSeasonStats).filter_by(
                team_id=team_id, 
                season=season
            ).first()
            
            if existing_stats:
                # Update existing record
                team_stats = existing_stats
                logger.debug(f"Updating existing team season stats for {team_name}")
            else:
                # Create new record
                team_stats = TeamSeasonStats(team_id=team_id, season=season)
                logger.debug(f"Creating new team season stats for {team_name}")
                self.session.add(team_stats)
            
            # Update team information
            team_stats.team_name = team_name
            team_stats.team_abbreviation = abbreviation
            team_stats.club_name = club_name
            team_stats.location_name = location_name
            
            # League/Division
            team_stats.league_id = league_info.get('id')
            team_stats.league_name = league_info.get('name')
            team_stats.division_id = division_info.get('id')
            team_stats.division_name = division_info.get('name')
            
            # Season record
            team_stats.wins = record.get('wins', 0)
            team_stats.losses = record.get('losses', 0)
            team_stats.ties = record.get('ties', 0)
            team_stats.winning_percentage = record.get('pct', 0.0)
            team_stats.games_played = (team_stats.wins or 0) + (team_stats.losses or 0) + (team_stats.ties or 0)
            
            # Division standings
            team_stats.division_rank = record.get('divisionRank')
            team_stats.games_back_division = record.get('gamesBack')
            team_stats.is_division_leader = (team_stats.division_rank == 1)
            
            # League record
            league_record = record.get('leagueRecord', {})
            team_stats.league_wins = league_record.get('wins')
            team_stats.league_losses = league_record.get('losses')
            team_stats.league_pct = league_record.get('pct')
            
            # Update timestamp
            team_stats.last_updated = datetime.now()
            if not hasattr(team_stats, 'created_at') or not team_stats.created_at:
                team_stats.created_at = datetime.now()
            
            self.stats['team_season_stats_loaded'] += 1
            logger.debug(f"Processed team season stats for {team_name}: {team_stats.wins}-{team_stats.losses}")
            
        except Exception as e:
            logger.error(f"Error processing {team_type} team season stats: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    def _process_player_season_stats(self, game_data, team_type):
        """Process player season statistics from boxscore players"""
        try:
            boxscore = game_data.get('boxscore', {})
            teams = boxscore.get('teams', {})
            team_data = teams.get(team_type, {})
            players = team_data.get('players', {})
            
            season = 2025  # Current season
            
            for player_key, player_data in players.items():
                if not player_key.startswith('ID'):
                    continue
                
                try:
                    player_id = int(player_key.replace('ID', ''))
                    person = player_data.get('person', {})
                    player_name = person.get('fullName', f'Player {player_id}')
                    
                    # Get season stats
                    season_stats = player_data.get('seasonStats', {})
                    if not season_stats:
                        continue  # Skip if no season stats
                    
                    # Get position
                    position_info = player_data.get('position', {})
                    primary_position = position_info.get('name', 'Unknown')
                    
                    # Determine if player is pitcher or batter
                    is_pitcher = 'Pitcher' in primary_position
                    is_dh = primary_position == 'Designated Hitter'
                    
                    # Check if player season stats already exist
                    existing_stats = self.session.query(PlayerSeasonStats).filter_by(
                        player_id=player_id, 
                        season=season
                    ).first()
                    
                    if existing_stats:
                        # Update existing record
                        player_stats = existing_stats
                        logger.debug(f"Updating existing player season stats for {player_name}")
                    else:
                        # Create new record
                        player_stats = PlayerSeasonStats(player_id=player_id, season=season)
                        logger.debug(f"Creating new player season stats for {player_name}")
                        self.session.add(player_stats)
                    
                    # Update player info
                    player_stats.player_name = player_name
                    player_stats.primary_position = primary_position
                    
                    # Process stats based on player type
                    if is_pitcher:
                        # PITCHERS: Only load pitching stats
                        pitching_stats = season_stats.get('pitching', {})
                        if pitching_stats:
                            player_stats.pitching_games_played = pitching_stats.get('gamesPlayed', 0)
                            player_stats.games_started = pitching_stats.get('gamesStarted', 0)
                            player_stats.complete_games = pitching_stats.get('completeGames', 0)
                            player_stats.shutouts = pitching_stats.get('shutouts', 0)
                            player_stats.wins = pitching_stats.get('wins', 0)
                            player_stats.losses = pitching_stats.get('losses', 0)
                            player_stats.saves = pitching_stats.get('saves', 0)
                            player_stats.save_opportunities = pitching_stats.get('saveOpportunities', 0)
                            player_stats.holds = pitching_stats.get('holds', 0)
                            player_stats.blown_saves = pitching_stats.get('blownSaves', 0)
                            player_stats.innings_pitched = self._clean_float(pitching_stats.get('inningsPitched', 0.0))
                            player_stats.hits_allowed = pitching_stats.get('hits', 0)
                            player_stats.runs_allowed = pitching_stats.get('runs', 0)
                            player_stats.earned_runs = pitching_stats.get('earnedRuns', 0)
                            player_stats.home_runs_allowed = pitching_stats.get('homeRuns', 0)
                            player_stats.walks_allowed = pitching_stats.get('baseOnBalls', 0)
                            player_stats.hit_batsmen = pitching_stats.get('hitBatsmen', 0)
                            player_stats.pitcher_strikeouts = pitching_stats.get('strikeOuts', 0)
                            player_stats.wild_pitches = pitching_stats.get('wildPitches', 0)
                            player_stats.balks = pitching_stats.get('balks', 0)
                            player_stats.era = self._clean_float(pitching_stats.get('era', 0.0))
                            player_stats.whip = self._clean_float(pitching_stats.get('whip', 0.0))
                            player_stats.batters_faced = pitching_stats.get('battersFaced', 0)
                            player_stats.pitches_thrown = pitching_stats.get('pitchesThrown', 0)
                            player_stats.strikes = pitching_stats.get('strikes', 0)
                            player_stats.balls = pitching_stats.get('balls', 0)
                            player_stats.strike_percentage = self._clean_float(pitching_stats.get('strikePercentage', 0.0))
                    
                    else:
                        # BATTERS: Load batting stats + fielding (unless DH)
                        batting_stats = season_stats.get('batting', {})
                        if batting_stats:
                            player_stats.batting_games_played = batting_stats.get('gamesPlayed', 0)
                            player_stats.at_bats = batting_stats.get('atBats', 0)
                            player_stats.runs = batting_stats.get('runs', 0)
                            player_stats.hits = batting_stats.get('hits', 0)
                            player_stats.doubles = batting_stats.get('doubles', 0)
                            player_stats.triples = batting_stats.get('triples', 0)
                            player_stats.home_runs = batting_stats.get('homeRuns', 0)
                            player_stats.rbi = batting_stats.get('rbi', 0)
                            player_stats.walks = batting_stats.get('baseOnBalls', 0)
                            player_stats.strikeouts = batting_stats.get('strikeOuts', 0)
                            player_stats.stolen_bases = batting_stats.get('stolenBases', 0)
                            player_stats.caught_stealing = batting_stats.get('caughtStealing', 0)
                            player_stats.batting_avg = self._clean_float(batting_stats.get('avg', 0.0))
                            player_stats.on_base_pct = self._clean_float(batting_stats.get('obp', 0.0))
                            player_stats.slugging_pct = self._clean_float(batting_stats.get('slg', 0.0))
                            player_stats.ops = self._clean_float(batting_stats.get('ops', 0.0))
                            player_stats.plate_appearances = batting_stats.get('plateAppearances', 0)
                            player_stats.hit_by_pitch = batting_stats.get('hitByPitch', 0)
                            player_stats.sac_flies = batting_stats.get('sacFlies', 0)
                            player_stats.sac_bunts = batting_stats.get('sacBunts', 0)
                            player_stats.intentional_walks = batting_stats.get('intentionalWalks', 0)
                            player_stats.ground_into_double_play = batting_stats.get('groundIntoDoublePlay', 0)
                            player_stats.total_bases = batting_stats.get('totalBases', 0)
                        
                        # Fielding stats (skip for DH)
                        if not is_dh:
                            fielding_stats = season_stats.get('fielding', {})
                            if fielding_stats:
                                player_stats.fielding_games = fielding_stats.get('games', 0)
                                player_stats.fielding_games_started = fielding_stats.get('gamesStarted', 0)
                                player_stats.innings_fielded = self._clean_float(fielding_stats.get('innings', 0.0))
                                player_stats.putouts = fielding_stats.get('putOuts', 0)
                                player_stats.assists = fielding_stats.get('assists', 0)
                                player_stats.errors = fielding_stats.get('errors', 0)
                                player_stats.double_plays = fielding_stats.get('doublePlays', 0)
                                player_stats.fielding_percentage = self._clean_float(fielding_stats.get('fielding', 0.0))
                                player_stats.range_factor = self._clean_float(fielding_stats.get('rangeFactor', 0.0))
                    
                    # Update timestamp
                    player_stats.last_updated = datetime.now()
                    if not hasattr(player_stats, 'created_at') or not player_stats.created_at:
                        player_stats.created_at = datetime.now()
                    
                    self.stats['player_season_stats_loaded'] += 1
                    
                except ValueError:
                    # Skip invalid player IDs
                    continue
                except Exception as e:
                    logger.error(f"Error processing player {player_key}: {e}")
                    continue
            
            logger.debug(f"Processed {self.stats['player_season_stats_loaded']} player season stats for {team_type} team")
            
        except Exception as e:
            logger.error(f"Error processing {team_type} player season stats: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    def _clean_float(self, value):
        """Clean float values from API (handle -.-- and - strings)"""
        if value is None:
            return None
        
        # Convert to string to check for invalid values
        str_value = str(value)
        
        # Handle invalid string representations
        if str_value in ['-.--', '-', '-.---', '']:
            return None
        
        try:
            return float(str_value)
        except (ValueError, TypeError):
            return None
    
    def get_stats(self):
        """Return processing statistics"""
        return self.stats.copy()
        
    def close(self):
        """Close database session if owned"""
        if self.owns_session and self.session:
            self.session.close()