from sqlalchemy.orm import Session
from . import models
from typing import List, Optional

def get_player(db: Session, player_id: int):
    return db.query(models.Player).filter(models.Player.id == player_id).first()

def get_players(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Player).offset(skip).limit(limit).all()

def get_batting_stats(db: Session, player_id: int, year: Optional[int] = None):
    query = db.query(models.BattingStats).filter(models.BattingStats.player_id == player_id)
    if year:
        query = query.filter(models.BattingStats.year == year)
    return query.all()

def create_player(db: Session, name: str, team: str, position: str, birth_date: str):
    db_player = models.Player(
        name=name,
        team=team,
        position=position,
        birth_date=birth_date
    )
    db.add(db_player)
    db.commit()
    db.refresh(db_player)
    return db_player

def update_player(db: Session, player_id: int, **kwargs):
    db_player = get_player(db, player_id)
    for key, value in kwargs.items():
        setattr(db_player, key, value)
    db.commit()
    db.refresh(db_player)
    return db_player

def delete_player(db: Session, player_id: int):
    db_player = get_player(db, player_id)
    db.delete(db_player)
    db.commit()
    return db_player 