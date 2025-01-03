from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.orm import relationship
from .database import Base

class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    team = Column(String)
    position = Column(String)
    birth_date = Column(Date)

    # Relationships
    batting_stats = relationship("BattingStats", back_populates="player")
    pitching_stats = relationship("PitchingStats", back_populates="player")

class BattingStats(Base):
    __tablename__ = "batting_stats"

    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"), index=True)
    year = Column(Integer, index=True)
    
    # Relationships
    player = relationship("Player", back_populates="batting_stats")
    
    # Standard Stats
    games = Column(Integer)
    pa = Column(Integer)
    ab = Column(Integer)
    runs = Column(Integer)
    hits = Column(Integer)
    doubles = Column(Integer)
    triples = Column(Integer)
    hr = Column(Integer)
    rbi = Column(Integer)
    sb = Column(Integer)
    cs = Column(Integer)
    bb = Column(Integer)
    ibb = Column(Integer)
    so = Column(Integer)
    hbp = Column(Integer)
    sf = Column(Integer)
    sh = Column(Integer)
    gdp = Column(Integer)
    
    # Rate Stats
    avg = Column(Float)
    obp = Column(Float)
    slg = Column(Float)
    ops = Column(Float)
    iso = Column(Float)
    babip = Column(Float)
    
    # Advanced Stats
    woba = Column(Float)
    wrc_plus = Column(Float)
    war = Column(Float)
    
    # Plate Discipline
    o_swing_pct = Column(Float)
    z_swing_pct = Column(Float)
    swing_pct = Column(Float)
    o_contact_pct = Column(Float)
    z_contact_pct = Column(Float)
    contact_pct = Column(Float)
    zone_pct = Column(Float)
    f_strike_pct = Column(Float)
    swstr_pct = Column(Float)
    cstr_pct = Column(Float)
    csw_pct = Column(Float)
    
    # Batted Ball
    gb_pct = Column(Float)
    fb_pct = Column(Float)
    ld_pct = Column(Float)
    iffb_pct = Column(Float)
    hr_fb = Column(Float)
    pull_pct = Column(Float)
    cent_pct = Column(Float)
    oppo_pct = Column(Float)
    soft_pct = Column(Float)
    med_pct = Column(Float)
    hard_pct = Column(Float)
    
    # Value Stats
    batting_runs = Column(Float)
    baserunning_runs = Column(Float)
    fielding_runs = Column(Float)
    positional = Column(Float)
    offense = Column(Float)
    defense = Column(Float)
    league = Column(Float)
    replacement = Column(Float)
    rar = Column(Float)
    dollars = Column(Float)
    
    # Win Probability
    wpa = Column(Float)
    neg_wpa = Column(Float)
    pos_wpa = Column(Float)
    re24 = Column(Float)
    rew = Column(Float)
    pli = Column(Float)
    phli = Column(Float)
    clutch = Column(Float)
    
    # Pitch Values
    wfb = Column(Float)  # Fastball runs
    wsl = Column(Float)  # Slider runs
    wct = Column(Float)  # Cutter runs
    wcb = Column(Float)  # Curveball runs
    wch = Column(Float)  # Changeup runs
    
    # Statcast Data
    exit_velocity = Column(Float)
    launch_angle = Column(Float)
    barrel_pct = Column(Float)
    hard_hit_pct = Column(Float)
    xba = Column(Float)
    xslg = Column(Float)
    xwoba = Column(Float)

class PitchingStats(Base):
    __tablename__ = "pitching_stats"

    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"), index=True)
    year = Column(Integer, index=True)
    
    # Relationships
    player = relationship("Player", back_populates="pitching_stats")
    
    # Traditional Stats
    games = Column(Integer)
    games_started = Column(Integer)
    wins = Column(Integer)
    losses = Column(Integer)
    saves = Column(Integer)
    holds = Column(Integer)
    innings = Column(Float)
    hits_allowed = Column(Integer)
    runs = Column(Integer)
    earned_runs = Column(Integer)
    hr_allowed = Column(Integer)
    bb = Column(Integer)
    ibb = Column(Integer)
    so = Column(Integer)
    hbp = Column(Integer)
    wp = Column(Integer)
    bk = Column(Integer)
    
    # Rate Stats
    era = Column(Float)
    whip = Column(Float)
    k_9 = Column(Float)
    bb_9 = Column(Float)
    hr_9 = Column(Float)
    k_bb = Column(Float)
    
    # Advanced Stats
    fip = Column(Float)
    xfip = Column(Float)
    siera = Column(Float)
    war = Column(Float)
    babip = Column(Float)
    lob_pct = Column(Float)
    k_pct = Column(Float)
    bb_pct = Column(Float)
    hr_fb = Column(Float)
    gb_pct = Column(Float)
    fb_pct = Column(Float)
    ld_pct = Column(Float)
    
    # Win Probability
    wpa = Column(Float)
    neg_wpa = Column(Float)
    pos_wpa = Column(Float)
    re24 = Column(Float)
    rew = Column(Float)
    pli = Column(Float)
    inli = Column(Float)
    clutch = Column(Float)
    
    # Pitch Type Stats
    fa_pct = Column(Float)  # Fastball percentage
    fc_pct = Column(Float)  # Cutter percentage
    fs_pct = Column(Float)  # Splitter percentage
    si_pct = Column(Float)  # Sinker percentage
    sl_pct = Column(Float)  # Slider percentage
    cu_pct = Column(Float)  # Curveball percentage
    kc_pct = Column(Float)  # Knuckle curve percentage
    ch_pct = Column(Float)  # Changeup percentage
    
    # Pitch Values
    wfb = Column(Float)  # Fastball runs
    wsl = Column(Float)  # Slider runs
    wct = Column(Float)  # Cutter runs
    wcb = Column(Float)  # Curveball runs
    wch = Column(Float)  # Changeup runs
    
    # Statcast Data
    avg_velocity = Column(Float)
    max_velocity = Column(Float)
    spin_rate = Column(Float)
    whiff_pct = Column(Float)
    chase_rate = Column(Float)
    csw_rate = Column(Float)
    barrel_pct = Column(Float)
    hard_hit_pct = Column(Float)
