"""The central module containing all code dealing with importing and
adjusting data from demandRegio

"""
import os
import pandas as pd
import subprocess
import egon.data.config
from egon.data import db
from sqlalchemy import Column, String, Float, Integer
from sqlalchemy.ext.declarative import declarative_base

if not os.path.exists('disaggregator'):
    subprocess.run(
        "git clone --branch features/adjustments_hh " +
        egon.data.config.datasets(
            )['demandregio']['disaggregator_code']['url'],
        shell=True,
        cwd=os.path.dirname(__file__),
        )
    subprocess.run(
        "pip install -e disaggregator",
        shell=True,
        cwd=os.path.dirname(__file__))

from disaggregator import data, spatial
# will be later imported from another file ###
Base = declarative_base()


class EgonDemandRegioHH(Base):
    __tablename__ = 'egon_demandregio_hh'
    __table_args__ = {'schema': 'demand'}
    nuts3 = Column(String(5), primary_key=True)
    hh_size = Column(Integer, primary_key=True)
    scenario = Column(String(50), primary_key=True)
    year = Column(Integer)
    demand = Column(Float)


class EgonDemandRegioCtsInd(Base):
    __tablename__ = 'egon_demandregio_cts_ind'
    __table_args__ = {'schema': 'demand'}
    nuts3 = Column(String(5), primary_key=True)
    wz = Column(Integer, primary_key=True)
    scenario = Column(String(50), primary_key=True)
    year = Column(Integer)
    demand = Column(Float)


class EgonDemandRegioPopulation(Base):
    __tablename__ = 'egon_demandregio_population'
    __table_args__ = {'schema': 'society'}
    nuts3 = Column(String(5), primary_key=True)
    year = Column(Integer, primary_key=True)
    population = Column(Float)


class EgonDemandRegioHouseholds(Base):
    __tablename__ = 'egon_demandregio_household'
    __table_args__ = {'schema': 'society'}
    nuts3 = Column(String(5), primary_key=True)
    hh_size = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)
    households = Column(Integer)


class EgonDemandRegioWz(Base):
    __tablename__ = 'egon_demandregio_wz'
    __table_args__ = {'schema': 'demand'}
    wz = Column(Integer, primary_key=True)
    sector = Column(String(50))
    definition = Column(String(150))


def create_tables():
    """Create tables for demandregio data
    Returns
    -------
    None.
    """
    cfg = egon.data.config.datasets()['demandregio']
    db.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {cfg['demand_data']['schema']};")
    db.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {cfg['society_data']['schema']};")
    engine = db.engine()
    EgonDemandRegioHH.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioCtsInd.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioPopulation.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioHouseholds.__table__.create(bind=engine, checkfirst=True)
    EgonDemandRegioWz.__table__.create(bind=engine, checkfirst=True)


def insert_cts_ind_wz_definitions():
    """ Insert demandregio's definitions of CTS and industrial branches

    Returns
    -------
    None.

    """

    cfg = egon.data.config.datasets()['demandregio']['demand_data']

    engine = db.engine()

    for sector in cfg['wz_definitions']:
        df = pd.read_csv(
            os.path.join(
                os.path.dirname(__file__),
                cfg['wz_definitions'][sector])).rename(
                    {'WZ': 'wz', 'Name': 'definition'},
                    axis='columns').set_index('wz')
        df['sector'] = sector
        df.to_sql(cfg['table_names']['wz_definitions'],
                  engine,
                  schema=cfg['schema'],
                  if_exists='append')


def match_nuts3_bl():
    """ Function that maps the federal state to each nuts3 region

    Returns
    -------
    df : pandas.DataFrame
        List of nuts3 regions and the federal state of Germany.

    """
    engine = db.engine()

    df = pd.read_sql(
        "SELECT DISTINCT ON (boundaries.vg250_krs.nuts) "
        "boundaries.vg250_krs.nuts, boundaries.vg250_lan.gen "
        "FROM boundaries.vg250_lan, boundaries.vg250_krs "
        " WHERE ST_CONTAINS("
        "boundaries.vg250_lan.geometry, boundaries.vg250_krs.geometry)",
        con=engine)

    df.gen[df.gen == 'Baden-Württemberg (Bodensee)'] = 'Baden-Württemberg'
    df.gen[df.gen == 'Bayern (Bodensee)'] = 'Bayern'

    return df.set_index('nuts')


def adjust_cts_ind_nep(ec_cts_ind, sector, cfg):
    """ Add electrical demand of new largescale CTS und industrial consumers
    according to NEP 2021, scneario C 2035. Values per federal state are
    linear distributed over all CTS branches and nuts3 regions.

    Parameters
    ----------
    ec_cts_ind : pandas.DataFrame
        CTS or industry demand without new largescale consumers.

    Returns
    -------
    ec_cts_ind : pandas.DataFrame
        CTS or industry demand including new largescale consumers.

    """
    # get data from NEP per federal state
    new_con = pd.read_csv(os.path.join(
                os.path.dirname(__file__),
                cfg['new_consumers_2035']),
        delimiter=';', decimal=',', index_col=0)

    # match nuts3 regions to federal states
    ec_cts_ind['bl'] = match_nuts3_bl().gen
    groups = ec_cts_ind.groupby('bl')

    # update demands per federal state
    for group in groups.indices.keys():
        ec_cts_ind[ec_cts_ind.bl == group] = ec_cts_ind[
            ec_cts_ind.bl == group].drop(
                columns='bl').mul(
                    1 + new_con[sector][group] * 1e3 /
                    ec_cts_ind[ec_cts_ind.bl == group].sum(
                        numeric_only=True).sum())

    ec_cts_ind = ec_cts_ind.drop(columns='bl')

    return ec_cts_ind


def insert_hh_demand(scenario, year, engine, cfg):
    """ Calculates electrical demands of private households using demandregio's
    disaggregator and insert results into the database.

    Parameters
    ----------
    scenario : str
        Name of the corresponing scenario.
    year : int
        The number of households per region is taken from this year.

    Returns
    -------
    None.

    """

    # get demands of private households per nuts and size from demandregio
    ec_hh = spatial.disagg_households_power(
        by='households',
        weight_by_income=False,
        year=year,
        source='local',
        filename='elc_consumption_by_HH_size_'+str(year)+'.csv')

    # insert into database
    for hh_size in ec_hh.columns:
        df = pd.DataFrame(ec_hh[hh_size])
        df['year'] = year
        df['scenario'] = scenario
        df['hh_size'] = hh_size
        df = df.rename({hh_size: 'demand'}, axis='columns')
        df.to_sql(cfg['table_names']['household'],
                  engine,
                  schema=cfg['schema'],
                  if_exists='append')


def insert_cts_ind_demand(scenario, year, engine, target_values, cfg):
    """ Calculates electrical demands of CTS and industry using demandregio's
    disaggregator, adjusts them according to resulting values of NEP 2021 or
    JRC IDEES and insert results into the database.

    Parameters
    ----------
    scenario : str
        Name of the corresponing scenario.
    year : int
        The number of households per region is taken from this year.
    target_values : dict
        List of target values for each scenario and sector.

    Returns
    -------
    None.

    """

    for sector in ['CTS', 'industry']:
        # get demands per nuts3 and wz of demandregio
        ec_cts_ind = spatial.disagg_CTS_industry(
                    use_nuts3code=True,
                    source='power',
                    sector=sector,
                    year=year).transpose()

        # exclude mobility sector from GHD
        ec_cts_ind = ec_cts_ind.drop(columns='49', errors='ignore')

        # scale values according to target_values
        ec_cts_ind *= target_values[scenario][sector]*1e3 / \
            ec_cts_ind.sum().sum()

        # include new largescale consumers according to NEP
        if scenario == 'eGon2035':
            ec_cts_ind = adjust_cts_ind_nep(ec_cts_ind, sector, cfg)

        # insert into database
        for wz in ec_cts_ind.columns:
            df = pd.DataFrame(ec_cts_ind[wz])
            df['year'] = year
            df['wz'] = wz
            df['scenario'] = scenario
            df = df.rename({wz: 'demand'}, axis='columns')
            df.index = df.index.rename('nuts3')
            df.to_sql(
                cfg['table_names'][sector],
                engine,
                schema=cfg['schema'],
                if_exists='append')


def insert_demands():
    """ Insert electricity demands per nuts3-region in Germany according to
    demandregio using its disaggregator-tool

    Returns
    -------
    None.

    """
    cfg = egon.data.config.datasets()['demandregio']['demand_data']
    engine = db.engine()

    for table in cfg['table_names']:
        db.execute_sql(
            f"DELETE FROM {cfg['schema']}.{cfg['table_names'][table]};")

    for scenario in cfg['scenarios'].keys():

        year = cfg['scenarios'][scenario]

        # Insert demands of private households
        insert_hh_demand(scenario, year, engine, cfg)

        # Insert demands of CTS and industry
        # data only available for years before 2036
        if cfg['scenarios'][scenario] > 2035:
            year = 2035

        target_values = {
            'eGon2035': {  # according to NEP 2021 without new consumers
                'CTS': 135.3,
                'industry': 225.4},
            'eGon100RE': {  # source: JRC IDEES, data from 2011 without heat
                'CTS': 125.92,
                'industry': 224.08}}

        insert_cts_ind_demand(scenario, year, engine, target_values, cfg)


def insert_society_data():
    """ Insert population and number of households per nuts3-region in Germany
    according to demandregio using its disaggregator-tool

    Returns
    -------
    None.

    """
    cfg = egon.data.config.datasets()['demandregio']['society_data']
    engine = db.engine()

    for table in cfg['table_names']:
        db.execute_sql(
            f"DELETE FROM {cfg['schema']}.{cfg['table_names'][table]};")

    for year in cfg['target_years']:
        df_pop = pd.DataFrame(data.population(year=year))
        df_pop['year'] = year
        df_pop = df_pop.rename({'value': 'population'}, axis='columns')
        df_pop.to_sql(cfg['table_names']['population'],
                      engine,
                      schema=cfg['schema'],
                      if_exists='append')
        df_hh = pd.DataFrame(data.households_per_size(year=year))

        for hh_size in df_hh.columns:
            df = pd.DataFrame(df_hh[hh_size])
            df['year'] = year
            df['hh_size'] = hh_size
            df = df.rename({hh_size: 'households'}, axis='columns')
            df.to_sql(cfg['table_names']['household'],
                      engine,
                      schema=cfg['schema'],
                      if_exists='append')


def insert_data():
    """ Overall function for importing data from demandregio

    Returns
    -------
    None.

    """

    create_tables()
    insert_demands()
    insert_cts_ind_wz_definitions()
    insert_society_data()