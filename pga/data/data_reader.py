import os
import numpy as np
import pandas as pd
from tqdm import tqdm

from workbench.projects.pga.data.stat_downloader import StatDownloader
from workbench.projects.pga.data.event_downloader import EventDownloader


BASE_DATA_PATH = os.path.join(os.getenv('DATA'), 'pydata', 'projects', 'pga')


class DataReader(object):
    """docstring for data_reader"""
    def __init__(self, data_path=BASE_DATA_PATH):
        self.stat_manager = StatDownloader(os.path.join(data_path, 'stats'),
                                           research=True)
        self.result_manager = EventDownloader(os.path.join(data_path,
                                                           'events'),
                                              research=True)

    def get_tourn_info(self):
        return self.result_manager.tourn_meta_df

    def get_event_info(self):
        return self.result_manager.event_meta_df

    def get_stat_info(self):
        return self.stat_manager.stat_meta_df

    def build_result_df(self, tourn_ids, min_year=None):
        '''
        Load a list of tournament data and filter to a minimum year.  Rename
            columns and filter.  Append tournaments along row axis.
        '''
        if isinstance(tourn_ids, (float, int, str)):
            tourn_ids = [str(tourn_ids)]

        col_map = {'PLAYER': 'player_name', 'POS': 'result',
                   'POS_pct': 'result_pct', 'TOTALSCORE': 'score',
                   'year': 'year', 'date': 'end_date', 'course': 'course_name',
                   'par': 'course_par'}
        out = pd.DataFrame([])
        for t_id in tqdm(tourn_ids):
            tdata = self.result_manager.load_csv(t_id, min_year=min_year)
            if not set(col_map.keys()).issubset(set(tdata.columns)):
                raise KeyError('Expected cols not available in tourn df')
            tdata.rename(columns=col_map, inplace=True)
            out = out.append(tdata, ignore_index=True, sort=False)

        out = out[['player_name', 'event_id', 'tourn_id', 'result',
                   'result_pct', 'year', 'end_date']]
        self.result_data = out
        return out

    def build_stat_df(self, stat_ids, min_year=None, drop_prev_cols=True):
        '''
        Load a list of stat data and filter to a minimum year. Join stats
            along column axis outer joining on player name and year.
        '''
        if isinstance(stat_ids, (float, int, str)):
            stat_ids = [str(stat_ids)]

        col_map = {'PLAYER NAME': 'player_name'}
        out = pd.DataFrame([], columns=['player_name', 'year'])
        for s_id in tqdm(stat_ids):
            col_map['RANK THIS WEEK'] = 'rank_{}'.format(s_id)
            col_map['RANK LAST WEEK'] = 'prev_rank_{}'.format(s_id)
            sdata = self.stat_manager.load_csv(s_id, min_year=min_year)
            if not set(col_map.keys()).issubset(set(sdata.columns)):
                raise KeyError('Expected cols not available in tourn df')
            sdata = sdata[['PLAYER NAME', 'year', 'RANK THIS WEEK',
                           'RANK LAST WEEK']]
            sdata.rename(columns=col_map, inplace=True)
            out = out.merge(sdata, on=['player_name', 'year'], how='outer')
        if drop_prev_cols:
            drop_cols = [x for x in out.columns if x.find('prev_rank') == 0]
            out.drop(columns=drop_cols, inplace=True)
        self.stat_data = out
        return out

    def build_base_data(self, stat_data=None, result_data=None,
                        backfill_stats=False):
        '''
        Combine stat and result data to produce a base data df that will
            be used as basic of feature creation and modeling.
        '''
        if stat_data is None:
            if not hasattr(self, 'stat_data'):
                raise ValueError('No stat data available')
            stat_data = self.stat_data
        if result_data is None:
            if not hasattr(self, 'result_data'):
                raise ValueError('No stat data available')
            result_data = self.result_data
        # Merge data sets
        stat_data.year += 1
        stat_data = stat_data[stat_data.year < max(stat_data.year)]
        base_data = pd.merge(stat_data, result_data,
                             on=['player_name', 'year'], how='outer')
        if backfill_stats:
            base_data = self.backfill_stats(base_data)
        base_data = base_data.dropna()
        base_data = base_data.sort_values(['player_name',
                                          'year']).reset_index(drop=True)
        self.base_data = base_data
        return base_data

    def backfill_stats(self, inp_data, stat_col=None):
        '''
        Fill in values of stat_col with previous years data if null
        '''
        assert set(['player_name', 'year']).issubset(set(inp_data.columns))

        if stat_col is None:
            fill_columns = [x for x in inp_data.columns if
                            x.find('rank_') == 0]
        elif not isinstance(stat_col, list):
            fill_columns = [stat_col]

        filled_data = inp_data.copy()
        filled_data.drop(columns=fill_columns, inplace=True)
        stats_data = inp_data[['player_name', 'year'] + fill_columns].copy()
        for fc in fill_columns:
            sdata = stats_data[['player_name', 'year', fc]].copy()
            sdata.drop_duplicates(inplace=True)
            piv_data = sdata.pivot(index='year', columns='player_name',
                                   values=fc)
            piv_data.fillna(method='pad', inplace=True)
            piv_data = piv_data.unstack()
            piv_data.name = fc
            piv_data = piv_data.reset_index()
            filled_data = filled_data.merge(piv_data)
        return filled_data


if __name__ == '__main__':

    sample_stat_ids = ['127', '101', '102', '129', '158', '103', '111', '119',
                       '115', '104', '190', '130', '413', '398', '426']

    sample_tourn_ids = ['63', '64', '65', '66', '67', '70', '79', '73',
                        '74', '81', '84', '85', '89', '94']

    dr = DataReader()
    sdata = dr.build_stat_df(stat_ids=sample_stat_ids, min_year=1999)
    rdata = dr.build_result_df(tourn_ids=sample_tourn_ids, min_year=2000)
    base = dr.build_base_data(sdata, rdata, backfill_stats=True)
