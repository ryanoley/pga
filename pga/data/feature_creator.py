import os
import numpy as np
import pandas as pd

from gearbox import convert_date_array


class FeatureCreator(object):
    """docstring for FeatureCreator"""
    def __init__(self, inp_data, result_col='result'):
        req_cols = ['player_name', 'year', 'event_id', 'tourn_id', 'end_date',
                    result_col]
        assert set(req_cols).issubset(inp_data.columns)
        inp_data.end_date = convert_date_array(inp_data.end_date)
        inp_data.sort_values(['player_name', 'end_date'], inplace=True)
        inp_data.reset_index(inplace=True, drop=True)
        self.stat_cols = [x for x in inp_data if x.find('rank_') == 0]
        self.result_col = result_col
        self._base = inp_data.copy()
        self.data = inp_data.copy()

    def stat_feature(self, stat_id, func, window):
        '''
        Return some numpy math func over a window for a specific stat id.
        i.e Max/Avg/Min over last 5 Years
        '''
        pass

    def tourn_performance(self, func, window):
        '''
        Return prior performance in same tournament using some math func and
        a window. i.e Min/Avg Tournament performance in last 5
        '''
        assert func in ['min', 'max', 'mean', 'median']
        grp_data = self._base[['player_name', 'tourn_id', self.result_col]]
        grp = grp_data.groupby(['player_name', 'tourn_id'])

        pivot = pivot.shift(1)
        if pad_events:
            pivot.fillna(method='pad', inplace=True)

        eval_str = 'pivot.rolling({}, min_periods=1).{}()'.format(window, func)
        feat = eval(eval_str)
        feat = feat.unstack()
        feat.name = 'ev_perf_{}_{}'.format(func, window)
        feat = feat.reset_index()
        self.data = pd.merge(self.data, feat, on=['player_name', 'end_date'],
                             how='left')

    def event_performance(self, func, window, pad_events=True):
        '''
        Return math func across all events recently.  i.e Avg, Max, Min
        over last 5 events (across all tournamnets)
        '''
        assert func in ['min', 'max', 'mean', 'median']
        pivot = self._base.pivot(index='end_date', columns='player_name',
                                 values=self.result_col)
        pivot = pivot.shift(1)
        if pad_events:
            pivot.fillna(method='pad', inplace=True)

        eval_str = 'pivot.rolling({}, min_periods=1).{}()'.format(window, func)
        feat = eval(eval_str)
        feat = feat.unstack()
        feat.name = 'ev_perf_{}_{}'.format(func, window)
        feat = feat.reset_index()
        self.data = pd.merge(self.data, feat, on=['player_name', 'end_date'],
                             how='left')

    def tourn_binaries(self, tourn_id):
        '''
        Binary flags for each tourn_id
        '''
        pass

    def cluster_tournaments(self):
        '''
        Placeholder for some work on thinking about how to group tournaments
        together
        '''
        pass


    def sample_build(self):
        # self.event_performance('mean', 1)
        # self.event_performance('median', 10)
        # self.event_performance('max', 5)
        # self.event_performance('min', 5)
        self.tourn_performance('max', 5)







if __name__ == '__main__':


    dpath = os.path.join(os.getenv('DATA'), 'pydata', 'projects', 'pga',
                         'processed_data', 'base_data1.csv')
    data = pd.read_csv(dpath)

    import ipdb; ipdb.set_trace()

    fc = FeatureCreator(data)

    fc.sample_build()




    def _get_meta(self, stat_data):
        grp_data = stat_data.groupby('stat_id')
        meta = grp_data.year.value_counts()
        meta = meta.to_frame(name='count')
        meta.reset_index(inplace=True)
        return meta

    def get_available_stat_ids(self):
        return list(self.meta.stat_id.unique())




dr.build_stat_data([2568, 103])
dat = dr.stat_data.copy()

fc = FeatureCreator(dat)