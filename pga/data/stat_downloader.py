import os
import csv
import json
import gevent
import urllib
import requests
import pandas as pd
import datetime as dt
from tqdm import tqdm
from bs4 import BeautifulSoup

import workbench.utils.read_write as rw


#######################
FOLDER_REPLACE_CHARS = {' ': '', ':': '', '/': '', '\\': '', '?': '', '*': '',
                        '<=': '_LT_', '>=': '_GT_', '<': '_LT_', '>': '_GT_'}
DEFAULT_DATA_DIR = os.path.join(os.getenv('DATA'), 'pydata', 'projects', 'pga',
                                'stats')


def replace_multiple(inp_string, replacements):
    for key in replacements.keys():
        inp_string = inp_string.replace(key, replacements[key])
    return inp_string


def gather_pages(url, filename):
    urllib.request.urlretrieve(url, filename)
######################


class StatDownloader(object):
    """
    Manage the download and parsing of statistic information from pga website.
        data_dir should point to /pga/stats directory. Research flag auto
        loads local data for use in serving vs donwloading data.
    """
    def __init__(self, data_dir=DEFAULT_DATA_DIR, research=False):
        self.data_dir = data_dir
        self.csv_base = os.path.join(data_dir, 'csv')
        self.html_base = os.path.join(data_dir, 'html')
        # Configure for research if needed
        if research:
            self.prep_for_research()

    def prep_for_research(self):
        self.load_local_meta()
        self.stat_meta_df = self.get_stat_meta_df()

    def download_stat_info(self):
        """
        Get available statistics from pga.com and write new json file with
            category information, stat ids and stat names within each
            category
        """
        url_stub = 'http://www.pgatour.com/stats/categories.%s.html'
        category_labels = ['RPTS_INQ', 'ROTT_INQ', 'RAPP_INQ', 'RARG_INQ',
                           'RPUT_INQ', 'RSCR_INQ', 'RSTR_INQ', 'RMNY_INQ']
        info = {}
        for c_lbl in category_labels:
            category_url = url_stub % c_lbl
            page = requests.get(category_url)
            soup = BeautifulSoup(page.text, 'lxml')
            cat_name = soup.find("title").text
            cat_name = cat_name.replace('Categories', '')[3:]
            for table in soup.find_all("div", class_="table-content"):
                for link in table.find_all("a"):
                    s_id = link['href'].split('.')[1]
                    s_name = link.text
                    s_label = replace_multiple(s_name, FOLDER_REPLACE_CHARS)
                    # Check to see if stat already handled and has text name
                    if ((s_id not in info.keys()) and (len(s_name) > 0) and
                            (len(s_label) > 0)):
                        info[s_id] = dict(cat_name=cat_name,
                                          cat_abbr=c_lbl,
                                          stat_name=s_name,
                                          stat_label=s_label)
        # Write file out and set isntance attributes
        stat_meta_path = os.path.join(self.data_dir, 'stat_meta.json')
        rw.verify_overwrite(stat_meta_path)
        rw.write_dict_to_json(info, stat_meta_path)
        self.stat_meta = info

    def download_html(self, stat_ids=None):
        """
        Create directories in the html base directory for each of the stat_ids
            and download individual html files for each year that stat is
            available
        """
        # Validate stat_ids argument ad validate
        self.check_stat_meta()
        if stat_ids is None:
            stat_ids = list(self.stat_meta.keys())
        else:
            self.verify_ids(stat_ids)

        # Iterate through stats and download available files
        url_stub = "http://www.pgatour.com/stats/stat.%s.%s.html"  # stat, yr
        pilot_year = dt.datetime.now().year - 1
        no_stats = []
        for s_id in tqdm(stat_ids):
            s_id_label = self.stat_meta[s_id]['stat_label']
            csv_dir_path = os.path.join(self.html_base, s_id_label)
            url = url_stub % (s_id, pilot_year)
            page = requests.get(url)
            soup = BeautifulSoup(page.text, 'lxml')

            # Get all available years
            yr_select = soup.find("select", class_="statistics-details-select")
            if yr_select is None:
                no_stats.append(url)
                continue
            years = [x['value'] for x in yr_select.find_all("option")]

            # Create new directory if needed and pull individual files
            if not os.path.exists(csv_dir_path):
                os.makedirs(csv_dir_path)

            url_paths = []
            for yr in years:
                url = url_stub % (s_id, yr)
                html_path = "%s/%s.html" % (csv_dir_path, yr)
                # Check if already downloaded
                if not os.path.isfile(html_path):
                    url_paths.append((url, html_path))
            jobs = [gevent.spawn(gather_pages, x[0], x[1]) for x in url_paths]
            gevent.joinall(jobs)
        print("No stats found at URLs: {}".format(no_stats))

    def process_html(self, stat_ids=None):
        """
        Extract statistics from html files and write out csv files using the
            same directory structure in csv base
        """
        # Validate stat_ids argument and validate
        self.check_stat_meta()
        if stat_ids is None:
            stat_ids = list(self.stat_meta.keys())
        else:
            self.verify_ids(stat_ids)

        no_html = []
        no_data = []
        for s_id in tqdm(stat_ids):
            stat_label = self.stat_meta[s_id]['stat_label']
            html_dir = os.path.join(self.html_base, stat_label)
            # Check to make sure html directoy exists
            if not os.path.exists(html_dir):
                no_html.append(html_dir)
                continue
            html_fls = os.listdir(html_dir)

            for s_hfl in html_fls:
                csv_lines = []
                html_path = os.path.join(html_dir, s_hfl)
                csv_dir = os.path.join(self.csv_base, stat_label)
                csv_path = os.path.join(csv_dir, s_hfl.replace('html', 'csv'))
                # Check if file already processed
                if os.path.isfile(csv_path):
                    continue
                # Load html data
                with open(html_path, 'r', encoding="utf-8") as h_fl:
                    soup = BeautifulSoup(h_fl, 'lxml')
                table = soup.find('table', id='statsTable')
                # Check if there is a table
                if table is None:
                    no_data.append(html_path)
                    continue
                # Process html
                hdrs = [th.text for th in table.find('thead').find_all('th')]
                csv_lines.append(hdrs)
                for tr in table.find('tbody').find_all('tr'):
                    info = [td.text.strip() for td in tr.find_all('td')]
                    csv_lines.append(info)
                # Check that the table is non-empty
                if len(csv_lines) <= 1:
                    no_data.append(html_path)
                    continue
                # Write to csv - make directory if needed
                if not os.path.exists(csv_dir):
                    os.makedirs(csv_dir)
                with open(csv_path, 'w', encoding='utf-8', newline='') as c_fl:
                    writer = csv.writer(c_fl, delimiter=',')
                    for row in csv_lines:
                        writer.writerow(row)
        print("No HTML data found: {}".format(no_html), '\n')
        print("Unable to parse tables: {}".format(no_data))

    def update_meta_file(self):
        """
        Iterate through stat_ids and add information on files to the meta
            json - number of files, min_year, max_year.  Overwrite existing
            meta file.
        """
        # Validate stat_ids argument and validate
        self.check_stat_meta()
        stat_ids = list(self.stat_meta.keys())

        for s_id in stat_ids:
            stat_label = self.stat_meta[s_id]['stat_label']
            csv_dir = os.path.join(self.csv_base, stat_label)
            if not os.path.exists(csv_dir):
                self.stat_meta[s_id]['n_files'] = 0
                self.stat_meta[s_id]['min_year'] = None
                self.stat_meta[s_id]['max_year'] = None
                continue
            dir_files = os.listdir(csv_dir)
            min_yr = int(min(dir_files).replace('.csv', ''))
            max_yr = int(max(dir_files).replace('.csv', ''))
            self.stat_meta[s_id]['n_files'] = len(dir_files)
            self.stat_meta[s_id]['min_year'] = min_yr
            self.stat_meta[s_id]['max_year'] = max_yr
        write_path = os.path.join(self.data_dir, 'stat_meta.json')
        rw.write_dict_to_json(self.stat_meta, write_path)

    def load_csv(self, stat_id, year=None, min_year=None):
        '''
        Load a csv data for a single stat_id. If no year is passed, all
            all available years greater than min_year will be loaded.
        '''
        self.check_stat_meta()
        self.verify_ids(stat_id)
        stat_label = self.stat_meta[stat_id]['stat_label']
        stat_dir_path = os.path.join(self.csv_base, stat_label)
        assert os.path.exists(stat_dir_path)
        if year is None:
            load_files = os.listdir(stat_dir_path)
        else:
            stat_fl_path = os.path.join(stat_dir_path, '{}.csv'.format(year))
            if not os.path.exists(stat_fl_path):
                raise FileNotFoundError('No file at {}'.format(stat_fl_path))
            load_files = ['{}.csv'.format(year)]
        repl_results = ['nan']
        # Load csv(s) and add some meta data
        out_data = pd.DataFrame([])
        for fl in load_files:
            yr = int(fl.replace('.csv', ''))
            if min_year:
                if yr < int(min_year):
                    continue
            stat_fl_path = os.path.join(stat_dir_path, fl)
            yr_csv = pd.read_csv(stat_fl_path)
            last_place = len(yr_csv) + 1
            res_fltr = (lambda x: last_place if x in repl_results else
                        int(x.replace('T','')))
            yr_csv['RANK THIS WEEK'] = yr_csv['RANK THIS WEEK'].astype(str).map(res_fltr)
            yr_csv['RANK LAST WEEK'] = yr_csv['RANK LAST WEEK'].astype(str).map(res_fltr)
            yr_csv['year'] = yr
            yr_csv.drop_duplicates(['PLAYER NAME'], inplace=True)
            out_data = out_data.append(yr_csv, ignore_index=True, sort=False)
        out_data['stat_label'] = stat_label
        out_data['stat_id'] = stat_id
        return out_data

    ################################################################

    def check_stat_meta(self):
        """
        Verify stat_meta exists
        """
        if not hasattr(self, 'stat_meta'):
            raise NameError("No stat_meta attribute found")

    def verify_ids(self, inp_ids):
        """
        Verify all ids are contained in the associated meta object
        """
        # Check all ids are contained in the meta object
        self.check_stat_meta()
        if isinstance(inp_ids, (float, int, str)):
            inp_ids = [str(inp_ids)]
        assert(isinstance(inp_ids, list))
        if not set(inp_ids).issubset(set(self.stat_meta.keys())):
            raise ValueError("ids in input that do not exist in meta")

    def load_local_meta(self, stats_data_dir=None):
        """
        Load stat_meta dict and stats_ids from a local source
        """
        if stats_data_dir is None:
            stats_data_dir = self.data_dir
        stat_meta_path = os.path.join(self.data_dir, 'stat_meta.json')

        if os.path.exists(stat_meta_path):
            stat_meta = rw.read_dict_from_json(stat_meta_path)
            self.stat_meta = stat_meta

    def get_stat_meta_df(self, stat_meta=None):
        """
        Return pandas DataFrame represenation of stat meta data
        """
        # Check to see if a stat_ids arg passed and validate non-empty
        if stat_meta is None:
            self.check_stat_meta()
            stat_meta = self.stat_meta
        # Create DataFrame
        out_df = pd.DataFrame(stat_meta).transpose()
        out_df.index.name = 'stat_id'
        out_df.reset_index(inplace=True)
        return out_df


if __name__ == '__main__':
    sd = StatDownloader()
    sd.load_local_meta()
    # sd.download_stat_info()
    # sd.download_html()
    # sd.process_html()
    # sd.update_meta_file()
    # data = sd.load_csv('127', year=2010)
