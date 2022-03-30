import os
import re
import csv
import gevent
import urllib
import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup

import workbench.utils.read_write as rw


#######################
PGA_BASE_URL = 'https://www.pgatour.com'
PGA_DATA_STUB = '%s/jcr:content/mainParsys/pastresults.selectedYear.%s.html'
DEFAULT_DATA_DIR = os.path.join(os.getenv('DATA'), 'pydata', 'projects', 'pga',
                                'events')


def gather_pages(url, filename):
    urllib.request.urlretrieve(url, filename)
######################


class EventDownloader(object):
    """
    Manage the download and parsing of event information from pga website.
        data_dir should point to /pga/events directory. Research flag auto
        loads local data for use in serving vs donwloading data.
    """
    def __init__(self, data_dir=DEFAULT_DATA_DIR, research=False):
        self.data_dir = data_dir
        self.csv_base = os.path.join(data_dir, 'csv')
        self.html_base = os.path.join(data_dir, 'html')
        if research:
            self.prep_for_research()

    def prep_for_research(self):
        self.load_local_meta()
        self.tourn_meta_df = self.get_tourn_meta_df()
        self.event_meta_df = self.get_event_meta_df()

    def download_tourn_info(self):
        """
        Download and process tourn data from pga website.  Create formatted
            tourn_meta json file and save it to disk.
        """
        events_all = self.download_available_events()
        tourn_meta = self.process_events(events_all)
        self.tourn_meta = tourn_meta
        write_path = os.path.join(self.data_dir, 'tourn_meta.json')
        rw.verify_overwrite(write_path)
        rw.write_dict_to_json(tourn_meta, write_path)

    def download_available_events(self):
        """
        Get a list of all links and event names for all available years from
            PGA website. Return events that have hyperlinks.
        """
        schedule_url = PGA_BASE_URL + '/tournaments/schedule.html'
        page = requests.get(schedule_url)
        html = BeautifulSoup(page.text, 'lxml')

        yr_select = html.find("div", class_="schedule-tournament-select " +
                              "history-select js-season-select")
        yr_select = yr_select.find_all("option")
        yr_links = [PGA_BASE_URL + o['data-link'] for o in yr_select]

        events_all = []
        for lk in tqdm(yr_links):
            page = requests.get(lk)
            html = BeautifulSoup(page.text, 'lxml')
            # Two table classes exist
            table_a = html.find("table", class_="table-styled js-table" +
                                " schedule-history-table")
            table_b = html.find("table", class_="table-styled js-table")
            table = table_a if table_a else table_b
            # Get all events with hyperlinks from the table
            for er in table.find_all("tr"):
                link_elem = er.find("a", class_="bottom-string " +
                                    "js-tournament-name")
                if (link_elem is None) or (not link_elem.has_attr('href')):
                    continue
                tourn_name = link_elem.text
                event_link = link_elem['href']
                events_all.append((tourn_name, event_link))
        return events_all

    def process_events(self, event_links):
        """
        Process a list of tuples of raw (event name, event link) and return
            filtered dictionary of tournament information
        """
        tourn_meta = dict()
        processed = []
        for i, (tn, el) in enumerate(event_links):
            # Match event name from url(#1&#2) and sample event year(#3)
            match1 = re.search(r'https://www\.(.*)\.com/past-results', el)
            match2 = re.search(r'/tournaments/(.*?)/.*past-results', el)
            match3 = re.search(r'past-results\.(\d\d\d\d)\.html', el)
            match = match2 if match2 else match1
            if (match is None) or (match3 is None):
                continue
            t_label = match.groups()[0]
            t_year = int(match3.groups()[0])
            t_link_head = match.group()
            # Check if this event has been processed
            if t_label in processed:
                continue
            processed.append(t_label)
            # Add url stub if needed
            if match2:
                t_link_head = PGA_BASE_URL + t_link_head
            tourn_meta[i] = dict(tourn_name=tn,
                                 tourn_label=t_label,
                                 link_head=t_link_head,
                                 sample_year=t_year)
        return tourn_meta

    def download_html(self, tourn_ids=None, min_yr=1980):
        """
        Download event data for specific tournament ids or all available
            if tourn_ids is None.  Filter to min_yr, some tournaments
            have erroneous years in  deep history.
        """
        self.check_tourn_meta()
        if tourn_ids is None:
            tourn_ids = list(self.tourn_meta.keys())
        else:
            tourn_ids = self.verify_ids(tourn_ids=tourn_ids)

        no_dropdown = []
        for t_id in tqdm(tourn_ids):
            t_label = self.tourn_meta[t_id]['tourn_label']
            t_link_head = self.tourn_meta[t_id]['link_head']
            t_smpl_yr = self.tourn_meta[t_id]['sample_year']
            e_data_url = PGA_DATA_STUB % (t_link_head, t_smpl_yr)
            # Confirm year select exists on page
            page = requests.get(e_data_url)
            html = BeautifulSoup(page.text, 'lxml')
            year_select = html.find('select', id='pastResultsYearSelector')
            if year_select is None:
                no_dropdown.append(e_data_url)
                continue
            # Filter to min_yr
            years_avail = [x['value'] for x in year_select.find_all('option')]
            years_avail = [x for x in years_avail if int(x) >= min_yr]
            # Create new directory if it does not exist
            dir_path = os.path.join(self.html_base, t_label)
            if not os.path.exists(dir_path) and len(years_avail) > 0:
                os.makedirs(dir_path)
            url_paths = []
            for e_yr in years_avail:
                e_data_url = PGA_DATA_STUB % (t_link_head, e_yr)
                # Check if already downloaded
                file_path = "%s/%s.html" % (dir_path, e_yr)
                if not os.path.isfile(file_path):
                    url_paths.append((e_data_url, file_path))
            # Pull html pages in parallel
            jobs = [gevent.spawn(gather_pages, tup[0], tup[1]) for tup in
                    url_paths]
            gevent.joinall(jobs)

        print("Tournaments missing year select: {}".format(no_dropdown))

    def process_html(self, tourn_ids=None):
        '''
        Process all available html files for a tournament.  If tourn_ids is
            None process all files.
        '''
        self.check_tourn_meta()
        if tourn_ids is None:
            tourn_ids = list(self.tourn_meta.keys())
        else:
            tourn_ids = self.verify_ids(tourn_ids=tourn_ids)

        no_data = []
        for t_id in tqdm(tourn_ids):
            t_label = self.tourn_meta[t_id]['tourn_label']
            t_html_dir = os.path.join(self.html_base, t_label)
            t_csv_dir = os.path.join(self.csv_base, t_label)
            if not os.path.isdir(t_html_dir):
                continue

            html_fls = os.listdir(t_html_dir)
            for e_hfl in html_fls:
                html_path = os.path.join(self.html_base, t_label, e_hfl)
                csv_path = os.path.join(t_csv_dir, e_hfl.replace('html',
                                                                 'csv'))
                # Check if file already processed
                if os.path.isfile(csv_path):
                    continue
                # Process html file
                with open(html_path, 'r', encoding="utf-8") as h_fl:
                    soup = BeautifulSoup(h_fl, 'lxml')
                e_table_data = self._parse_html_table(soup)

                if e_table_data is None:
                    no_data.append(html_path)
                    continue
                elif len(e_table_data) <= 1:
                    no_data.append(html_path)
                    continue
                # Create csv directory if needed
                if not os.path.exists(t_csv_dir):
                    os.makedirs(t_csv_dir)
                with open(csv_path, 'w', encoding='utf-8', newline='') as c_fl:
                    writer = csv.writer(c_fl, delimiter=',')
                    for row in e_table_data:
                        writer.writerow(row)

        print("Unable to parse tables: {}".format(no_data))

    def build_update_meta_files(self):
        '''
        Update tourn_meta files and generate a NEW event_meta file based
            upone all available data in csv directory.  Old event_meta
            file will be replaced.
        '''
        self.check_tourn_meta()
        tourn_ids = list(self.tourn_meta.keys())
        self.event_meta = {}
        for t_id in tqdm(tourn_ids):
            t_label = self.tourn_meta[t_id]['tourn_label']
            t_html_dir = os.path.join(self.html_base, t_label)
            t_csv_dir = os.path.join(self.csv_base, t_label)
            if not os.path.exists(t_csv_dir):
                self.tourn_meta[t_id]['n_files'] = 0
                self.tourn_meta[t_id]['min_year'] = None
                self.tourn_meta[t_id]['max_year'] = None
                continue
            csv_files = os.listdir(t_csv_dir)
            min_yr = int(min(csv_files).replace('.csv', ''))
            max_yr = int(max(csv_files).replace('.csv', ''))
            self.tourn_meta[t_id]['n_files'] = len(csv_files)
            self.tourn_meta[t_id]['min_year'] = min_yr
            self.tourn_meta[t_id]['max_year'] = max_yr

            for c_fl in csv_files:
                e_yr = int(c_fl.replace('.csv', ''))
                html_path = os.path.join(t_html_dir, '{}.html'.format(e_yr))
                with open(html_path, 'r', encoding="utf-8") as h_fl:
                    soup = BeautifulSoup(h_fl, 'lxml')
                (date, par, course) = self._parse_html_meta(soup)
                # Add new event meta entrty
                e_id = len(self.event_meta)
                self.event_meta[e_id] = dict(tourn_id=t_id,
                                             tourn_label=t_label,
                                             yea=e_yr,
                                             date=date,
                                             par=par,
                                             course=course)
        write_path = os.path.join(self.data_dir, 'tourn_meta.json')
        rw.write_dict_to_json(self.tourn_meta, write_path)
        write_path = os.path.join(self.data_dir, 'event_meta.json')
        rw.write_dict_to_json(self.event_meta, write_path)

    def _parse_html_table(self, inp_soup):
        '''
        Return headers and data rows from the html soup. If they cannot be
            found or another issue arises return empty list
        '''
        table = inp_soup.find('table', class_='table-styled')
        if table is None:
            return
        # Extract table sections
        table_head = table.find('thead')
        table_body = table.find('tbody')
        if table_head is None or table_body is None:
            return
        # Extract data
        th_data = [t.text.strip() for t in table_head.find_all('th')]
        data_rows = table_body.find_all('tr')
        if len(th_data) == 0 or len(data_rows) == 0:
            return
        # Process header and data rows
        rnd_th_ix = [x for x in th_data if x.find('ROUNDS') > -1]
        if len(rnd_th_ix) == 0:
            return
        rnd_th_ix = th_data.index(rnd_th_ix[0])
        rnd_indv_th = re.findall(r'\d', th_data[rnd_th_ix])
        th_out = th_data[:rnd_th_ix] + rnd_indv_th + th_data[rnd_th_ix + 1:]
        csv_lines = [th_out]
        for rw in data_rows:
            info = [cl.text.strip() for cl in rw.find_all('td')]
            csv_lines.append(info)
        return csv_lines

    def _parse_html_meta(self, inp_soup):
        # Get tournament meta info
        date = par = course = None
        info_rows = inp_soup.find_all("span", class_="header-row")
        for hr in info_rows:
            match = re.search(r'Ending: ([\d/]+)', hr.text)
            if match:
                date = match.groups()[0]

            match = re.search(r'PAR: ([\d]+)', hr.text)
            if match:
                par = int(match.groups()[0])

            match = re.search(r'Course: (.*)', hr.text)
            if match:
                course = match.groups()[0]
        return (date, par, course)

    def load_csv(self, tourn_id, year=None, min_year=None):
        '''
        Load a csv data for a single tournament. If year is passed that single
            year is loaded. If no year arg then all available years are
            loaded and filtered based on min_year.
        '''
        self.check_tourn_meta()
        self.check_event_meta()
        self.verify_ids(tourn_ids=tourn_id)
        tourn_label = self.tourn_meta[tourn_id]['tourn_label']
        tourn_dir_path = os.path.join(self.csv_base, tourn_label)
        assert os.path.exists(tourn_dir_path)
        # If year empty load all years
        if year is None:
            load_files = os.listdir(tourn_dir_path)
        else:
            event_path = os.path.join(tourn_dir_path, '{}.csv'.format(year))
            if not os.path.exists(event_path):
                raise FileNotFoundError('No file at {}'.format(event_path))
            load_files = ['{}.csv'.format(year)]
        # Load csv(s) and add some meta data
        out_data = pd.DataFrame([])
        repl_results = ['nan', 'CUT', 'W/D', 'DQ']
        for fl in load_files:
            yr = int(fl.replace('.csv', ''))
            if min_year:
                if yr < int(min_year):
                    continue
            e_row = self.event_meta_df.query("tourn_id == @tourn_id and " +
                                             "year == @yr")
            if len(e_row) != 1:
                raise ValueError("Could not identify event")
            e_dat = e_row.iloc[0]
            event_path = os.path.join(tourn_dir_path, fl)
            ev_csv = pd.read_csv(event_path)
            # Add meta fields to event df and manage result column
            finishes = set(ev_csv.POS) - set(repl_results)
            last_place = ev_csv.POS.isin(finishes).sum() + 1
            res_fltr = (lambda x: last_place if x in repl_results else
                        int(x.replace('T','')))
            ev_csv['POS'] = ev_csv.POS.astype(str).map(res_fltr)
            ev_csv['year'] = yr
            ev_csv['event_id'] = e_dat.event_id
            ev_csv['date'] = e_dat.date
            ev_csv['course'] = e_dat.course
            ev_csv['par'] = e_dat.par
            ev_csv['POS_pct'] = ev_csv.POS / float(last_place)
            ev_csv.drop_duplicates(['PLAYER'], inplace=True)
            out_data = out_data.append(ev_csv, ignore_index=True, sort=False)

        out_data['tourn_label'] = tourn_label
        out_data['tourn_id'] = tourn_id
        return out_data

    #########################################################

    def check_tourn_meta(self):
        """
        Verify tourn_meta exists
        """
        if not hasattr(self, 'tourn_meta'):
            raise NameError("No tourn_meta attribute found")

    def check_event_meta(self):
        """
        Verify event_meta exists
        """
        if not hasattr(self, 'event_meta'):
            raise NameError("No event_meta attribute found")

    def verify_ids(self, tourn_ids=None, event_ids=None):
        """
        Verify all ids are contained in the associated meta object
        """
        if tourn_ids:
            self.check_tourn_meta()
            inp_ids = tourn_ids
            meta_obj = self.tourn_meta
        elif event_ids:
            self.check_event_meta()
            inp_ids = event_ids
            meta_obj = self.event_meta
        # Check all ids are contained in the meta object
        if isinstance(inp_ids, (float, int, str)):
            inp_ids = [str(inp_ids)]
        assert(isinstance(inp_ids, list))
        if not set(inp_ids).issubset(set(meta_obj.keys())):
            raise ValueError("ids in input that do not exist in meta")
        return inp_ids

    def load_local_meta(self, event_data_dir=None):
        """
        Load  meta data from a local source
        """
        if event_data_dir is None:
            event_data_dir = self.data_dir
        tourn_meta_path = os.path.join(event_data_dir, 'tourn_meta.json')
        event_meta_path = os.path.join(event_data_dir, 'event_meta.json')

        if os.path.exists(tourn_meta_path):
            tourn_meta = rw.read_dict_from_json(tourn_meta_path)
            self.tourn_meta = tourn_meta
        if os.path.exists(event_meta_path):
            event_meta = rw.read_dict_from_json(event_meta_path)
            self.event_meta = event_meta

    def get_tourn_meta_df(self, tourn_meta=None):
        """
        Return pandas DataFrame represenation of tournament meta data
        """
        # Check to see if a event_ids arg passed and validate non-empty
        if tourn_meta is None:
            self.check_tourn_meta()
            tourn_meta = self.tourn_meta
        # Create DataFrame
        out_df = pd.DataFrame(tourn_meta).transpose()
        out_df.index.name = 'tourn_id'
        out_df.reset_index(inplace=True)
        return out_df

    def get_event_meta_df(self, event_meta=None):
        """
        Return pandas DataFrame represenation of event meta data
        """
        # Check to see if a event_ids arg passed and validate non-empty
        if event_meta is None:
            self.check_event_meta()
            event_meta = self.event_meta
        # Create DataFrame
        out_df = pd.DataFrame(event_meta).transpose()
        out_df.index.name = 'event_id'
        out_df.reset_index(inplace=True)
        return out_df


if __name__ == '__main__':
    ed = EventDownloader(research=True)
    ed.load_local_meta()
    # ed.download_tourn_info()
    # ed.download_html()
    # ed.process_html()
    # ed.build_update_meta_files()
    data = ed.load_csv('84', 2002)
