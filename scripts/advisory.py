import sys
import os
import logging.config
import requests
from bs4 import BeautifulSoup
import ConfigParser
from florida_wq_data import florida_sample_sites
import re
import zipfile
import csv
import io
from datetime import datetime
from selenium import webdriver

"""
Names are slight variations almost every document/page we have.
"""
name_mapping = {
 'Longboat Key': 'LONGBOAT KEY ACCESS',
  'Ringling Causeway (Bird Key Park)': 'RINGLING CAUSEWAY',
  'North Lido': 'NORTH LIDO BEACH',
  'Lido Casino': 'LIDO CASINO BEACH',
  'South Lido': 'SOUTH LIDO BEACH'
}

class florida_wq_advisory(object):
  def __init__(self, county, config_filename):
    self.logger = logging.getLogger('swim_advisory_logger')
    config_file = ConfigParser.RawConfigParser()
    config_file.read(config_filename)
    try:
      self.county = county
      self.page_url = config_file.get(county, 'page_url')
      self.sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
      self.boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
      self.data_directory = config_file.get(county, 'data_dir')
      self.driver = config_file.get('browser_driver', 'name')
    except ConfigParser as e:
      self.logger.exception(e)
      raise


  def get_advisories(self):
    advisories = []
    fl_sites = florida_sample_sites(True)
    if fl_sites.load_sites(file_name=self.sites_location_file, boundary_file=self.boundaries_location_file):
      try:
        #For debugging on a system with a GUI and firefox we use this.
        if self.driver == "firefox":
          driver = webdriver.Firefox()
        #Otherwise we go headless.
        else:
          driver = webdriver.PhantomJS('phantomjs')
        driver.get(self.page_url)
        cookie = driver.get_cookie('cbParamList')
        page = requests.get(self.page_url)
        pattern = re.compile(r"document\.write\(?([^']*?)(?:\n\s*)?\);")
        table_data = pattern.sub('\g<1>', page.content)
      except Exception as e:
        self.logger.exception(e)
      else:
        try:
          soup = BeautifulSoup(table_data, 'html.parser')
          download_data = soup.find(text=re.compile('Download Data'))
          a_tag = download_data.parent
          href = a_tag['href'].replace("\\", "").replace("\"", "")
          self.logger.debug("Downloading %s advisories from: %s" % (self.county, href))
          data = requests.get(href, cookies={'cbParamList': cookie['value']}
          )
          data_file = os.path.join(self.data_directory, "%s.zip" % (self.county))
          with open(data_file, 'wb') as zip_data_file:
            zip_data_file.write(data.content)

          unzip = zipfile.ZipFile(data_file)
          self.logger.debug("Unzipping %s advisories to: %s" % (self.county, self.data_directory))
          unzip.extractall(path=self.data_directory)
          unzip.close()
          data_file = os.path.join(self.data_directory, unzip.namelist()[0])
          with io.open(data_file, "rt") as zip_data_file:
            csv_file = csv.reader(zip_data_file)
            latest_date = None
            line_num = 0
            for row in csv_file:
              if line_num > 0:
                row_date = datetime.strptime(row[2], "%m/%d/%Y %H:%M:%S")
                if latest_date is None:
                  latest_date = row_date
                if row_date == latest_date:
                  site_name = row[1].strip().replace('  ', ' ').lower()
                  #Normalize the name, there is no consistency of naming throughout the
                  #various bits of data we harvesy from.
                  for site in fl_sites:
                    if site_name == site.description.lower() or \
                        site_name == site.name.lower() or\
                      site.name.lower().find(site_name) != -1 or\
                      site.description.lower().find(site_name) != -1:
                      site_name = site.description.lower()
                      break
                  advisories.append({
                    'site_name': site_name,
                    'advisory': bool(int(row[3])),
                    'date': row_date
                  })
                else:
                  break
              line_num += 1
          self.logger.debug("Deleting file: %s" % (data_file))
          os.remove(data_file)

        except Exception as e:
          self.logger.exception(e)
    return advisories

def get_advisories(**kwargs):
  swim_adv = florida_wq_advisory(kwargs['county'], kwargs['config_file'])
  swim_adv.get_advisories()