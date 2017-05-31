import argparse
import sys
from urllib.parse import urlparse
import requests

import bs4


class Crawler:
    def __init__(self, url, output_sitemap_file='sitemap.txt', output_assets_file='assets.txt', log_file='error.log'):
        """
        Crawler class provide gettin sitemap and collect assets
        for each page (e.g. CSS, Images, Javascripts) and links between pages
        """
        url = urlparse(url)
        self.scheme = url.scheme
        self.netloc = url.netloc
        self.url = url.scheme + '://' + url.netloc
        self.log_file = open(log_file, 'a')
        self.output_sitemap_file = output_sitemap_file
        self.output_assets_file = output_assets_file
        self.urls = set([self.url])
        self.visited = set([])
        self.assets = {}
        self.exclude_res = ('.iso', '.rar', '.tar', '.tgz', '.zip', '.dmg', '.exe',
                            '.avi', '.mkv', '.mp4',
                            '.jpg', '.jpeg', '.png', '.gif', '.pdf' )


    def errlog(self, msg):
        self.log_file.write(msg)
        self.log_file.write('\n')

    @staticmethod
    def normalaze(url, link):
        if link.startswith('/'):
            link = url.scheme + '://' + url[1] + link
        elif link.startswith('#'):
            link = url.scheme + '://' + url[1] + url[2] + link
        elif not link.startswith(('http', "https")):
            link = url.scheme + '://' + url[1] + '/' + link

        # Remove the anchor part if needed
        if "#" in link:
            link = link[:link.index('#')]

        if link.endswith('/'):
            link = link[:-1]

        return link

    def is_valid(self, url):
        if self.netloc != url.netloc:
            return False
        return True

    def run(self):
        """
          Main crawl loop
        """
        while self.urls:
            self.parse()

        self.save_results()
        self.log_file.close()

    def parse(self):
        if not self.urls:
            return
        url = self.urls.pop()
        _url = urlparse(url)
        if url not in self.visited and not _url.path.endswith(self.exclude_res):
            try:
                response = requests.get(url)
            except requests.exceptions.Timeout:
                self.errlog('Timeout occurred for url {}'.format(url))
                return
            except requests.exceptions.TooManyRedirects:
                self.errlog('Too Many Redirects for url {}'.format(url))
                return
            except requests.exceptions.RequestException as e:
                # fatal error.
                self.errlog("Error: {}".format(e))
                sys.exit(1)
            if response.status_code >= 400:
                self.errlog("Error {} at url {}".format(response.status_code, url))
                return
            soup = bs4.BeautifulSoup(response.text)
            try:
                for link in soup.find_all('a', {'href': True}):
                    newurl = self.normalaze(_url, link['href'])
                    _newurl = urlparse(newurl)
                    newurl = _newurl.scheme + '://' + _newurl.netloc + _newurl.path
                    if self.is_valid(_newurl):
                        self.urls.update([newurl])

            except Exception as e:
                self.errlog(e.message)
            self.visited.update([url])
            self.parse_assets(_url, soup)

    def get_tags(self, _url, tag, attr, soup):
        for tags in soup.find_all(tag, {attr: True}):
            val = self.normalaze(_url, tags[attr])
            yield val

    def parse_assets(self, _url, soup):
        url = _url.path
        if not url:
            url = '/'
        self.assets[url] = list(self.get_tags(_url, 'a', 'href', soup))
        self.assets[url] += list(self.get_tags(_url, 'link', 'href', soup))
        self.assets[url] += list(self.get_tags(_url, 'img', 'src', soup))
        self.assets[url] += list(self.get_tags(_url, 'script', 'src', soup))

    def save_results(self):
        with open(self.output_sitemap_file, 'w') as file:
            file.writelines(page + '\n' for page in self.assets.keys())
        with open(self.output_assets_file, 'w', encoding='utf8') as file:
            for page in self.assets:
                file.writelines(page + '\n')
                file.writelines(asset + '\n' for asset in self.assets[page])


def parse_args():
    parser = argparse.ArgumentParser(description='Web crawler.')
    parser.add_argument('-url', default='http://yoyowallet.com/', help='Given URl.')
    parser.add_argument('-output_sitemap', default='sitemap.txt', help='Sitemap file name')
    parser.add_argument('-output_assets', default='assets.txt', help='Assets file name')
    parser.add_argument('-log_file', default='error.log', help='Error log file name')
    return parser


if __name__ == '__main__':
    options = parse_args().parse_args(sys.argv[1:])
    crl = Crawler(url = options.url,
                  output_assets_file = options.output_assets,
                  output_sitemap_file = options.output_sitemap,
                  log_file = options.log_file)
    crl.run()
