import sys
import argparse
from urllib.parse import urlparse
import asyncio
from asyncio import Queue

import aiohttp
import bs4


class Crawler:
    def __init__(self, url,
                 output_sitemap_file='sitemap.txt',
                 output_assets_file='assets.txt',
                 log_file='error.log',
                 thread_cnt=8, max_urls_visited=1000):
        """
        Crawler class provide gettin sitemap and collect assets
        for each page (e.g. CSS, Images, Javascripts) and links between pages

        Parameters:
            url - Given URl,
            output_sitemap_file - Sitemap file name,
            output_assets_file - Assets file name,
            log_file - Error log file name,
            thread_cnt - Count of treads for crawling,
            max_urls_visited - Max count of visited urls (stop if reach this number)
        """
        url = urlparse(url)
        self.scheme = url.scheme
        self.netloc = url.netloc
        self.url = url.scheme + '://' + url.netloc
        self.log_file = open(log_file, 'a')
        self.output_sitemap_file = output_sitemap_file
        self.output_assets_file = output_assets_file
        loop = asyncio.get_event_loop()
        self.session = aiohttp.ClientSession(loop=loop)
        self.queue = Queue(loop=loop)
        self.queue.put_nowait(self.url)
        self.loop = loop
        self.workers_cnt = thread_cnt
        self.urls = set([self.url])
        self.visited = set([])
        self.max_urls = max_urls_visited
        self.assets = {}
        self.urls_adds = 0
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
        elif url.path.endswith(self.exclude_res):
            return False
        else:
            return True

    async def check_max_url_visited(self):
        if len(self.visited) > self.max_urls:
            while True:
                url = await self.queue.get()
                self.queue.task_done()

    async def run(self):
        """Run the crawler until all finished."""
        workers = [asyncio.Task(self.work(), loop=self.loop) for _ in range(self.workers_cnt)]
        await self.queue.join()    # will be await all urls processed
        for worker in workers:
            worker.cancel()
        self.save_results()
        self.log_file.close()

    async def work(self):
        """
          Main crawl loop
        """
        #while self.urls:
        #    self.parse()
        try:
            """Infinit loop"""
            while True:
                await self.check_max_url_visited()
                url = await self.queue.get()
                try:
                    if url in self.visited:
                        continue
                    try:
                        response = await self.session.get(url, allow_redirects=False)
                    except aiohttp.ClientError as client_error:
                        self.errlog('Error occurred for url {}:{}'.format(url, client_error))
                        #sys.exit(1)
                    try:
                        if response.status == 200:
                            newurls, assets, short_url = await self.parse(response, url)
                            self.visited.update([url])
                            newurls = newurls.difference(self.visited)
                            for newurl in newurls:
                                self.queue.put_nowait(newurl)
                            if short_url:
                                self.assets[short_url] = assets
                            self.urls_adds += len(newurls)
                            print('Visited {} of {}. Added {} new urls'.format(len(self.visited), self.urls_adds, len(newurls)))
                    finally:
                        await response.release()
                finally:
                    self.queue.task_done()
        except asyncio.CancelledError:    # to stop loop
            pass

    async  def parse(self, response, url):
        newurls = set()
        text = await response.text()
        _url = urlparse(url)
        if url not in self.visited and self.is_valid(_url):
            soup = bs4.BeautifulSoup(text, "html.parser")
            try:
                for link in soup.find_all('a', {'href': True}):
                    newurl = self.normalaze(_url, link['href'])
                    _newurl = urlparse(newurl)
                    newurl = _newurl.scheme + '://' + _newurl.netloc + _newurl.path
                    if self.is_valid(_newurl):
                        newurls.add(newurl)
            except Exception as e:
                self.errlog(e.message)

            short_url = _url.path
            if not short_url:
                short_url = '/'
            return newurls, self.parse_assets(_url, soup), short_url
        else:
            return set(), [], None

    def get_tags(self, _url, tag, attr, soup):
        for tags in soup.find_all(tag, {attr: True}):
            val = self.normalaze(_url, tags[attr])
            yield val

    def parse_assets(self, _url, soup):
        assets = list(self.get_tags(_url, 'a', 'href', soup))
        assets += list(self.get_tags(_url, 'link', 'href', soup))
        assets += list(self.get_tags(_url, 'img', 'src', soup))
        assets += list(self.get_tags(_url, 'script', 'src', soup))
        return assets

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
    parser.add_argument('-threads', type=int, default=8, help='Count of treads for crawling')
    parser.add_argument('-max_visited', type=int, default=8, help='Max count of visited urls')
    return parser


if __name__ == '__main__':
    options = parse_args().parse_args(sys.argv[1:])
    crl = Crawler(url=options.url,
                  output_assets_file=options.output_assets,
                  output_sitemap_file=options.output_sitemap,
                  log_file=options.log_file,
                  thread_cnt=options.threads, max_urls_visited=options.max_visited)
    #crl = Crawler(url='http://www.bbc.co.uk/')
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(crl.run())
    loop.run_until_complete(future)
    crl.session.close()
    loop.stop()
    loop.run_forever()
    loop.close()
