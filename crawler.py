"""
Script to crawl websites and create a sitemap and assets output files
Do processing in N os processes with M coroutine treads in each
Run:
    python crawler.py url
Parameters
    -output_sitemap default='sitemap.txt' Sitemap file name
    -output_assets  default='assets.txt'  Assets file name
    -log_file       default='error.log'   Error log file name
    -workers        default=Сpu сount     Count of process for processing (N)
    -threads        default=10            Count of treads per process (M)
    -max_visited    default=1000          Max count of visited urls, stops when reached
    -timeout        default=60            Timeout for waiting for response in sec
"""

import sys
import argparse
from urllib.parse import urlparse
import asyncio
from queue import Empty
import multiprocessing as mp
from multiprocessing import Process, Manager
import time

import aiohttp
import bs4

EXCLUDE_RES = ('.iso', '.rar', '.tar', '.tgz', '.zip', '.dmg', '.exe',
               '.avi', '.mkv', '.mp4',
               '.jpg', '.jpeg', '.png', '.gif', '.pdf' )

class Parser(Process):
    def __init__(self, netloc,
                 url_queue,
                 visited_q,
                 newurls_queue,
                 assets_queue,
                 stop_signal,
                 err_file,
                 thread_cnt,
                 process_cnt,
                 max_urls,
                 timeout):
        super().__init__()
        self.main_url_q = url_queue
        self.url_queue = None
        self.newurls_queue = newurls_queue
        self.assets_queue = assets_queue
        self.visited_q = visited_q
        self.stop_signal = stop_signal
        self.netloc = netloc
        self.workers_cnt = thread_cnt
        self.loop = None
        self.session = None
        self.err_file = err_file
        self.threads_cnt = thread_cnt
        self.process_cnt = process_cnt
        self.max_urls = max_urls
        self.timeout = timeout

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
        elif url.path.endswith(EXCLUDE_RES):
            return False
        else:
            return True

    def errlog(self, msg):
        with open(self.err_file,'a') as f:
            f.write('Process id={} raise: {}\n'.format(str(id(self)), msg))


    def _run(self):
        # Fill local queue and do processing
        while True:
            # Calculating count of all urls for processing
            rest_cnt = self.max_urls - self.visited_q.qsize()
            # Divide current main queue tasks between all forked processe
            size = min(rest_cnt, self.main_url_q.qsize()) // self.process_cnt + 1
            while self.url_queue.qsize() < size and self.main_url_q.qsize() >0:
                try:
                    # Get new url from main queue and put into local tread-save queue
                    # If we try use just multiprocessing.Queue inside of coroutine threads (with more one treads)
                    # aiohttp.ClientSession.get() got blocked
                    url = self.main_url_q.get()
                    self.url_queue.put_nowait(url)
                    # Coordinator send empty url - means to stop
                    if not url:
                        return
                except:
                    pass
            if self.url_queue.qsize() >0:
                # Do processing in coroutine treads
                self.loop.run_until_complete(asyncio.gather(
                    *[self.work() for _ in range(self.threads_cnt)]
                ))
                if self.stop_signal.is_set():
                    break
                else:
                    assert self.url_queue.qsize() == 0
            else:
                # Where are no new urls in main queue
                time.sleep(1)

    def run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.url_queue = asyncio.Queue(loop=self.loop)
        self._run()
        self.session.close()
        self.loop.stop()
        self.loop.run_forever()
        self.loop.close()

    async def find_new(self, url):
        """ Process url: extract new link and assets """
        try:
            response = await self.session.get(url, allow_redirects=False, timeout=self.timeout)
        except aiohttp.ClientError as e:
            self.errlog('Url {} do not processed. Error occurred: {} ({}).'.format(url, e, e.__class__.__name__))
            #print(e)
            return
        except asyncio.TimeoutError:
            self.errlog('Url {} do not processed. Timeout error occurred.'.format(url))
            return
        try:
            if response.status == 200:
                newurls = await self.parse(response, url)
                if newurls:
                    self.newurls_queue.put_nowait(newurls)
        finally:
            await response.release()

    async def work(self):
        """Thread processing local url queue """
        try:
            while True:
                # Check all queue processed
                if self.url_queue.qsize() == 0:
                    break
                url = await self.url_queue.get()
                try:
                    # Finishing if coordinator have send command to stop
                    if not url or self.stop_signal.is_set():
                        break
                    await self.find_new(url)
                finally:
                    self.visited_q.put_nowait(url)
        except asyncio.CancelledError:
            pass

    async def parse(self, response, url):
        """ Get and parse response body """
        newurls = set()
        try:
            text = await response.text()
        except asyncio.TimeoutError:
            self.errlog('Url {} do not processed. Timeout error occurred.'.format(url))
            return
        _url = urlparse(url)
        if self.is_valid(_url):
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

            if short_url:
                self.assets_queue.put_nowait((short_url, self._collect_assets(url, text)))

            return newurls

    @staticmethod
    def _collect_assets(url, text):
        _url = urlparse(url)
        soup = bs4.BeautifulSoup(text, "html.parser")
        return Parser.parse_assets(_url, soup)

    @staticmethod
    def get_tags(_url, tag, attr, soup):
        for tags in soup.find_all(tag, {attr: True}):
            val = Parser.normalaze(_url, tags[attr])
            yield val

    @staticmethod
    def parse_assets(_url, soup):
        assets = list(Parser.get_tags(_url, 'a', 'href', soup))
        assets += list(Parser.get_tags(_url, 'link', 'href', soup))
        assets += list(Parser.get_tags(_url, 'img', 'src', soup))
        assets += list(Parser.get_tags(_url, 'script', 'src', soup))
        return set(assets)


class CrawlerMP:
    def __init__(self, url,
                 output_sitemap_file='sitemap.txt',
                 output_assets_file='assets.txt',
                 log_file='error.log',
                 threads_cnt=10,
                 workers_cnt=mp.cpu_count(),
                 max_urls_visited=1000,
                 timeout=10
                 ):
        url = urlparse(url)
        self.scheme = url.scheme
        self.netloc = url.netloc
        self.url = url.scheme + '://' + url.netloc
        self.log_file_name = log_file
        self.output_sitemap_file = output_sitemap_file
        self.output_assets_file = output_assets_file
        mp = Manager()
        self.url_queue = mp.Queue()
        self.visited_queue = mp.Queue()
        self.newurls_queue = mp.Queue()
        self.assets_queue = mp.Queue()
        self.stop_signal = mp.Event()
        self.workers_cnt = workers_cnt
        self.threads_cnt = threads_cnt
        self.timeout = timeout
        self.visited = 0
        self.send = set([])
        self.max_urls = max_urls_visited
        self.assets = {}
        self.urls_adds = 0

    def collect_assets(self):
        while not self.assets_queue.empty():
            (url, assets) = self.assets_queue.get()
            self.assets[url] = sorted(assets)

    def save_results(self):
        """Store results to files"""
        self.collect_assets()
        with open(self.output_sitemap_file, 'w', encoding='utf8') as file:
            file.writelines(page + '\n' for page in sorted(self.assets.keys()))
        with open(self.output_assets_file, 'w', encoding='utf8') as file:
            for page in sorted(self.assets):
                file.writelines(page + '\n')
                file.writelines(asset + '\n' for asset in self.assets[page])

    def stop_workers(self):
        self.stop_signal.set()
        for i in range(self.workers_cnt):
            self.url_queue.put_nowait(None)

    def check_max_url_visited(self):
        """Check and clear urls queue for max_urls visited"""
        if self.visited >= self.max_urls or self.visited == len(self.send):
            # clear queue
            while not self.url_queue.empty():
                try:
                    self.url_queue.get_nowait()
                except Empty:
                    pass
            self.stop_workers()
            return True
        return False

    def process_newurls(self):
        """Main url counter and process coordinator"""
        try:
            while True:
                # Await for new visited urls
                visited_url = self.visited_queue.get()
                self.visited += 1
                try:
                    # Check for new urls
                    newurls = self.newurls_queue.get_nowait()
                except Empty:
                    newurls = set()

                if newurls:
                    newurls = newurls.difference(self.send)
                    for newurl in newurls:
                        self.url_queue.put_nowait(newurl)
                    self.send.update(newurls)
                    self.urls_adds += len(newurls)
                print('Visited {} of {}. Added {} new urls send={}'.format(self.visited, self.urls_adds, len(newurls), len(self.send)))
                if self.check_max_url_visited():
                    return
                self.collect_assets()
        except asyncio.CancelledError:
            pass

    def run(self):
        workers = [Parser(self.netloc,
                          self.url_queue,
                          self.visited_queue,
                          self.newurls_queue,
                          self.assets_queue,
                          self.stop_signal,
                          self.log_file_name,
                          self.threads_cnt,
                          self.workers_cnt,
                          self.max_urls,
                          self.timeout
                          )
                    for _ in range(self.workers_cnt)]
        for w in workers:
            w.start()

        self.url_queue.put(self.url)
        self.send.update([self.url])

        # Run main coordinator loop
        self.process_newurls()

        for w in workers:
            w.join()
        self.save_results()

def parse_args():
    parser = argparse.ArgumentParser(description='Web crawler.')
    parser.add_argument('url', default='http://www.bbc.co.uk/', help='Given URl.')
    parser.add_argument('-output_sitemap', default='sitemap.txt', help='Sitemap file name')
    parser.add_argument('-output_assets', default='assets.txt', help='Assets file name')
    parser.add_argument('-log_file', default='error.log', help='Error log file name')
    parser.add_argument('-workers', type=int, default=mp.cpu_count(), help='Count of process for crawling')
    parser.add_argument('-threads', type=int, default=10, help='Count of treads per process')
    parser.add_argument('-max_visited', type=int, default=1000, help='Max count of visited urls')
    parser.add_argument('-timeout', type=int, default=60, help='Timeout for waiting for response in sec')
    return parser


if __name__ == '__main__':
    options = parse_args().parse_args(sys.argv[1:])
    crl = CrawlerMP(url=options.url,
                    output_assets_file=options.output_assets,
                    output_sitemap_file=options.output_sitemap,
                    log_file=options.log_file,
                    workers_cnt=options.workers,
                    threads_cnt=options.threads,
                    max_urls_visited=options.max_visited,
                    timeout=options.timeout
                    )
    now = time.time()
    try:
        crl.run()
    except KeyboardInterrupt:
        sys.exit(1)
    print('Total time is: {}'.format(time.time()-now))
