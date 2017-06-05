# Python-Sitemap

Simple script to crawl websites and create a sitemap and assets output files

Goal:
Write a web crawler without using existing crawl frameworks.
Given a URL, the crawler only visit HTML pages within the same
domain and not follow external links (e.g. Facebook, Twitter).

Crawler output a site map, and for each page a list of assets (e.g. CSS, Images,
Javascripts) and links between pages.

Warning : This script only works with ***Python3.5+***

## Setup Instructions
- $ pip install aiohttp bs4

### Parameters
    -output_sitemap default='sitemap.txt' Sitemap file name
    -output_assets  default='assets.txt'  Assets file name
    -log_file       default='error.log'   Error log file name
    -workers        default=Сpu сount     Count of process for processing (N)
    -threads        default=10            Count of treads per process (M)
    -max_visited    default=1000          Max count of visited urls, stops when reached
    -timeout        default=60            Timeout for waiting for response in sec

#### Samples usage

- $ python crawler.py "http://www.bbc.co.uk/" -max_visited 1000

- $ python crawler.py "http://www.bbc.co.uk/" -output_sitemap "out_sitemap.txt" -max_visited 100

- $ python crawler.py "http://www.bbc.co.uk/" -max_visited 100 -threads 20 -timeout 30

- $ python crawler.py "http://www.bbc.co.uk/" -workers 20 -threads 3 -max_visited 1000 -timeout 15