# Python-Sitemap

Simple script to crawl websites and create a sitemap and assets output files

Goal:
Write a web crawler without using existing crawl frameworks.
Given a URL, the crawler only visit HTML pages within the same
domain and not follow external links (e.g. Facebook, Twitter).

Crawler output a site map, and for each page a list of assets (e.g. CSS, Images,
Javascripts) and links between pages.

Warning : This script only works with ***Python3***

## Setup Instructions
- $ pip install -r requirements.txt

### Simple usage

- $ python main.py

- $ python crawler.py -url "http://www.zyvra.org/"

- $ python crawler.py -url "http://www.zyvra.org/" -output_sitemap "out_sitemap.txt"
