import http.client
import logging
import queue
import urllib
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from url_normalize import url_normalize
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import threading
import time
import urllib.robotparser as urobot
from datetime import datetime
import heapdict
import socket
from fake_useragent import UserAgent
from googlesearch import search


class WebCrawler(threading.Thread):

    def __init__(self, total_pages, priority_queue, visited_urls, priority_queue_lock, max_depth,
                 error_urls, return_codes, url_depth_map, importance_scores, novelty_scores, mode, queue, logger,total_size):
        threading.Thread.__init__(self)
        self.visited_urls = visited_urls  # dictionary to keep track of all visited URLs
        self.priority_queue = priority_queue    # the queue for the priority crawl
        self.max_depth = max_depth              # max depth allowed to be crawled
        self.total_pages = total_pages  # number of pages to be crawled
        self.threads = []
        self.priority_queue_lock = priority_queue_lock  # lock for the shared queue
        self.error_urls = error_urls    # dictionary to keep track of the URLs which return an error
        self.return_codes = return_codes  # dictionary to keep track of all the return codes
        self.url_depth_map = url_depth_map  # dictionary to map URLs with their depths
        self.importance_scores = importance_scores  # dictionary to map URLs with their importance
        self.novelty_scores = novelty_scores  # dictionary to map URLs with their novelty
        self.mode = mode  # mode 0 -> BFS crawl, mode 1 -> priority crawl
        self.queue = queue  # queue for BFS crawl
        self.ua = UserAgent()  # to fake user agent in headers
        self.logger = logger  # to log progress in log files
        self.total_size = total_size
        self.total_time_sleep = 0

    def run(self):
        while not len(self.visited_urls) >= self.total_pages:
            # if the queue has been empty for more than 3 seconds, then return
            if self.total_time_sleep > 3:
                return
            # if queue is empty, then wait a few seconds to fetch new URLs
            if (self.mode == 1 and len(self.priority_queue) == 0) or (self.mode == 0 and self.queue.qsize() == 0):
                self.total_time_sleep += 0.5
                time.sleep(0.5)
                continue
            url = ""
            priority = 0
            self.priority_queue_lock.acquire()
            if self.mode == 1:
                node = self.priority_queue.popitem()
                domain_url = get_domain(node[0])
                novelty_score = 0
                if domain_url in self.novelty_scores:
                    novelty_score = self.novelty_scores[domain_url]
                node_priority = novelty_score - self.importance_scores[node[0]]
                # check if not empty before comparing
                if len(self.priority_queue) != 0:
                    next_node = self.priority_queue.peekitem()
                    if next_node is not None:
                        if node_priority > next_node[1]:
                            self.priority_queue[node[0]] = node_priority
                            self.priority_queue_lock.release()
                            continue
                url = node[0]
                priority = node_priority
            else:
                url = self.queue.get()
            self.priority_queue_lock.release()
            if self.url_depth_map[url] > self.max_depth:
                continue
            can_fetch = self.can_fetch(url)
            # I assume it is fine to fetch anything from this website if I cannot parse its robots.txt file
            if can_fetch is None:
                can_fetch = True
            if url not in self.visited_urls and url not in self.error_urls and can_fetch and len(
                    self.visited_urls) < self.total_pages:
                self.parse_web_page(url, priority)
        return None

    # function to perform robot parser check
    def can_fetch(self, url):
        try:
            rp = urobot.RobotFileParser()
            domain = get_domain(url)
            rp.set_url(urlparse(url).scheme + "://" + domain + '/robots.txt')
            rp.read()
        except URLError as error:
            self.error_urls[url] = 1
            return None
        except:
            self.error_urls[url] = 1
            return None
        return rp.can_fetch("*", url)

    # function to check if link is valid by using stop words
    def is_valid_link(self, url):
        # list of stop words
        stop_words = [".png", ".jpeg", ".jpg", ".cgi", ".gz", ".zip", ".rar", ".mp4", "javascript", "mailto:", ".pdf",
                     "tel:", ".java", ".xml", "irc://", ".mp3", ".tgz", ".m3u", ".txt", ".xlsm", ".xlsx", ".rss", ".iso", ".md", ".atom", ".exe", ".docx", ".aspx", ".gif", "ircs:"]
        for stop_word in stop_words:
            if stop_word in url.lower():
                return False
        return True

    # function to check if the link is absolute
    def is_absolute(self, url):
        return bool(get_domain(url))

    # function to download the webpage
    def download_page(self, url):
        if url not in self.visited_urls and url not in self.error_urls and len(self.visited_urls) < self.total_pages:
            self.visited_urls[url] = 1
            domain_url = get_domain(url)
            if domain_url in self.novelty_scores:
                self.novelty_scores[domain_url] += 1 - (1 / (len(self.visited_urls) + 1))
            else:
                self.novelty_scores[domain_url] = 0
            try:
                request = urllib.request.Request(url, data=None, headers={'User-Agent': self.ua.random})
                response = urllib.request.urlopen(request)

                # check if correct type
                if response.info().get_content_type() != "text/html":
                    if "not correct mime type:" in self.return_codes:
                        self.return_codes["not correct mime type"] += 1
                    else:
                        self.return_codes["not correct mime type"] = 1
                    return None
                response = response.read().decode('ISO-8859-1')
                if 200 in self.return_codes:
                    self.return_codes[200] += 1
                else:
                    self.return_codes[200] = 1
                return {"response": response, "code": 200}
            except HTTPError as error:
                if error.code in self.return_codes:
                    self.return_codes[error.code] += 1
                else:
                    self.return_codes[error.code] = 1
                return {"code": error.code}
            except socket.timeout as error:
                if 408 in self.return_codes:
                    self.return_codes[408] += 1
                else:
                    self.return_codes[408] = 1
                return {"code": 408}
            except http.client.IncompleteRead as error:
                if error.partial in self.return_codes:
                    self.return_codes[error.partial] += 1
                else:
                    self.return_codes[error.partial] = 1
                return {"code": error.partial}
            except URLError as error:
                if error.reason in self.return_codes:
                    self.return_codes[error.reason] += 1
                else:
                    self.return_codes[error.reason] = 1
                return {"code": error.reason}
            except http.client.RemoteDisconnected as error:
                if error.args[0] in self.return_codes:
                    self.return_codes[error.args[0]] += 1
                else:
                    self.return_codes[error.args[0]] = 1
                return {"code": error.args[0]}
            except ConnectionResetError as error:
                if error.args[0] in self.return_codes:
                    self.return_codes[error.args[0]] += 1
                else:
                    self.return_codes[error.args[0]] = 1
                return {"code": error.args[0]}

    #This function is used to parse webpages and add the URLs to the queue with updated priority
    def parse_web_page(self, url, priority=0):
        if url not in self.url_depth_map:
            return
        depth = self.url_depth_map[url]
        priority = priority
        if url not in self.visited_urls and url not in self.error_urls and len(self.visited_urls) < self.total_pages:
            start_download_time = time.time()
            response = self.download_page(url)
            end_download_time = time.time()
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            if response is None:
                self.logger.info(
                    "%s: Crawled webpage %s . Priority score: %s. Download time: %s seconds. Error: Incorrect mime type . Depth: %s",
                    dt_string, url, priority, format(end_download_time - start_download_time), depth)
                return None
            if response["code"] == 200:
                web_page = response["response"]
                length = len(web_page)
                self.total_size[0] += length
                self.logger.info(
                    "%s: Crawled webpage %s . Priority score: %s. Download time: %s seconds. Status code: %s . Size: %s bytes. Depth: %s",
                    dt_string, url, priority, format(end_download_time - start_download_time), response["code"],
                    length, depth)
                soup = BeautifulSoup(web_page, 'html.parser')

                links_inserted = {}     # to avoid adding the same link from the same website twice
                number_of_links = 0     # count number of links in the webpage. To be used for importance score

                if self.mode == 1:
                    for link in soup.find_all('a'):
                        if link.get('href') is not None:
                            number_of_links += 1
                for link in soup.find_all('a'):        # parse the links from the web page
                    if link.get('href') is not None:
                        if not self.is_absolute(link.get('href')):
                            newUrl = normalize(urljoin(url, link.get('href')))
                        else:
                            newUrl = normalize(link.get('href'))
                        if newUrl is None:
                            continue
                        if newUrl not in self.visited_urls and newUrl not in self.error_urls and self.is_valid_link(newUrl) and newUrl not in links_inserted:
                            links_inserted[newUrl] = 1
                            self.priority_queue_lock.acquire()
                            if self.mode == 1:
                                # calculate importance and update if it already exists in the queue, otherwise, it is set to 0
                                importance_score = 1 / number_of_links
                                if newUrl in self.priority_queue:
                                    self.importance_scores[newUrl] += importance_score
                                    self.priority_queue[newUrl] -= self.importance_scores[newUrl]
                                else:
                                    self.importance_scores[newUrl] = importance_score
                                    self.priority_queue[newUrl] = - importance_score
                                    self.url_depth_map[newUrl] = depth + 1
                            else:
                                self.queue.put(newUrl)
                                self.url_depth_map[newUrl] = depth + 1
                            self.priority_queue_lock.release()
            else:
                self.logger.info("%s: Crawled webpage %s. Priority: %s. Download time: %s seconds. Status code: %s . Size: 0 bytes ",
                             dt_string, url, priority,format(end_download_time - start_download_time), response["code"])


def get_domain(url):
    return urlparse(url).netloc


def normalize(url):
    try:
        return url_normalize(url)
    except UnicodeError as error:
        return None
    except KeyError as error:
        return None

def get_seeded_pages(keyword, num_results, url_depth_map, importance_scores, priority_queue, bfs_queue):
    for result in search(keyword, num=num_results, stop=10, pause=1):
        # init the priorities for all URLs to 0
        importance_scores[normalize(result)] = 0
        url_depth_map[normalize(result)] = 0
        priority_queue[normalize(result)] = 0
        bfs_queue.put(normalize(result))


def main():
    socket.setdefaulttimeout(10)
    input_keyword = input("Please enter your keyword: ")
    url_depth_map = {}   # to keep track of depth of each url
    importance_scores = {}   # to keep track of importance score of each url
    priority_queue = heapdict.heapdict()
    bfs_queue = queue.Queue()
    get_seeded_pages(input_keyword, 10, url_depth_map, importance_scores, priority_queue, bfs_queue)    # get the seed pages
    bfs_depth_map = url_depth_map
    execute_crawl("priority_crawl.log", priority_queue, bfs_depth_map, importance_scores, 1, bfs_queue)   # execute priority crawl (mode = 1)
    execute_crawl("bfs_crawl.log", priority_queue, bfs_depth_map, importance_scores, 0, bfs_queue)   # execute BFS crawl (mode = 0)


def execute_crawl(filename, priority_queue, url_depth_map, importance_scores, mode, bfs_queue):
    error_urls = {}
    return_codes = {}
    max_depth = 60
    visited_urls = {}
    priority_queue_lock = threading.Lock()
    novelty_scores = {}
    total_pages_fetch = 12000
    max_num_threads = 50
    total_size = [0]
    threads = []
    logger = logging.getLogger(filename)
    file_handler = logging.FileHandler(filename)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    logger.info("%s Crawler started", dt_string)
    start = time.time()
    for i in range(int(max_num_threads)):
        web_crawler = WebCrawler(total_pages_fetch, priority_queue, visited_urls, priority_queue_lock, max_depth,
                                 error_urls, return_codes,
                                 url_depth_map, importance_scores, novelty_scores, mode, bfs_queue, logger,
                                 total_size)
        web_crawler.daemon = True
        threads.append(web_crawler)
        web_crawler.start()
    for thread in threads:
        thread.join()
    end = time.time()
    logger.info("time: %s seconds", format(end - start))
    logger.info("Total websites crawled: %s", len(visited_urls))
    logger.info("Total size: %s bytes", total_size[0])
    for key, value in return_codes.items():
        logger.info("Number of %s status: %s", key, value)

if __name__ == '__main__':
    main()
