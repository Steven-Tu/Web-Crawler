import logging
import re
from urllib.parse import urlparse
from lxml import etree
from urllib.request import urlopen
import re


logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.discovered = set()

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = []
        parser = etree.HTMLParser()
        print(url_data['url'])
        if url_data['content'] is not None:
            html_root = etree.fromstring(url_data['content'], parser)
            result = etree.tostring(html_root,method = 'html')

            pattern = r'href="([^"]*)"'
            print(result)

            # Find all matches
            urls = re.findall(pattern, str(result))

            # Print the results

            for url in urls:
                if(url[0] != "#"):
                    if(len(url) > 1 and url[0] == '/' and url[1] == '/'): 
                        if(url_data['url'][4] == ':'):
                            outputLinks.append("http:"+url)
                        else:
                            outputLinks.append("https:"+url) 
                    elif(len(url) > 1 and url[0] == '/'):
                        outputLinks.append(url_data['url']+url)
                    else:
                        outputLinks.append(url)

            print(outputLinks)
        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
 
        
        trap_patterns = [
            # add more trap patterns as needed
        ]

        # check if URL is accessible
        try:
            response = urlopen(url)
            assert response.getcode() == 200
        except:
            return False

        # check for redirects
        if response.geturl() != url:
            return False
        #check for very long urls
        if self.is_long_url(url):
            return False
        # check for dynamic url that...
        '''if self.is_dynamic_url():
            # if...
            #   return False
            pass'''
        # if self.contains_trap_patterns():
        #   pass
        #check for history traps
        if self.is_history_trap(url):
            return False
        
        #check for all duplicates, including ones that have exited frontier
        #check if already in discovered SET
        if self.is_duplicate(url):
            return False
        
        
        '''
        4. Are all dynamic URLs trap?
        -- Not necessarily. For example, https://www.ics.uci.edu/community/news/view_news?id=1473 is not
        a trap.

        How to check if a dynamic url is a trap?

        5. Is every URL which contains the word 'calendar' a trap?
        -- No. For example, "https://www.reg.uci.edu/calendars/quarterly/2018-2019/quarterly18-19.html"
        is not a trap. 

        How to check if a url that contains a trigger word is a trap?

        '''

        '''for pattern in trap_patterns:
            if re.match(pattern, url):
                return False  # URL is a trap'''

        

        parsed = urlparse(url)

        
        if parsed.scheme not in set(["http", "https"]):
            return False
        try:
            
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False
        
        return True

    '''
    def is_dynamic_url(self, url):
        return '?' in url'''
    def is_long_url(self, url):
        return len(url) > 200
    def is_duplicate(self, url):
        if url in self.discovered:
            return True
        else:
            self.discovered.add(url)
            return False
    
    def is_history_trap(self, url):
        path_segments = url.split('/')

      
        # 1: check if there are continuously repeating sub-directories?
        for i in range(1, len(path_segments) - 1):
            if path_segments[i] == path_segments[i + 1]:
                consecutive_segments += 1
                if consecutive_segments >= 3:
                    return True

        return False
        

