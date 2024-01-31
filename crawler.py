import logging
import re
from urllib.parse import urlparse, urljoin
from lxml import etree, html
import heapq
from urllib.request import urlopen
import re
from collections import defaultdict
from bs4 import BeautifulSoup 


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

        self.discovered = set()

        self.identified_traps = set()

        self.page_with_most_outlinks = {"url": None, "count": 0}
        
        
        self.stop_words = {
            "a", "able", "about", "above", "abst", "accordance", "according", "accordingly", "across", "act", "actually", "added", "adj", "affected", "affecting", "affects", "after", "afterwards", "again", "against", "ah", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "announce", "another", "any", "anybody", "anyhow", "anymore", "anyone", "anything", "anyway", "anyways", "anywhere", "apparently", "approximately", "are", "aren", "arent", "arise", "around", "as", "aside", "ask", "asking", "at", "auth", "available", "away", "awfully", "b", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "begin", "beginning", "beginnings", "begins", "behind", "being", "believe", "below", "beside", "besides", "between", "beyond", "biol", "both", "brief", "briefly", "but", "by", "c", "ca", "came", "can", "cannot", "can't", "cause", "causes", "certain", "certainly", "co", "com", "come", "comes", "contain", "containing", "contains", "could", "couldnt", "d", "date", "did", "didn't", "different", "do", "does", "doesn't", "doing", "done", "don't", "down", "downwards", "due", "during", "e", "each", "ed", "edu", "effect", "eg", "eight", "eighty", "either", "else", "elsewhere", "end", "ending", "enough", "especially", "et", "et-al", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "except", "f", "far", "few", "ff", "fifth", "first", "five", "fix", "followed", "following", "follows", "for", "former", "formerly", "forth", "found", "four", "from", "further", "furthermore", "g", "gave", "get", "gets", "getting", "give", "given", "gives", "giving", "go", "goes", "gone", "got", "gotten", "h", "had", "happens", "hardly", "has", "hasn't", "have", "haven't", "having", "he", "hed", "hence", "her", "here", "hereafter", "hereby", "herein", "heres", "hereupon", "hers", "herself", "hes", "hi", "hid", "him", "himself", "his", "hither", "home", "how", "howbeit", "however", "hundred", "i", "id", "ie", "if", "i'll", "im", "immediate", "immediately", "importance", "important", "in", "inc", "indeed", "index", "information", "instead", "into", "invention", "inward", "is", "isn't", "it", "itd", "it'll", "its", "itself", "i've", "j", "just", "k", "keep keeps", "kept", "kg", "km", "know", "known", "knows", "l", "largely", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "line", "little", "'ll", "look", "looking", "looks", "ltd", "m", "made", "mainly", "make", "makes", "many", "may", "maybe", "me", "mean", "means", "meantime", "meanwhile", "merely", "mg", "might", "million", "miss", "ml", "more", "moreover", "most", "mostly", "mr", "mrs", "much", "mug", "must", "my", "myself", "n", "na", "name", "namely", "nay", "nd", "near", "nearly", "necessarily", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "ninety", "no", "nobody", "non", "none", "nonetheless", "noone", "nor", "normally", "nos", "not", "noted", "nothing", "now", "nowhere", "o", "obtain", "obtained", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "omitted", "on", "once", "one", "ones", "only", "onto", "or", "ord", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "owing", "own", "p", "page", "pages", "part", "particular", "particularly", "past", "per", "perhaps", "placed", "please", "plus", "poorly", "possible", "possibly", "potentially", "pp", "predominantly", "present", "previously", "primarily", "probably", "promptly", "proud", "provides", "put", "q", "que", "quickly", "quite", "qv", "r", "ran", "rather", "rd", "re", "readily", "really", "recent", "recently", "ref", "refs", "regarding", "regardless", "regards", "related", "relatively", "research", "respectively", "resulted", "resulting", "results", "right", "run", "s", "said", "same", "saw", "say", "saying", "says", "sec", "section", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sent", "seven", "several", "shall", "she", "shed", "she'll", "shes", "should", "shouldn't", "show", "showed", "shown", "showns", "shows", "significant", "significantly", "similar", "similarly", "since", "six", "slightly", "so", "some", "somebody", "somehow", "someone", "somethan", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specifically", "specified", "specify", "specifying", "still", "stop", "strongly", "sub", "substantially", "successfully", "such", "sufficiently", "suggest", "sup", "sure t", "take", "taken", "taking", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "that'll", "thats", "that've", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "thered", "therefore", "therein", "there'll", "thereof", "therere", "theres", "thereto", "thereupon", "there've", "these", "they", "theyd", "they'll", "theyre", "they've", "think", "this", "those", "thou", "though", "thoughh", "thousand", "throug", "through", "throughout", "thru", "thus", "til", "tip", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "ts", "twice", "two", "u", "un", "under", "unfortunately", "unless", "unlike", "unlikely", "until", "unto", "up", "upon", "ups", "us", "use", "used", "useful", "usefully", "usefulness", "uses", "using", "usually", "v", "value", "various", "'ve", "very", "via", "viz", "vol", "vols", "vs", "w", "want", "wants", "was", "wasnt", "way", "we", "wed", "welcome", "we'll", "went", "were", "werent", "we've", "what", "whatever", "what'll", "whats", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "wheres", "whereupon", "wherever", "whether", "which", "while", "whim", "whither", "who", "whod", "whoever", "whole", "who'll", "whom", "whomever", "whos", "whose", "why", "widely", "willing", "wish", "with", "within", "without", "wont", "words", "world", "would", "wouldnt", "www", "x", "y", "yes", "yet", "you", "youd", "you'll", "your", "youre", "yours", "yourself", "yourselves", "you've", "z", "zero",
        }
        self.word_frequencies = defaultdict(int)

        self.most_words_page = ("", 0)

        self.subdomain_frequency = defaultdict(int)

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """

        #Tracks num of max outlinks and name of link w/ max outlinks for Analytics #1 & #2
        max_outlinks = 0
        max_link = ""
        
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)
            outlinks = 0
            link = url_data["url"]
            hostname = urlparse(link).hostname

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    self.subdomain_frequency[hostname] += 1 # Analytics #1
                    print("Subdomain:", hostname, "Frequency:", self.subdomain_frequency[hostname])
                    outlinks += 1
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
                    if outlinks > max_outlinks:
                        max_outlinks = outlinks
                        max_link = link

        #Adds to class attributes for analytics #2
        self.page_with_most_outlinks["url"] = max_link
        self.page_with_most_outlinks["count"] = max_outlinks


    def extract_next_links(self, url_data): # http://www.ics.uci.edu/
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """

        outputLinks = []
        # Checks if binary content exists and if the URL is accessible (not 404)
        if url_data['content'] is not None and url_data['http_code'] != 404:
            parsed_soup = BeautifulSoup(url_data['content'], 'html.parser')
            soup_content = parsed_soup.get_text()
            words = soup_content.split()

            '''
            4. What is the longest page in terms of number of words? (HTML markup doesn’t count as words)
            5. What are the 50 most common words in the entire set of pages? (Ignore English stop words)
            '''
            self.set_most_words_page(url_data["url"], len(words))
            for word in words:
                if self.is_stop_word(word) is False:
                    self.word_frequencies[word] += 1

            if parsed_soup is None: # if there is no html content
                return []

            pattern = r'href="([^"]*)"'
            # Find all matches

            urls = re.findall(pattern, str(parsed_soup))
            for url in urls:
                converted_url = self.convert_relative_to_absolute(url, url_data["url"])
                outputLinks.append(converted_url)

        return outputLinks
 
        

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        # check if URL is accessible
        try:
            response = urlopen(url)
            assert response.getcode() == 200
        except:
            # print("code != 200")
            #self.identified_traps.add(url)
            return False


        #check for very long urls
        if self.is_long_url(url):
<<<<<<< HEAD
            print("long url")
            self.identified_traps.add(url)
            return False
        
        #check for history traps
        if self.is_history_trap(url):
            print(">=3 consective history segments", UnicodeTranslateError)
            self.identified_traps.add(url)
=======
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
>>>>>>> 511a3ffc20d1df33bca112e9c51132fa6ba82486
            return False
        
        #check for all duplicates, including ones that have exited frontier
        #check if already in discovered SET
        if self.is_duplicate(url):
<<<<<<< HEAD
            # print("Already in discovered SET:", url)
            #self.identified_traps.add(url)
            return False

=======
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

        
>>>>>>> 511a3ffc20d1df33bca112e9c51132fa6ba82486

        parsed = urlparse(url)

        # print("Parsed hostname:", parsed.hostname, "is a subdomain of ics.uci.edu", ".ics.uci.edu" in parsed.hostname)
        if parsed.scheme not in set(["http", "https"]):
            print('parsed.scheme not in set(["http", "https"])')
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
    

<<<<<<< HEAD
    def convert_relative_to_absolute(self, relative_url, base_url):
        ''' If the URL is relative, convert it to absolute URL '''
        print(base_url, relative_url)
        if relative_url[0:2] == "//":
            if base_url[0:5] == "https":
                return urljoin("https:", relative_url)
            else:
                return urljoin("http:", relative_url)
        elif relative_url[0:1] == "/":
            return urljoin(base_url, relative_url[1:])
        elif relative_url[0:2] == "./":
            return urljoin(base_url, relative_url[2:])
        elif relative_url[0:2] == "..":
            base_url_segments = base_url.split('/')
            base_url_segments[0] = base_url_segments[0] + "/" # add back a slash for later joining
            base_url_segments = [segment for segment in base_url_segments if segment != ""] # remove empty strings
            num_up_dir = relative_url.count('../') # count number of directories to go up by
            for i in range(num_up_dir):
                base_url_segments.pop()
            relative_url = relative_url.replace('../', '')
            print(base_url_segments)
            absolute_path = '/'.join(base_url_segments) + '/' + relative_url
            print("CREATED ABSOLUTE PATH @:", absolute_path)
            return absolute_path
        else:
            return relative_url



    def is_long_url(self, url):
        return len(url) > 300
    
=======
    '''
    def is_dynamic_url(self, url):
        return '?' in url'''
    def is_long_url(self, url):
        return len(url) > 200
>>>>>>> 511a3ffc20d1df33bca112e9c51132fa6ba82486
    def is_duplicate(self, url):
        if url in self.discovered:
            return True
        else:
            self.discovered.add(url)
            return False
    
    def is_history_trap(self, url):
        path_segments = url.split('/')
        consecutive_segments=0
      
        # 1: check if there are continuously repeating sub-directories?
        for i in range(1, len(path_segments) - 1):
            if path_segments[i] == path_segments[i + 1]:
                consecutive_segments += 1
                if consecutive_segments >= 3:
                    return True
        return False

<<<<<<< HEAD
    def is_stop_word(self, word):
        return word in self.stop_words
    
    def get_most_common_words(self):
        if len(self.word_frequencies) < 50:
            heapq.nlargest(len(self.word_frequencies), self.word_frequencies, key=self.word_frequencies.get)
        heapq.nlargest(50, self.word_frequencies, key=self.word_frequencies.get)
    
    def set_most_words_page(self, url, length):
        if length > self.most_words_page[1]:
            self.most_words_page = (url, length)
=======
        return False
        

>>>>>>> 511a3ffc20d1df33bca112e9c51132fa6ba82486
