from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import threading
import time
from os import remove
import re
import dataset
import collections
import ConfigParser

class tweetStreamer(StreamListener):
	def __init__(self,timelimit):
		self.startTime = time.time()
		self.limit = timelimit
		super(tweetStreamer, self).__init__()

	def on_status(self, status):
		if time.time() - self.startTime > self.limit:
			return False

		tweetTable = db['tweetTable']
		tweetTable.insert(dict(username = status.user.screen_name, tweet = status.text))

		#recording urls without Username as only urls are needed
		if len(status.entities['urls']) > 0:
			for url in status.entities['urls']:
				urlTable = db['urlTable']
				urlTable.insert(dict(url=url['expanded_url']))

		return True

	def on_error(self, status_code):
		print "API Limit Reached, Please try after sometime"
		if status_code == 420:
			return False


def print_report(db):
	db = dataset.connect('sqlite:///test.db')

	#User Report
	result = db.query('select username,count(*) cnt from tweetTable GROUP BY username')
	print "\n" + "="*80 + "\n" + " "*30 + "User Report\n" + "="*80
	print "="*60
	print "{:40s} {}".format("Username","No. Of Tweets")
	print "-"*60
	for row in result:
		print "{:40s} {}".format(row['username'], row['cnt'])

	print "-"*80

	#Links Report
	print "\n" + "="*80 + "\n" + " "*30 + "Link Report\n" + "="*80
	print "{:40s} : {}".format("Total Number of Links ",len(db['urlTable']))
	print "-"*60

	urls = dict()
	for link in db['urlTable']:
		base_url = "/".join(link['url'].split("/",3)[:3])

		if base_url in urls:
			urls[base_url] = urls[base_url] + 1
		else:
			urls[base_url] = 1 

	print "{:40s} {}".format("URL","No. Of Occurence")
	print "-"*60
	for url,occ in sorted(urls.iteritems(), key=lambda (k,v): (v,k)):
	 	print "{:40s} {}".format(url,occ)
	print "-"*80

	#Content Report
	print "\n" + "="*80 + "\n" + " "*30 + "Content Report\n" + "="*80
	result = db.query('select tweet from tweetTable')
	wordcount = dict()

	#Stopwords from NLTK
	stopwords = ('ourselves', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about', 'once', 'during', 'out', 'very', 'having','with', 'they', 'own', 'an', 'be', 'some', 'for', 'do', 'its', 'yours', 'such', 'into', 'of', 'most', 'itself', 'other', 'off', 'is', 's', 'am', 'or', 'who', 'as', 'from', 'him', 'each', 'the', 'themselves', 'until', 'below', 'are', 'we', 'these', 'your', 'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more', 'himself', 'this', 'down', 'should', 'our', 'their', 'while', 'above', 'both', 'up', 'to', 'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any', 'before', 'them', 'same', 'and', 'been', 'have', 'in', 'will', 'on', 'does', 'yourselves', 'then', 'that', 'because', 'what', 'over', 'why', 'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself', 'has', 'just', 'where', 'too', 'only', 'myself', 'which', 'those', 'i', 'after', 'few', 'whom', 't', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by', 'doing', 'it', 'how', 'further', 'was', 'here', 'than','could')
	totalwords = 0
	for row in result:
		tweet = row['tweet']
		stweet = str(tweet.encode('utf-8'))
		#Removing links, RT Mentions and special characters
		stweet = re.sub(r"https\S+","",stweet)
		stweet = re.sub(r"RT @\S+","",stweet)
		stweet = re.sub(r"@\S+","",stweet)
		stweet = re.sub('[^A-Za-z 0-9]+', '', stweet)
		stweet = stweet.strip()
		stweet = stweet.lower()
		
		for word in stweet.split():
			if word not in stopwords and re.sub('[0-9]+','',word):
				totalwords = totalwords + 1
				if word not in wordcount:
					wordcount[word] = 1
				else:
					wordcount[word] = wordcount[word] + 1


	print "{:40s} : {}".format("Total Unique Words ",totalwords)
	print "-"*60
	print "{:40s} {}".format("Word","No. Of Occurence")
	print "-"*60
	word_counter = collections.Counter(wordcount)
	for word, count in word_counter.most_common(10):
		print "{:40s} {}".format(word,count)
	print "-"*80

config = ConfigParser.ConfigParser()
config.read("app.cfg")

consumer_key = config.get("config","consumer_key")
consumer_secret = config.get("config","consumer_secret")
access_token = config.get("config","access_token")
access_token_secret = config.get("config","access_token_secret")

if __name__ == '__main__':
	trackword = raw_input("Which word would you like to track?\t")
	minutes_to_run = 5
	streamer =  tweetStreamer(minutes_to_run*60)
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)

	db = dataset.connect('sqlite:///test.db')
	stream = Stream(auth,streamer)
	t = threading.Thread(target = stream.filter,kwargs = {'track' : [trackword]})
	t.deamon = True
	t.start()

	for i in range(minutes_to_run):
		time.sleep(60)
		print "\n\n\n\n" + "."*80 + "\n" + " "*30 + "Report of last : " +  str(i+1) +" Minute\n" + "."*80 + "\n\n"
		print_report(db)
	remove('test.db')
