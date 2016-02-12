import urllib2
from BeautifulSoup import *
from urlparse import urljoin
import sqlite3
#import MySQLdb


ignore_words=set(['the','of','to','and','a','in','is','it'])

class crawler(object):

	def __init__(self,dbname):
#		self.con=MySQLdb.connect(host="localhost",user="root",passwd="960219",db=dbname,charset='utf8')
		self.con=sqlite3.connect(dbname)
		self.c=self.con.cursor()

	def __del__(self):
		self.con.close()

	def dbcommit(self):
		self.con.commit()

	def get_entry_id(self,table,field,value,create_new=True):
		cur=self.c.execute("SELECT ROWID FROM %s WHERE %s='%s'" % (table,field,value))
		res=cur.fetchone()
		if res==None:
			cur=self.c.execute("INSERT INTO %s (%s) VALUES ('%s')" % (table,field,value))
			return cur.lastrowid
		else:
			return res[0]

	def add_to_index(self,url,soup):
#		print "Indexing %s" % url
		if self.is_indexed(url):
			return
		print "Indexing "+url

		text=self.get_text_only(soup)
		words=self.separate_words(text)

		urlid=self.get_entry_id('urllist','url',url)

		for i in range(len(words)):
			word=words[i]
			if word in ignore_words:
				continue
			wordid=self.get_entry_id('wordlist','word',word)
			self.c.execute("INSERT INTO wordlocation(urlid,wordid,location) VALUES (%d,%d,%d)" % (urlid,wordid,i))

	def get_text_only(self,soup):
		v=soup.string
		if v==None:
			c=soup.contents
			result_text=''
			for t in c:
				sub_text=self.get_text_only(t)
				result_text+=sub_text+'\n'
				print result_text.strip()
			return result_text
		else:
			return v.strip()

	def separate_words(self,text):
		split_ter=re.compile("\\W*")
		return [s.lower() for s in split_ter.split(text) if s!='']

	def is_indexed(self,url):
		u=self.c.execute("SELECT ROWID FROM urllist WHERE url='%s'" % url).fetchone()
		if u!=None:
			v=self.c.execute("SELECT * FROM wordlocation WHERE urlid=%d" % u[0]).fetchone()
			if v!=None:
				return True
		return False

	def add_link_ref(self,url_form,url_to,link_text):
		pass

	def crawl(self,pages,depth=2):
		for i in range(depth):
			new_pages=set()
			for page in pages:
				try:
					c=urllib2.urlopen(page)
				except:
					print "could not open %s" % page
					continue

				soup=BeautifulSoup(c.read())
				self.add_to_index(page,soup)

				links=soup("a")
				for link in links:
					if ('href' in dict(link.attrs)):
						url=urljoin(page,link['href'])

						if url.find("'")!=-1:
							continue
						url=url.split("#")[0]
						if url[0:4]=='http' and not self.is_indexed(url):
							new_pages.add(url)
						linkText=self.get_text_only(link)
						self.add_link_ref(page,url,linkText)

				self.dbcommit()

			pages=new_pages

	def create_index_tables(self):
		self.c.execute("CREATE TABLE urllist(url  CHAR(50))")
		self.c.execute("CREATE TABLE wordlist(word  CHAR(50))")
		self.c.execute("CREATE TABLE wordlocation(urlid INT(50),wordid INT(50),location CHAR(50))")
		self.c.execute("CREATE TABLE link(fromid INTEGER(50),toid INTEGER(50))")
		self.c.execute("CREATE TABLE linkwords(wordid INT(50),linkid INT(50))")
		self.c.execute("CREATE INDEX wordidx ON wordlist(word)")
		self.c.execute("CREATE INDEX urlidx ON urllist(url)")
		self.c.execute("CREATE INDEX wordurlidx ON wordlocation(wordid)")
		self.c.execute("CREATE INDEX urltoidx ON link(toid)")
		self.c.execute("CREATE INDEX urlfromidx ON link(fromid)")
		self.dbcommit()


class searcher:
	def __init__(self,dbname):
		self.con=sqlite3.connect(dbname)
		self.c=self.con.cursor()

	def __del__(self):
		self.con.close()
	def get_match_rows(self,q):
		field_list='w0.urlid'
		table_list=''
		clause_list=''
		word_ids=[]

		words=q.split(' ')
		table_number=0

		for word in words:
			print word
			word_row=self.c.execute("SELECT ROWID FROM wordlist WHERE word='%s'" % word).fetchone()

			print word_row

			if word_row!=None:
				word_id=word_row[0]
				word_ids.append(word_id)
				if table_number>0:
					table_list+=','
					clause_list+=' and '
					clause_list+='w%d.urlid=w%d.urlid and ' % (table_number-1,table_number)
				field_list+=',w%d.location' % table_number
				table_list+='wordlocation w%d' % table_number
				clause_list='w%d.wordid=%d' % (table_number,word_id)
				table_number+=1

		print (field_list,table_list,clause_list)

		full_query="SELECT %s FROM %s WHERE %s" % (field_list,table_list,clause_list)
		cur=self.c.execute(full_query)
		rows=[row for row in cur]

		return rows,word_ids
